require('dotenv').config({ path: '.env.local' })
const { Client } = require('pg')
const argv = require('minimist')(process.argv.slice(2))

// Usage examples:
// node scripts/auto_reprocess_webhook_events.js --dry-run --limit=20
// node scripts/auto_reprocess_webhook_events.js --once --limit=50
// node scripts/auto_reprocess_webhook_events.js --poll-interval=60000 (runs continuously every 60s)

const dryRun = argv['dry-run'] || argv.dryrun || false
const once = argv.once || false
const limit = parseInt(argv.limit || argv.l || 100, 10)
const provider = argv.provider || 'flutterwave'
const pollInterval = parseInt(argv['poll-interval'] || argv.pollInterval || 0, 10) // ms

async function getClient() {
  const client = new Client({
    host: process.env.DB_HOST,
    port: process.env.DB_PORT ? parseInt(process.env.DB_PORT, 10) : 5432,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    ssl: process.env.DB_SSL && (process.env.DB_SSL === 'true' || process.env.DB_SSL === '1') ? { rejectUnauthorized: false } : false,
  })
  await client.connect()
  return client
}

async function fetchPendingEvents(client) {
  // Select unprocessed webhook_events for the provider
  // Support both boolean false and null for older DBs
  const q = `SELECT * FROM public.webhook_events WHERE provider = $1 AND (processed = false OR processed IS NULL) ORDER BY created_at ASC LIMIT $2`
  const r = await client.query(q, [provider, limit])
  return r.rows
}

async function reprocessEvent(client, eventRow) {
  let payload = eventRow.payload
  if (typeof payload === 'string') {
    try { payload = JSON.parse(payload) } catch (e) {}
  }
  const data = payload && (payload.data || payload.payload || payload.body) ? (payload.data || payload.payload || payload.body) : payload
  if (!data) throw new Error('No data in webhook payload')

  // CRITICAL: Check transaction status - only process successful transactions
  const txStatus = data.status ? String(data.status).toLowerCase() : null
  if (txStatus && txStatus !== 'successful') {
    // Mark as processed to avoid retrying, but don't credit the user
    await client.query('UPDATE public.webhook_events SET processed = true, processed_at = NOW() WHERE id = $1', [eventRow.id]).catch(() => {})
    return { skipped: true, reason: `Transaction status is "${data.status}" (not successful)` }
  }

  const amount = parseFloat(data.amount)
  const tx_ref = data.tx_ref || data.reference
  const flw_ref = data.flw_ref || data.flwRef || null
  const account_number = data.account_number || (data.destination_account_number || null)

  // Resolve user id
  let userRes = null
  if (account_number) {
    userRes = await client.query(`SELECT au.id as user_id FROM public.virtual_accounts va JOIN auth.users au ON va.user_id = au.id WHERE va.account_number = $1 AND va.is_active = true LIMIT 1`, [account_number])
  }
  if ((!userRes || userRes.rows.length === 0) && flw_ref) {
    userRes = await client.query(`SELECT au.id as user_id FROM public.virtual_accounts va JOIN auth.users au ON va.user_id = au.id WHERE va.flw_ref = $1 LIMIT 1`, [flw_ref])
  }
  if ((!userRes || userRes.rows.length === 0) && tx_ref) {
    const m = tx_ref.match(/VA_([0-9a-fA-F-]{8,})_\d+/)
    if (m) userRes = await client.query(`SELECT au.id as user_id FROM auth.users au WHERE au.id = $1 LIMIT 1`, [m[1]])
  }
  if (!userRes || userRes.rows.length === 0) throw new Error('Could not resolve user for webhook event')
  const userId = userRes.rows[0].user_id

  // Start transaction
  await client.query('BEGIN')
  try {
    // Lock profile
    const prof = await client.query('SELECT wallet_balance FROM public.users_profiles WHERE user_id = $1 FOR UPDATE', [userId])
    const current = parseFloat(prof.rows[0]?.wallet_balance || 0)
    const newBal = current + amount

  const reference = flw_ref || tx_ref || `reproc_${Date.now()}`

    // Check idempotency; prefer flw_ref
    let existingTx = { rows: [] }
    if (flw_ref) existingTx = await client.query('SELECT id, amount FROM public.wallet_transactions WHERE flw_ref = $1 AND user_id = $2 LIMIT 1', [flw_ref, userId])
    if (!flw_ref || existingTx.rows.length === 0) existingTx = await client.query('SELECT id, amount FROM public.wallet_transactions WHERE reference = $1 AND user_id = $2 LIMIT 1', [reference, userId])
    if (existingTx.rows.length > 0) {
      const existAmount = parseFloat(existingTx.rows[0]?.amount || 0)
      if (Math.abs(existAmount - amount) < 0.0001) {
        // already processed
        await client.query('UPDATE public.webhook_events SET processed = true, processed_at = NOW() WHERE id = $1', [eventRow.id]).catch(() => {})
        await client.query('ROLLBACK')
        return { alreadyProcessed: true }
      }
      const delta = amount - existAmount
      if (delta <= 0) {
        await client.query('UPDATE public.webhook_events SET processed = true, processed_at = NOW() WHERE id = $1', [eventRow.id]).catch(() => {})
        await client.query('ROLLBACK')
        return { alreadyProcessed: true, note: 'existing tx amount >= payload amount' }
      }

      // insert delta transaction
      const deltaRef = `${reference}_${eventRow.provider_event_id}`
  const metadata = JSON.stringify({ recoveredFromWebhookEvent: eventRow.provider_event_id, flw_ref, original_reference: reference })
  await client.query(`INSERT INTO public.wallet_transactions (user_id, transaction_type, type, amount, balance_before, balance_after, reference, flw_ref, status, description, metadata, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())`, [userId, 'CREDIT', 'credit', delta, current, current + delta, deltaRef, flw_ref, 'completed', 'Adjustment for webhook reconciliation', metadata])
      await client.query('UPDATE public.users_profiles SET wallet_balance = $1, updated_at = NOW() WHERE user_id = $2', [current + delta, userId])
      await client.query('UPDATE public.webhook_events SET processed = true, processed_at = NOW() WHERE id = $1', [eventRow.id])
      await client.query('COMMIT')
      return { alreadyProcessed: false, userId, newBalance: current + delta, delta }
    }

  const metadata = JSON.stringify({ recoveredFromWebhookEvent: eventRow.provider_event_id, flw_ref })
  await client.query(`INSERT INTO public.wallet_transactions (user_id, transaction_type, type, amount, balance_before, balance_after, reference, flw_ref, status, description, metadata, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())`, [userId, 'CREDIT', 'credit', amount, current, newBal, reference, flw_ref, 'completed', 'Payment through transfer to VA', metadata])
    await client.query('UPDATE public.users_profiles SET wallet_balance = $1, updated_at = NOW() WHERE user_id = $2', [newBal, userId])
    await client.query('UPDATE public.webhook_events SET processed = true, processed_at = NOW() WHERE id = $1', [eventRow.id])
    await client.query('COMMIT')
    return { alreadyProcessed: false, userId, newBalance: newBal }
  } catch (err) {
    await client.query('ROLLBACK')
    throw err
  }
}

async function processBatch() {
  const client = await getClient()
  try {
    const rows = await fetchPendingEvents(client)
    if (!rows || rows.length === 0) {
      console.log('No pending webhook_events found')
      await client.end()
      return { processed: 0 }
    }

    console.log(`Found ${rows.length} pending events — processing`)
    let processed = 0
    for (const row of rows) {
      try {
        console.log(`Processing webhook_event id=${row.id} provider_event_id=${row.provider_event_id}`)
        if (dryRun) {
          console.log('Dry run: skipping DB writes')
          processed++
          continue
        }
        const res = await reprocessEvent(client, row)
        if (res.skipped) {
          console.log(`Event ${row.id} skipped: ${res.reason}`)
        } else if (res.alreadyProcessed) {
          console.log(`Event ${row.id} already processed`) 
        } else {
          console.log(`Event ${row.id} credited user ${res.userId} new balance ${res.newBalance}`)
        }
        processed++
      } catch (err) {
        console.error(`Error processing event id=${row.id}:`, err.message || err)
        // continue with next event — don't stop the batch
      }
    }

    await client.end()
    return { processed: processed }
  } catch (err) {
    console.error('Batch processing error:', err.message || err)
    try { await client.end() } catch (e) {}
    throw err
  }
}

async function run() {
  try {
    if (pollInterval && pollInterval > 0 && !once) {
      console.log(`Starting poll loop: interval ${pollInterval}ms`)
      while (true) {
        try {
          await processBatch()
        } catch (err) {
          console.error('Error in poll iteration:', err.message || err)
        }
        await new Promise(r => setTimeout(r, pollInterval))
      }
    } else {
      const r = await processBatch()
      console.log('Done — processed:', r.processed)
    }
  } catch (err) {
    console.error('Fatal error:', err.message || err)
    process.exit(1)
  }
}

run()
