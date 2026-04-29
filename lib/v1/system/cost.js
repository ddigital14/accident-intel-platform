/**
 * GET /api/v1/system/cost
 *
 * Tracks OpenAI + external API costs by pipeline.
 * Reads counts from system_api_calls table (auto-created).
 *
 * Pipelines instrument calls via:
 *   await trackApiCall(db, 'pipeline_name', 'service', tokensIn, tokensOut)
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');

let _tableEnsured = false;
async function ensureTable(db) {
  if (_tableEnsured) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS system_api_calls (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        pipeline VARCHAR(80) NOT NULL,
        service VARCHAR(80) NOT NULL,
        tokens_in INTEGER DEFAULT 0,
        tokens_out INTEGER DEFAULT 0,
        cost_usd NUMERIC(10,6) DEFAULT 0,
        success BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_api_calls_created ON system_api_calls(created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_api_calls_pipeline ON system_api_calls(pipeline);
      CREATE INDEX IF NOT EXISTS idx_api_calls_service ON system_api_calls(service);
    `);
    _tableEnsured = true;
  } catch (e) {
    console.error('cost table ensure failed:', e.message);
  }
}

// Pricing as of Apr 2026 (USD per 1M tokens)
const PRICING = {
  'gpt-4o-mini': { in: 0.15, out: 0.60 },
  'gpt-4o':       { in: 2.50, out: 10.0 },
  // Anthropic Claude (Apr 2026 pricing per 1M tokens)
  'claude-haiku-4-5-20251001':  { in: 0.80,  out: 4.00 },
  'claude-sonnet-4-6': { in: 3.00,  out: 15.00 },
  'claude-opus-4-7':   { in: 15.00, out: 75.00 },
  'whisper-1':    { in: 0.006, out: 0 },
  'pdl':          { flat: 0.02 },
  'hunter':       { flat: 0.04 },
  'numverify':    { flat: 0 },         // free tier
  'openweather':  { flat: 0 },         // free tier
  'tracerfy':     { flat: 0.10 },
  'searchbug':    { flat: 0.05 },
  // Trestle per-endpoint
  'trestle':                  { flat: 0.05 },
  'trestle_other':            { flat: 0.05 },
  'trestle_reverse_phone':    { flat: 0.07 },
  'trestle_cnam':             { flat: 0.05 },
  'trestle_reverse_address':  { flat: 0.07 },
  'trestle_caller_id':        { flat: 0.07 },
  'trestle_real_contact':     { flat: 0.03 },
  'trestle_phone_intel':      { flat: 0.015 },
  'trestle_error':            { flat: 0 },
  // Twilio per-surface
  'twilio_sms':         { flat: 0.0079 },   // outbound SMS US
  'twilio_mms':         { flat: 0.02 },     // outbound MMS US
  'twilio_voice':       { flat: 0.014 },    // outbound voice min US
  'twilio_lookup':      { flat: 0.008 },    // line_type+caller_name combined
  'twilio_lookup_basic': { flat: 0.005 },
  'twilio_lookup_caller_name': { flat: 0.01 },
  'twilio_verify':      { flat: 0.05 },     // per verify request
  // Google CSE
  'google_cse':         { flat: 0 },        // free 100/day, $5/1000 after
  'google_cse_paid':    { flat: 0.005 },    // $5 per 1000 over free tier
  // Free public APIs
  'courtlistener': { flat: 0 },
  'nhtsa_vin':     { flat: 0 },
  'reddit':        { flat: 0 },
  // Wave-2 engines (2026-04-26)
  'tcpa_scrape':   { flat: 0 },          // tcpalitigatorlist.com + tcpaworld scrape
  // family-tree uses gpt-4o-mini (already priced above)
  // vehicle-history uses nhtsa_vin (free, already priced)
  // relatives-search uses searchbug (already priced) — no new entry needed
};

function estimateCost(service, tokensIn = 0, tokensOut = 0) {
  const p = PRICING[service];
  if (!p) return 0;
  if (p.flat !== undefined) return p.flat;
  return (tokensIn / 1_000_000) * p.in + (tokensOut / 1_000_000) * p.out;
}

async function trackApiCall(db, pipeline, service, tokensIn = 0, tokensOut = 0, success = true) {
  if (!db) return;
  try {
    await ensureTable(db);
    const cost = estimateCost(service, tokensIn, tokensOut);
    await db('system_api_calls').insert({
      pipeline, service,
      tokens_in: tokensIn,
      tokens_out: tokensOut,
      cost_usd: cost,
      success,
      created_at: new Date()
    });
  } catch (e) {
    console.error('trackApiCall failed:', e.message, '|', service, '|', pipeline);
    // Don't silently swallow — log to system_errors for visibility
    try {
      await db('system_errors').insert({
        pipeline: 'cost-tracker',
        source: service,
        message: String(e.message || e).substring(0, 500),
        context: JSON.stringify({ service, pipeline_name: pipeline, success }).substring(0, 1000),
        severity: 'warning',
        created_at: new Date()
      });
    } catch (_) {}
  }
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const db = getDb();
  await ensureTable(db);

  try {
    const day = new Date(Date.now() - 86400000);
    const week = new Date(Date.now() - 7 * 86400000);
    const month = new Date(Date.now() - 30 * 86400000);

    const [byService24h, byPipeline24h, totals] = await Promise.all([
      db.raw(`SELECT service, COUNT(*) as calls, SUM(tokens_in) as tin, SUM(tokens_out) as tout, SUM(cost_usd) as cost
              FROM system_api_calls WHERE created_at > ? GROUP BY service ORDER BY cost DESC`, [day])
        .then(r => r.rows || []).catch(() => []),
      db.raw(`SELECT pipeline, COUNT(*) as calls, SUM(cost_usd) as cost
              FROM system_api_calls WHERE created_at > ? GROUP BY pipeline ORDER BY cost DESC`, [day])
        .then(r => r.rows || []).catch(() => []),
      Promise.all([
        db.raw(`SELECT SUM(cost_usd) as total FROM system_api_calls WHERE created_at > ?`, [day])
          .then(r => parseFloat(r.rows?.[0]?.total || 0)).catch(() => 0),
        db.raw(`SELECT SUM(cost_usd) as total FROM system_api_calls WHERE created_at > ?`, [week])
          .then(r => parseFloat(r.rows?.[0]?.total || 0)).catch(() => 0),
        db.raw(`SELECT SUM(cost_usd) as total FROM system_api_calls WHERE created_at > ?`, [month])
          .then(r => parseFloat(r.rows?.[0]?.total || 0)).catch(() => 0)
      ])
    ]);

    res.json({
      success: true,
      total_cost_usd: { '24h': totals[0], '7d': totals[1], '30d': totals[2] },
      monthly_run_rate: totals[2],
      by_service_24h: byService24h.map(r => ({ ...r, cost: parseFloat(r.cost) })),
      by_pipeline_24h: byPipeline24h.map(r => ({ ...r, cost: parseFloat(r.cost) })),
      pricing_table: PRICING,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'cost', null, err.message);
    res.status(500).json({ error: err.message });
  }
};

module.exports = handler;
module.exports.handler = handler;
module.exports.trackApiCall = trackApiCall;
module.exports.estimateCost = estimateCost;
module.exports.default = handler;
