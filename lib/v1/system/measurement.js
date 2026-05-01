/**
 * Phase 70: Platform measurement endpoint.
 * Returns week-over-week + day-over-day metrics for tracking platform improvement.
 */
const { getDb } = require('../../_db');
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function snapshot(db) {
  const cuts = {
    today: new Date(Date.now() - 24*3600*1000),
    week: new Date(Date.now() - 7*24*3600*1000),
    month: new Date(Date.now() - 30*24*3600*1000)
  };

  const incidents_total = await db('incidents').count('* as c').first();
  const incidents_today = await db('incidents').where('discovered_at', '>', cuts.today).count('* as c').first();
  const incidents_week = await db('incidents').where('discovered_at', '>', cuts.week).count('* as c').first();
  const incidents_month = await db('incidents').where('discovered_at', '>', cuts.month).count('* as c').first();
  const qualified_total = await db('incidents').where('qualification_state', 'qualified').count('* as c').first();
  const qualified_today = await db('incidents').where('qualification_state','qualified').where('discovered_at','>',cuts.today).count('* as c').first();
  const qualified_week = await db('incidents').where('qualification_state','qualified').where('discovered_at','>',cuts.week).count('* as c').first();
  const persons_total = await db('persons').count('* as c').first();
  const persons_verified = await db('persons').where('victim_verified', true).count('* as c').first();
  const persons_with_phone = await db('persons').whereNotNull('phone').count('* as c').first();
  const persons_with_email = await db('persons').whereNotNull('email').count('* as c').first();
  const persons_with_address = await db('persons').whereNotNull('address').count('* as c').first();
  const enrichment_logs_today = await db('enrichment_logs').where('created_at','>',cuts.today).count('* as c').first();
  const cross_checked = await db('enrichment_logs').where('field_name','evidence_cross_check_summary').count('* as c').first();

  // Per-engine performance from strategist
  let engine_perf = [];
  try {
    engine_perf = await db('engine_performance')
      .select('engine_id', 'input_shape', 'attempts', 'successes')
      .orderByRaw('CAST(successes AS FLOAT) / NULLIF(attempts, 0) DESC NULLS LAST')
      .limit(15);
  } catch (_) {}

  // Top conversion patterns
  let signals = [];
  try {
    signals = await db('lead_score_signals')
      .orderByRaw('ABS(suggested_score_delta) DESC')
      .limit(8)
      .select('signal_type', 'pattern', 'sample_size', 'conversion_rate', 'suggested_score_delta');
  } catch (_) {}

  return {
    ok: true,
    timestamp: new Date().toISOString(),
    incidents: {
      total: parseInt(incidents_total?.c || 0),
      last_24h: parseInt(incidents_today?.c || 0),
      last_7d: parseInt(incidents_week?.c || 0),
      last_30d: parseInt(incidents_month?.c || 0)
    },
    qualified: {
      total: parseInt(qualified_total?.c || 0),
      last_24h: parseInt(qualified_today?.c || 0),
      last_7d: parseInt(qualified_week?.c || 0),
      conversion_rate: parseInt(qualified_total?.c || 0) / Math.max(1, parseInt(incidents_total?.c || 1))
    },
    persons: {
      total: parseInt(persons_total?.c || 0),
      verified: parseInt(persons_verified?.c || 0),
      with_phone: parseInt(persons_with_phone?.c || 0),
      with_email: parseInt(persons_with_email?.c || 0),
      with_address: parseInt(persons_with_address?.c || 0)
    },
    enrichment: {
      logs_last_24h: parseInt(enrichment_logs_today?.c || 0),
      total_cross_checked: parseInt(cross_checked?.c || 0)
    },
    top_engines: engine_perf,
    top_signals: signals
  };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'snapshot').toLowerCase();
  if (action === 'health') return res.json({ success: true, service: 'measurement' });
  if (action === 'snapshot') return res.json(await snapshot(db));
  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.snapshot = snapshot;
