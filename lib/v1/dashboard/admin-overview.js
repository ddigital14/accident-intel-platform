/**
 * Admin Overview Dashboard — Phase 44B
 * GET /api/v1/dashboard/admin-overview?secret=ingest-now
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');

const SECRET = 'ingest-now';
const CACHE_MS = 60 * 1000;
let _cache = { ts: 0, payload: null };

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function getCreditCaps(db) {
  const out = { apollo_monthly: 235, pdl_monthly: 1000, auto_purchase_budget_usd_per_day: 0 };
  try {
    const rows = await db('system_config').whereIn('key',
      ['apollo_monthly_credits', 'pdl_monthly_credits', 'auto_purchase_budget_usd_per_day']).select();
    for (const r of rows) {
      const v = typeof r.value === 'string' ? r.value.replace(/^"|"$/g, '') : r.value;
      if (r.key === 'apollo_monthly_credits') out.apollo_monthly = parseInt(v) || out.apollo_monthly;
      if (r.key === 'pdl_monthly_credits') out.pdl_monthly = parseInt(v) || out.pdl_monthly;
      if (r.key === 'auto_purchase_budget_usd_per_day') out.auto_purchase_budget_usd_per_day = parseFloat(v) || 0;
    }
  } catch (_) {}
  return out;
}

async function pipelineHealth(db) {
  const out = { sources_active: 0, errors_24h: 0, incidents_24h: 0, jobs_passing: 0 };
  try {
    const r = await db.raw(`SELECT COUNT(DISTINCT pipeline) AS sources FROM system_api_calls WHERE created_at > NOW() - INTERVAL '24 hours'`);
    out.sources_active = parseInt(r.rows?.[0]?.sources || 0);
  } catch (_) {}
  try {
    const r = await db.raw(`SELECT COUNT(*) AS c FROM system_errors WHERE created_at > NOW() - INTERVAL '24 hours'`);
    out.errors_24h = parseInt(r.rows?.[0]?.c || 0);
  } catch (_) {}
  try {
    const r = await db.raw(`SELECT COUNT(*) AS c FROM incidents WHERE created_at > NOW() - INTERVAL '24 hours'`);
    out.incidents_24h = parseInt(r.rows?.[0]?.c || 0);
  } catch (_) {}
  try {
    const r = await db.raw(`SELECT COUNT(*) FILTER (WHERE success=TRUE) AS ok, COUNT(*) AS total FROM system_api_calls WHERE created_at > NOW() - INTERVAL '24 hours'`);
    const t = parseInt(r.rows?.[0]?.total || 0);
    const ok = parseInt(r.rows?.[0]?.ok || 0);
    out.jobs_passing = t > 0 ? Math.round((ok * 100) / t) : 0;
  } catch (_) {}
  return out;
}

async function leadsToday(db) {
  const o = { qualified: 0, verified_awaiting: 0, pending_named: 0, pending: 0 };
  try {
    const r = await db.raw(`
      SELECT
        COUNT(*) FILTER (WHERE qualification_state = 'qualified')        AS qualified,
        COUNT(*) FILTER (WHERE qualification_state = 'verified_awaiting') AS verified_awaiting,
        COUNT(*) FILTER (WHERE qualification_state = 'pending_named')    AS pending_named,
        COUNT(*) FILTER (WHERE qualification_state = 'pending')          AS pending
      FROM incidents WHERE created_at > NOW() - INTERVAL '24 hours'
    `);
    const row = r.rows?.[0] || {};
    o.qualified = parseInt(row.qualified || 0);
    o.verified_awaiting = parseInt(row.verified_awaiting || 0);
    o.pending_named = parseInt(row.pending_named || 0);
    o.pending = parseInt(row.pending || 0);
  } catch (_) {}
  return o;
}

async function creditUsageToday(db) {
  const caps = await getCreditCaps(db);
  const out = {
    apollo_used: 0, apollo_remaining: caps.apollo_monthly,
    pdl_used: 0, pdl_remaining: caps.pdl_monthly,
    openai_cost: 0, anthropic_cost: 0, voyageai_cost: 0,
    brave_used: 0, deepgram_used: 0
  };
  try {
    const r = await db.raw(`
      SELECT pipeline, service, COUNT(*) AS calls, COALESCE(SUM(cost_usd), 0) AS cost
        FROM system_api_calls
       WHERE created_at > NOW() - INTERVAL '24 hours'
       GROUP BY pipeline, service
    `);
    for (const row of (r.rows || [])) {
      const p = (row.pipeline || '').toLowerCase();
      const s = (row.service || '').toLowerCase();
      const calls = parseInt(row.calls || 0);
      const cost = parseFloat(row.cost || 0);
      if (s.includes('apollo') || p.includes('apollo')) out.apollo_used += calls;
      if (s.includes('pdl') || p.includes('pdl') || p.includes('peopledatalabs')) out.pdl_used += calls;
      if (s.includes('openai') || p.includes('openai') || s.includes('gpt')) out.openai_cost += cost;
      if (s.includes('claude') || s.includes('anthropic') || p.includes('claude')) out.anthropic_cost += cost;
      if (s.includes('voyage')) out.voyageai_cost += cost;
      if (s.includes('brave') || p === 'brave-search') out.brave_used += calls;
      if (s.includes('deepgram') || p.includes('deepgram')) out.deepgram_used += calls;
    }
  } catch (_) {}
  out.apollo_remaining = Math.max(0, caps.apollo_monthly - out.apollo_used);
  out.pdl_remaining = Math.max(0, caps.pdl_monthly - out.pdl_used);
  out.auto_purchase_budget_usd_per_day = caps.auto_purchase_budget_usd_per_day;
  out.openai_cost = Math.round(out.openai_cost * 10000) / 10000;
  out.anthropic_cost = Math.round(out.anthropic_cost * 10000) / 10000;
  out.voyageai_cost = Math.round(out.voyageai_cost * 10000) / 10000;
  return out;
}

async function top5Qualified(db) {
  try {
    const r = await db.raw(`
      SELECT i.id, i.headline, i.city, i.state, i.lead_score, i.qualification_state, i.created_at,
             (SELECT full_name FROM persons WHERE incident_id = i.id ORDER BY identity_confidence DESC NULLS LAST LIMIT 1) AS top_person
        FROM incidents i
       WHERE i.qualification_state = 'qualified'
       ORDER BY COALESCE(i.lead_score, 0) DESC, i.created_at DESC
       LIMIT 5
    `);
    return r.rows || [];
  } catch (_) { return []; }
}

async function blockingIssues(db) {
  try {
    const r = await db.raw(`
      SELECT pipeline AS engine, COUNT(*) AS error_count,
             MAX(created_at) AS last_error_at,
             (ARRAY_AGG(message ORDER BY created_at DESC))[1] AS last_error
        FROM system_errors
       WHERE created_at > NOW() - INTERVAL '24 hours'
       GROUP BY pipeline
       HAVING COUNT(*) >= 5
       ORDER BY COUNT(*) DESC
       LIMIT 10
    `);
    return (r.rows || []).map(row => ({
      engine: row.engine,
      error_count: parseInt(row.error_count),
      last_error_at: row.last_error_at,
      last_error: (row.last_error || '').slice(0, 200)
    }));
  } catch (_) { return []; }
}

async function recentAlerts(db) {
  try {
    const r = await db.raw(`
      SELECT id, alert_type, person_id, incident_id, created_at, payload
        FROM system_alerts WHERE created_at > NOW() - INTERVAL '24 hours'
       ORDER BY created_at DESC LIMIT 10
    `);
    return r.rows || [];
  } catch (_) {
    try {
      const r = await db.raw(`
        SELECT id, trigger_source AS alert_type, person_id, incident_id, enqueued_at AS created_at
          FROM cascade_queue WHERE enqueued_at > NOW() - INTERVAL '24 hours'
         ORDER BY enqueued_at DESC LIMIT 10
      `);
      return r.rows || [];
    } catch (_) {}
    return [];
  }
}

async function buildOverview(db) {
  const [pipeline_health, leads, credits, top5, issues, alerts] = await Promise.all([
    pipelineHealth(db).catch(() => ({})),
    leadsToday(db).catch(() => ({})),
    creditUsageToday(db).catch(() => ({})),
    top5Qualified(db).catch(() => []),
    blockingIssues(db).catch(() => []),
    recentAlerts(db).catch(() => [])
  ]);
  return {
    pipeline_health, leads_today: leads, credit_usage_today: credits,
    top_5_qualified: top5, blocking_issues: issues, recent_alerts: alerts,
    timestamp: new Date().toISOString()
  };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const now = Date.now();
    if (_cache.payload && (now - _cache.ts) < CACHE_MS && !req.query?.fresh) {
      return res.json({ success: true, cached: true, age_ms: now - _cache.ts, ..._cache.payload });
    }
    const db = getDb();
    const payload = await buildOverview(db);
    _cache = { ts: now, payload };
    return res.json({ success: true, cached: false, ...payload });
  } catch (e) {
    try { await reportError(getDb(), 'admin-overview', null, e.message); } catch (_) {}
    return res.status(500).json({ success: false, error: e.message });
  }
};
module.exports.buildOverview = buildOverview;
