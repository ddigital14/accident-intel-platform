/**
 * Saved-search alerts. Reps subscribe to filters like
 * "fatals in Houston with no attorney" → pushed to Slack/SMS when matched.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('./cost');
const { reportError } = require('./_errors');

async function ensureTable(db) {
  await db.raw(`CREATE TABLE IF NOT EXISTS saved_alerts (
    id SERIAL PRIMARY KEY,
    user_id TEXT,
    name TEXT,
    filter_json JSONB,
    notify_channel TEXT, -- 'slack' | 'sms' | 'email'
    notify_target TEXT,  -- channel name, phone, or email
    last_fired_at TIMESTAMPTZ,
    last_match_count INT DEFAULT 0,
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
  )`).catch(() => {});
}

function buildWhere(filter, q) {
  if (filter.severity) q.where('severity', filter.severity);
  if (filter.state) q.where('state', filter.state);
  if (filter.city) q.where('city', filter.city);
  if (filter.min_score) q.where('lead_score', '>=', filter.min_score);
  if (filter.qualification_state) q.where('qualification_state', filter.qualification_state);
  if (filter.no_attorney) q.where(function () { this.whereNull('has_attorney').orWhere('has_attorney', false); });
  if (filter.has_phone) q.whereNotNull('phone').where('phone', '!=', '');
  if (filter.metro) q.whereIn('city', filter.metro);
  if (filter.since_hours) q.where('created_at', '>=', new Date(Date.now() - filter.since_hours * 3600 * 1000));
  return q;
}

async function evaluate(db) {
  await ensureTable(db);
  const alerts = await db('saved_alerts').where('enabled', true);
  const fired = [];
  for (const alert of alerts) {
    const filter = typeof alert.filter_json === 'string' ? JSON.parse(alert.filter_json) : alert.filter_json;
    let q = db('incidents').leftJoin('persons', 'persons.incident_id', 'incidents.id')
      .select('incidents.id', 'incidents.description', 'incidents.severity', 'incidents.city', 'incidents.lead_score', 'persons.full_name', 'persons.phone');
    q = buildWhere(filter || {}, q);
    if (alert.last_fired_at) q = q.where('incidents.created_at', '>', alert.last_fired_at);
    const matches = await q.limit(20).catch(() => []);
    if (matches.length > 0) {
      await db('saved_alerts').where({ id: alert.id }).update({ last_fired_at: new Date(), last_match_count: matches.length });
      fired.push({ alert_id: alert.id, name: alert.name, matches: matches.length, sample: matches.slice(0, 3) });
      // Trigger Slack/SMS via existing notify pipeline
      try {
        const notify = require('./notify');
        if (notify.sendAlert) await notify.sendAlert(db, { kind: 'saved_alert', name: alert.name, channel: alert.notify_channel, target: alert.notify_target, matches });
      } catch (_) {}
    }
  }
  await trackApiCall(db, 'system-saved-alerts', 'evaluate', 0, 0, true).catch(() => {});
  return { evaluated: alerts.length, fired: fired.length, details: fired };
}

async function create(db, body) {
  await ensureTable(db);
  const r = await db('saved_alerts').insert({
    user_id: body.user_id || null,
    name: body.name || 'Untitled',
    filter_json: JSON.stringify(body.filter || {}),
    notify_channel: body.notify_channel || 'slack',
    notify_target: body.notify_target || null,
    enabled: body.enabled !== false
  }).returning('id');
  return { id: r[0]?.id || r[0] };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { action, id } = req.query || {};
    if (action === 'health') { await ensureTable(db); const c = await db('saved_alerts').count('* as n').first(); return res.json({ ok: true, engine: 'saved-alerts', count: parseInt(c?.n || 0) }); }
    if (action === 'list') { await ensureTable(db); const rows = await db('saved_alerts').orderBy('created_at', 'desc').limit(50); return res.json({ rows }); }
    if (action === 'evaluate') { const out = await evaluate(db); return res.json({ success: true, ...out }); }
    if (action === 'create' && req.method === 'POST') {
      const body = await new Promise(r => { let d = ''; req.on('data', c => d += c); req.on('end', () => { try { r(JSON.parse(d)); } catch (_) { r({}); } }); });
      const out = await create(db, body); return res.json({ success: true, ...out });
    }
    if (action === 'delete' && id) { await db('saved_alerts').where({ id: parseInt(id) }).del(); return res.json({ success: true, deleted: id }); }
    return res.status(400).json({ error: 'unknown action' });
  } catch (err) { await reportError(db, 'saved-alerts', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.evaluate = evaluate;
module.exports.create = create;
