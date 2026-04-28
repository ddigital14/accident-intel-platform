/**
 * A/B SMS template tracker. Tracks which templates convert best per metro.
 * Adds template_id to outbound SMS, measures reply rate + claim rate per template.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('./cost');
const { reportError } = require('./_errors');

async function ensureTables(db) {
  await db.raw(`CREATE TABLE IF NOT EXISTS sms_templates (
    id SERIAL PRIMARY KEY,
    name TEXT,
    body TEXT,
    metro TEXT,
    severity_filter TEXT,
    enabled BOOLEAN DEFAULT TRUE,
    sent_count INT DEFAULT 0,
    reply_count INT DEFAULT 0,
    convert_count INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
  )`).catch(() => {});
}

async function pickTemplate(db, { metro, severity }) {
  await ensureTables(db);
  // Thompson sampling: weighted pick by reply_rate
  const candidates = await db('sms_templates')
    .where('enabled', true)
    .where(function () { this.whereNull('metro').orWhere('metro', metro || ''); })
    .where(function () { this.whereNull('severity_filter').orWhere('severity_filter', severity || ''); });
  if (candidates.length === 0) return null;
  // Pick template with highest (reply_count + 1) / (sent_count + 2) — Beta-Bernoulli prior
  let best = candidates[0], bestScore = 0;
  for (const t of candidates) {
    const score = ((t.reply_count || 0) + 1) / ((t.sent_count || 0) + 2);
    if (score > bestScore) { best = t; bestScore = score; }
  }
  return best;
}

async function trackSent(db, templateId) {
  if (!templateId) return;
  await db.raw(`UPDATE sms_templates SET sent_count = COALESCE(sent_count, 0) + 1 WHERE id = ?`, [templateId]).catch(() => {});
}

async function trackReply(db, templateId) {
  if (!templateId) return;
  await db.raw(`UPDATE sms_templates SET reply_count = COALESCE(reply_count, 0) + 1 WHERE id = ?`, [templateId]).catch(() => {});
}

async function leaderboard(db) {
  await ensureTables(db);
  const rows = await db('sms_templates').orderBy('sent_count', 'desc').limit(50);
  return rows.map(r => ({
    ...r,
    reply_rate: r.sent_count > 0 ? Math.round((r.reply_count / r.sent_count) * 1000) / 10 : 0,
    convert_rate: r.sent_count > 0 ? Math.round((r.convert_count / r.sent_count) * 1000) / 10 : 0
  }));
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { action, metro, severity } = req.query || {};
    if (action === 'health') { await ensureTables(db); const c = await db('sms_templates').count('* as n').first(); return res.json({ ok: true, engine: 'ab-sms-optimizer', templates: parseInt(c?.n || 0) }); }
    if (action === 'pick') { const t = await pickTemplate(db, { metro, severity }); return res.json({ template: t }); }
    if (action === 'leaderboard') { const r = await leaderboard(db); return res.json({ leaderboard: r }); }
    if (action === 'create' && req.method === 'POST') {
      const body = await new Promise(r => { let d = ''; req.on('data', c => d += c); req.on('end', () => { try { r(JSON.parse(d)); } catch (_) { r({}); } }); });
      await ensureTables(db);
      const ins = await db('sms_templates').insert({ name: body.name, body: body.body, metro: body.metro, severity_filter: body.severity_filter, enabled: true }).returning('id');
      return res.json({ success: true, id: ins[0]?.id || ins[0] });
    }
    return res.status(400).json({ error: 'unknown action' });
  } catch (err) { await reportError(db, 'ab-sms-optimizer', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.pickTemplate = pickTemplate;
module.exports.trackSent = trackSent;
module.exports.trackReply = trackReply;
module.exports.leaderboard = leaderboard;
