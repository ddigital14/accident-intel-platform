/**
 * Lead Stale Detector + Auto-Recycler.
 * If rep claims a lead but doesn't make first contact within 24h
 * (no SMS sent, no note logged, no email sent), auto-release back to pool.
 * Prevents sandbagging.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

async function run(db, hours = 24) {
  const sql = `
    SELECT i.id, i.assigned_to, i.assigned_at, u.email AS rep_email
    FROM incidents i
    LEFT JOIN users u ON u.id = i.assigned_to
    WHERE i.assigned_to IS NOT NULL
      AND i.assigned_at < NOW() - INTERVAL '${parseInt(hours)} hours'
      AND i.qualification_state = 'qualified'
      AND NOT EXISTS (SELECT 1 FROM sms_log sl WHERE sl.incident_id = i.id AND sl.created_at > i.assigned_at)
      AND NOT EXISTS (SELECT 1 FROM email_log el WHERE el.incident_id = i.id AND el.created_at > i.assigned_at)
      AND NOT EXISTS (SELECT 1 FROM lead_notes ln WHERE ln.incident_id = i.id AND ln.created_at > i.assigned_at)
    LIMIT 50
  `;
  let rows = []; try { const r = await db.raw(sql); rows = r.rows || r; } catch (_) {}
  let recycled = 0;
  for (const r of rows) {
    try {
      await db('incidents').where({ id: r.id }).update({ assigned_to: null, assigned_at: null, recycled_count: db.raw('COALESCE(recycled_count, 0) + 1'), recycled_at: new Date() });
      await enqueueCascade(db, 'incident', r.id, 'lead-stale-recycler', { weight: 0, prev_rep: r.rep_email, hours_idle: hours });
      recycled++;
    } catch (_) {}
  }
  await trackApiCall(db, 'enrich-lead-stale-recycler', 'sql', 0, 0, true).catch(() => {});
  return { stale: rows.length, recycled, threshold_hours: hours };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'lead-stale-recycler', threshold_hours_default: 24 });
    const out = await run(db, parseInt(req.query.hours) || 24);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'lead-stale-recycler', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
