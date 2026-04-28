/**
 * Multi-touch Engagement Score.
 * persons.engagement_score = SMS replies + email opens + form fills.
 * Reps prioritize responsive prospects.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');

async function run(db, limit = 100) {
  const sql = `
    SELECT p.id, p.full_name,
      COALESCE((SELECT COUNT(*) FROM sms_log s WHERE s.person_id = p.id AND s.direction = 'inbound'), 0) AS sms_replies,
      COALESCE((SELECT COUNT(*) FROM email_log e WHERE e.person_id = p.id AND e.event = 'opened'), 0) AS email_opens,
      COALESCE((SELECT COUNT(*) FROM email_log e WHERE e.person_id = p.id AND e.event = 'clicked'), 0) AS email_clicks,
      COALESCE((SELECT COUNT(*) FROM form_submissions fs WHERE fs.person_id = p.id), 0) AS form_fills
    FROM persons p
    WHERE p.qualification_state = 'qualified'
    LIMIT ${parseInt(limit)}
  `;
  let rows = []; try { const r = await db.raw(sql); rows = r.rows || r; } catch (_) {}
  let updated = 0;
  for (const r of rows) {
    const score = (parseInt(r.sms_replies) || 0) * 30
      + (parseInt(r.email_opens) || 0) * 5
      + (parseInt(r.email_clicks) || 0) * 15
      + (parseInt(r.form_fills) || 0) * 50;
    try {
      await db('persons').where({ id: r.id }).update({ engagement_score: Math.min(100, score), updated_at: new Date() });
      updated++;
    } catch (_) {}
  }
  await trackApiCall(db, 'enrich-engagement-score', 'sql', 0, 0, true).catch(() => {});
  return { rows: rows.length, updated };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'engagement-score' });
    const out = await run(db, parseInt(req.query.limit) || 100);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'engagement-score', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
