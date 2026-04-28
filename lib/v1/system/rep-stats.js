/**
 * Active-rep dashboard data — engagement_score, claim rate, conversion rate per rep.
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');

async function run(db, days = 7) {
  const sql = `
    SELECT u.id, u.email, u.first_name, u.last_name,
      COUNT(DISTINCT i.id) AS leads_claimed,
      COUNT(DISTINCT i.id) FILTER (WHERE i.qualification_state = 'qualified') AS leads_qualified,
      COUNT(DISTINCT s.id) FILTER (WHERE s.direction = 'outbound') AS sms_sent,
      COUNT(DISTINCT s.id) FILTER (WHERE s.direction = 'inbound') AS sms_replies,
      AVG(p.engagement_score) FILTER (WHERE p.engagement_score IS NOT NULL) AS avg_engagement,
      AVG(EXTRACT(EPOCH FROM (s.created_at - i.assigned_at)) / 60) FILTER (WHERE s.direction = 'outbound') AS avg_minutes_to_first_contact
    FROM users u
    LEFT JOIN incidents i ON i.assigned_to = u.id AND i.assigned_at > NOW() - INTERVAL '${parseInt(days)} days'
    LEFT JOIN persons p ON p.incident_id = i.id
    LEFT JOIN sms_log s ON s.incident_id = i.id
    WHERE u.role IN ('rep', 'manager')
    GROUP BY u.id, u.email, u.first_name, u.last_name
    ORDER BY leads_qualified DESC NULLS LAST, leads_claimed DESC NULLS LAST
  `;
  let rows = []; try { const r = await db.raw(sql); rows = r.rows || r; } catch (_) {}
  return rows.map(r => ({
    ...r,
    leads_claimed: parseInt(r.leads_claimed || 0),
    leads_qualified: parseInt(r.leads_qualified || 0),
    sms_sent: parseInt(r.sms_sent || 0),
    sms_replies: parseInt(r.sms_replies || 0),
    reply_rate: r.sms_sent > 0 ? Math.round((r.sms_replies / r.sms_sent) * 1000) / 10 : 0,
    avg_engagement: r.avg_engagement ? Math.round(parseFloat(r.avg_engagement)) : 0,
    avg_minutes_to_first_contact: r.avg_minutes_to_first_contact ? Math.round(parseFloat(r.avg_minutes_to_first_contact)) : null
  }));
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const days = parseInt(req.query.days) || 7;
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'rep-stats' });
    const rows = await run(db, days);
    return res.json({ days, reps: rows });
  } catch (err) { await reportError(db, 'rep-stats', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
