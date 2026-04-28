/**
 * Self-correcting confidence decay on bounce signals.
 * SMS bounce / email hard bounce → -15 identity_confidence + re-cascade.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

async function decay(db, limit = 50) {
  let bounced = []; try {
    bounced = await db.raw(`
      SELECT person_id, kind FROM (
        SELECT person_id, 'sms_bounce' AS kind FROM sms_log WHERE status IN ('failed','undelivered','bounced') AND created_at > NOW() - INTERVAL '14 days'
        UNION ALL
        SELECT person_id, 'email_bounce' AS kind FROM email_log WHERE status IN ('hard_bounce','invalid','rejected') AND created_at > NOW() - INTERVAL '14 days'
      ) x WHERE person_id IS NOT NULL LIMIT ${parseInt(limit) || 50}
    `).then(r => r.rows || r).catch(() => []);
  } catch (_) {}
  let decayed = 0;
  for (const b of bounced) {
    try {
      await db('persons').where({ id: b.person_id }).update({
        identity_confidence: db.raw('GREATEST(0, COALESCE(identity_confidence, 0) - 15)'),
        updated_at: new Date()
      });
      await enqueueCascade(db, 'person', b.person_id, 'confidence-decay', { weight: -15, reason: b.kind });
      decayed++;
    } catch (_) {}
  }
  await trackApiCall(db, 'enrich-confidence-decay', 'sql', 0, 0, true).catch(() => {});
  return { bounced: bounced.length, decayed };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'confidence-decay', penalty: 15 });
    const out = await decay(db, parseInt(req.query.limit) || 50);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'confidence-decay', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.decay = decay;
