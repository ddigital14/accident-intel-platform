/**
 * Bidirectional cascade: when a person hits 90+ confidence, re-score their incident;
 * when an incident gets new evidence, re-cascade ALL attached persons.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('./cost');
const { reportError } = require('./_errors');
const { enqueueCascade } = require('./_cascade');

async function run(db, limit = 30) {
  let highConfPersons = []; try {
    highConfPersons = await db('persons').where('identity_confidence', '>=', 90)
      .where(function () { this.whereNull('bidir_resync_at').orWhere('bidir_resync_at', '<', db.raw("NOW() - INTERVAL '1 hour'")); })
      .limit(limit);
  } catch (_) {}
  let incidentRescore = 0, peopleRecascade = 0;
  for (const p of highConfPersons) {
    if (!p.incident_id) continue;
    try {
      await enqueueCascade(db, 'incident', p.incident_id, 'bidir-from-person', { weight: 90, person_id: p.id });
      incidentRescore++;
      const others = await db('persons').where('incident_id', p.incident_id).whereNot('id', p.id);
      for (const o of others) {
        await enqueueCascade(db, 'person', o.id, 'bidir-from-incident', { weight: 25 });
        peopleRecascade++;
      }
      await db('persons').where({ id: p.id }).update({ bidir_resync_at: new Date() });
    } catch (_) {}
  }
  await trackApiCall(db, 'system-bidirectional-cascade', 'sql', 0, 0, true).catch(() => {});
  return { highConfPersons: highConfPersons.length, incidentRescore, peopleRecascade };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'bidirectional-cascade', cost: 0 });
    const out = await run(db, parseInt(req.query.limit) || 30);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'bidirectional-cascade', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
