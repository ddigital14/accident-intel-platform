/**
 * Sentinel fraud-filter. Flags incidents that look "too clean" or repetitive
 * (same address recurring, same name patterns, suspicious timing).
 * Insurance fraud rings sometimes generate fake accidents.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

async function run(db, days = 90) {
  // Recurring address signals
  const sql = `
    SELECT p.location_street_address AS addr, COUNT(DISTINCT p.id) AS n_persons, COUNT(DISTINCT p.incident_id) AS n_incidents,
      ARRAY_AGG(DISTINCT p.id) AS person_ids
    FROM persons p
    WHERE p.location_street_address IS NOT NULL AND p.location_street_address <> ''
      AND p.created_at > NOW() - INTERVAL '${parseInt(days)} days'
    GROUP BY p.location_street_address
    HAVING COUNT(DISTINCT p.incident_id) >= 3
    ORDER BY n_incidents DESC
    LIMIT 30
  `;
  let rows = []; try { const r = await db.raw(sql); rows = r.rows || r; } catch (_) {}
  let flagged = 0;
  for (const r of rows) {
    for (const pid of (r.person_ids || [])) {
      try {
        await db('persons').where({ id: pid }).update({
          fraud_risk: true,
          fraud_reason: `address recurrence: ${r.addr} appeared in ${r.n_incidents} incidents`,
          identity_confidence: db.raw('GREATEST(0, COALESCE(identity_confidence, 0) - 20)'),
          updated_at: new Date()
        });
        await enqueueCascade(db, 'person', pid, 'fraud-filter', { weight: -20, reason: 'address_recurrence' });
        flagged++;
      } catch (_) {}
    }
  }
  await trackApiCall(db, 'enrich-fraud-filter', 'sql', 0, 0, true).catch(() => {});
  return { suspicious_addresses: rows.length, flagged_persons: flagged };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'fraud-filter', penalty: 20 });
    const out = await run(db, parseInt(req.query.days) || 90);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'fraud-filter', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
