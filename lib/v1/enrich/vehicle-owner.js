/**
 * Plate + VIN + address ownership inference. No paid plate API needed.
 * Same plate at same address across multiple incidents = same owner.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

async function infer(db, limit = 50) {
  const sql = `
    SELECT v.license_plate, v.vin, p.location_street_address AS street, p.id AS person_id, p.full_name, COUNT(*) OVER (PARTITION BY v.license_plate, p.location_street_address) AS recurrence
    FROM vehicles v JOIN incidents i ON v.incident_id = i.id JOIN persons p ON p.incident_id = i.id
    WHERE v.license_plate IS NOT NULL AND v.license_plate <> '' AND p.location_street_address IS NOT NULL
  `;
  let rows = []; try { const r = await db.raw(sql); rows = r.rows || r; } catch (_) {}
  const inferred = rows.filter(r => parseInt(r.recurrence) >= 2);
  let linked = 0;
  for (const r of inferred) {
    try {
      await db('persons').where({ id: r.person_id }).update({ vehicle_owner_inferred: true, updated_at: new Date() });
      await enqueueCascade(db, 'person', r.person_id, 'vehicle-owner-inference', { weight: 70, plate: r.license_plate });
      linked++;
    } catch (_) {}
  }
  await trackApiCall(db, 'enrich-vehicle-owner', 'sql', 0, 0, true).catch(() => {});
  return { candidates: rows.length, inferred: inferred.length, linked };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'vehicle-owner', cost: 0, weight: 70 });
    const out = await infer(db, parseInt(req.query.limit) || 50);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'vehicle-owner', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.infer = infer;
