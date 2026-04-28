/**
 * Phone-pattern co-residence inference.
 * Two persons sharing area code + last 4 + same street → same household.
 * Pure SQL, +12 weight bonus. Zero external cost.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

async function findHouseholds(db, limit = 50) {
  const sql = `
    WITH cleaned AS (
      SELECT id, full_name, phone, location_street_address AS street,
             SUBSTRING(REGEXP_REPLACE(phone, '\\D', '', 'g') FROM 1 FOR 3) AS area,
             RIGHT(REGEXP_REPLACE(phone, '\\D', '', 'g'), 4) AS last4
      FROM persons
      WHERE phone IS NOT NULL AND phone <> '' AND location_street_address IS NOT NULL
    )
    SELECT a.id AS a_id, b.id AS b_id, a.full_name AS a_name, b.full_name AS b_name, a.street, a.area, a.last4
    FROM cleaned a JOIN cleaned b
      ON a.id < b.id AND a.area = b.area AND a.last4 = b.last4 AND LOWER(a.street) = LOWER(b.street)
    LIMIT ${parseInt(limit) || 50}
  `;
  let rows = []; try { const r = await db.raw(sql); rows = r.rows || r; } catch (_) {}
  let linked = 0;
  for (const h of rows) {
    try {
      await db('person_relationships').insert({ person_a_id: h.a_id, person_b_id: h.b_id, relationship: 'household', confidence: 85, source: 'co-residence', created_at: new Date() }).onConflict(['person_a_id', 'person_b_id']).ignore().catch(() => {});
      await enqueueCascade(db, 'person', h.a_id, 'co-residence', { weight: 12, partner: h.b_id });
      await enqueueCascade(db, 'person', h.b_id, 'co-residence', { weight: 12, partner: h.a_id });
      linked++;
    } catch (_) {}
  }
  await trackApiCall(db, 'enrich-co-residence', 'sql', 0, 0, true).catch(() => {});
  return { candidates: rows.length, linked };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'co-residence', cost: 0, weight: 12 });
    if (action === 'batch' || !action) { const out = await findHouseholds(db, parseInt(req.query.limit) || 50); return res.json({ success: true, ...out }); }
    return res.status(400).json({ error: 'unknown action' });
  } catch (err) { await reportError(db, 'co-residence', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.findHouseholds = findHouseholds;
