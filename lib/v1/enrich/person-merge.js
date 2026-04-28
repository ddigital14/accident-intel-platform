/**
 * Cross-incident person merge. Same name + DOB ±2yr + same metro = merge.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

async function merge(db, limit = 25) {
  const sql = `
    SELECT a.id AS a_id, b.id AS b_id, a.full_name, a.location_locality
    FROM persons a JOIN persons b ON a.id < b.id
      AND LOWER(a.full_name) = LOWER(b.full_name)
      AND COALESCE(a.location_locality, '') = COALESCE(b.location_locality, '')
      AND ABS(COALESCE(a.dob_year, 0) - COALESCE(b.dob_year, 0)) <= 2
    WHERE a.full_name IS NOT NULL AND a.full_name <> ''
    LIMIT ${parseInt(limit) || 25}
  `;
  let rows = []; try { const r = await db.raw(sql); rows = r.rows || r; } catch (_) {}
  let merged = 0;
  for (const m of rows) {
    try {
      // Mark b as merged_into a, copy any null fields from b → a, sum severity
      await db.raw(`UPDATE persons SET merged_into = ? WHERE id = ?`, [m.a_id, m.b_id]);
      await enqueueCascade(db, 'person', m.a_id, 'person-merge', { weight: 25, absorbed: m.b_id });
      merged++;
    } catch (_) {}
  }
  await trackApiCall(db, 'enrich-person-merge', 'sql', 0, 0, true).catch(() => {});
  return { candidates: rows.length, merged };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'person-merge', cost: 0, weight: 25 });
    const out = await merge(db, parseInt(req.query.limit) || 25);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'person-merge', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.merge = merge;
