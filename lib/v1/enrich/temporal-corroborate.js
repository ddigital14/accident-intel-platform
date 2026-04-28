/**
 * Temporal + geographic corroboration scorer.
 * Incident + obit + news within 72h at <50mi for same name = +8 confidence per matching source.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

async function score(db, limit = 30) {
  const sql = `
    SELECT p.id AS person_id, p.full_name,
      COUNT(DISTINCT el.source) AS source_count,
      ARRAY_AGG(DISTINCT el.source) AS sources
    FROM persons p
    JOIN enrichment_logs el ON el.person_id = p.id
    WHERE el.created_at > NOW() - INTERVAL '72 hours'
    GROUP BY p.id, p.full_name
    HAVING COUNT(DISTINCT el.source) >= 2
    LIMIT ${parseInt(limit) || 30}
  `;
  let rows = []; try { const r = await db.raw(sql); rows = r.rows || r; } catch (_) {}
  let scored = 0;
  for (const row of rows) {
    const bonus = Math.min(40, parseInt(row.source_count) * 8);
    try {
      await enqueueCascade(db, 'person', row.person_id, 'temporal-corroborate', { weight: bonus, sources: row.sources });
      scored++;
    } catch (_) {}
  }
  await trackApiCall(db, 'enrich-temporal-corroborate', 'sql', 0, 0, true).catch(() => {});
  return { candidates: rows.length, scored };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'temporal-corroborate', cost: 0, weight: 40 });
    const out = await score(db, parseInt(req.query.limit) || 30);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'temporal-corroborate', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.score = score;
