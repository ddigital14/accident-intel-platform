/**
 * Cross-source contradiction detector.
 * When two sources give conflicting names/addresses for the same person, flag for cross-exam priority.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

async function run(db, limit = 30) {
  const sql = `
    SELECT person_id, COUNT(DISTINCT data::jsonb->>'full_name') AS name_variants,
           COUNT(DISTINCT data::jsonb->>'address') AS addr_variants
    FROM enrichment_logs
    WHERE created_at > NOW() - INTERVAL '14 days'
    GROUP BY person_id
    HAVING COUNT(DISTINCT data::jsonb->>'full_name') > 1 OR COUNT(DISTINCT data::jsonb->>'address') > 1
    LIMIT ${parseInt(limit) || 30}
  `;
  let rows = []; try { const r = await db.raw(sql); rows = r.rows || r; } catch (_) {}
  let flagged = 0;
  for (const row of rows) {
    try {
      await db('persons').where({ id: row.person_id }).update({
        has_contradiction: true,
        identity_confidence: db.raw('GREATEST(0, COALESCE(identity_confidence, 0) - 10)'),
        updated_at: new Date()
      });
      await enqueueCascade(db, 'person', row.person_id, 'contradiction-detector', { weight: -10, name_variants: row.name_variants, addr_variants: row.addr_variants });
      flagged++;
    } catch (_) {}
  }
  await trackApiCall(db, 'enrich-contradiction-detector', 'sql', 0, 0, true).catch(() => {});
  return { candidates: rows.length, flagged };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'contradiction-detector', penalty: 10 });
    const out = await run(db, parseInt(req.query.limit) || 30);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'contradiction-detector', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
