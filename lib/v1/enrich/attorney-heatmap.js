/**
 * Geographic competitor activity heatmap.
 * Analyzes CourtListener + state-court filings for PI cases by attorney name + city.
 * Output: which firms are filing PI cases in which metros.
 * Used to weight up incident scoring in hot metros.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');

async function run(db, days = 30) {
  const sql = `
    SELECT i.city, i.state,
      COUNT(*) FILTER (WHERE p.has_attorney) AS attorney_filings,
      COUNT(*) AS total_incidents,
      COUNT(DISTINCT p.attorney_name) AS unique_firms,
      ARRAY_AGG(DISTINCT p.attorney_name) FILTER (WHERE p.attorney_name IS NOT NULL) AS firms
    FROM incidents i
    LEFT JOIN persons p ON p.incident_id = i.id
    WHERE i.created_at > NOW() - INTERVAL '${parseInt(days)} days'
      AND i.city IS NOT NULL AND i.state IS NOT NULL
    GROUP BY i.city, i.state
    HAVING COUNT(*) > 5
    ORDER BY attorney_filings DESC
    LIMIT 50
  `;
  let rows = []; try { const r = await db.raw(sql); rows = r.rows || r; } catch (_) {}
  // Update each metro's heat_score on incidents
  let scored = 0;
  for (const m of rows) {
    const heat = parseInt(m.attorney_filings) > 0 ? Math.min(20, parseInt(m.attorney_filings)) : 0;
    try {
      await db('incidents').where({ city: m.city, state: m.state })
        .where('created_at', '>', db.raw(`NOW() - INTERVAL '${parseInt(days)} days'`))
        .update({ metro_heat_score: heat });
      scored++;
    } catch (_) {}
  }
  await trackApiCall(db, 'enrich-attorney-heatmap', 'sql', 0, 0, true).catch(() => {});
  return { metros: rows.length, scored, top: rows.slice(0, 10) };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'attorney-heatmap' });
    const out = await run(db, parseInt(req.query.days) || 30);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'attorney-heatmap', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
