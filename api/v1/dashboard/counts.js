/**
 * Dashboard Table Counts (no auth required)
 * GET /api/v1/dashboard/counts
 */
const { getDb } = require('../../_db');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    const db = getDb();
    const tables = ['incidents', 'persons', 'vehicles', 'source_reports', 'enrichment_logs', 'cross_references'];
    const counts = {};

    for (const t of tables) {
      try {
        const r = await db.raw(`SELECT COUNT(*) as count FROM ${t}`);
        counts[t] = parseInt(r.rows[0].count, 10);
      } catch (e) {
        counts[t] = `error: ${e.message}`;
      }
    }

    // Recent activity
    let recentIncidents = [];
    try {
      const r = await db.raw(`
        SELECT id, source, confidence_score, created_at
        FROM incidents ORDER BY created_at DESC LIMIT 5
      `);
      recentIncidents = r.rows;
    } catch (e) { /* ignore */ }

    let enrichmentStats = {};
    try {
      const r = await db.raw(`
        SELECT
          COUNT(*) as total,
          COUNT(*) FILTER (WHERE enrichment_score > 0) as enriched,
          ROUND(AVG(enrichment_score)::numeric, 1) as avg_score,
          MAX(enrichment_score) as max_score
        FROM persons
      `);
      enrichmentStats = r.rows[0];
    } catch (e) { /* ignore */ }

    res.json({
      success: true,
      counts,
      enrichment: enrichmentStats,
      recent_incidents: recentIncidents,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
