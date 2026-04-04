/**
 * Dashboard Table Counts & Stats (no auth required)
 * GET /api/v1/dashboard/counts
 *
 * Returns: table row counts, enrichment stats, recent incidents,
 * data source breakdown, enrichment pipeline health, and person-level stats
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

    // Recent incidents — use discovered_at (the main timestamp), fall back to created_at
    let recentIncidents = [];
    try {
      const r = await db.raw(`
        SELECT id, source, confidence_score, severity, city, state, incident_type,
          COALESCE(discovered_at, created_at) as timestamp
        FROM incidents
        ORDER BY COALESCE(discovered_at, created_at) DESC NULLS LAST
        LIMIT 10
      `);
      recentIncidents = r.rows;
    } catch (e) {
      // Try simpler query if columns don't exist
      try {
        const r = await db.raw(`SELECT id, source, confidence_score FROM incidents ORDER BY id DESC LIMIT 10`);
        recentIncidents = r.rows;
      } catch (e2) { /* ignore */ }
    }

    // Enrichment stats
    let enrichmentStats = {};
    try {
      const r = await db.raw(`
        SELECT
          COUNT(*) as total,
          COUNT(*) FILTER (WHERE enrichment_score > 0) as enriched,
          COUNT(*) FILTER (WHERE enrichment_score >= 80) as high_quality,
          COUNT(*) FILTER (WHERE enrichment_score < 50 OR enrichment_score IS NULL) as needs_enrichment,
          ROUND(AVG(enrichment_score)::numeric, 1) as avg_score,
          MAX(enrichment_score) as max_score,
          MIN(CASE WHEN enrichment_score > 0 THEN enrichment_score END) as min_enriched_score
        FROM persons
      `);
      enrichmentStats = r.rows[0];
    } catch (e) { /* ignore */ }

    // Data source breakdown
    let sourceBreakdown = [];
    try {
      const r = await db.raw(`
        SELECT source, COUNT(*) as count,
          ROUND(AVG(confidence_score)::numeric, 1) as avg_confidence
        FROM incidents
        GROUP BY source
        ORDER BY count DESC
      `);
      sourceBreakdown = r.rows;
    } catch (e) { /* ignore */ }

    // Enrichment pipeline health — which APIs have contributed
    let pipelineHealth = [];
    try {
      const r = await db.raw(`
        SELECT
          CASE
            WHEN source_url IS NOT NULL THEN 'api_verified'
            WHEN verified = true THEN 'api_sourced'
            ELSE 'fallback'
          END as data_quality,
          COUNT(*) as field_count,
          COUNT(DISTINCT person_id) as person_count
        FROM enrichment_logs
        GROUP BY 1
        ORDER BY field_count DESC
      `);
      pipelineHealth = r.rows;
    } catch (e) { /* ignore */ }

    // Cross-reference stats
    let crossRefStats = {};
    try {
      const r = await db.raw(`
        SELECT
          COUNT(*) as total,
          COUNT(*) FILTER (WHERE resolution = 'auto_resolved') as auto_resolved,
          COUNT(*) FILTER (WHERE resolution = 'pending') as pending,
          ROUND(AVG(match_score)::numeric, 1) as avg_match_score
        FROM cross_references
      `);
      crossRefStats = r.rows[0];
    } catch (e) { /* ignore */ }

    // Incidents by severity
    let bySeverity = [];
    try {
      const r = await db.raw(`
        SELECT severity, COUNT(*) as count
        FROM incidents
        WHERE severity IS NOT NULL
        GROUP BY severity
        ORDER BY count DESC
      `);
      bySeverity = r.rows;
    } catch (e) { /* ignore */ }

    // Person fields completeness
    let fieldCompleteness = {};
    try {
      const r = await db.raw(`
        SELECT
          COUNT(*) as total,
          COUNT(*) FILTER (WHERE phone IS NOT NULL) as has_phone,
          COUNT(*) FILTER (WHERE email IS NOT NULL) as has_email,
          COUNT(*) FILTER (WHERE address IS NOT NULL) as has_address,
          COUNT(*) FILTER (WHERE employer IS NOT NULL) as has_employer,
          COUNT(*) FILTER (WHERE insurance_company IS NOT NULL) as has_insurance,
          COUNT(*) FILTER (WHERE has_attorney = true) as has_attorney,
          COUNT(*) FILTER (WHERE litigator = true) as is_litigator,
          COUNT(*) FILTER (WHERE deceased = true) as is_deceased,
          COUNT(*) FILTER (WHERE property_owner = true) as is_property_owner
        FROM persons
      `);
      fieldCompleteness = r.rows[0];
    } catch (e) { /* ignore */ }

    res.json({
      success: true,
      counts,
      enrichment: enrichmentStats,
      field_completeness: fieldCompleteness,
      recent_incidents: recentIncidents,
      source_breakdown: sourceBreakdown,
      by_severity: bySeverity,
      pipeline_health: pipelineHealth,
      cross_references: crossRefStats,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
