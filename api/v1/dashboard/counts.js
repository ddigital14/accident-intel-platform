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

    // Recent incidents — discover columns dynamically
    let recentIncidents = [];
    try {
      // First get available columns
      const colCheck = await db.raw(`
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'incidents'
        AND column_name IN ('discovered_at','created_at','source','confidence_score','severity','city','state','incident_type')
      `);
      const cols = colCheck.rows.map(r => r.column_name);
      const hasDisco = cols.includes('discovered_at');
      const hasCreated = cols.includes('created_at');
      const orderCol = hasDisco ? 'discovered_at' : hasCreated ? 'created_at' : null;

      const selectCols = ['id', ...cols.filter(c => c !== 'discovered_at' && c !== 'created_at')];
      if (hasDisco) selectCols.push('discovered_at');
      if (hasCreated) selectCols.push('created_at');

      const orderClause = orderCol ? `ORDER BY ${orderCol} DESC NULLS LAST` : 'ORDER BY id DESC';
      const r = await db.raw(`SELECT ${selectCols.join(', ')} FROM incidents ${orderClause} LIMIT 10`);
      recentIncidents = r.rows.map(row => ({
        ...row,
        timestamp: row.discovered_at || row.created_at || null
      }));
    } catch (e) {
      // Ultra-fallback
      try {
        const r = await db.raw(`SELECT id FROM incidents LIMIT 10`);
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

    // Data source breakdown — try 'source' column, fallback to 'incident_type'
    let sourceBreakdown = [];
    try {
      const r = await db.raw(`
        SELECT COALESCE(source, 'unknown') as source, COUNT(*) as count,
          ROUND(AVG(confidence_score)::numeric, 1) as avg_confidence
        FROM incidents
        GROUP BY COALESCE(source, 'unknown')
        ORDER BY count DESC
      `);
      sourceBreakdown = r.rows.filter(row => row.count > 0);
      // If all are 'unknown', try incident_type instead
      if (sourceBreakdown.length <= 1 && sourceBreakdown[0]?.source === 'unknown') {
        const r2 = await db.raw(`
          SELECT COALESCE(incident_type, 'unknown') as source, COUNT(*) as count,
            ROUND(AVG(confidence_score)::numeric, 1) as avg_confidence
          FROM incidents
          GROUP BY COALESCE(incident_type, 'unknown')
          ORDER BY count DESC
        `);
        sourceBreakdown = r2.rows;
      }
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
