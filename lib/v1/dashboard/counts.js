/**
 * Dashboard Table Counts & Stats (no auth required)
 * GET /api/v1/dashboard/counts
 *
 * Returns: table row counts, enrichment stats, recent incidents,
 * data source breakdown, enrichment pipeline health, and person-level stats
 */
const { getDb } = require('../../_db');

// Simple in-memory rate limiter (per IP, 30 requests per minute)
const rateLimitMap = new Map();
const RATE_LIMIT = 30;
const RATE_WINDOW = 60000; // 1 minute

function checkRateLimit(ip) {
  const now = Date.now();
  const entry = rateLimitMap.get(ip);
  if (!entry || now - entry.windowStart > RATE_WINDOW) {
    rateLimitMap.set(ip, { windowStart: now, count: 1 });
    return true;
  }
  entry.count++;
  if (entry.count > RATE_LIMIT) return false;
  return true;
}

// Clean up stale entries every 5 minutes
setInterval(() => {
  const cutoff = Date.now() - RATE_WINDOW;
  for (const [ip, entry] of rateLimitMap) {
    if (entry.windowStart < cutoff) rateLimitMap.delete(ip);
  }
}, 300000);

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // Rate limit check
  const clientIp = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket?.remoteAddress || 'unknown';
  if (!checkRateLimit(clientIp)) {
    return res.status(429).json({ error: 'Too many requests. Please wait a moment.' });
  }

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
        AND column_name IN ('discovered_at','occurred_at','created_at','source','confidence_score','severity','city','state','incident_type','address')
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
      recentIncidents = [{ error: e.message }];
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

    // Data source breakdown — check which column exists, prefer 'source', fallback 'incident_type'
    let sourceBreakdown = [];
    try {
      const srcCol = await db.raw(`
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'incidents' AND column_name IN ('source', 'incident_type')
        ORDER BY CASE column_name WHEN 'source' THEN 1 ELSE 2 END
      `);
      const groupCol = srcCol.rows[0]?.column_name || null;
      if (groupCol) {
        const r = await db.raw(`
          SELECT COALESCE(${groupCol}, 'unknown') as source, COUNT(*) as count,
            ROUND(AVG(confidence_score)::numeric, 1) as avg_confidence
          FROM incidents
          GROUP BY COALESCE(${groupCol}, 'unknown')
          ORDER BY count DESC
        `);
        sourceBreakdown = r.rows;
      }
    } catch (e) { sourceBreakdown = [{ error: e.message }]; }

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


    // Lead qualification counts
    let qualificationStats = {};
    try {
      const r = await db.raw(`
        SELECT
          qualification_state,
          COUNT(*) as count,
          AVG(lead_score) as avg_score
        FROM incidents
        WHERE discovered_at > NOW() - INTERVAL '7 days'
        GROUP BY qualification_state
      `);
      qualificationStats = {};
      for (const row of r.rows) {
        qualificationStats[row.qualification_state || 'unknown'] = {
          count: parseInt(row.count),
          avg_score: parseFloat(row.avg_score) || 0
        };
      }
    } catch (e) {
      qualificationStats = { error: e.message };
    }

    // Top qualified leads (main view preview)
    let topQualifiedLeads = [];
    try {
      const r = await db.raw(`
        SELECT i.id, i.incident_type, i.severity, i.city, i.state,
               i.address, i.discovered_at, i.occurred_at, i.qualified_at,
               i.lead_score, i.confidence_score, i.source_count,
               (SELECT json_agg(json_build_object(
                  'name', p.full_name, 'phone', p.phone, 'email', p.email,
                  'address', p.address, 'has_attorney', p.has_attorney
               )) FROM persons p WHERE p.incident_id = i.id LIMIT 5) as persons
        FROM incidents i
        WHERE i.qualification_state = 'qualified'
        ORDER BY i.lead_score DESC, i.qualified_at DESC NULLS LAST
        LIMIT 20
      `);
      topQualifiedLeads = r.rows;
    } catch (e) { /* ignore */ }

    res.json({
      qualification: qualificationStats,
      top_qualified_leads: topQualifiedLeads,
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
