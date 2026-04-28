const { apply: applyEdgeCache } = require('./_edge_cache');
/**
 * GET /api/v1/system/health — pipeline health snapshot for the dashboard
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');

module.exports = async function handler(req, res) {
  applyEdgeCache(res, { sMaxAge: 30, swr: 120 });
  res.setHeader('Access-Control-Allow-Origin', '*');
  // Phase 25: 30s edge cache for health snapshot
  res.setHeader('Cache-Control', 'public, max-age=30, s-maxage=30, stale-while-revalidate=60');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const db = getDb();
  try {
    const day = new Date(Date.now() - 86400000);
    const [sources, incidents24, errors24, byPipeline] = await Promise.all([
      db('data_sources').select('id','name','type','is_active','last_polled_at','last_success_at','error_count'),
      db('incidents').count('* as count').where('discovered_at','>',day).first(),
      db.raw(`SELECT COUNT(*) as count FROM system_errors WHERE created_at > $1`, [day])
        .then(r => r.rows?.[0]?.count || 0).catch(() => 0),
      db.raw(`SELECT pipeline, COUNT(*) as errors_24h, MAX(created_at) as last_error
              FROM system_errors WHERE created_at > $1 GROUP BY pipeline`, [day])
        .then(r => r.rows || []).catch(() => [])
    ]);
    const sourceBreakdown = await db.raw(`
      SELECT source_type, COUNT(*) as count FROM source_reports
      WHERE created_at > $1 GROUP BY source_type ORDER BY count DESC
    `, [day]).then(r => r.rows || []).catch(() => []);

    let postgisAvailable = false;
    let geomColumn = false;
    try {
      const ext = await db.raw(`SELECT extname FROM pg_extension WHERE extname = 'postgis'`);
      postgisAvailable = ext.rows.length > 0;
      const col = await db.raw(`SELECT column_name FROM information_schema.columns
        WHERE table_name = 'incidents' AND column_name = 'geom'`);
      geomColumn = col.rows.length > 0;
    } catch (_) {}

    const pipelines = [
      { name: 'tomtom',     path: '/api/v1/ingest/run',       cron: '*/10 * * * *' },
      { name: 'waze',       path: '/api/v1/ingest/waze',      cron: '*/10 * * * *' },
      { name: 'opendata',   path: '/api/v1/ingest/opendata',  cron: '*/10 * * * *' },
      { name: 'scanner',    path: '/api/v1/ingest/scanner',   cron: '*/15 * * * *' },
      { name: 'news',       path: '/api/v1/ingest/news',      cron: '*/30 * * * *' },
      { name: 'state-crash',path: '/api/v1/ingest/state-crash',cron: '0 */2 * * *' },
      { name: 'court',      path: '/api/v1/ingest/court',     cron: '0 */6 * * *' },
      { name: 'correlate',  path: '/api/v1/ingest/correlate', cron: '*/20 * * * *' },
      { name: 'enrich',     path: '/api/v1/enrich/run',       cron: '*/15 * * * *' }
    ];
    const errorMap = {};
    for (const e of byPipeline) errorMap[e.pipeline] = { errors_24h: parseInt(e.errors_24h), last_error: e.last_error };

    res.json({
      success: true,
      timestamp: new Date().toISOString(),
      database: { postgis: postgisAvailable, geom_column: geomColumn },
      counts: {
        active_sources: sources.filter(s => s.is_active).length,
        total_sources: sources.length,
        incidents_24h: parseInt(incidents24?.count || 0),
        errors_24h: parseInt(errors24)
      },
      pipelines: pipelines.map(p => ({ ...p, ...(errorMap[p.name] || { errors_24h: 0, last_error: null }) })),
      data_sources: sources,
      source_breakdown_24h: sourceBreakdown
    });
  } catch (err) {
    await reportError(db, 'system_health', null, err.message);
    res.status(500).json({ error: err.message });
  }
};
