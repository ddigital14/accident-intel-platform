/**
 * Phase 50: CEI counters observability endpoint.
 *
 * GET /api/v1/system/cei-counters?secret=ingest-now
 *   → returns full engine_capabilities telemetry table with derived stats.
 *
 * GET /api/v1/system/cei-counters?secret=ingest-now&engine=spanish-detector
 *   → single engine row.
 *
 * GET /api/v1/system/cei-counters?secret=ingest-now&action=summary
 *   → aggregate (engines tracked, total invocations, top 10 by count).
 *
 * POST {action: 'bump', engine: '...', success: true, latency_ms: 123}
 *   → manually bump a counter (used by callers without DB context).
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { bumpCounter, getAllCounters, getCounter, ensureTable } = require('./_cei_telemetry');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();

  try {
    await ensureTable(db);

    if (req.method === 'POST') {
      let body = req.body;
      if (!body) {
        body = await new Promise(r => { let d = ''; req.on('data', c => d += c); req.on('end', () => { try { r(JSON.parse(d)); } catch (_) { r({}); } }); });
      }
      if (body?.action === 'bump' && body?.engine) {
        await bumpCounter(db, body.engine, body.success !== false, body.latency_ms || 0);
        const row = await getCounter(db, body.engine);
        return res.json({ success: true, message: `bumped ${body.engine}`, row });
      }
    }

    const action = req.query?.action || 'list';
    const engineFilter = req.query?.engine;

    if (engineFilter) {
      const row = await getCounter(db, engineFilter);
      return res.json({ success: true, engine: engineFilter, telemetry: row || null });
    }

    if (action === 'summary') {
      const rows = await getAllCounters(db, { limit: 200 });
      const total = rows.reduce((a, r) => a + (parseInt(r.invocation_count, 10) || 0), 0);
      const successes = rows.reduce((a, r) => a + (parseInt(r.success_count, 10) || 0), 0);
      const failures = rows.reduce((a, r) => a + (parseInt(r.failure_count, 10) || 0), 0);
      return res.json({
        success: true,
        engines_tracked: rows.length,
        total_invocations: total,
        total_successes: successes,
        total_failures: failures,
        overall_success_rate: total ? (successes / total) : 0,
        top_10: rows.slice(0, 10).map(r => ({
          engine: r.engine_name,
          invocations: parseInt(r.invocation_count, 10) || 0,
          success_rate: r.success_rate,
          avg_latency_ms: r.avg_latency_ms,
          last_invoked_at: r.last_invoked_at
        })),
        timestamp: new Date().toISOString()
      });
    }

    // list (default)
    const rows = await getAllCounters(db, { limit: 500 });
    return res.json({
      success: true,
      message: `${rows.length} engines tracked`,
      engines_tracked: rows.length,
      counters: rows.map(r => ({
        engine: r.engine_name,
        invocations: parseInt(r.invocation_count, 10) || 0,
        successes: parseInt(r.success_count, 10) || 0,
        failures: parseInt(r.failure_count, 10) || 0,
        success_rate: r.success_rate,
        avg_latency_ms: r.avg_latency_ms,
        last_invoked_at: r.last_invoked_at,
        last_success_at: r.last_success_at,
        last_failure_at: r.last_failure_at
      })),
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'cei-counters', null, err.message).catch(() => {});
    return res.status(500).json({ error: err.message });
  }
};
