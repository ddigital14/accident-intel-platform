/**
 * GET /api/v1/system/smoke-test
 *
 * Hits every cron endpoint sequentially with timeouts, returns pass/fail summary.
 * Used by ops dashboard for at-a-glance health.
 *
 * Cron: every 30 minutes (separate from regular crons so it doesn't cascade)
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');

const ENDPOINTS = [
  { name: 'health',       path: '/api/v1/system/health',                 timeout: 10000, expect: 'success' },
  { name: 'tomtom',       path: '/api/v1/ingest/run?secret=ingest-now',  timeout: 25000, expect: 'success' },
  { name: 'waze',         path: '/api/v1/ingest/waze?secret=ingest-now', timeout: 20000, expect: 'success' },
  { name: 'opendata',     path: '/api/v1/ingest/opendata?secret=ingest-now', timeout: 25000, expect: 'success' },
  { name: 'correlate',    path: '/api/v1/ingest/correlate?secret=ingest-now', timeout: 25000, expect: 'success' },
  { name: 'qualify',      path: '/api/v1/system/qualify?secret=ingest-now',   timeout: 20000, expect: 'success' },
  { name: 'auto-assign',  path: '/api/v1/system/auto-assign?secret=ingest-now', timeout: 15000, expect: 'success' },
];

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  // Public endpoint — don't require secret for read-only smoke test results

  const db = getDb();
  const baseUrl = `https://${req.headers.host || 'accident-intel-platform.vercel.app'}`;
  const results = { passed: 0, failed: 0, total: 0, latency_ms: {}, endpoints: [] };

  try {
    // Run all endpoint tests in PARALLEL (was sequential — exceeded 60s function limit)
    const checks = await Promise.all(ENDPOINTS.map(async (ep) => {
      const startT = Date.now();
      try {
        const resp = await fetch(`${baseUrl}${ep.path}`, {
          signal: AbortSignal.timeout(Math.min(ep.timeout, 25000)),
          headers: { 'X-Smoke-Test': '1' }
        });
        const latency = Date.now() - startT;
        if (resp.ok) {
          const data = await resp.json().catch(() => null);
          if (data?.success) return { name: ep.name, path: ep.path, status: 'pass', latency_ms: latency };
          return { name: ep.name, path: ep.path, status: 'fail', error: data?.error || 'no success flag', latency_ms: latency };
        }
        return { name: ep.name, path: ep.path, status: 'fail', error: `HTTP ${resp.status}`, latency_ms: latency };
      } catch (e) {
        return { name: ep.name, path: ep.path, status: 'fail', error: e.message, latency_ms: Date.now() - startT };
      }
    }));

    for (const c of checks) {
      if (c.status === 'pass') results.passed++; else results.failed++;
      results.total++;
      results.latency_ms[c.name] = c.latency_ms;
      results.endpoints.push(c);
    }

    // Optionally log summary to changelog if anything failed
    if (results.failed > 0) {
      await reportError(db, 'smoke-test', null,
        `${results.failed}/${results.total} endpoints failed`,
        { failed: results.endpoints.filter(e => e.status === 'fail') });
    }

    res.json({
      success: true,
      summary: `${results.passed}/${results.total} passed`,
      ...results,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'smoke-test', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
