/**
 * GET /api/v1/cron/dispatch?secret=ingest-now&jobs=foo,bar,baz
 *
 * Single Vercel cron endpoint that internally calls multiple jobs.
 * This bypasses Vercel Hobby's 11-cron-per-project limit by funneling
 * many workloads through one scheduled call.
 *
 * Each job is fired in parallel with timeout protection. Results
 * collected and returned. Failures isolated per job.
 *
 * EXAMPLE jobs (run every 10 min): "tomtom,waze,opendata,correlate"
 * EXAMPLE jobs (run every 30 min): "news-rss,news,reddit,obituaries,pd-press,people-search"
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');

const JOB_REGISTRY = {
  // Dispatch
  'tomtom':       '/api/v1/ingest/run',
  'waze':         '/api/v1/ingest/waze',
  'opendata':     '/api/v1/ingest/opendata',
  'scanner':      '/api/v1/ingest/scanner',
  // News / social
  'news':         '/api/v1/ingest/news',
  'news-rss':     '/api/v1/ingest/news-rss',
  'reddit':       '/api/v1/ingest/reddit',
  'pd-press':     '/api/v1/ingest/pd-press',
  'police-social':'/api/v1/ingest/police-social',
  // Records / specialized
  'state-crash':  '/api/v1/ingest/state-crash',
  'court':        '/api/v1/ingest/court',
  'obituaries':   '/api/v1/ingest/obituaries',
  'trauma':       '/api/v1/ingest/trauma',
  // Engine
  'correlate':    '/api/v1/ingest/correlate',
  'qualify':      '/api/v1/system/qualify',
  'notify':       '/api/v1/system/notify',
  'auto-assign':  '/api/v1/system/auto-assign',
  // Enrichment
  'enrich':       '/api/v1/enrich/run',
  'enrich-trigger':'/api/v1/enrich/trigger',
  'people-search':'/api/v1/enrich/people-search',
  // Maintenance
  'audit':        '/api/v1/system/audit?fix=true',
  'digest':       '/api/v1/system/digest?post=true',
  'errors-clean': '/api/v1/system/errors?action=clear&days=14',
};

// Different secrets per endpoint
function appendSecret(path) {
  if (path.includes('/enrich/run')) return path + (path.includes('?') ? '&' : '?') + 'secret=enrich-now';
  return path + (path.includes('?') ? '&' : '?') + 'secret=ingest-now';
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();

  const jobsParam = (req.query.jobs || '').toString();
  const requested = jobsParam.split(',').map(s => s.trim()).filter(Boolean);
  if (requested.length === 0) {
    return res.status(400).json({
      error: 'no jobs requested',
      registry: Object.keys(JOB_REGISTRY)
    });
  }

  const baseUrl = `https://${req.headers.host || 'accident-intel-platform.vercel.app'}`;
  const startAll = Date.now();
  const TIME_BUDGET = 55000;

  // Fire all jobs in parallel (each with own timeout)
  const promises = requested.map(async (jobName) => {
    const startT = Date.now();
    const path = JOB_REGISTRY[jobName];
    if (!path) {
      return { job: jobName, status: 'unknown_job', latency_ms: 0 };
    }
    try {
      const url = baseUrl + appendSecret(path);
      const resp = await fetch(url, {
        signal: AbortSignal.timeout(Math.min(50000, TIME_BUDGET - (Date.now() - startAll))),
        headers: { 'X-Cron-Dispatch': '1' }
      });
      const data = await resp.json().catch(() => null);
      const latency = Date.now() - startT;
      if (resp.ok && data?.success) {
        return {
          job: jobName,
          status: 'pass',
          latency_ms: latency,
          message: data.message || 'OK',
          stats: extractStats(data)
        };
      }
      return {
        job: jobName,
        status: 'fail',
        latency_ms: latency,
        error: data?.error || `HTTP ${resp.status}`
      };
    } catch (e) {
      return { job: jobName, status: 'fail', latency_ms: Date.now() - startT, error: e.message };
    }
  });

  const results = await Promise.all(promises);
  const passed = results.filter(r => r.status === 'pass').length;
  const failed = results.filter(r => r.status === 'fail').length;

  if (failed > 0) {
    await reportError(db, 'cron-dispatch', null,
      `${failed}/${results.length} jobs failed`, { failed: results.filter(r => r.status === 'fail') });
  }

  res.json({
    success: true,
    summary: `${passed}/${results.length} jobs passed (${failed} failed)`,
    requested,
    results,
    total_latency_ms: Date.now() - startAll,
    timestamp: new Date().toISOString()
  });
};

function extractStats(data) {
  // Pluck commonly-used count fields
  const out = {};
  for (const k of ['inserted', 'corroborated', 'persons_added', 'incidents_created',
                    'evaluated', 'promoted', 'enriched', 'matched', 'victims_extracted',
                    'fields_filled', 'crashes_found', 'pds_polled', 'subs_polled',
                    'accounts_polled', 'feeds_polled', 'candidates']) {
    if (data[k] !== undefined) out[k] = data[k];
  }
  return out;
}
