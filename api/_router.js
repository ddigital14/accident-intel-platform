/**
 * Vercel catch-all router for /api/v1/*
 *
 * Dispatches all sub-paths to handlers in /lib/v1/.
 * This consolidates 40+ serverless functions into one, fitting under
 * Vercel Hobby's 12-function-per-deployment limit.
 *
 * URL → handler mapping:
 *   /api/v1/ingest/run         → /lib/v1/ingest/run.js
 *   /api/v1/system/health      → /lib/v1/system/health.js
 *   /api/v1/incidents/abc-123  → /lib/v1/incidents/[id].js (with req.query.id = "abc-123")
 *   /api/v1/cron/dispatch      → /lib/v1/cron/dispatch.js
 *
 * Falls back to 404 for unknown paths.
 */
const path = require('path');

// Static handler map — explicit list keeps things predictable.
// Add new handlers here when you create them.
const ROUTES = {
  // Auth
  'auth/login':           '../lib/v1/auth/login.js',
  'auth/me':              '../lib/v1/auth/me.js',

  // Dashboard
  'dashboard/counts':       '../lib/v1/dashboard/counts.js',
  'dashboard/feed':         '../lib/v1/dashboard/feed.js',
  'dashboard/metro-areas':  '../lib/v1/dashboard/metro-areas.js',
  'dashboard/my-assignments': '../lib/v1/dashboard/my-assignments.js',
  'dashboard/stats':        '../lib/v1/dashboard/stats.js',

  // Incidents — both static and dynamic (handled below)
  'incidents':            '../lib/v1/incidents/index.js',
  'contacts':             '../lib/v1/contacts/index.js',
  'integrations':         '../lib/v1/integrations/index.js',
  'alerts/notifications': '../lib/v1/alerts/notifications.js',
  'migrate/columns':      '../lib/v1/migrate/columns.js',

  // Ingestion pipelines
  'ingest/run':           '../lib/v1/ingest/run.js',
  'ingest/waze':          '../lib/v1/ingest/waze.js',
  'ingest/opendata':      '../lib/v1/ingest/opendata.js',
  'ingest/scanner':       '../lib/v1/ingest/scanner.js',
  'ingest/news':          '../lib/v1/ingest/news.js',
  'ingest/news-rss':      '../lib/v1/ingest/news-rss.js',
  'ingest/state-crash':   '../lib/v1/ingest/state-crash.js',
  'ingest/court':         '../lib/v1/ingest/court.js',
  'ingest/correlate':     '../lib/v1/ingest/correlate.js',
  'ingest/obituaries':    '../lib/v1/ingest/obituaries.js',
  'ingest/trauma':        '../lib/v1/ingest/trauma.js',
  'ingest/reddit':        '../lib/v1/ingest/reddit.js',
  'ingest/police-social': '../lib/v1/ingest/police-social.js',
  'ingest/pd-press':      '../lib/v1/ingest/pd-press.js',
  'ingest/fars':          '../lib/v1/ingest/fars.js',

  // Enrichment
  'enrich/run':           '../lib/v1/enrich/run.js',
  'enrich/trigger':       '../lib/v1/enrich/trigger.js',
  'enrich/people-search': '../lib/v1/enrich/people-search.js',
  'enrich/crossref':      '../lib/v1/enrich/crossref.js',

  // System
  'system/health':        '../lib/v1/system/health.js',
  'system/postgis':       '../lib/v1/system/postgis.js',
  'system/qualify':       '../lib/v1/system/qualify.js',
  'system/notify':        '../lib/v1/system/notify.js',
  'system/auto-assign':   '../lib/v1/system/auto-assign.js',
  'system/changelog':     '../lib/v1/system/changelog.js',
  'system/errors':        '../lib/v1/system/errors.js',
  'system/cost':          '../lib/v1/system/cost.js',
  'system/setup':         '../lib/v1/system/setup.js',
  'system/digest':        '../lib/v1/system/digest.js',
  'system/smoke-test':    '../lib/v1/system/smoke-test.js',
  'system/triggers':      '../lib/v1/system/triggers.js',
  'system/audit':         '../lib/v1/system/audit.js',

  // Cron dispatcher
  'cron/dispatch':        '../lib/v1/cron/dispatch.js',
};

// Dynamic incident sub-routes
function tryDynamicIncident(slug, req) {
  // /incidents/{id}            → GET single
  // /incidents/{id}/assign     → POST assign
  // /incidents/{id}/note       → POST note
  if (slug[0] !== 'incidents' || slug.length < 2) return null;
  const id = slug[1];
  const action = slug[2];
  req.query = req.query || {};
  req.query.id = id;
  if (!action) return require('../lib/v1/incidents/[id].js');
  if (action === 'assign') return require('../lib/v1/incidents/[id]/assign.js');
  if (action === 'note')   return require('../lib/v1/incidents/[id]/note.js');
  return null;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, PATCH, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Cron-Secret');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // Path comes from rewrite source — try several locations Vercel may use
  let slug = req.query?.slug || req.query?.path;
  if (!slug) {
    // Try the original X-Forwarded headers
    const fwd = req.headers['x-forwarded-uri'] || req.headers['x-original-uri'] || req.url || '';
    const path = fwd.split('?')[0];
    const m = path.match(/\/api\/v1\/(.+)$/);
    slug = m ? m[1] : '';
  }
  if (typeof slug === 'string') slug = slug.split('/');
  slug = (slug || []).filter(Boolean);

  if (slug.length === 0) {
    return res.status(200).json({
      ok: true,
      service: 'AIP',
      version: 1,
      routes: Object.keys(ROUTES).sort()
    });
  }

  const key = slug.join('/');

  try {
    // Static route?
    if (ROUTES[key]) {
      const handler = require(ROUTES[key]);
      const fn = typeof handler === 'function' ? handler : handler.default || handler.handler;
      if (typeof fn !== 'function') {
        return res.status(500).json({ error: `Handler at ${key} has no callable export` });
      }
      return fn(req, res);
    }

    // Dynamic incident route?
    const dyn = tryDynamicIncident(slug, req);
    if (dyn) {
      const fn = typeof dyn === 'function' ? dyn : dyn.default || dyn.handler;
      return fn(req, res);
    }

    return res.status(404).json({ error: 'Not found', path: key, hint: 'GET /api/v1/ to list routes' });
  } catch (err) {
    console.error('Router error:', err);
    return res.status(500).json({ error: err.message, path: key });
  }
};
