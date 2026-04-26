/**
 * AIP Master Router
 * Vercel only auto-includes files referenced via STATIC require().
 * So every handler must be require()d at the top, not dynamically.
 */

// ── Auth ──
const authLogin = require('../lib/v1/auth/login');
const authMe = require('../lib/v1/auth/me');

// ── Dashboard ──
const dashCounts = require('../lib/v1/dashboard/counts');
const dashFeed = require('../lib/v1/dashboard/feed');
const dashMetros = require('../lib/v1/dashboard/metro-areas');
const dashAssign = require('../lib/v1/dashboard/my-assignments');
const dashStats = require('../lib/v1/dashboard/stats');

// ── Incidents (static + dynamic) ──
const incidentsIndex = require('../lib/v1/incidents/index');
const incidentDetail = require('../lib/v1/incidents/[id]');
const incidentAssign = require('../lib/v1/incidents/[id]/assign');
const incidentNote = require('../lib/v1/incidents/[id]/note');

// ── Contacts / Integrations / Alerts / Migrate ──
const contacts = require('../lib/v1/contacts/index');
const integrations = require('../lib/v1/integrations/index');
const alertsNotif = require('../lib/v1/alerts/notifications');
const migrateColumns = require('../lib/v1/migrate/columns');

// ── Ingest ──
const ingestRun = require('../lib/v1/ingest/run');
const ingestWaze = require('../lib/v1/ingest/waze');
const ingestOpendata = require('../lib/v1/ingest/opendata');
const ingestScanner = require('../lib/v1/ingest/scanner');
const ingestNews = require('../lib/v1/ingest/news');
const ingestNewsRss = require('../lib/v1/ingest/news-rss');
const ingestStateCrash = require('../lib/v1/ingest/state-crash');
const ingestCourt = require('../lib/v1/ingest/court');
const ingestCorrelate = require('../lib/v1/ingest/correlate');
const ingestObituaries = require('../lib/v1/ingest/obituaries');
const ingestTrauma = require('../lib/v1/ingest/trauma');
const ingestReddit = require('../lib/v1/ingest/reddit');
const ingestPoliceSocial = require('../lib/v1/ingest/police-social');
const ingestPdPress = require('../lib/v1/ingest/pd-press');
const ingestCourtListener = require('../lib/v1/ingest/courtlistener');
const ingestFars = require('../lib/v1/ingest/fars');

// ── Enrich ──
const enrichRun = require('../lib/v1/enrich/run');
const enrichTrigger = require('../lib/v1/enrich/trigger');
const enrichPeopleSearch = require('../lib/v1/enrich/people-search');
const enrichTrestleTest = require('../lib/v1/enrich/trestle-test');
const enrichVoterRolls = require('../lib/v1/enrich/voter-rolls');
const enrichPropertyRecords = require('../lib/v1/enrich/property-records');
const enrichCrossExam = require('../lib/v1/enrich/cross-exam');
const enrichSocialSearch = require('../lib/v1/enrich/social-search');
const enrichFlVoterLoader = require('../lib/v1/enrich/fl-voter-loader');
const enrichCrossref = require('../lib/v1/enrich/crossref');

// ── System ──
const sysHealth = require('../lib/v1/system/health');
const sysPostgis = require('../lib/v1/system/postgis');
const sysQualify = require('../lib/v1/system/qualify');
const sysNotify = require('../lib/v1/system/notify');
const sysAutoAssign = require('../lib/v1/system/auto-assign');
const sysChangelog = require('../lib/v1/system/changelog');
const sysErrors = require('../lib/v1/system/errors');
const sysCost = require('../lib/v1/system/cost');
const sysSetup = require('../lib/v1/system/setup');
const sysDigest = require('../lib/v1/system/digest');
const sysSmoke = require('../lib/v1/system/smoke-test');
const sysTriggers = require('../lib/v1/system/triggers');
const sysAudit = require('../lib/v1/system/audit');
const sysResync = require('../lib/v1/system/resync');

// ── Cron ──
const cronDispatch = require('../lib/v1/cron/dispatch');

const ROUTES = {
  'auth/login': authLogin,
  'auth/me': authMe,
  'dashboard/counts': dashCounts,
  'dashboard/feed': dashFeed,
  'dashboard/metro-areas': dashMetros,
  'dashboard/my-assignments': dashAssign,
  'dashboard/stats': dashStats,
  'incidents': incidentsIndex,
  'contacts': contacts,
  'integrations': integrations,
  'alerts/notifications': alertsNotif,
  'migrate/columns': migrateColumns,
  'ingest/run': ingestRun,
  'ingest/waze': ingestWaze,
  'ingest/opendata': ingestOpendata,
  'ingest/scanner': ingestScanner,
  'ingest/news': ingestNews,
  'ingest/news-rss': ingestNewsRss,
  'ingest/state-crash': ingestStateCrash,
  'ingest/court': ingestCourt,
  'ingest/courtlistener': ingestCourtListener,
  'ingest/correlate': ingestCorrelate,
  'ingest/obituaries': ingestObituaries,
  'ingest/trauma': ingestTrauma,
  'ingest/reddit': ingestReddit,
  'ingest/police-social': ingestPoliceSocial,
  'ingest/pd-press': ingestPdPress,
  'ingest/fars': ingestFars,
  'enrich/run': enrichRun,
  'enrich/trigger': enrichTrigger,
  'enrich/people-search': enrichPeopleSearch,
  'enrich/trestle-test': enrichTrestleTest,
  'enrich/voter-rolls': enrichVoterRolls,
  'enrich/property-records': enrichPropertyRecords,
  'enrich/cross-exam':      enrichCrossExam,
  'enrich/social-search':   enrichSocialSearch,
  'enrich/fl-voter-loader': enrichFlVoterLoader,
  'enrich/crossref': enrichCrossref,
  'system/health': sysHealth,
  'system/postgis': sysPostgis,
  'system/qualify': sysQualify,
  'system/notify': sysNotify,
  'system/auto-assign': sysAutoAssign,
  'system/changelog': sysChangelog,
  'system/errors': sysErrors,
  'system/cost': sysCost,
  'system/setup': sysSetup,
  'system/digest': sysDigest,
  'system/smoke-test': sysSmoke,
  'system/triggers': sysTriggers,
  'system/audit': sysAudit,
  'system/resync': sysResync,
  'cron/dispatch': cronDispatch,
};

function tryDynamicIncident(slug, req) {
  if (slug[0] !== 'incidents' || slug.length < 2) return null;
  req.query = req.query || {};
  req.query.id = slug[1];
  const action = slug[2];
  if (!action) return incidentDetail;
  if (action === 'assign') return incidentAssign;
  if (action === 'note') return incidentNote;
  return null;
}

function getCallable(mod) {
  if (typeof mod === 'function') return mod;
  if (mod && typeof mod.default === 'function') return mod.default;
  if (mod && typeof mod.handler === 'function') return mod.handler;
  return null;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, PATCH, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Cron-Secret');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // Path comes from x-forwarded-uri (set by Vercel rewrite) or query param
  let slug = req.query?.slug || req.query?.path;
  if (!slug) {
    const fwd = req.headers['x-forwarded-uri'] || req.headers['x-original-uri'] || req.url || '';
    const path = fwd.split('?')[0];
    const m = path.match(/\/api\/v1\/(.+)$/);
    slug = m ? m[1] : '';
  }
  if (typeof slug === 'string') slug = slug.split('/');
  slug = (slug || []).filter(Boolean);

  if (slug.length === 0) {
    return res.status(200).json({
      ok: true, service: 'AIP', version: 1,
      routes: Object.keys(ROUTES).sort()
    });
  }

  const key = slug.join('/');
  try {
    if (ROUTES[key]) {
      const fn = getCallable(ROUTES[key]);
      if (!fn) return res.status(500).json({ error: `Handler ${key} has no callable export` });
      return fn(req, res);
    }
    const dyn = tryDynamicIncident(slug, req);
    if (dyn) {
      const fn = getCallable(dyn);
      return fn(req, res);
    }
    return res.status(404).json({ error: 'Not found', path: key });
  } catch (err) {
    console.error('Router error:', err);
    return res.status(500).json({ error: err.message, path: key });
  }
};
