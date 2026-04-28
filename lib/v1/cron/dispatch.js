/**
 * GET /api/v1/cron/dispatch?secret=ingest-now&jobs=foo,bar,baz
 *
 * Calls handlers IN-PROCESS (not via HTTP) — avoids self-looping back to
 * the same Vercel router function, which caused timeouts/failures.
 *
 * Each job is fired in parallel with isolated try/catch.
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');

// Static handler imports — Vercel needs these for bundling
const ingestRun = require('../ingest/run');
const ingestWaze = require('../ingest/waze');
const ingestOpendata = require('../ingest/opendata');
const ingestScanner = require('../ingest/scanner');
const ingestNews = require('../ingest/news');
const ingestNewsRss = require('../ingest/news-rss');
const ingestReddit = require('../ingest/reddit');
const ingestPdPress = require('../ingest/pd-press');
const ingestPoliceSocial = require('../ingest/police-social');
const ingestStateCrash = require('../ingest/state-crash');
const ingestCourt = require('../ingest/court');
const ingestObituaries = require('../ingest/obituaries');
const ingestTrauma = require('../ingest/trauma');
const ingestCorrelate = require('../ingest/correlate');
const sysQualify = require('../system/qualify');
const sysNotify = require('../system/notify');
const sysAutoAssign = require('../system/auto-assign');
const enrichRun = require('../enrich/run');
const enrichTrigger = require('../enrich/trigger');
const enrichPeopleSearch = require('../enrich/people-search');
const enrichAddressToResidents = require('../enrich/address-to-residents');
const sysAudit = require('../system/audit');
const sysCascade = require('../system/cascade');
const sysTrestleProbe = require('../system/trestle-probe');
const enrichTwilio = require('../enrich/twilio');
const enrichSocialSearch = require('../enrich/social-search');
const enrichCrossExam = require('../enrich/cross-exam');
const enrichFamilyTree = require('../enrich/family-tree');
const enrichVehicleHistory = require('../enrich/vehicle-history');
const enrichRelativesSearch = require('../enrich/relatives-search');
const enrichTcpaCheck = require('../enrich/tcpa-litigator-check');
const sysDigest = require('../system/digest');
const sysErrors = require('../system/errors');
const sysBackfillNameless = require('../system/backfill-nameless');
const enrichCourtReverseLink = require('../enrich/court-reverse-link');
const enrichObitBackfill = require('../enrich/obit-backfill');
const claudeCrossReasoner = require('../enrich/claude-cross-reasoner');
const enrichSmartRouter = require('../enrich/_smart_router');
const enrichPdlByName = require('../enrich/pdl-by-name');
const claudeIdentityInvestigator = require('../enrich/claude-identity-investigator');
const sysConstantCrossLoop = require('../system/_constant_cross_loop');
const ingestHomegrownRotation = require('../ingest/_homegrown_rotation');

// Map: job-name → { handler, defaultQuery }
const JOB_HANDLERS = {
  'tomtom':         { handler: ingestRun,         query: { secret: 'ingest-now' } },
  'waze':           { handler: ingestWaze,        query: { secret: 'ingest-now' } },
  'opendata':       { handler: ingestOpendata,    query: { secret: 'ingest-now' } },
  'scanner':        { handler: ingestScanner,     query: { secret: 'ingest-now' } },
  'news':           { handler: ingestNews,        query: { secret: 'ingest-now' } },
  'news-rss':       { handler: ingestNewsRss,     query: { secret: 'ingest-now' } },
  'reddit':         { handler: ingestReddit,      query: { secret: 'ingest-now' } },
  'pd-press':       { handler: ingestPdPress,     query: { secret: 'ingest-now' } },
  'police-social':  { handler: ingestPoliceSocial,query: { secret: 'ingest-now' } },
  'state-crash':    { handler: ingestStateCrash,  query: { secret: 'ingest-now' } },
  'court':          { handler: ingestCourt,       query: { secret: 'ingest-now' } },
  'obituaries':     { handler: ingestObituaries,  query: { secret: 'ingest-now' } },
  'trauma':         { handler: ingestTrauma,      query: { secret: 'ingest-now' } },
  'correlate':      { handler: ingestCorrelate,   query: { secret: 'ingest-now' } },
  'qualify':        { handler: sysQualify,        query: { secret: 'ingest-now' } },
  'notify':         { handler: sysNotify,         query: { secret: 'ingest-now' } },
  'auto-assign':    { handler: sysAutoAssign,     query: { secret: 'ingest-now' } },
  'enrich':         { handler: enrichRun,         query: { secret: 'enrich-now' } },
  'enrich-trigger': { handler: enrichTrigger,     query: { secret: 'ingest-now' } },
  'people-search':  { handler: enrichPeopleSearch,query: { secret: 'ingest-now' } },
  'address-to-residents':{ handler: enrichAddressToResidents, query: { secret: 'ingest-now', action: 'batch', limit: '15' } },
  'cascade':        { handler: sysCascade,        query: { secret: 'ingest-now' } },
  'audit':          { handler: sysAudit,          query: { secret: 'ingest-now', fix: 'true' } },
  'digest':         { handler: sysDigest,         query: { secret: 'ingest-now', post: 'true' } },
  'trestle-probe':  { handler: sysTrestleProbe, query: { secret: 'ingest-now' } },
  'errors-clean':   { handler: sysErrors,         query: { secret: 'ingest-now', action: 'clear', days: '14' } },
  'twilio-lookup':  { handler: enrichTwilio,      query: { secret: 'ingest-now', action: 'enrich_pending', limit: '25' } },
  'social-search':  { handler: enrichSocialSearch,query: { secret: 'ingest-now', action: 'batch', limit: '15' } },
  'cross-exam':     { handler: enrichCrossExam,   query: { secret: 'ingest-now', action: 'examine_all' } },
  'family-tree':    { handler: enrichFamilyTree,    query: { secret: 'ingest-now', action: 'process', limit: '20' } },
  'vehicle-history':{ handler: enrichVehicleHistory,query: { secret: 'ingest-now', action: 'process', limit: '20' } },
  'relatives-search':{ handler: enrichRelativesSearch,query: { secret: 'ingest-now', action: 'process', limit: '20' } },
  'tcpa-refresh':   { handler: enrichTcpaCheck,     query: { secret: 'ingest-now', action: 'refresh_list' } },
  'claude-reason':  { handler: claudeCrossReasoner, query: { secret: 'ingest-now', action: 'top', limit: '15' } },
  'smart-router':   { handler: enrichSmartRouter, query: { secret: 'ingest-now', action: 'batch', limit: '15' } },
  'backfill-nameless': { handler: sysBackfillNameless, query: { secret: 'ingest-now', limit: '20' } },
  'court-reverse-link':{ handler: enrichCourtReverseLink, query: { secret: 'ingest-now', limit: '15' } },
  'obit-backfill':     { handler: enrichObitBackfill,    query: { secret: 'ingest-now', limit: '12' } },
  'pdl-by-name':       { handler: enrichPdlByName,       query: { secret: 'ingest-now', action: 'batch', limit: '20' } },
  'identity-investigator': { handler: claudeIdentityInvestigator, query: { secret: 'ingest-now', action: 'batch', limit: '10' } },
  'constant-cross-loop':   { handler: sysConstantCrossLoop,       query: { secret: 'ingest-now', minutes: '30' } },
  'homegrown-rotation':    { handler: ingestHomegrownRotation,    query: { secret: 'ingest-now' } },
};

// Build a fake res object that captures status + body
function makeFakeRes() {
  const fakeRes = {
    _statusCode: 200,
    _body: null,
    _headers: {},
    setHeader(k, v) { this._headers[k] = v; return this; },
    status(code) { this._statusCode = code; return this; },
    json(obj) { this._body = obj; return this; },
    end() { return this; },
  };
  return fakeRes;
}

function callableOf(mod) {
  if (typeof mod === 'function') return mod;
  if (mod && typeof mod.default === 'function') return mod.default;
  if (mod && typeof mod.handler === 'function') return mod.handler;
  return null;
}

async function runJob(jobName, parentReq) {
  const startT = Date.now();
  const entry = JOB_HANDLERS[jobName];
  if (!entry) {
    return { job: jobName, status: 'unknown_job', latency_ms: 0 };
  }
  try {
    const fn = callableOf(entry.handler);
    if (!fn) return { job: jobName, status: 'fail', error: 'no callable export', latency_ms: 0 };

    const fakeReq = {
      method: 'GET',
      query: { ...entry.query },
      headers: { ...(parentReq?.headers || {}), 'x-internal-cron': '1' },
      body: null,
      url: ''
    };
    const fakeRes = makeFakeRes();

    // Phase 25: per-job safety timeout. Default 25s; trivially-fast jobs get 8s.
    // Keeps a parallel batch under 30s so the 60s function envelope is safe.
    const FAST_JOBS = new Set(['notify','qualify','auto-assign','errors-clean']);
    const perJobTimeoutMs = FAST_JOBS.has(jobName) ? 8000 : 25000;
    await Promise.race([
      fn(fakeReq, fakeRes),
      new Promise((_, rej) => setTimeout(() => rej(new Error(`job timeout ${perJobTimeoutMs}ms`)), perJobTimeoutMs))
    ]);

    const latency = Date.now() - startT;
    const body = fakeRes._body;

    if (fakeRes._statusCode === 200 && body?.success) {
      return {
        job: jobName,
        status: 'pass',
        latency_ms: latency,
        message: body.message || 'OK',
        stats: extractStats(body)
      };
    }
    return {
      job: jobName,
      status: 'fail',
      latency_ms: latency,
      error: body?.error || `status ${fakeRes._statusCode}`
    };
  } catch (e) {
    const isTimeout = /job timeout/i.test(e.message || '');
    return {
      job: jobName,
      status: isTimeout ? 'timeout' : 'fail',
      latency_ms: Date.now() - startT,
      error: e.message
    };
  }
}

function extractStats(data) {
  const out = {};
  for (const k of [
    'inserted', 'corroborated', 'persons_added', 'incidents_created',
    'evaluated', 'promoted', 'enriched', 'matched', 'victims_extracted',
    'fields_filled', 'crashes_found', 'pds_polled', 'subs_polled',
    'accounts_polled', 'feeds_polled', 'candidates'
  ]) {
    if (data[k] !== undefined) out[k] = data[k];
  }
  return out;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();

  const jobsParam = (req.query?.jobs || '').toString();
  const requested = jobsParam.split(',').map(s => s.trim()).filter(Boolean);
  if (requested.length === 0) {
    return res.status(400).json({
      error: 'no jobs requested',
      registry: Object.keys(JOB_HANDLERS)
    });
  }

  const startAll = Date.now();

  // Run all in parallel — IN-PROCESS, not HTTP
  const results = await Promise.all(requested.map(j => runJob(j, req)));

  const passed = results.filter(r => r.status === 'pass').length;
  const failed = results.filter(r => r.status === 'fail').length;
  const timedout = results.filter(r => r.status === 'timeout').length;

  if (failed > 0) {
    await reportError(db, 'cron-dispatch', null,
      `${failed}/${results.length} jobs failed`,
      { failed: results.filter(r => r.status === 'fail') });
  }
  if (timedout > 0) {
    await reportError(db, 'cron-dispatch', null,
      `${timedout}/${results.length} jobs timed out`,
      { timeouts: results.filter(r => r.status === 'timeout').map(r => r.job) });
  }

  res.json({
    success: true,
    summary: `${passed}/${results.length} jobs passed (${failed} failed, ${timedout} timed out)`,
    timedout_count: timedout,
    requested,
    results,
    total_latency_ms: Date.now() - startAll,
    timestamp: new Date().toISOString()
  });
};
