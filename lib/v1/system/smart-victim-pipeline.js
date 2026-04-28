/**
 * SMART VICTIM PIPELINE (Phase 38 Wave B composite)
 *
 * Orchestrates the full victim qualification chain in one call:
 *   1. victim-verifier batch  -> classify unverified persons
 *   2. victim-resolver batch  -> enrich verified victims (PDL/Apollo/etc)
 *   3. evidence-cross-checker -> validate conflicts across sources
 *   4. ensemble-qualifier     -> promote to qualified
 *
 * Returns aggregated stats per stage.
 *
 * GET /api/v1/system/smart-victim-pipeline?secret=ingest-now
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { trackApiCall } = require('./cost');

const SECRET = 'ingest-now';

const victimVerifier = require('../enrich/victim-verifier');
const victimResolver = require('../enrich/victim-resolver');
const evidenceCrossChecker = require('../enrich/evidence-cross-checker');
const ensembleQualifier = require('../enrich/ensemble-qualifier');

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

function callable(mod) {
  if (typeof mod === 'function') return mod;
  if (mod && typeof mod.handler === 'function') return mod.handler;
  if (mod && typeof mod.default === 'function') return mod.default;
  return null;
}

function makeFakeRes() {
  return {
    _statusCode: 200, _body: null, _headers: {},
    setHeader(k, v) { this._headers[k] = v; return this; },
    status(code) { this._statusCode = code; return this; },
    json(obj) { this._body = obj; return this; },
    end() { return this; }
  };
}

async function callStage(handlerMod, query, parentReq, label, timeoutMs = 50000) {
  const fn = callable(handlerMod);
  if (!fn) return { stage: label, status: 'fail', error: 'no_callable' };
  const fakeReq = {
    method: 'GET',
    query: { ...query },
    headers: { ...(parentReq?.headers || {}), 'x-internal-pipeline': '1' },
    body: null,
    url: ''
  };
  const fakeRes = makeFakeRes();
  const start = Date.now();
  try {
    await Promise.race([
      fn(fakeReq, fakeRes),
      new Promise((_, rej) => setTimeout(() => rej(new Error(`stage_timeout_${timeoutMs}ms`)), timeoutMs))
    ]);
    const body = fakeRes._body;
    return {
      stage: label,
      status: (fakeRes._statusCode === 200 && body?.success) ? 'pass' : 'fail',
      latency_ms: Date.now() - start,
      result: body
    };
  } catch (e) {
    return {
      stage: label,
      status: /timeout/i.test(e.message || '') ? 'timeout' : 'fail',
      latency_ms: Date.now() - start,
      error: e.message
    };
  }
}

async function runPipeline(req, opts = {}) {
  const stages = [];
  const verifyLimit = parseInt(opts.verify_limit || req.query?.verify_limit) || 25;
  const resolveLimit = parseInt(opts.resolve_limit || req.query?.resolve_limit) || 10;
  const crossLimit = parseInt(opts.cross_limit || req.query?.cross_limit) || 30;

  // Stage 1: verify
  stages.push(await callStage(
    victimVerifier,
    { secret: SECRET, action: 'batch', limit: String(verifyLimit) },
    req,
    'victim-verifier',
    50000
  ));

  // Stage 2: resolve
  stages.push(await callStage(
    victimResolver,
    { secret: SECRET, action: 'batch', limit: String(resolveLimit) },
    req,
    'victim-resolver',
    55000
  ));

  // Stage 3: evidence cross-check
  stages.push(await callStage(
    evidenceCrossChecker,
    { secret: SECRET, action: 'batch', limit: String(crossLimit) },
    req,
    'evidence-cross-checker',
    30000
  ));

  // Stage 4: qualify
  stages.push(await callStage(
    ensembleQualifier,
    { secret: SECRET, action: 'batch', limit: '50' },
    req,
    'ensemble-qualifier',
    30000
  ));

  // Aggregate
  const summary = { total_stages: stages.length, passed: 0, failed: 0, timed_out: 0 };
  for (const s of stages) {
    if (s.status === 'pass') summary.passed++;
    else if (s.status === 'timeout') summary.timed_out++;
    else summary.failed++;
  }

  // Pull headline numbers per stage
  const verifier = stages[0]?.result || {};
  const resolver = stages[1]?.result || {};
  const checker = stages[2]?.result || {};
  const qualifier = stages[3]?.result || {};
  summary.headline = {
    verified: verifier.accepted ?? null,
    rejected: verifier.rejected ?? null,
    resolved: resolver.resolved ?? null,
    fully_resolved: resolver.fully_resolved ?? null,
    fields_filled: resolver.fields_filled ?? null,
    cross_matches: checker.matches_total ?? null,
    cross_conflicts: checker.conflicts_total ?? null,
    qualified_promoted: qualifier.promoted ?? qualifier.evaluated ?? null
  };

  return { summary, stages };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  let db;
  try { db = getDb(); } catch (e) {
    return res.status(500).json({ success: false, error: 'db_unavailable:' + e.message });
  }

  const startAll = Date.now();
  try {
    const out = await runPipeline(req);
    await trackApiCall(db, 'smart-victim-pipeline', 'run', 0, 0, true).catch(() => {});
    return res.status(200).json({
      success: true,
      service: 'smart-victim-pipeline',
      total_latency_ms: Date.now() - startAll,
      ...out,
      ts: new Date().toISOString()
    });
  } catch (e) {
    await reportError(db, 'smart-victim-pipeline', null, e.message, { severity: 'error' });
    return res.status(500).json({ success: false, error: e.message });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.runPipeline = runPipeline;
