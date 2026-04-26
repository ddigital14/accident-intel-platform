/**
 * GET /api/v1/system/resync?secret=ingest-now
 *
 * "Refresh Sync" — triggers a full enrichment pass against all named
 * persons missing contact info (ignores 6h cooldown), then re-runs
 * qualification + auto-assign so freshly-qualified leads appear.
 *
 * Public-facing endpoint behind the standard secret. Fires:
 *   1. enrich/trigger?force=true (Trestle, PDL, Tracerfy, SearchBug, NumVerify)
 *   2. enrich/people-search (TruePeopleSearch + FastPeopleSearch + Whitepages + Spokeo-free)
 *   3. system/qualify?force=all (recomputes all qualification states + scores)
 *   4. system/auto-assign (route newly-qualified leads to reps)
 *
 * Returns combined results from all 4 jobs.
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');

const enrichTrigger = require('../enrich/trigger');
const enrichPeopleSearch = require('../enrich/people-search');
const sysQualify = require('./qualify');
const sysAutoAssign = require('./auto-assign');

function makeFakeRes() {
  return {
    _statusCode: 200, _body: null, _headers: {},
    setHeader(k, v) { this._headers[k] = v; return this; },
    status(code) { this._statusCode = code; return this; },
    json(obj) { this._body = obj; return this; },
    end() { return this; },
  };
}

async function callJob(handler, query) {
  const fakeReq = { method: 'GET', query, headers: { 'x-internal': '1' }, body: null, url: '' };
  const fakeRes = makeFakeRes();
  const startT = Date.now();
  try {
    await Promise.race([
      handler(fakeReq, fakeRes),
      new Promise((_, rej) => setTimeout(() => rej(new Error('job timeout')), 50000))
    ]);
    return {
      success: !!fakeRes._body?.success,
      latency_ms: Date.now() - startT,
      message: fakeRes._body?.message,
      stats: fakeRes._body
    };
  } catch (e) {
    return { success: false, latency_ms: Date.now() - startT, error: e.message };
  }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const db = getDb();
  const startAll = Date.now();
  const results = {};

  try {
    // 1. Aggressive enrichment (force, bypass 6h cooldown)
    results.enrich_trigger = await callJob(enrichTrigger, { secret: 'ingest-now', force: 'true' });
    // 2. People-search cascade (TPS + FPS + Whitepages + Spokeo-free)
    results.people_search = await callJob(enrichPeopleSearch, { secret: 'ingest-now' });
    // 3. Re-qualify everything
    results.qualify = await callJob(sysQualify, { secret: 'ingest-now', force: 'all' });
    // 4. Auto-assign newly qualified
    results.auto_assign = await callJob(sysAutoAssign, { secret: 'ingest-now' });

    // Summary
    const passed = Object.values(results).filter(r => r.success).length;
    const fieldsFilled = results.enrich_trigger?.stats?.fields_filled || 0;
    const psMatches = results.people_search?.stats?.matches || 0;
    const promoted = results.qualify?.stats?.promoted || 0;
    const assigned = results.auto_assign?.stats?.assigned || 0;

    res.json({
      success: true,
      summary: `Resync: ${passed}/4 jobs OK, ${fieldsFilled} fields filled, ${psMatches} people-search matches, ${promoted} promoted, ${assigned} assigned`,
      total_latency_ms: Date.now() - startAll,
      results,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'resync', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
