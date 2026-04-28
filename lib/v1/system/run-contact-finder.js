/**
 * RUN-CONTACT-FINDER — Phase 39 composite pipeline.
 * Sequentially runs the full victim-rescue chain in one HTTP call.
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { trackApiCall } = require('./cost');

const verifier = require('../enrich/victim-verifier');
const finder   = require('../enrich/victim-contact-finder');
const xcheck   = require('../enrich/evidence-cross-checker');
const refreshMv = require('./refresh-mv');

function makeFakeRes() {
  return {
    _statusCode: 200, _body: null, _headers: {},
    setHeader(k, v) { this._headers[k] = v; return this; },
    status(code) { this._statusCode = code; return this; },
    json(obj) { this._body = obj; return this; },
    end() { return this; }
  };
}
function callable(mod) {
  if (typeof mod === 'function') return mod;
  if (mod && typeof mod.handler === 'function') return mod.handler;
  if (mod && typeof mod.default === 'function') return mod.default;
  return null;
}
async function runStep(label, mod, query, timeoutMs = 25000) {
  const start = Date.now();
  try {
    const fn = callable(mod);
    if (!fn) return { step: label, ok: false, error: 'no_callable_export', latency_ms: 0 };
    const fakeReq = { method: 'GET', query: { secret: 'ingest-now', ...query }, headers: {}, body: null, url: '' };
    const fakeRes = makeFakeRes();
    await Promise.race([
      fn(fakeReq, fakeRes),
      new Promise((_, rej) => setTimeout(() => rej(new Error('step_timeout_' + timeoutMs)), timeoutMs))
    ]);
    return {
      step: label,
      ok: fakeRes._statusCode === 200 && fakeRes._body?.success !== false,
      status: fakeRes._statusCode,
      latency_ms: Date.now() - start,
      summary: extractSummary(fakeRes._body)
    };
  } catch (e) {
    return { step: label, ok: false, error: e.message, latency_ms: Date.now() - start };
  }
}
function extractSummary(b) {
  if (!b || typeof b !== 'object') return null;
  const keys = ['candidates', 'resolved', 'fully_resolved', 'fields_filled', 'sources_succeeded',
    'verified', 'denied', 'unsure', 'examined', 'updated', 'flagged', 'mv_refreshed', 'count'];
  const out = {};
  for (const k of keys) if (b[k] !== undefined) out[k] = b[k];
  if (Array.isArray(b.samples) && b.samples.length) out.samples = b.samples.slice(0, 4);
  return Object.keys(out).length ? out : null;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }

  const verifierLimit = parseInt(req.query?.verifier_limit || '20', 10);
  const finderLimit = parseInt(req.query?.finder_limit || '10', 10);
  const crossLimit = parseInt(req.query?.cross_limit || '30', 10);

  let db;
  try { db = getDb(); } catch (e) {
    return res.status(500).json({ success: false, error: 'db_unavailable:' + e.message });
  }

  const startedAt = Date.now();
  const steps = [];

  steps.push(await runStep('verifier_batch', verifier, { action: 'batch', limit: String(verifierLimit) }));
  steps.push(await runStep('contact_finder_batch', finder, { action: 'batch', limit: String(finderLimit) }, 35000));
  steps.push(await runStep('cross_checker', xcheck, { action: 'batch', limit: String(crossLimit) }));
  steps.push(await runStep('refresh_mv', refreshMv, {}, 10000));

  const finderSummary = steps.find(s => s.step === 'contact_finder_batch')?.summary || {};
  const verifierSummary = steps.find(s => s.step === 'verifier_batch')?.summary || {};
  const passed = steps.filter(s => s.ok).length;
  const totalLatency = Date.now() - startedAt;

  let buckets = null;
  try {
    const rows = await db.raw(`
      SELECT
        SUM(CASE WHEN phone IS NOT NULL AND email IS NOT NULL AND address IS NOT NULL THEN 1 ELSE 0 END) AS complete,
        SUM(CASE WHEN (phone IS NOT NULL OR email IS NOT NULL OR address IS NOT NULL) AND NOT (phone IS NOT NULL AND email IS NOT NULL AND address IS NOT NULL) THEN 1 ELSE 0 END) AS partial,
        SUM(CASE WHEN phone IS NULL AND email IS NULL AND address IS NULL THEN 1 ELSE 0 END) AS none,
        COUNT(*) AS total_verified
      FROM persons
      WHERE COALESCE(victim_verified, false) = true
    `).then(r => (r.rows ? r.rows[0] : (r[0] || null)));
    if (rows) {
      buckets = {
        complete_contacts: parseInt(rows.complete || 0, 10),
        partial_contacts: parseInt(rows.partial || 0, 10),
        no_contacts: parseInt(rows.none || 0, 10),
        total_verified: parseInt(rows.total_verified || 0, 10)
      };
    }
  } catch (e) {
    await reportError(db, 'run-contact-finder', null, 'bucket_rollup:' + e.message).catch(() => {});
  }

  await trackApiCall(db, 'system-run-contact-finder', 'composite', 0, 0, passed === steps.length).catch(() => {});

  res.status(200).json({
    success: true,
    summary: passed + '/' + steps.length + ' steps passed (' + totalLatency + 'ms)',
    steps,
    aggregates: {
      contact_finder: finderSummary,
      verifier: verifierSummary,
      buckets
    },
    total_latency_ms: totalLatency,
    timestamp: new Date().toISOString()
  });
};
module.exports.handler = module.exports;
