/**
 * Phase 34: Sentry SDK — reads DSN from system_config OR SENTRY_DSN env.
 * Initialized lazily on first capture call. Free tier: 5K errors/month.
 */
let sentryClient = null;
let dsnCache = null;
let dsnCacheAt = 0;

async function resolveDSN() {
  if (process.env.SENTRY_DSN) return process.env.SENTRY_DSN;
  if (dsnCache && Date.now() - dsnCacheAt < 60000) return dsnCache;
  try {
    const { getDb } = require('../../_db');
    const row = await getDb()('system_config').where({ key: 'sentry_dsn' }).first();
    if (row?.value) {
      const v = typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
      dsnCache = v; dsnCacheAt = Date.now();
      return v;
    }
  } catch (_) {}
  return null;
}

async function getSentry() {
  if (sentryClient && sentryClient !== 'init') return sentryClient;
  if (sentryClient === 'init') return null;
  sentryClient = 'init';
  const dsn = await resolveDSN();
  if (!dsn) { sentryClient = false; return false; }
  try {
    const Sentry = require('@sentry/node');
    Sentry.init({
      dsn,
      tracesSampleRate: parseFloat(process.env.SENTRY_TRACES || '0.1'),
      environment: process.env.VERCEL_ENV || 'development',
      release: (process.env.VERCEL_GIT_COMMIT_SHA || 'main').slice(0, 7)
    });
    sentryClient = Sentry;
    return Sentry;
  } catch (e) { sentryClient = false; return false; }
}

async function captureError(err, context = {}) {
  const s = await getSentry(); if (!s) return false;
  s.withScope((scope) => {
    Object.entries(context).forEach(([k, v]) => scope.setTag(k, v));
    s.captureException(err);
  });
  return true;
}

async function captureMessage(msg, level = 'info', context = {}) {
  const s = await getSentry(); if (!s) return false;
  s.withScope((scope) => {
    Object.entries(context).forEach(([k, v]) => scope.setTag(k, v));
    s.captureMessage(msg, level);
  });
  return true;
}

module.exports = async function handler(req, res) {
  if (req.query?.action === 'health') {
    const dsn = await resolveDSN();
    return res.json({ ok: true, engine: 'sentry', enabled: !!dsn, source: process.env.SENTRY_DSN ? 'env' : (dsn ? 'system_config' : 'none'), env: process.env.VERCEL_ENV });
  }
  if (req.query?.action === 'test') {
    const ok = await captureMessage('AIP Sentry test fired', 'info', { source: 'aip', verify: 'true' });
    return res.json({ sent: ok, message: ok ? 'Check Sentry inbox in 10-30s' : 'sentry not initialized' });
  }
  if (req.query?.action === 'test-error') {
    const ok = await captureError(new Error('AIP Sentry test error — ignore'), { source: 'aip', test: 'true' });
    return res.json({ sent: ok });
  }
  res.json({ hint: 'use action=health|test|test-error' });
};
module.exports.captureError = captureError;
module.exports.captureMessage = captureMessage;
module.exports.getSentry = getSentry;
