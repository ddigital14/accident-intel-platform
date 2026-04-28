/**
 * Phase 34: Sentry SDK scaffold. Activates when SENTRY_DSN env var is set.
 * Free tier: 5K errors/month. Mason creates account at sentry.io and pastes DSN.
 */
let sentryClient = null;
function getSentry() {
  if (sentryClient !== null) return sentryClient;
  if (!process.env.SENTRY_DSN) { sentryClient = false; return false; }
  try {
    const Sentry = require('@sentry/node');
    Sentry.init({
      dsn: process.env.SENTRY_DSN,
      tracesSampleRate: parseFloat(process.env.SENTRY_TRACES || '0.1'),
      environment: process.env.VERCEL_ENV || 'development',
      release: process.env.VERCEL_GIT_COMMIT_SHA || 'unknown'
    });
    sentryClient = Sentry;
    return Sentry;
  } catch (_) { sentryClient = false; return false; }
}

function captureError(err, context = {}) {
  const s = getSentry(); if (!s) return false;
  s.withScope((scope) => {
    Object.entries(context).forEach(([k, v]) => scope.setTag(k, v));
    s.captureException(err);
  });
  return true;
}

function captureMessage(msg, level = 'info', context = {}) {
  const s = getSentry(); if (!s) return false;
  s.withScope((scope) => {
    Object.entries(context).forEach(([k, v]) => scope.setTag(k, v));
    s.captureMessage(msg, level);
  });
  return true;
}

module.exports = async function handler(req, res) {
  const enabled = !!process.env.SENTRY_DSN;
  if (req.query?.action === 'health') return res.json({ ok: true, engine: 'sentry', enabled, env: process.env.VERCEL_ENV });
  if (req.query?.action === 'test' && enabled) { captureMessage('AIP Sentry test from /system/sentry', 'info', { source: 'aip' }); return res.json({ sent: true }); }
  res.json({ enabled, hint: enabled ? 'set' : 'POST SENTRY_DSN to /api/v1/system/setup' });
};
module.exports.captureError = captureError;
module.exports.captureMessage = captureMessage;
module.exports.getSentry = getSentry;
