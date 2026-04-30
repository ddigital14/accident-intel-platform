/**
 * Phase 51 — Wave 12 pattern: cron staleness watchdog.
 *
 * Reads system_api_calls grouped by pipeline. If any expected cron job's
 * last successful call is > threshold (default 60 min) old, post Slack +
 * email Mason via Resend.
 *
 * Endpoints:
 *   GET ?action=health  — quick stats
 *   GET ?action=scan    — scan + alert if stale (default action)
 *   GET ?action=stale   — list stale jobs only, no alert
 *
 * Designed to run every 15 min via cron-staleness-check job.
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { bumpCounter } = require('./_cei_telemetry');
const { trackApiCall } = require('./cost');

const ENGINE = 'cron-staleness';
const DEFAULT_STALE_MIN = 60;

// Pipelines we expect to log to system_api_calls regularly. Mirrors
// JOB_HANDLERS keys + frequencies in vercel.json. If you add a new cron
// job, drop it here so we get an alert when it stops logging.
const EXPECTED_JOBS = [
  // Frequent (every 1-15 min)
  { pipeline: 'system-refresh-mv',           max_min: 15 },
  { pipeline: 'system-cei-poll',             max_min: 15 },
  { pipeline: 'enrich-predictive-at-source', max_min: 30 },
  { pipeline: 'ingest-pulsepoint',           max_min: 30 },
  { pipeline: 'ingest-tomtom',               max_min: 30 },
  { pipeline: 'ingest-waze',                 max_min: 30 },
  { pipeline: 'ingest-opendata',             max_min: 30 },
  { pipeline: 'ingest-scanner',              max_min: 60 },
  { pipeline: 'system-qualify',              max_min: 30 },
  { pipeline: 'system-notify',               max_min: 30 },
  { pipeline: 'system-auto-assign',          max_min: 30 },
  { pipeline: 'system-cascade',              max_min: 30 },
  { pipeline: 'enrich-cross-exam',           max_min: 30 },
  { pipeline: 'enrich-claude-cross-reasoner',max_min: 60 },
  { pipeline: 'system-error-watchdog',       max_min: 30 },
  // Less frequent (30-60 min)
  { pipeline: 'ingest-news',                 max_min: 90 },
  { pipeline: 'ingest-news-rss',             max_min: 90 },
  { pipeline: 'ingest-reddit',               max_min: 120 },
  { pipeline: 'ingest-obituaries',           max_min: 90 },
  { pipeline: 'ingest-pd-press',             max_min: 90 },
  { pipeline: 'ingest-trauma',               max_min: 180 },
  // Phase 51
  { pipeline: 'embedding-queue',             max_min: 30 }
];

async function getSlackWebhook(db) {
  try {
    const row = await db('system_config').where('key', 'slack').first();
    return row?.value?.webhook_url || row?.value?.alerts_webhook || process.env.SLACK_WEBHOOK_URL || null;
  } catch (_) { return process.env.SLACK_WEBHOOK_URL || null; }
}

async function getResendKey(db) {
  try {
    const row = await db('system_config').where('key', 'resend').first();
    return row?.value?.api_key || process.env.RESEND_API_KEY || null;
  } catch (_) { return process.env.RESEND_API_KEY || null; }
}

async function postSlack(webhook, text) {
  if (!webhook) return false;
  try {
    const r = await fetch(webhook, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
      signal: AbortSignal.timeout(15000)
    });
    return r.ok;
  } catch (_) { return false; }
}

async function emailMason(apiKey, subject, html) {
  if (!apiKey) return false;
  try {
    const r = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from: 'AIP Watchdog <alerts@accidentcommandcenter.com>',
        to: ['donovan@donovandigitalsolutions.com'],
        subject,
        html
      }),
      signal: AbortSignal.timeout(15000)
    });
    return r.ok;
  } catch (_) { return false; }
}

async function lastRunsByPipeline(db) {
  // Most recent successful API-call row per pipeline.
  const rows = await db.raw(`
    SELECT pipeline, MAX(created_at) AS last_run
    FROM system_api_calls
    WHERE success = TRUE
    GROUP BY pipeline
  `).then(r => r.rows || r || []).catch(() => []);
  const map = {};
  for (const r of rows) map[r.pipeline] = r.last_run;
  return map;
}

async function findStale(db, defaultMin = DEFAULT_STALE_MIN) {
  const map = await lastRunsByPipeline(db);
  const now = Date.now();
  const stale = [];
  for (const job of EXPECTED_JOBS) {
    const last = map[job.pipeline];
    const lim  = job.max_min || defaultMin;
    if (!last) {
      stale.push({ pipeline: job.pipeline, last_run: null, age_min: null, max_min: lim, reason: 'never_logged' });
      continue;
    }
    const ageMin = Math.round((now - new Date(last).getTime()) / 60000);
    if (ageMin > lim) {
      stale.push({ pipeline: job.pipeline, last_run: last, age_min: ageMin, max_min: lim, reason: 'stale' });
    }
  }
  return stale;
}

async function scanAndAlert(db, opts = {}) {
  const t0 = Date.now();
  const stale = await findStale(db, opts.minutes || DEFAULT_STALE_MIN);

  // Suppress noise: only alert if at least 1 stale job.
  let slackPosted = false, emailSent = false;
  if (stale.length > 0) {
    const webhook = await getSlackWebhook(db);
    const apiKey  = await getResendKey(db);

    const lines = stale.slice(0, 25).map(s => {
      if (s.reason === 'never_logged') return `* \`${s.pipeline}\` — never logged a successful run`;
      return `* \`${s.pipeline}\` — last run ${s.age_min} min ago (limit ${s.max_min} min)`;
    });
    const slackText = `:warning: *AIP cron staleness alert* — ${stale.length} job(s) stale\n${lines.join('\n')}`;
    const html = `<h3>AIP cron staleness alert</h3>
<p>${stale.length} expected cron job(s) have not logged a successful run within their threshold:</p>
<ul>${stale.slice(0, 25).map(s => s.reason === 'never_logged'
  ? `<li><code>${s.pipeline}</code> — never logged a successful run</li>`
  : `<li><code>${s.pipeline}</code> — last run ${s.age_min} min ago (limit ${s.max_min} min)</li>`).join('')}</ul>
<p style="color:#64748B;font-size:12px">Triggered ${new Date().toISOString()} by cron-staleness watchdog.</p>`;

    slackPosted = await postSlack(webhook, slackText);
    emailSent   = await emailMason(apiKey, `AIP cron staleness — ${stale.length} stale job(s)`, html);
  }

  const ms = Date.now() - t0;
  await trackApiCall(db, ENGINE, 'scan', 0, 0, true).catch(() => {});
  await bumpCounter(db, ENGINE, true, ms).catch(() => {});
  return {
    success: true,
    engine: ENGINE,
    expected_jobs: EXPECTED_JOBS.length,
    stale_count: stale.length,
    stale,
    slack_posted: slackPosted,
    email_sent: emailSent,
    ms,
    timestamp: new Date().toISOString()
  };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const action = (req.query && req.query.action) || 'scan';
  const minutes = parseInt(req.query?.minutes, 10) || DEFAULT_STALE_MIN;

  try {
    if (action === 'health') {
      const map = await lastRunsByPipeline(db);
      return res.json({ success: true, engine: ENGINE, pipelines_logging: Object.keys(map).length, expected: EXPECTED_JOBS.length, timestamp: new Date().toISOString() });
    }
    if (action === 'stale') {
      const stale = await findStale(db, minutes);
      return res.json({ success: true, engine: ENGINE, expected_jobs: EXPECTED_JOBS.length, stale_count: stale.length, stale, timestamp: new Date().toISOString() });
    }
    if (action === 'scan') {
      const out = await scanAndAlert(db, { minutes });
      return res.json(out);
    }
    return res.status(400).json({ error: 'unknown action', supported: ['health', 'stale', 'scan'] });
  } catch (err) {
    await reportError(db, ENGINE, null, err.message).catch(() => {});
    await bumpCounter(db, ENGINE, false).catch(() => {});
    return res.status(500).json({ error: err.message });
  }
};

module.exports.findStale = findStale;
module.exports.scanAndAlert = scanAndAlert;
module.exports.EXPECTED_JOBS = EXPECTED_JOBS;
