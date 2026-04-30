/**
 * Phase 50: Error watchdog — Wave 12 auto-recovery learning.
 *
 * Scans system_errors for clusters in the last 10 min:
 *   - 3+ same-message errors in a single pipeline → schema-drift/API-outage flag
 *   - Posts to Slack + Resend email to Mason if pattern detected
 *   - Bumps CEI counter for 'error-watchdog'
 *
 * Borrows the CaseFlow learning: "error_inbox is clean (0 errors last 10 min)
 * — schema-drift fixes are working live."
 *
 * GET /api/v1/system/error-watchdog?secret=ingest-now&action=health
 * GET /api/v1/system/error-watchdog?secret=ingest-now&action=scan&minutes=10
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { bumpCounter } = require('./_cei_telemetry');

const ENGINE = 'error-watchdog';

async function getSlackWebhook(db) {
  try {
    const row = await db('system_config').where('key', 'slack').first();
    return row?.value?.webhook_url || row?.value?.alerts_webhook || null;
  } catch (_) { return null; }
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
    const resp = await fetch(webhook, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
      signal: AbortSignal.timeout(15000)
    });
    return resp.ok;
  } catch (_) { return false; }
}

async function emailMason(apiKey, subject, html) {
  if (!apiKey) return false;
  try {
    const resp = await fetch('https://api.resend.com/emails', {
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
    return resp.ok;
  } catch (_) { return false; }
}

async function scan(db, minutes = 10) {
  const t0 = Date.now();
  const win = Math.max(1, parseInt(minutes, 10) || 10);

  // Cluster errors: same pipeline + same first 80 chars of message
  const clusters = await db.raw(`
    SELECT pipeline,
           LEFT(message, 80) AS msg_prefix,
           COUNT(*)::int AS count,
           MAX(created_at) AS last_seen,
           MIN(created_at) AS first_seen
    FROM system_errors
    WHERE created_at > NOW() - INTERVAL '${win} minutes'
    GROUP BY pipeline, LEFT(message, 80)
    HAVING COUNT(*) >= 3
    ORDER BY count DESC
    LIMIT 25
  `).catch(() => ({ rows: [] }));

  const flagged = clusters.rows || [];
  const total_errors_q = await db.raw(`
    SELECT COUNT(*)::int AS c FROM system_errors WHERE created_at > NOW() - INTERVAL '${win} minutes'
  `).catch(() => ({ rows: [{ c: 0 }] }));
  const total_errors = total_errors_q.rows[0]?.c || 0;

  let alerted = 0;
  if (flagged.length > 0) {
    const slack = await getSlackWebhook(db);
    const resend = await getResendKey(db);

    const lines = flagged.slice(0, 8).map(f =>
      `• *${f.pipeline}* (${f.count}x): ${String(f.msg_prefix).replace(/\n/g, ' ')}`
    ).join('\n');
    const slackText = `:rotating_light: *AIP Error Watchdog* — ${flagged.length} cluster${flagged.length > 1 ? 's' : ''} flagged (last ${win}m)\n${lines}`;
    const html = `
      <div style="font-family:Inter,sans-serif;color:#0F172A;">
        <h2 style="color:#0F2A5A;margin:0 0 8px">ACC Error Watchdog Alert</h2>
        <p style="color:#64748B;margin:0 0 16px">${flagged.length} error cluster(s) flagged in the last ${win} minutes. Total errors window: ${total_errors}.</p>
        <table cellpadding="6" cellspacing="0" style="border-collapse:collapse;border:1px solid #E2E8F0;width:100%;font-size:13px;">
          <tr style="background:#F4F6FB;color:#0F2A5A;text-align:left;">
            <th>Pipeline</th><th>Count</th><th>Message Prefix</th>
          </tr>
          ${flagged.slice(0, 15).map(f => `<tr style="border-top:1px solid #E2E8F0">
            <td><b>${f.pipeline}</b></td>
            <td style="color:#DC2626;font-weight:600">${f.count}</td>
            <td>${String(f.msg_prefix).replace(/</g, '&lt;')}</td>
          </tr>`).join('')}
        </table>
        <p style="color:#64748B;font-size:12px;margin-top:16px">Auto-fix not attempted — schema-drift fixes are best done in code review.</p>
      </div>`;

    if (slack) { if (await postSlack(slack, slackText)) alerted++; }
    if (resend) { if (await emailMason(resend, `[ACC Watchdog] ${flagged.length} error cluster(s) detected`, html)) alerted++; }
  }

  await bumpCounter(db, ENGINE, true, Date.now() - t0).catch(() => {});

  return {
    window_minutes: win,
    total_errors_in_window: total_errors,
    clusters_flagged: flagged.length,
    alerts_dispatched: alerted,
    flagged_clusters: flagged.slice(0, 10),
    is_clean: flagged.length === 0,
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
  const action = req.query?.action || 'scan';

  try {
    if (action === 'health') {
      return res.json({
        success: true,
        engine: ENGINE,
        message: 'Error watchdog online — scans system_errors every 10min for clusters',
        timestamp: new Date().toISOString()
      });
    }
    const minutes = parseInt(req.query?.minutes || '10', 10);
    const out = await scan(db, minutes);
    return res.json({
      success: true,
      message: out.is_clean
        ? `error_inbox clean (0 clusters, ${out.total_errors_in_window} total errors last ${minutes}m)`
        : `${out.clusters_flagged} cluster(s) flagged, ${out.alerts_dispatched} alerts sent`,
      ...out
    });
  } catch (err) {
    await reportError(db, ENGINE, null, err.message).catch(() => {});
    return res.status(500).json({ error: err.message });
  }
};

module.exports.scan = scan;
