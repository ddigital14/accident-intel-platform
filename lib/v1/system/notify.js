/**
 * GET /api/v1/system/notify?secret=ingest-now
 *
 * Sends Slack + (optional) Twilio SMS alerts for newly-qualified high-value leads.
 *
 * Triggers when:
 *   incident.qualification_state = 'qualified'
 *   AND notified_at IS NULL
 *   AND lead_score >= NOTIFY_MIN_SCORE (default 70)
 *
 * ENV:
 *   SLACK_WEBHOOK_URL — Slack incoming webhook (any channel)
 *   TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER
 *   ALERT_SMS_TO — fallback SMS recipient if no rep is assigned
 *   NOTIFY_MIN_SCORE — minimum lead_score to trigger (default 70)
 *
 * Cron: every 5 minutes (right after qualify)
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');

const SLACK_WEBHOOK = process.env.SLACK_WEBHOOK_URL;
const TWILIO_SID = process.env.TWILIO_ACCOUNT_SID;
const TWILIO_TOKEN = process.env.TWILIO_AUTH_TOKEN;
const TWILIO_FROM = process.env.TWILIO_FROM_NUMBER;
const ALERT_SMS_TO = process.env.ALERT_SMS_TO;
const MIN_SCORE = parseInt(process.env.NOTIFY_MIN_SCORE) || 70;

async function postSlack(text, blocks) {
  if (!SLACK_WEBHOOK) return false;
  try {
    const resp = await fetch(SLACK_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text, blocks }),
      signal: AbortSignal.timeout(8000)
    });
    return resp.ok;
  } catch (_) { return false; }
}

async function sendSMS(to, body) {
  if (!TWILIO_SID || !TWILIO_TOKEN || !TWILIO_FROM || !to) return false;
  try {
    const auth = Buffer.from(`${TWILIO_SID}:${TWILIO_TOKEN}`).toString('base64');
    const params = new URLSearchParams({ From: TWILIO_FROM, To: to, Body: body });
    const resp = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${TWILIO_SID}/Messages.json`, {
      method: 'POST',
      headers: { 'Authorization': `Basic ${auth}`, 'Content-Type': 'application/x-www-form-urlencoded' },
      body: params.toString(),
      signal: AbortSignal.timeout(8000)
    });
    return resp.ok;
  } catch (_) { return false; }
}

function buildSlackBlocks(inc, persons) {
  const personLines = (persons || []).slice(0, 3).map(p => {
    const contact = [p.phone, p.email, p.address].filter(Boolean).join(' • ');
    return `• *${p.full_name || (p.first_name + ' ' + p.last_name)}* — ${contact}`;
  }).join('\n') || '_no contact details parsed yet_';

  return [
    {
      type: 'header',
      text: { type: 'plain_text', text: `🚨 New Qualified Lead — Score ${inc.lead_score}` }
    },
    {
      type: 'section',
      fields: [
        { type: 'mrkdwn', text: `*Type:*\n${inc.incident_type}` },
        { type: 'mrkdwn', text: `*Severity:*\n${inc.severity}` },
        { type: 'mrkdwn', text: `*Location:*\n${inc.address || `${inc.city}, ${inc.state}`}` },
        { type: 'mrkdwn', text: `*Sources:*\n${inc.source_count}× confirmed` }
      ]
    },
    { type: 'section', text: { type: 'mrkdwn', text: `*People:*\n${personLines}` } },
    { type: 'section', text: { type: 'mrkdwn', text: `<https://accident-intel-platform.vercel.app/?incident=${inc.id}|Open in dashboard>` } }
  ];
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const db = getDb();
  const results = { candidates: 0, slack_sent: 0, sms_sent: 0, skipped: 0, errors: [] };

  try {
    // Find newly-qualified, unnotified, high-score leads
    const minScore = parseInt(req.query.min_score) || MIN_SCORE;
    const candidates = await db('incidents')
      .where('qualification_state', 'qualified')
      .whereNull('notified_at')
      .where('lead_score', '>=', minScore)
      .where('qualified_at', '>', new Date(Date.now() - 6 * 3600000)) // last 6h only
      .select('*')
      .orderBy('lead_score', 'desc')
      .limit(20);

    results.candidates = candidates.length;

    for (const inc of candidates) {
      try {
        const persons = await db('persons').where('incident_id', inc.id).select('*');
        const blocks = buildSlackBlocks(inc, persons);
        const text = `New qualified lead: ${inc.incident_type} in ${inc.city}, ${inc.state} — score ${inc.lead_score}`;

        const slackOK = await postSlack(text, blocks);
        if (slackOK) results.slack_sent++;

        // SMS to assigned rep, or fallback
        let smsTo = null;
        if (inc.assigned_to) {
          const rep = await db('users').where('id', inc.assigned_to).first();
          if (rep?.phone) smsTo = rep.phone;
        }
        if (!smsTo) smsTo = ALERT_SMS_TO;
        if (smsTo) {
          const smsOK = await sendSMS(smsTo, `AIP: New ${inc.severity} ${inc.incident_type} in ${inc.city} ${inc.state}. Score ${inc.lead_score}. ${persons[0]?.full_name || ''} ${persons[0]?.phone || persons[0]?.email || ''}`);
          if (smsOK) results.sms_sent++;
        }

        await db('incidents').where('id', inc.id).update({
          notified_at: new Date(),
          updated_at: new Date()
        });
      } catch (e) {
        results.errors.push(`${inc.id}: ${e.message}`);
        await reportError(db, 'notify', inc.id, e.message);
      }
    }

    res.json({
      success: true,
      message: `Notify: ${results.candidates} candidates, ${results.slack_sent} Slack, ${results.sms_sent} SMS`,
      ...results,
      slack_configured: !!SLACK_WEBHOOK,
      twilio_configured: !!(TWILIO_SID && TWILIO_TOKEN && TWILIO_FROM),
      min_score: minScore,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'notify', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
