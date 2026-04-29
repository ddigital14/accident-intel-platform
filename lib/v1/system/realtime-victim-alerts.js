/**
 * Realtime Victim Alerts (Phase 44A)
 *
 * Fires within ~60s of any new persons.victim_verified=true:
 *   - Slack webhook with rep call brief attached
 *   - SMS to Mason (+13308145683) when incidents.lead_score >= 75
 *   - Cooldown: don't re-alert same person within 24h
 *
 * Hooks into the cascade ACTION_HANDLERS so 'auto_resolve' triggers
 * also queue a 'realtime_alert' job.
 *
 * HTTP:
 *   GET /api/v1/system/realtime-victim-alerts?secret=ingest-now&action=health
 *   GET /api/v1/system/realtime-victim-alerts?secret=ingest-now&action=scan
 *   GET /api/v1/system/realtime-victim-alerts?secret=ingest-now&action=alert&person_id=<uuid>
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { trackApiCall } = require('./cost');

const SECRET = 'ingest-now';
const MASON_PHONE = '+13308145683';
const COOLDOWN_MS = 24 * 60 * 60 * 1000;
const SMS_MIN_SCORE = 75;

function authed(req) {
  const s = (req.query && req.query.secret) || (req.headers && req.headers['x-cron-secret']);
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function getCfg(db, key) {
  const envName = key.toUpperCase();
  if (process.env[envName]) return process.env[envName];
  try {
    const row = await db('system_config').where({ key }).first();
    if (row && row.value) return typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
  } catch (_) {}
  return null;
}

async function postSlack(webhook, text, blocks) {
  if (!webhook) return false;
  try {
    const r = await fetch(webhook, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text, blocks }),
      signal: AbortSignal.timeout(8000)
    });
    return r.ok;
  } catch (_) { return false; }
}

async function sendSms(db, to, body) {
  const sid = await getCfg(db, 'twilio_account_sid');
  const tok = await getCfg(db, 'twilio_auth_token');
  const from = await getCfg(db, 'twilio_from_number');
  if (!sid || !tok || !from || !to) return false;
  try {
    const auth = Buffer.from(`${sid}:${tok}`).toString('base64');
    const params = new URLSearchParams({ From: from, To: to, Body: body });
    const r = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${sid}/Messages.json`, {
      method: 'POST',
      headers: { 'Authorization': `Basic ${auth}`, 'Content-Type': 'application/x-www-form-urlencoded' },
      body: params.toString(),
      signal: AbortSignal.timeout(8000)
    });
    await trackApiCall(db, 'realtime-victim-alerts', 'twilio_sms', 0, 0, r.ok).catch(() => {});
    return r.ok;
  } catch (_) { return false; }
}

async function buildBrief(db, person, incident) {
  try {
    const brief = require('../dashboard/rep-call-brief');
    if (typeof brief.buildBrief === 'function') return await brief.buildBrief(db, person, incident);
  } catch (_) {}
  const lines = [];
  lines.push(`*${person.full_name || 'Unknown'}* - ${person.role || 'victim'} (verified)`);
  if (incident) {
    lines.push(`Incident: ${incident.severity || 'crash'} ${incident.incident_type || ''} in ${incident.city || ''} ${incident.state || ''} (score ${incident.lead_score == null ? '-' : incident.lead_score})`);
    if (incident.occurred_at) lines.push(`When: ${new Date(incident.occurred_at).toISOString()}`);
    if (incident.description) lines.push(`Detail: ${String(incident.description).substring(0, 200)}`);
  }
  const contact = [person.phone, person.email, person.address].filter(Boolean).join(' / ');
  if (contact) lines.push(`Contact: ${contact}`);
  return lines.join('\n');
}

function buildSlackBlocks(person, incident, brief) {
  return [
    { type: 'header', text: { type: 'plain_text', text: `Verified Victim - ${person.full_name || 'Unknown'}` } },
    { type: 'section', fields: [
      { type: 'mrkdwn', text: `*City:*\n${(incident && incident.city) || '-'} ${(incident && incident.state) || ''}` },
      { type: 'mrkdwn', text: `*Score:*\n${(incident && incident.lead_score) == null ? '-' : incident.lead_score}` },
      { type: 'mrkdwn', text: `*Severity:*\n${(incident && incident.severity) || '-'}` },
      { type: 'mrkdwn', text: `*Phone:*\n${person.phone || '-'}` }
    ]},
    { type: 'section', text: { type: 'mrkdwn', text: brief.substring(0, 2900) } },
    { type: 'actions', elements: [
      { type: 'button', text: { type: 'plain_text', text: 'Open in Dashboard' }, url: `https://accident-intel-platform.vercel.app/?focus=${person.id}` }
    ]}
  ];
}

async function alreadyAlerted(db, personId) {
  const since = new Date(Date.now() - COOLDOWN_MS);
  try {
    const row = await db('enrichment_logs').where('person_id', personId).where('field_name', 'realtime_alert').where('created_at', '>', since).first();
    return !!row;
  } catch (_) { return false; }
}

async function logAlert(db, personId, payload) {
  try {
    await db('enrichment_logs').insert({
      person_id: personId, field_name: 'realtime_alert', old_value: null,
      new_value: JSON.stringify(payload), source_url: 'internal://realtime-victim-alerts',
      source: 'realtime', confidence: 100, verified: true, created_at: new Date()
    });
  } catch (_) {}
}

async function alertOne(db, person) {
  if (!person || !person.id) return { ok: false, error: 'no_person' };
  if (!person.victim_verified) return { ok: false, error: 'not_verified' };
  if (await alreadyAlerted(db, person.id)) return { ok: true, skipped: 'cooldown' };

  let incident = null;
  if (person.incident_id) {
    try { incident = await db('incidents').where('id', person.incident_id).first(); } catch (_) {}
  }
  const brief = await buildBrief(db, person, incident);
  const slackText = `New verified victim: ${person.full_name || 'Unknown'} (${(incident && incident.city) || ''} ${(incident && incident.state) || ''}) score ${(incident && incident.lead_score) == null ? '-' : incident.lead_score}`;
  const blocks = buildSlackBlocks(person, incident, brief);

  const slackUrl = await getCfg(db, 'slack_webhook_url');
  const slackOk = await postSlack(slackUrl, slackText, blocks);

  let smsOk = false;
  if (Number(incident && incident.lead_score) >= SMS_MIN_SCORE) {
    const smsBody = `AIP: ${person.full_name || 'Verified victim'} ${(incident && incident.city) || ''} ${(incident && incident.state) || ''} score ${incident.lead_score}. ${person.phone || person.email || 'no contact yet'}`;
    smsOk = await sendSms(db, MASON_PHONE, smsBody.substring(0, 320));
  }

  await logAlert(db, person.id, { slack_sent: !!slackOk, sms_sent: !!smsOk, score: (incident && incident.lead_score) || null, alerted_at: new Date().toISOString() });
  return { ok: true, slack: !!slackOk, sms: !!smsOk, person_id: person.id, incident_id: person.incident_id };
}

async function scanAndAlert(db, opts = {}) {
  const limit = Math.min(Number(opts.limit) || 25, 50);
  const since = new Date(Date.now() - 10 * 60 * 1000);
  const stats = { candidates: 0, alerted: 0, skipped: 0, slack_sent: 0, sms_sent: 0, errors: [] };
  let rows;
  try {
    rows = await db('persons as p')
      .leftJoin('incidents as i', 'p.incident_id', 'i.id')
      .where('p.victim_verified', true)
      .where(function () { this.where('p.updated_at', '>', since).orWhere('p.created_at', '>', since); })
      .select('p.id', 'p.full_name', 'p.phone', 'p.email', 'p.address', 'p.role', 'p.incident_id', 'p.victim_verified', 'i.lead_score', 'i.city', 'i.state', 'i.severity', 'i.incident_type')
      .orderBy('p.updated_at', 'desc')
      .limit(limit);
  } catch (e) { return { ...stats, error: e.message }; }
  stats.candidates = rows.length;
  const start = Date.now();
  for (const p of rows) {
    if (Date.now() - start > 50000) break;
    try {
      const r = await alertOne(db, p);
      if (r.skipped) stats.skipped++;
      else if (r.ok) {
        stats.alerted++;
        if (r.slack) stats.slack_sent++;
        if (r.sms) stats.sms_sent++;
      }
    } catch (e) { stats.errors.push(`${p.id}: ${e.message}`); }
  }
  return stats;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  const db = getDb();
  const action = String((req.query && req.query.action) || 'scan').toLowerCase();

  if (action === 'health') {
    const slackUrl = await getCfg(db, 'slack_webhook_url');
    const sid = await getCfg(db, 'twilio_account_sid');
    return res.json({ success: true, pipeline: 'realtime-victim-alerts', slack_configured: !!slackUrl, twilio_configured: !!sid, sms_min_score: SMS_MIN_SCORE, cooldown_hours: COOLDOWN_MS / 3600000, mason_phone: MASON_PHONE, timestamp: new Date().toISOString() });
  }
  if (action === 'alert') {
    const personId = req.query.person_id;
    if (!personId) return res.status(400).json({ error: 'person_id required' });
    try {
      const person = await db('persons').where('id', personId).first();
      if (!person) return res.status(404).json({ error: 'person not found' });
      if (person.incident_id) {
        const inc = await db('incidents').where('id', person.incident_id).first();
        if (inc) {
          person.lead_score = inc.lead_score;
          person.city = person.city || inc.city;
          person.state = person.state || inc.state;
          person.severity = inc.severity;
          person.incident_type = inc.incident_type;
        }
      }
      const r = await alertOne(db, person);
      return res.json({ success: true, ...r, timestamp: new Date().toISOString() });
    } catch (e) {
      await reportError(db, 'realtime-victim-alerts', personId, e.message).catch(() => {});
      return res.status(500).json({ error: e.message });
    }
  }
  try {
    const stats = await scanAndAlert(db, { limit: req.query && req.query.limit });
    return res.json({ success: true, message: `realtime-alerts: ${stats.alerted} alerts (${stats.slack_sent} slack, ${stats.sms_sent} sms, ${stats.skipped} cooldown)`, ...stats, timestamp: new Date().toISOString() });
  } catch (e) {
    await reportError(db, 'realtime-victim-alerts', null, e.message).catch(() => {});
    return res.status(500).json({ error: e.message });
  }
};
module.exports.alertOne = alertOne;
module.exports.scanAndAlert = scanAndAlert;
module.exports.handler = module.exports;
