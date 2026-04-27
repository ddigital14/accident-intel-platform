/**
 * Twilio inbound SMS webhook
 *   POST /api/v1/webhooks/twilio-sms
 *
 * Wired into Twilio number's "A MESSAGE COMES IN" config.
 * When a rep replies to an alert SMS (e.g., "ENGAGE 1234" or just "OK"),
 * we mark the lead as engaged and re-cross-examine.
 *
 * Per AIP NEW ENGINE RULE: every inbound webhook event triggers the cascade.
 *
 * Twilio posts application/x-www-form-urlencoded:
 *   From, To, Body, MessageSid, NumMedia, MediaUrl0, MediaContentType0...
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

function twiml(message) {
  return `<?xml version="1.0" encoding="UTF-8"?><Response>${
    message ? `<Message>${message}</Message>` : ''
  }</Response>`;
}

async function findIncidentByCommand(db, body) {
  // Simple commands: "ENGAGE <id>" / "OK <id>" / "DECLINE <id>"
  const m = (body || '').trim().match(/^(ENGAGE|OK|YES|DECLINE|NO|STATUS)\s*([a-f0-9-]{36})?/i);
  if (!m) return null;
  const verb = m[1].toUpperCase();
  const id = m[2];
  return { verb, id };
}

async function findRepByPhone(db, fromPhone) {
  // Optional rep lookup — system_config.value.users[] or system_config.reps[]
  try {
    const row = await db('system_config').where('key', 'reps').first();
    if (!row || !row.value) return null;
    const v = typeof row.value === 'string' ? JSON.parse(row.value) : row.value;
    const list = Array.isArray(v) ? v : (v.reps || v.users || []);
    return list.find(r => r.phone === fromPhone || r.sms === fromPhone) || null;
  } catch (_) { return null; }
}

module.exports = async function handler(req, res) {
  // Twilio expects 200 + TwiML XML
  res.setHeader('Content-Type', 'text/xml');
  if (req.method !== 'POST') {
    return res.status(200).send(twiml());
  }
  const db = getDb();
  const From = req.body?.From || '';
  const Body = req.body?.Body || '';
  const MessageSid = req.body?.MessageSid || '';
  try {
    // Log every inbound message to system_changelog for audit
    try {
      await db('system_changelog').insert({
        kind: 'pipeline',
        title: 'Twilio inbound SMS',
        description: `From=${From} Body=${Body.slice(0, 200)}`,
        metadata: JSON.stringify({ MessageSid, From, body: Body }),
        created_at: new Date()
      });
    } catch (_) { /* table may not exist */ }

    const cmd = await findIncidentByCommand(db, Body);
    const rep = await findRepByPhone(db, From);

    // No command — just record the inbound, return polite reply
    if (!cmd) {
      return res.status(200).send(twiml('AIP received your message. Reply ENGAGE <id> to claim a lead, STATUS <id> for details, or DECLINE <id> to skip.'));
    }

    if (cmd.verb === 'STATUS' && cmd.id) {
      const inc = await db('incidents').where('id', cmd.id).first();
      if (!inc) return res.status(200).send(twiml(`No incident ${cmd.id}.`));
      return res.status(200).send(twiml(`${inc.incident_type} • ${inc.severity} • ${inc.address || inc.city} • score ${inc.lead_score} • ${inc.qualification_state}`));
    }

    if ((cmd.verb === 'ENGAGE' || cmd.verb === 'OK' || cmd.verb === 'YES') && cmd.id) {
      const inc = await db('incidents').where('id', cmd.id).first();
      if (!inc) return res.status(200).send(twiml(`No incident ${cmd.id}.`));
      // Mark engaged
      try {
        await db('incidents').where('id', cmd.id).update({
          engaged_by_rep: rep?.email || From,
          engaged_at: new Date(),
          updated_at: new Date()
        });
      } catch (_) { /* engaged_by_rep column may not exist; degrade gracefully */ }
      // Re-cross-exam every person on the incident
      try {
        const persons = await db('persons').where('incident_id', cmd.id);
        for (const p of persons) {
          await enqueueCascade(db, p.id, 'twilio_sms_reply').catch(() => {});
        }
      } catch (_) {}
      return res.status(200).send(twiml(`✓ Engaged ${cmd.id}. Lead is yours. Open dashboard for full detail.`));
    }

    if ((cmd.verb === 'DECLINE' || cmd.verb === 'NO') && cmd.id) {
      try {
        await db('incidents').where('id', cmd.id).update({ declined_count: db.raw('COALESCE(declined_count,0)+1'), updated_at: new Date() });
      } catch (_) {}
      return res.status(200).send(twiml(`Noted — ${cmd.id} declined. Will re-route.`));
    }

    return res.status(200).send(twiml());
  } catch (e) {
    await reportError(db, 'webhook-twilio-sms', e);
    return res.status(200).send(twiml());
  }
};
