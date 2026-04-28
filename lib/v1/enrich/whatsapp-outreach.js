/**
 * WhatsApp Business outreach via Twilio. WhatsApp Business API attached to our existing
 * Twilio number for Hispanic-market response rates. Same auth as twilio.js.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');

async function getCfg(db) {
  try {
    const row = await db('system_config').where({ key: 'twilio' }).first();
    if (row?.value) {
      const v = typeof row.value === 'string' ? JSON.parse(row.value) : row.value;
      return { sid: v.account_sid, token: v.auth_token, from: v.whatsapp_from || v.from_number };
    }
  } catch (_) {}
  return { sid: process.env.TWILIO_ACCOUNT_SID, token: process.env.TWILIO_AUTH_TOKEN, from: process.env.TWILIO_WHATSAPP_FROM || process.env.TWILIO_FROM_NUMBER };
}

async function sendWhatsApp(db, to, message) {
  const cfg = await getCfg(db);
  if (!cfg.sid || !cfg.token || !cfg.from) return { ok: false, error: 'twilio_not_configured' };
  const params = new URLSearchParams({
    From: cfg.from.startsWith('whatsapp:') ? cfg.from : `whatsapp:${cfg.from}`,
    To: to.startsWith('whatsapp:') ? to : `whatsapp:${to}`,
    Body: message
  });
  let body = null, ok = false;
  try {
    const r = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${cfg.sid}/Messages.json`, {
      method: 'POST',
      headers: { Authorization: `Basic ${Buffer.from(`${cfg.sid}:${cfg.token}`).toString('base64')}`, 'Content-Type': 'application/x-www-form-urlencoded' },
      body: params.toString(),
      timeout: 12000
    });
    if (r.ok) { body = await r.json(); ok = true; }
    else { body = await r.json(); }
  } catch (e) { return { ok: false, error: e.message }; }
  await trackApiCall(db, 'enrich-whatsapp', 'send', 0, 0, ok).catch(() => {});
  return { ok, sid: body?.sid, status: body?.status, error_code: body?.error_code, error_message: body?.error_message };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { action } = req.query || {};
    if (action === 'health') { const cfg = await getCfg(db); return res.json({ ok: true, engine: 'whatsapp-outreach', configured: !!(cfg.sid && cfg.token), from: cfg.from }); }
    if (req.method === 'POST') {
      const body = await new Promise(r => { let d = ''; req.on('data', c => d += c); req.on('end', () => { try { r(JSON.parse(d)); } catch (_) { r({}); } }); });
      const out = await sendWhatsApp(db, body.to, body.message);
      return res.json(out);
    }
    return res.status(400).json({ error: 'POST {to, message}' });
  } catch (err) { await reportError(db, 'whatsapp-outreach', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.sendWhatsApp = sendWhatsApp;
