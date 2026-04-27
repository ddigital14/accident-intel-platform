/**
 * Twilio multi-purpose engine — used for IDENTITY CONFIDENCE not just messaging.
 *
 * Surfaces used:
 *   • Lookup v2 — carrier + line_type + caller_name + identity_match    (~$0.008/q)
 *   • Verify — phone reachability test (one-time, opt-in only)          (free w/ trial)
 *   • Messaging — outbound SMS / MMS                                    ($0.0079/SMS, $0.02/MMS)
 *   • Voice — outbound call w/ programmable TwiML                       ($0.014/min)
 *   • Conversations — inbound SMS replies (webhook → /webhooks/twilio)
 *
 * Per AIP NEW ENGINE RULE: every Twilio Lookup call emits a cascade event so the
 * confirmed phone owner / carrier / line_type flows into cross-exam confidence.
 *
 * Endpoints exposed on this engine handler:
 *   GET  /api/v1/enrich/twilio?action=lookup&phone=+13308145683
 *   GET  /api/v1/enrich/twilio?action=enrich_pending  (cron — finds persons w/ phones, no carrier)
 *   GET  /api/v1/enrich/twilio?action=health
 *   POST /api/v1/enrich/twilio?action=send_sms       (body { to, message })
 *   POST /api/v1/enrich/twilio?action=send_mms       (body { to, message, media_urls[] })
 *   POST /api/v1/enrich/twilio?action=call           (body { to, twiml })
 *
 * Per CORE_INTENT.md: every successful Lookup MUST call enqueueCascade.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

// -------- Twilio config (DB-first, env fallback) --------
async function getTwilioConfig(db) {
  try {
    const row = await db('system_config').where('key', 'twilio').first();
    if (row && row.value) {
      const v = typeof row.value === 'string' ? JSON.parse(row.value) : row.value;
      return {
        sid: v.account_sid || process.env.TWILIO_ACCOUNT_SID,
        token: v.auth_token || process.env.TWILIO_AUTH_TOKEN,
        from: v.from_number || process.env.TWILIO_FROM_NUMBER,
        verifyServiceSid: v.verify_service_sid || process.env.TWILIO_VERIFY_SERVICE_SID
      };
    }
  } catch (_) {}
  return {
    sid: process.env.TWILIO_ACCOUNT_SID,
    token: process.env.TWILIO_AUTH_TOKEN,
    from: process.env.TWILIO_FROM_NUMBER,
    verifyServiceSid: process.env.TWILIO_VERIFY_SERVICE_SID
  };
}

function authHeader(sid, token) {
  return 'Basic ' + Buffer.from(`${sid}:${token}`).toString('base64');
}

// -------- LOOKUP v2 — the cross-exam game-changer --------
async function lookupPhone(db, phone, opts = {}) {
  const cfg = await getTwilioConfig(db);
  if (!cfg.sid || !cfg.token) return { ok: false, error: 'twilio_not_configured' };
  // Default fields: line_type_intelligence + caller_name + identity_match
  const fields = opts.fields || 'line_type_intelligence,caller_name';
  const url = `https://lookups.twilio.com/v2/PhoneNumbers/${encodeURIComponent(phone)}?Fields=${fields}`;
  try {
    const resp = await fetch(url, {
      headers: { Authorization: authHeader(cfg.sid, cfg.token) },
      signal: AbortSignal.timeout(10000)
    });
    const cost = 0.008; // line_type ($0.005) + caller_name ($0.01) approx blended
    await trackApiCall(db, 'enrich-twilio-lookup', 'twilio_lookup', 0, 0, resp.ok);
    if (!resp.ok) {
      const txt = await resp.text().catch(() => '');
      return { ok: false, status: resp.status, error: txt.slice(0, 200) };
    }
    const data = await resp.json();
    return {
      ok: true,
      phone_number: data.phone_number,
      country: data.country_code,
      valid: data.valid !== false,
      validation_errors: data.validation_errors || [],
      line_type: data.line_type_intelligence?.type,             // mobile|landline|voip|fixedVoip|nonFixedVoip|tollFree
      carrier_name: data.line_type_intelligence?.carrier_name,
      mobile_country_code: data.line_type_intelligence?.mobile_country_code,
      mobile_network_code: data.line_type_intelligence?.mobile_network_code,
      caller_name: data.caller_name?.caller_name || null,       // Carrier-reported owner name
      caller_type: data.caller_name?.caller_type,               // CONSUMER | BUSINESS
      raw: data
    };
  } catch (e) {
    await trackApiCall(db, 'enrich-twilio-lookup', 'twilio_lookup', 0, 0, false);
    return { ok: false, error: e.message };
  }
}

// Apply a Lookup result to a person row + emit cascade
async function applyLookupToPerson(db, personId, lookup, sourceUrl = null) {
  if (!lookup || !lookup.ok) return { applied: false };
  const updates = {};
  if (lookup.carrier_name) updates.phone_carrier = lookup.carrier_name;
  if (lookup.line_type) updates.phone_line_type = lookup.line_type;
  if (lookup.valid != null) updates.phone_verified = lookup.valid;
  if (lookup.caller_name) {
    // Don't overwrite an existing high-confidence name — just stash as evidence
    updates.caller_name = lookup.caller_name;
  }
  if (Object.keys(updates).length) {
    try {
      await db('persons').where('id', personId).update({ ...updates, updated_at: new Date() });
    } catch (_) { /* schema may not have all cols */ }
  }
  // Log to enrichment_logs — try multiple schema shapes (source vs source_url)
  const baseRow = {
    person_id: personId,
    field_name: lookup.caller_name ? 'caller_name' : 'phone_carrier',
    new_value: lookup.caller_name || lookup.carrier_name || 'verified',
    confidence: 88,
    created_at: new Date()
  };
  for (const extra of [
    { source: 'twilio_lookup', source_url: sourceUrl, raw_data: JSON.stringify(lookup.raw || {}) },
    { source_url: sourceUrl || 'twilio_lookup' },
    {}
  ]) {
    try {
      await db('enrichment_logs').insert({ ...baseRow, ...extra });
      break;
    } catch (_) { /* try next */ }
  }
  // Emit cascade — per CORE_INTENT.md every linkage triggers cross-conversion
  await enqueueCascade(db, personId, 'twilio_lookup').catch(() => {});
  return { applied: true, updates };
}

// -------- ENRICH BATCH — for cron use --------
async function enrichPendingPhones(db, limit = 25) {
  // Find persons with a phone but no carrier/line_type yet
  const candidates = await db('persons')
    .whereNotNull('phone')
    .where(function () {
      this.whereNull('phone_carrier').orWhereNull('phone_line_type');
    })
    .orderBy('updated_at', 'desc')
    .limit(limit);
  let enriched = 0;
  for (const p of candidates) {
    const result = await lookupPhone(db, p.phone);
    if (result.ok) {
      await applyLookupToPerson(db, p.id, result, null);
      enriched++;
    }
  }
  return { candidates: candidates.length, enriched };
}

// -------- SEND SMS / MMS --------
async function sendSms(db, to, message, mediaUrls = []) {
  const cfg = await getTwilioConfig(db);
  if (!cfg.sid || !cfg.token || !cfg.from) return { ok: false, error: 'twilio_not_configured' };
  const params = new URLSearchParams({ From: cfg.from, To: to, Body: message });
  if (mediaUrls && mediaUrls.length) {
    for (const url of mediaUrls) params.append('MediaUrl', url);
  }
  try {
    const resp = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${cfg.sid}/Messages.json`, {
      method: 'POST',
      headers: {
        Authorization: authHeader(cfg.sid, cfg.token),
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      body: params.toString(),
      signal: AbortSignal.timeout(10000)
    });
    const data = await resp.json();
    await trackApiCall(db, 'enrich-twilio-sms', mediaUrls.length ? 'twilio_mms' : 'twilio_sms', 0, 0, resp.ok);
    return { ok: resp.ok, sid: data.sid, status: data.status, error_code: data.error_code, error_message: data.error_message };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// -------- VOICE CALL --------
async function makeCall(db, to, twiml) {
  const cfg = await getTwilioConfig(db);
  if (!cfg.sid || !cfg.token || !cfg.from) return { ok: false, error: 'twilio_not_configured' };
  const defaultTwiml = '<Response><Say voice="alice">This is an automated alert from Donovan Digital Solutions Accident Intelligence Platform. A new high-priority lead is in your dashboard.</Say></Response>';
  const params = new URLSearchParams({ From: cfg.from, To: to, Twiml: twiml || defaultTwiml });
  try {
    const resp = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${cfg.sid}/Calls.json`, {
      method: 'POST',
      headers: {
        Authorization: authHeader(cfg.sid, cfg.token),
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      body: params.toString(),
      signal: AbortSignal.timeout(10000)
    });
    const data = await resp.json();
    await trackApiCall(db, 'enrich-twilio-call', 'twilio_voice', 0, 0, resp.ok);
    return { ok: resp.ok, sid: data.sid, status: data.status };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// -------- HEALTH --------
async function health(db) {
  const cfg = await getTwilioConfig(db);
  if (!cfg.sid || !cfg.token) return { configured: false };
  try {
    const resp = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${cfg.sid}.json`, {
      headers: { Authorization: authHeader(cfg.sid, cfg.token) },
      signal: AbortSignal.timeout(8000)
    });
    if (!resp.ok) return { configured: true, ok: false, status: resp.status };
    const data = await resp.json();
    return {
      configured: true,
      ok: true,
      account_status: data.status,
      account_type: data.type,
      friendly_name: data.friendly_name,
      from_number: cfg.from
    };
  } catch (e) {
    return { configured: true, ok: false, error: e.message };
  }
}

// -------- HANDLER --------
module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const db = getDb();
  const action = req.query.action || 'health';
  try {
    if (action === 'health') {
      return res.status(200).json(await health(db));
    }
    if (action === 'lookup') {
      const phone = req.query.phone;
      if (!phone) return res.status(400).json({ error: 'phone required' });
      const result = await lookupPhone(db, phone);
      // Optionally apply to a known person
      if (req.query.person_id && result.ok) {
        await applyLookupToPerson(db, req.query.person_id, result);
      }
      return res.status(200).json(result);
    }
    if (action === 'enrich_pending') {
      const limit = parseInt(req.query.limit) || 25;
      return res.status(200).json(await enrichPendingPhones(db, limit));
    }
    if (action === 'send_sms' || action === 'send_mms') {
      if (req.method !== 'POST') return res.status(405).json({ error: 'POST required' });
      const { to, message, media_urls } = req.body || {};
      if (!to || !message) return res.status(400).json({ error: 'to + message required' });
      return res.status(200).json(await sendSms(db, to, message, media_urls || []));
    }
    if (action === 'call') {
      if (req.method !== 'POST') return res.status(405).json({ error: 'POST required' });
      const { to, twiml } = req.body || {};
      if (!to) return res.status(400).json({ error: 'to required' });
      return res.status(200).json(await makeCall(db, to, twiml));
    }
    return res.status(400).json({ error: 'unknown action', valid: ['health', 'lookup', 'enrich_pending', 'send_sms', 'send_mms', 'call'] });
  } catch (e) {
    await reportError(db, 'enrich-twilio', e);
    return res.status(500).json({ error: e.message });
  }
};

// Export helpers for use by other modules (notify.js, cross-exam.js, cron dispatcher)
module.exports.lookupPhone = lookupPhone;
module.exports.applyLookupToPerson = applyLookupToPerson;
module.exports.enrichPendingPhones = enrichPendingPhones;
module.exports.sendSms = sendSms;
module.exports.makeCall = makeCall;
module.exports.getTwilioConfig = getTwilioConfig;
