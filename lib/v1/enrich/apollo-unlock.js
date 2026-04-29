/**
 * Apollo /people/match - UNLOCK pass (Phase 44A)
 *
 * Spend 1 of Mason's monthly Apollo unlock credits to reveal hidden
 * email + phone for a person whose Apollo /people/match returned a hit
 * but no contact (paywall).
 *
 *   POST https://api.apollo.io/v1/people/match
 *     body: { first_name, last_name, organization_name?, city?, state?,
 *             reveal_personal_emails: true, reveal_phone_number: true }
 *     header: X-Api-Key: <APOLLO_API_KEY>
 *
 * Public:
 *   unlockPerson(db, person)  - direct call, returns { ok, fields, person_updated }
 *   handler                    - HTTP entry
 *
 * HTTP:
 *   GET /api/v1/enrich/apollo-unlock?secret=ingest-now&action=health
 *   GET /api/v1/enrich/apollo-unlock?secret=ingest-now&action=unlock&person_id=<uuid>
 *   GET /api/v1/enrich/apollo-unlock?secret=ingest-now&action=batch&limit=N
 *
 * Cost: tracked as 'apollo_unlock' (1 credit ~= $0.41 on $98 / 235 plan).
 * Cascade weight on success: 80.
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');
const { enqueueCascade } = require('../system/_cascade');

const APOLLO_URL = 'https://api.apollo.io/v1/people/match';
const HTTP_TIMEOUT_MS = 15000;
const SECRET = 'ingest-now';

function authed(req) {
  const s = (req.query && req.query.secret) || (req.headers && req.headers['x-cron-secret']);
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function getApolloKey(db) {
  if (process.env.APOLLO_API_KEY) return process.env.APOLLO_API_KEY;
  try {
    const row = await db('system_config').where({ key: 'apollo_api_key' }).first();
    if (row && row.value) return typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
  } catch (_) {}
  return null;
}

function splitName(full) {
  const parts = String(full || '').trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return { first: '', last: '' };
  if (parts.length === 1) return { first: parts[0], last: '' };
  return { first: parts[0], last: parts[parts.length - 1] };
}

async function callApolloUnlock(person, key) {
  const { first, last } = splitName(person.full_name);
  if (!first || !last) return { ok: false, error: 'no_name_split' };
  const body = { first_name: first, last_name: last, reveal_personal_emails: true, reveal_phone_number: true };
  if (person.employer) body.organization_name = person.employer;
  if (person.city) body.city = person.city;
  if (person.state) body.state = person.state;
  try {
    const resp = await fetch(APOLLO_URL, {
      method: 'POST',
      headers: { 'Cache-Control': 'no-cache', 'Content-Type': 'application/json', 'X-Api-Key': key },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS)
    });
    if (!resp.ok) {
      const t = await resp.text().catch(() => '');
      return { ok: false, error: `HTTP ${resp.status}`, body: t.substring(0, 200) };
    }
    const data = await resp.json().catch(() => null);
    return { ok: true, data };
  } catch (e) { return { ok: false, error: e.message }; }
}

function extractRevealed(data) {
  const p = data && data.person;
  if (!p) return null;
  const phones = [];
  if (Array.isArray(p.phone_numbers)) {
    for (const ph of p.phone_numbers) {
      const num = ph.sanitized_number || ph.raw_number || ph.number;
      if (num) phones.push(num);
    }
  }
  if (p.mobile_phone) phones.push(p.mobile_phone);
  if (p.direct_dial) phones.push(p.direct_dial);
  return {
    email: p.email || null,
    personal_emails: Array.isArray(p.personal_emails) ? p.personal_emails : [],
    phones: [...new Set(phones.filter(Boolean))],
    title: p.title || null,
    organization: (p.organization && p.organization.name) || null,
    linkedin_url: p.linkedin_url || null,
    apollo_id: p.id || null
  };
}

async function unlockPerson(db, person) {
  const key = await getApolloKey(db);
  if (!key) return { ok: false, error: 'no_apollo_key', deferred: true };
  if (!person || !person.id) return { ok: false, error: 'no_person' };
  if (!person.full_name) return { ok: false, error: 'no_full_name' };
  const r = await callApolloUnlock(person, key);
  await trackApiCall(db, 'apollo-unlock', 'apollo_unlock', 0, 0, r.ok).catch(() => {});
  if (!r.ok) return { ok: false, error: r.error };
  const fields = extractRevealed(r.data);
  if (!fields || (!fields.email && fields.phones.length === 0 && (fields.personal_emails || []).length === 0)) {
    return { ok: true, fields: fields || {}, person_updated: false, reason: 'no_revealed_contact' };
  }
  const update = { updated_at: new Date() };
  let touchedField = null, touchedValue = null;
  if (!person.email && (fields.email || fields.personal_emails[0])) {
    update.email = fields.email || fields.personal_emails[0];
    touchedField = 'email'; touchedValue = update.email;
  }
  if (!person.phone && fields.phones[0]) {
    update.phone = fields.phones[0];
    if (!touchedField) { touchedField = 'phone'; touchedValue = update.phone; }
  }
  try {
    const existing = await db('persons').where('id', person.id).first('enrichment_data');
    let prev = {};
    if (existing && existing.enrichment_data) {
      prev = typeof existing.enrichment_data === 'string' ? JSON.parse(existing.enrichment_data) : existing.enrichment_data;
    }
    const merged = { ...(prev || {}), apollo_unlock: {
      id: fields.apollo_id, title: fields.title, organization: fields.organization, linkedin: fields.linkedin_url,
      email_revealed: !!fields.email, personal_emails_count: fields.personal_emails.length,
      phones_count: fields.phones.length, unlocked_at: new Date().toISOString()
    }};
    update.enrichment_data = JSON.stringify(merged);
  } catch (_) {}
  let person_updated = false;
  if (Object.keys(update).length > 1) {
    try {
      await db('persons').where('id', person.id).update(update);
      person_updated = true;
    } catch (e) {
      await reportError(db, 'apollo-unlock', person.id, `update failed: ${e.message}`).catch(() => {});
    }
  }
  try {
    await db('enrichment_logs').insert({
      person_id: person.id, field_name: 'apollo_unlock', old_value: null,
      new_value: JSON.stringify(fields), source_url: APOLLO_URL, source: 'apollo',
      confidence: 80, verified: false, created_at: new Date()
    });
  } catch (_) {}
  if (person_updated && touchedField) {
    await enqueueCascade(db, {
      person_id: person.id, trigger_source: 'apollo-unlock',
      trigger_field: touchedField, trigger_value: touchedValue, weight: 80, priority: 7
    }).catch(() => {});
  }
  return { ok: true, fields, person_updated, touched: touchedField };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  const db = getDb();
  const action = String((req.query && req.query.action) || 'health').toLowerCase();

  if (action === 'health') {
    const key = await getApolloKey(db);
    return res.json({ success: true, pipeline: 'apollo-unlock', key_set: !!key, url: APOLLO_URL, cascade_weight: 80, timestamp: new Date().toISOString() });
  }
  if (action === 'unlock') {
    const personId = req.query.person_id;
    if (!personId) return res.status(400).json({ error: 'person_id required' });
    try {
      const person = await db('persons').where('id', personId).first();
      if (!person) return res.status(404).json({ error: 'person not found' });
      const r = await unlockPerson(db, person);
      return res.json({ success: true, person_id: personId, ...r, timestamp: new Date().toISOString() });
    } catch (e) {
      await reportError(db, 'apollo-unlock', personId, e.message).catch(() => {});
      return res.status(500).json({ error: e.message });
    }
  }
  if (action === 'batch') {
    const limit = Math.min(Number(req.query.limit) || 5, 20);
    const stats = { candidates: 0, attempted: 0, unlocked: 0, errors: [] };
    try {
      const candidates = await db('persons')
        .whereNotNull('full_name')
        .where(function () { this.whereNull('phone').orWhereNull('email'); })
        .where('victim_verified', true)
        .where('created_at', '>', new Date(Date.now() - 30 * 86400000))
        .orderBy('created_at', 'desc')
        .limit(limit);
      stats.candidates = candidates.length;
      const start = Date.now();
      for (const p of candidates) {
        if (Date.now() - start > 50000) break;
        stats.attempted++;
        try {
          const r = await unlockPerson(db, p);
          if (r.ok && r.person_updated) stats.unlocked++;
        } catch (e) { stats.errors.push(`${p.full_name}: ${e.message}`); }
      }
      return res.json({ success: true, message: `apollo-unlock: ${stats.unlocked}/${stats.attempted} unlocked`, ...stats, timestamp: new Date().toISOString() });
    } catch (e) {
      await reportError(db, 'apollo-unlock', null, e.message).catch(() => {});
      return res.status(500).json({ error: e.message, ...stats });
    }
  }
  return res.status(400).json({ error: 'unknown action', supported: ['health', 'unlock', 'batch'] });
};
module.exports.unlockPerson = unlockPerson;
module.exports.handler = module.exports;
