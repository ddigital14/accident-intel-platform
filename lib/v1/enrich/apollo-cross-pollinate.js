/**
 * Phase 37 Wave B - Apollo Cross-Pollinate
 *
 * For pending/pending_named persons, hit Apollo's people/match endpoint to
 * attach email + phone + work history. Apollo's free tier returns title +
 * org without unlocking - useful even without a paid plan.
 *
 * POST https://api.apollo.io/v1/people/match
 *   Headers: Cache-Control: no-cache, Content-Type: application/json,
 *            X-Api-Key: <APOLLO_API_KEY>
 *   Body:    { first_name, last_name, city?, state?, reveal_personal_emails }
 *
 * Graceful no-op when APOLLO_API_KEY is unset.
 *
 * GET /api/v1/enrich/apollo-cross-pollinate?secret=ingest-now&action=batch&limit=N
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');
const { enqueueCascade } = require('../system/_cascade');

const APOLLO_BASE = 'https://api.apollo.io/v1/people/match';

function splitName(full) {
  const parts = String(full || '').trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return { first: '', last: '' };
  if (parts.length === 1) return { first: parts[0], last: '' };
  return { first: parts[0], last: parts[parts.length - 1] };
}

async function apolloMatch(person, key) {
  const { first, last } = splitName(person.full_name);
  if (!first || !last) return { ok: false, error: 'no_name_split' };
  const body = { first_name: first, last_name: last, reveal_personal_emails: false };
  if (person.city) body.city = person.city;
  if (person.state) body.state = person.state;
  try {
    const resp = await fetch(APOLLO_BASE, {
      method: 'POST',
      headers: { 'Cache-Control': 'no-cache', 'Content-Type': 'application/json', 'X-Api-Key': key },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(15000)
    });
    if (!resp.ok) {
      const t = await resp.text().catch(() => '');
      return { ok: false, error: `HTTP ${resp.status}`, body: t.substring(0, 200) };
    }
    const data = await resp.json().catch(() => null);
    return { ok: true, data };
  } catch (e) { return { ok: false, error: e.message }; }
}

function extractApolloFields(data) {
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
  return {
    email: p.email || null,
    phones: phones.filter(Boolean),
    title: p.title || null,
    organization: (p.organization && p.organization.name) || null,
    linkedin_url: p.linkedin_url || null,
    apollo_id: p.id || null,
    seniority: p.seniority || null
  };
}

async function enrichOne(db, person, key, results) {
  const r = await apolloMatch(person, key);
  await trackApiCall(db, 'apollo-cross-pollinate', 'apollo_match', 0, 0, r.ok).catch(() => {});
  results.matched++;
  if (!r.ok) { results.errors.push(`${person.full_name}: ${r.error}`); return false; }
  const fields = extractApolloFields(r.data);
  if (!fields) return false;
  results.matches++;
  const update = { updated_at: new Date() };
  if (!person.email && fields.email) update.email = fields.email;
  if (!person.phone && fields.phones[0]) update.phone = fields.phones[0];
  const meta = { apollo: { id: fields.apollo_id, title: fields.title, organization: fields.organization, linkedin: fields.linkedin_url, seniority: fields.seniority, phones_count: fields.phones.length, has_email: !!fields.email } };
  try {
    const existing = await db('persons').where('id', person.id).first('enrichment_data');
    let merged = meta;
    if (existing && existing.enrichment_data) {
      const prev = typeof existing.enrichment_data === 'string' ? JSON.parse(existing.enrichment_data) : existing.enrichment_data;
      merged = { ...(prev || {}), ...meta };
    }
    update.enrichment_data = JSON.stringify(merged);
  } catch (_) { update.enrichment_data = JSON.stringify(meta); }
  await db('persons').where('id', person.id).update(update);
  results.updated++;
  await db('enrichment_logs').insert({
    person_id: person.id,
    field_name: 'apollo_cross_pollinate',
    old_value: null,
    new_value: JSON.stringify(fields),
    source_url: APOLLO_BASE,
    source: 'apollo',
    confidence: 70,
    verified: false,
    created_at: new Date()
  }).catch(() => {});
  if (update.email || update.phone) {
    await enqueueCascade(db, { person_id: person.id, trigger_source: 'apollo-cross-pollinate', trigger_field: update.email ? 'email' : 'phone', trigger_value: update.email || update.phone, weight: 70 }).catch(() => {});
  }
  return true;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  const db = getDb();
  const limit = Math.min(Number(req.query.limit) || 10, 30);
  const results = { candidates: 0, matched: 0, matches: 0, updated: 0, errors: [], deferred: false };
  let key = process.env.APOLLO_API_KEY;
  if (!key) {
    try {
      const row = await db('system_config').where({ key: 'apollo_api_key' }).first();
      if (row && row.value) key = typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
    } catch (_) {}
  }
  if (!key) {
    results.deferred = true;
    return res.json({ success: true, message: 'apollo-cross-pollinate: APOLLO_API_KEY not set - graceful no-op', ...results, timestamp: new Date().toISOString() });
  }
  const start = Date.now();
  try {
    const candidates = await db('persons')
      .whereNotNull('full_name')
      .whereIn('qualification_state', ['pending', 'pending_named'])
      .where(function () { this.whereNull('phone').orWhereNull('email'); })
      .where('created_at', '>', new Date(Date.now() - 30 * 86400000))
      .select('id', 'full_name', 'city', 'state', 'phone', 'email')
      .orderBy('created_at', 'desc')
      .limit(limit);
    results.candidates = candidates.length;
    for (const p of candidates) {
      if (Date.now() - start > 50000) break;
      try { await enrichOne(db, p, key, results); }
      catch (e) { results.errors.push(`${p.full_name}: ${e.message}`); await reportError(db, 'apollo-cross-pollinate', p.id, e.message).catch(() => {}); }
    }
    res.json({ success: true, message: `apollo-cross-pollinate: ${results.matched} probed, ${results.matches} hits, ${results.updated} updated`, ...results, timestamp: new Date().toISOString() });
  } catch (err) {
    await reportError(db, 'apollo-cross-pollinate', null, err.message).catch(() => {});
    res.status(500).json({ error: err.message, results });
  }
};
