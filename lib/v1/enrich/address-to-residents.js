/**
 * Address → Residents fallback chain (Trestle Reverse Address alternative).
 *
 * Runs while Trestle Reverse Address access is pending. Goes through every
 * free + cheap-paid alternative we have to convert an address into a resident
 * name + (sometimes) phone.
 *
 * Order (cheapest → most expensive):
 *   1. county property records (6 counties wired, FREE for owner_name)
 *   2. TruePeopleSearch reverse-address page (FREE web scrape)
 *   3. FastPeopleSearch reverse-address page (FREE web scrape)
 *   4. WhitePages reverse-address page (FREE — __NEXT_DATA__ extraction)
 *   5. SearchBug address API ($0.05/q)
 *   6. PDL Person Search location filter ($0.02/match)
 *
 * Stops at first usable hit. Emits cascade. Tracks cost.
 *
 * Endpoints:
 *   GET /api/v1/enrich/address-to-residents?street=...&city=...&state=...&person_id=...
 *   GET /api/v1/enrich/address-to-residents?action=batch&limit=20  (cron)
 *   GET /api/v1/enrich/address-to-residents?action=health
 *
 * When Trestle Reverse Address goes from pending→enabled, the smart router
 * will prefer that (90 weight) over this chain. Until then, this fills the gap.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

// Tier 1: property records — already implemented in property-records.js
async function tryPropertyRecords(db, { street, city, state }) {
  try {
    const pr = require('./property-records');
    if (!pr.lookupOwner) return null;
    const r = await pr.lookupOwner({ address: street, city, state });
    if (r?.owner_name) {
      return { source: 'property_records', cost: 0, full_name: r.owner_name, parcel_id: r.parcel_id, raw: r };
    }
  } catch (_) {}
  return null;
}

// Tier 2-4: web-scrape free people search reverse-address pages
const SCRAPE_USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

async function fetchHtml(url) {
  try {
    const r = await fetch(url, {
      headers: { 'User-Agent': SCRAPE_USER_AGENT, 'Accept': 'text/html', 'Accept-Language': 'en-US,en;q=0.9' },
      signal: AbortSignal.timeout(8000)
    });
    if (!r.ok) return null;
    return await r.text();
  } catch (_) { return null; }
}

function extractNameFromHtml(html) {
  if (!html) return null;
  // Simple: look for typical person card patterns. Many of these sites use schema.org or h2/h3 elements.
  // Try schema first
  const schema = html.match(/"name"\s*:\s*"([^"]{3,80})"/i);
  if (schema && schema[1] && /\b[A-Z][a-z]+\s+[A-Z][a-z]+/.test(schema[1])) return schema[1];
  // Fallback: first h2/h3 with two capitalized words
  const h = html.match(/<(?:h[1-3]|a[^>]*class="[^"]*name[^"]*")[^>]*>\s*([A-Z][a-z]+(?:\s+[A-Z]\.)?\s+[A-Z][a-z]+(?:[\s-][A-Z][a-z]+)?)/);
  if (h && h[1]) return h[1];
  return null;
}

async function tryTruePeopleSearch(db, { street, city, state }) {
  const url = `https://www.truepeoplesearch.com/results?streetaddress=${encodeURIComponent(street)}&citystatezip=${encodeURIComponent((city || '') + ', ' + (state || ''))}`;
  const html = await fetchHtml(url);
  await trackApiCall(db, 'enrich-address-to-residents', 'truepeoplesearch', 0, 0, !!html).catch(() => {});
  const name = extractNameFromHtml(html);
  return name ? { source: 'truepeoplesearch', cost: 0, full_name: name, source_url: url } : null;
}

async function tryFastPeopleSearch(db, { street, city, state }) {
  const url = `https://www.fastpeoplesearch.com/address/${encodeURIComponent(street.toLowerCase().replace(/\s+/g, '-'))}_${encodeURIComponent((city || '').toLowerCase().replace(/\s+/g, '-'))}-${(state || '').toLowerCase()}`;
  const html = await fetchHtml(url);
  await trackApiCall(db, 'enrich-address-to-residents', 'fastpeoplesearch', 0, 0, !!html).catch(() => {});
  const name = extractNameFromHtml(html);
  return name ? { source: 'fastpeoplesearch', cost: 0, full_name: name, source_url: url } : null;
}

async function tryWhitePages(db, { street, city, state }) {
  const url = `https://www.whitepages.com/address/${encodeURIComponent(street.toLowerCase().replace(/\s+/g, '-'))}/${encodeURIComponent((city || '').toLowerCase().replace(/\s+/g, '-'))}-${(state || '').toLowerCase()}`;
  const html = await fetchHtml(url);
  await trackApiCall(db, 'enrich-address-to-residents', 'whitepages', 0, 0, !!html).catch(() => {});
  if (!html) return null;
  // WhitePages uses __NEXT_DATA__ — try to extract from there first
  const nextMatch = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
  if (nextMatch) {
    try {
      const data = JSON.parse(nextMatch[1]);
      const pageProps = data?.props?.pageProps;
      const personObj = pageProps?.address?.current_residents?.[0] || pageProps?.persons?.[0];
      const name = personObj?.full_name || personObj?.name || (personObj?.first_name && personObj?.last_name ? `${personObj.first_name} ${personObj.last_name}` : null);
      if (name) return { source: 'whitepages', cost: 0, full_name: name, source_url: url };
    } catch (_) {}
  }
  const name = extractNameFromHtml(html);
  return name ? { source: 'whitepages', cost: 0, full_name: name, source_url: url } : null;
}

async function trySearchBug(db, { street, city, state }) {
  const key = process.env.SEARCHBUG_API_KEY;
  const co = process.env.SEARCHBUG_CO_CODE;
  if (!key || !co) return null;
  try {
    const url = `https://api.searchbug.com/api.aspx?TYPE=peoplesearch&CO_CODE=${co}&PASS=${key}&ADDRESS=${encodeURIComponent(street)}&CITY=${encodeURIComponent(city || '')}&STATE=${encodeURIComponent(state || '')}&FORMAT=json`;
    const r = await fetch(url, { signal: AbortSignal.timeout(10000) });
    const data = await r.json().catch(() => null);
    await trackApiCall(db, 'enrich-address-to-residents', 'searchbug', 0, 0, r.ok).catch(() => {});
    const rec = data?.results?.[0] || data?.[0] || null;
    if (rec?.full_name || rec?.first_name) {
      const full = rec.full_name || `${rec.first_name || ''} ${rec.last_name || ''}`.trim();
      return { source: 'searchbug', cost: 0.05, full_name: full, phone: rec.phone || rec.phone_number || null, raw: rec };
    }
  } catch (_) {}
  return null;
}

async function tryPDL(db, { street, city, state }) {
  const key = process.env.PDL_API_KEY;
  if (!key) return null;
  try {
    const sql = `SELECT * FROM person WHERE location_locality='${(city || '').replace(/'/g, "''")}' AND location_region='${(state || '').replace(/'/g, "''")}' AND location_street_address ILIKE '%${(street || '').replace(/'/g, "''")}%' LIMIT 1`;
    const r = await fetch(`https://api.peopledatalabs.com/v5/person/search?sql=${encodeURIComponent(sql)}&size=1`, {
      headers: { 'X-Api-Key': key },
      signal: AbortSignal.timeout(10000)
    });
    const data = await r.json().catch(() => null);
    await trackApiCall(db, 'enrich-address-to-residents', 'pdl', 0, 0, r.ok).catch(() => {});
    const hit = data?.data?.[0];
    if (hit?.full_name) {
      return {
        source: 'pdl_by_address',
        cost: 0.02,
        full_name: hit.full_name,
        first_name: hit.first_name,
        last_name: hit.last_name,
        phone: (hit.phone_numbers || [])[0],
        email: hit.work_email || (hit.personal_emails || [])[0],
        employer: hit.job_company_name,
        linkedin_url: hit.linkedin_url,
        raw: hit
      };
    }
  } catch (_) {}
  return null;
}

const TIERS = [
  { name: 'property_records', fn: tryPropertyRecords, cost: 0 },
  { name: 'truepeoplesearch', fn: tryTruePeopleSearch, cost: 0 },
  { name: 'fastpeoplesearch', fn: tryFastPeopleSearch, cost: 0 },
  { name: 'whitepages',       fn: tryWhitePages,       cost: 0 },
  { name: 'searchbug',        fn: trySearchBug,        cost: 0.05 },
  { name: 'pdl',              fn: tryPDL,              cost: 0.02 }
];

async function lookupResidents(db, addr) {
  for (const tier of TIERS) {
    try {
      const r = await tier.fn(db, addr);
      if (r && r.full_name) return r;
    } catch (_) {}
  }
  return null;
}

async function applyToPerson(db, personId, result) {
  if (!result || !result.full_name) return false;
  const updates = { updated_at: new Date() };
  const p = await db('persons').where('id', personId).first().catch(() => null);
  if (!p) return false;
  if (!p.full_name) updates.full_name = result.full_name;
  if (!p.first_name && result.first_name) updates.first_name = result.first_name;
  else if (!p.first_name && result.full_name) updates.first_name = result.full_name.split(' ')[0];
  if (!p.last_name && result.last_name) updates.last_name = result.last_name;
  else if (!p.last_name && result.full_name) updates.last_name = result.full_name.split(' ').slice(-1)[0];
  if (!p.phone && result.phone) updates.phone = result.phone;
  if (!p.email && result.email) updates.email = result.email;
  if (!p.employer && result.employer) updates.employer = result.employer;
  if (!p.linkedin_url && result.linkedin_url) updates.linkedin_url = result.linkedin_url;
  try { await db('persons').where('id', personId).update(updates); } catch (_) { return false; }
  // Log to enrichment_logs
  try {
    await db('enrichment_logs').insert({
      person_id: personId,
      field_name: 'full_name',
      new_value: result.full_name.substring(0, 200),
      source: result.source,
      source_url: result.source_url || null,
      created_at: new Date()
    });
  } catch (_) {}
  await enqueueCascade(db, personId, 'address_to_residents').catch(() => {});
  return true;
}

async function batchEnrich(db, limit = 20) {
  let candidates = [];
  try {
    candidates = await db('persons')
      .whereNotNull('location_street_address').where('location_street_address', '!=', '')
      .where(function () { this.whereNull('full_name').orWhere('full_name', ''); })
      .orderBy('updated_at', 'desc')
      .limit(limit);
  } catch (e) {
    try {
      candidates = await db('persons')
        .whereNotNull('address').where('address', '!=', '')
        .where(function () { this.whereNull('full_name').orWhere('full_name', ''); })
        .orderBy('updated_at', 'desc')
        .limit(limit);
    } catch (_) { candidates = []; }
  }
  let enriched = 0;
  const sourceCounts = {};
  for (const p of candidates) {
    const street = p.location_street_address || (p.address || '').split(',')[0];
    const city = p.location_locality || p.city;
    const state = p.location_region || p.state;
    if (!street || !city || !state) continue;
    const r = await lookupResidents(db, { street, city, state });
    if (r) {
      const ok = await applyToPerson(db, p.id, r);
      if (ok) {
        enriched++;
        sourceCounts[r.source] = (sourceCounts[r.source] || 0) + 1;
      }
    }
  }
  return { candidates: candidates.length, enriched, by_source: sourceCounts };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const db = getDb();
  const action = req.query.action;
  try {
    if (action === 'health') {
      return res.json({ ok: true, engine: 'address-to-residents', tiers: TIERS.map(t => ({ name: t.name, cost: t.cost })), weight: 80 });
    }
    if (action === 'batch') {
      const secret = req.query.secret || req.headers['x-cron-secret'];
      if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
        return res.status(401).json({ error: 'Unauthorized' });
      }
      const limit = Math.min(50, parseInt(req.query.limit) || 20);
      const out = await batchEnrich(db, limit);
      return res.json({ success: true, ...out });
    }
    // single lookup
    const { street, city, state, person_id } = req.query;
    if (!street || !city || !state) return res.status(400).json({ error: 'street, city, state required' });
    const r = await lookupResidents(db, { street, city, state });
    if (r && person_id) await applyToPerson(db, person_id, r);
    return res.json({ success: true, result: r });
  } catch (e) {
    await reportError(db, 'address-to-residents', null, e.message).catch(() => {});
    return res.status(500).json({ error: e.message });
  }
};
