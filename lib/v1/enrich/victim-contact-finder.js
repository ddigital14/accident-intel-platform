/**
 * VICTIM CONTACT FINDER — Phase 39
 *
 * Aggressive 12-source composite resolver for verified accident victims who
 * have no contact data. Public-records-FIRST, then B2B fallback.
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');
const { enqueueCascade } = require('../system/_cascade');

const SECRET = 'ingest-now';
const HTTP_TIMEOUT_MS = 15000;
const PERSON_TIME_BUDGET_MS = 50000;

const PDL_ENRICH_URL = 'https://api.peopledatalabs.com/v5/person/enrich';
const APOLLO_URL = 'https://api.apollo.io/v1/people/match';
const HUNTER_URL = 'https://api.hunter.io/v2/email-finder';
const COURTLISTENER_URL = 'https://www.courtlistener.com/api/rest/v3/search/';

const PROPERTY_COUNTIES = {
  IL: { county: 'cook', url: q => `https://assessor.cookcountyil.gov/Search?searchterm=${encodeURIComponent(q)}` },
  TX_HARRIS: { county: 'harris', url: q => `https://www.hcad.org/quick-search/?searchType=name&searchValue=${encodeURIComponent(q)}` },
  TX_TRAVIS: { county: 'travis', url: q => `https://search.tcadcentral.org/Search/Result?keywords=${encodeURIComponent(q)}` },
  CA_LA: { county: 'losangeles', url: q => `https://portal.assessor.lacounty.gov/parceldetail/?owner=${encodeURIComponent(q)}` },
  GA_FULTON: { county: 'fulton', url: q => `https://iaspublicaccess.fultoncountyga.gov/search/CommonSearch.aspx?mode=OWNER&owner=${encodeURIComponent(q)}` }
};

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}
function splitName(full) {
  const parts = String(full || '').trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return { first: '', last: '' };
  if (parts.length === 1) return { first: parts[0], last: '' };
  return { first: parts[0], last: parts[parts.length - 1] };
}
function digitsOnly(s) { return String(s || '').replace(/\D+/g, ''); }
function normalizeAddr(s) { return String(s || '').toLowerCase().replace(/[^a-z0-9 ]+/g, ' ').replace(/\s+/g, ' ').trim(); }
function isContactComplete(p) { return !!(p.phone && p.email && p.address); }

async function getCfg(db, key, envName) {
  if (envName && process.env[envName]) return process.env[envName];
  try {
    const row = await db('system_config').where({ key }).first();
    if (row?.value) return typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
  } catch (_) {}
  return null;
}

async function loadCseCfg(db) {
  try {
    const cfgRow = await db('system_config').where('key', 'google_cse').first();
    let key = process.env.GOOGLE_CSE_API_KEY;
    let cx = process.env.GOOGLE_CSE_ID;
    if (cfgRow?.value) {
      const v = typeof cfgRow.value === 'string' ? JSON.parse(cfgRow.value) : cfgRow.value;
      key = v.api_key || key;
      cx = v.cse_id || cx;
    }
    if (!key || !cx) return null;
    return { key, cx };
  } catch (_) { return null; }
}

// 1. Voter rolls
async function srcVoterRolls(db, person) {
  try {
    const { lookupVoter } = require('./voter-rolls');
    const { first, last } = splitName(person.full_name);
    if (!last) return { ok: false, error: 'no_last' };
    const rows = await lookupVoter(db, first, last, person.state);
    if (!Array.isArray(rows) || !rows.length) return { ok: false, error: 'no_match' };
    const out = { phones: [], emails: [], addresses: [] };
    for (const r of rows) {
      if (r.residence_address) {
        out.addresses.push({
          address: r.residence_address,
          city: r.residence_city || null,
          state: r.state || null,
          zip: r.residence_zip || null
        });
      }
      if (r.dob) out.dob = r.dob;
    }
    return { ok: out.addresses.length > 0, data: out };
  } catch (e) { return { ok: false, error: e.message }; }
}

// 2. Maricopa
async function srcMaricopa(db, person) {
  if (String(person.state || '').toUpperCase() !== 'AZ') return { ok: false, error: 'not_az' };
  if (!person.city) return { ok: false, error: 'no_city' };
  const { last } = splitName(person.full_name);
  if (!last) return { ok: false, error: 'no_last' };
  const token = await getCfg(db, 'maricopa_api_token', 'MARICOPA_API_TOKEN');
  if (!token) return { ok: false, error: 'no_token' };
  try {
    const url = `https://mcassessor.maricopa.gov/search/property/?q=${encodeURIComponent(last)}`;
    const r = await fetch(url, {
      headers: { 'AUTHORIZATION': token, 'Accept': 'application/json', 'User-Agent': '' },
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS)
    });
    if (!r.ok) return { ok: false, status: r.status };
    const d = await r.json().catch(() => null);
    const rows = (d && (d.RealPropertyResults || d.Real || d.results)) || [];
    const out = { phones: [], emails: [], addresses: [] };
    if (Array.isArray(rows)) {
      for (const row of rows.slice(0, 5)) {
        const owner = (row.Ownership || row.Owner || '').toString();
        if (last && owner && !owner.toUpperCase().includes(last.toUpperCase())) continue;
        const street = row.PropertyAddress || row.SitusAddress || row.StreetAddr;
        const city = row.PropertyAddressCity || row.City;
        if (street) out.addresses.push({ address: street, city, state: 'AZ', zip: row.PropertyAddressZip || row.Zip || null });
      }
    }
    return { ok: out.addresses.length > 0, data: out };
  } catch (e) { return { ok: false, error: e.message }; }
}

// 3. Obituary search
async function srcObituarySearch(db, person) {
  const cfg = await loadCseCfg(db);
  if (!cfg) return { ok: false, error: 'no_cse' };
  const q = `"${person.full_name}" obituary ${person.city || person.state || ''}`.trim();
  try {
    const url = `https://www.googleapis.com/customsearch/v1?key=${cfg.key}&cx=${cfg.cx}&q=${encodeURIComponent(q)}&num=5`;
    const r = await fetch(url, { signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (!r.ok) return { ok: false, status: r.status };
    const d = await r.json().catch(() => null);
    const items = d?.items || [];
    const out = { phones: [], emails: [], addresses: [], obit_links: [], funeral_homes: [] };
    for (const it of items.slice(0, 5)) {
      out.obit_links.push({ url: it.link, title: it.title, snippet: it.snippet });
      const snip = `${it.title || ''} ${it.snippet || ''}`;
      const fhMatch = snip.match(/([A-Z][a-zA-Z &'\-]+(?:Funeral\s+Home|Mortuary|Cremation|Memorial|Crematory|Chapel))/);
      if (fhMatch) out.funeral_homes.push(fhMatch[1].trim());
      const phoneMatch = snip.match(/\b\(?(\d{3})\)?[\s.\-]?(\d{3})[\s.\-]?(\d{4})\b/);
      if (phoneMatch) out.phones.push(`${phoneMatch[1]}-${phoneMatch[2]}-${phoneMatch[3]}`);
    }
    return { ok: out.obit_links.length > 0, data: out };
  } catch (e) { return { ok: false, error: e.message }; }
}

// 4. CourtListener
async function srcCourtListener(db, person) {
  const token = await getCfg(db, 'courtlistener_api_token', 'COURTLISTENER_API_TOKEN');
  try {
    const params = new URLSearchParams({ q: `"${person.full_name}"`, type: 'r', order_by: 'dateFiled desc' });
    const headers = { 'Accept': 'application/json' };
    if (token) headers['Authorization'] = `Token ${token}`;
    const r = await fetch(`${COURTLISTENER_URL}?${params.toString()}`, { headers, signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (!r.ok) return { ok: false, status: r.status };
    const d = await r.json().catch(() => null);
    const results = d?.results || [];
    const out = { phones: [], emails: [], addresses: [], cases: [] };
    for (const c of results.slice(0, 5)) {
      out.cases.push({ caseName: c.caseName, court: c.court, dateFiled: c.dateFiled });
      const blob = `${c.caseName || ''} ${c.snippet || ''}`;
      const addrMatch = blob.match(/\b(\d+\s+[A-Z][a-zA-Z]+\s+(?:St|Ave|Rd|Blvd|Dr|Way|Ln|Ct|Cir|Pl)\.?)\b/);
      if (addrMatch) out.addresses.push({ address: addrMatch[1], city: null, state: person.state || null, zip: null });
    }
    return { ok: results.length > 0, data: out };
  } catch (e) { return { ok: false, error: e.message }; }
}

// 5. GoFundMe
async function srcGoFundMe(db, person) {
  const cfg = await loadCseCfg(db);
  if (!cfg) return { ok: false, error: 'no_cse' };
  const q = `"${person.full_name}" site:gofundme.com`;
  try {
    const url = `https://www.googleapis.com/customsearch/v1?key=${cfg.key}&cx=${cfg.cx}&q=${encodeURIComponent(q)}&num=3`;
    const r = await fetch(url, { signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (!r.ok) return { ok: false, status: r.status };
    const d = await r.json().catch(() => null);
    const items = d?.items || [];
    const out = { phones: [], emails: [], addresses: [], campaigns: [], organizers: [] };
    for (const it of items.slice(0, 3)) {
      out.campaigns.push({ url: it.link, title: it.title, snippet: it.snippet });
      try {
        const pageR = await fetch(it.link, { signal: AbortSignal.timeout(8000), headers: { 'User-Agent': 'Mozilla/5.0' } });
        if (pageR.ok) {
          const html = (await pageR.text()).slice(0, 80000);
          const orgMatch = html.match(/"name"\s*:\s*"([^"]{2,80})"\s*,\s*"role"\s*:\s*"organizer"/i)
            || html.match(/Organi[sz]er[^<]{0,40}<[^>]+>([A-Z][a-zA-Z'\-\s]{2,60})</);
          if (orgMatch) out.organizers.push(orgMatch[1].trim());
          const emailMatch = html.match(/\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b/);
          if (emailMatch) out.emails.push(emailMatch[0].toLowerCase());
        }
      } catch (_) {}
    }
    return { ok: out.campaigns.length > 0, data: out };
  } catch (e) { return { ok: false, error: e.message }; }
}

// 6. News rescrape
async function srcNewsRescrape(db, person) {
  try {
    const reports = await db('reports')
      .where('incident_id', person.incident_id)
      .whereIn('source_type', ['news', 'rss', 'press'])
      .limit(3)
      .select('source_reference', 'parsed_data');
    if (!reports || !reports.length) return { ok: false, error: 'no_reports' };
    const out = { phones: [], emails: [], addresses: [], next_of_kin_mentions: [], rescraped_urls: [] };
    for (const rep of reports) {
      if (!rep.source_reference || !/^https?:\/\//.test(rep.source_reference)) continue;
      out.rescraped_urls.push(rep.source_reference);
      try {
        const r = await fetch(rep.source_reference, {
          signal: AbortSignal.timeout(8000),
          headers: { 'User-Agent': 'Mozilla/5.0 (compatible; AIPBot/1.0)' }
        });
        if (!r.ok) continue;
        const html = (await r.text()).slice(0, 200000);
        const text = html.replace(/<script[\s\S]*?<\/script>/gi, ' ').replace(/<style[\s\S]*?<\/style>/gi, ' ')
                         .replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ');
        const fullName = person.full_name;
        const fnEsc = fullName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const familyRx = new RegExp("(" + fnEsc + "\\'s\\s+(?:family|widow|husband|wife|brother|sister|son|daughter|mother|father|parents))[^.]{0,200}", 'i');
        const m = text.match(familyRx);
        if (m) out.next_of_kin_mentions.push(m[0].slice(0, 240));
        const idx = text.toLowerCase().indexOf(fullName.toLowerCase());
        if (idx >= 0) {
          const window = text.slice(Math.max(0, idx - 250), Math.min(text.length, idx + 500));
          const ph = window.match(/\b\(?(\d{3})\)?[\s.\-]?(\d{3})[\s.\-]?(\d{4})\b/);
          if (ph && !['000','111','555'].includes(ph[1])) out.phones.push(`${ph[1]}-${ph[2]}-${ph[3]}`);
          const em = window.match(/\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b/);
          if (em && !em[0].toLowerCase().includes('example.com')) out.emails.push(em[0].toLowerCase());
        }
      } catch (_) {}
    }
    return { ok: out.next_of_kin_mentions.length > 0 || out.phones.length > 0 || out.emails.length > 0, data: out };
  } catch (e) { return { ok: false, error: e.message }; }
}

// 7. Funeral home
async function srcFuneralHome(db, person) {
  const cfg = await loadCseCfg(db);
  if (!cfg) return { ok: false, error: 'no_cse' };
  const q = `"${person.full_name}" funeral home ${person.city || person.state || ''}`.trim();
  try {
    const url = `https://www.googleapis.com/customsearch/v1?key=${cfg.key}&cx=${cfg.cx}&q=${encodeURIComponent(q)}&num=3`;
    const r = await fetch(url, { signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (!r.ok) return { ok: false, status: r.status };
    const d = await r.json().catch(() => null);
    const items = d?.items || [];
    const out = { phones: [], emails: [], addresses: [], funeral_homes: [], family_names: [] };
    for (const it of items.slice(0, 3)) {
      const snip = `${it.title || ''} ${it.snippet || ''}`;
      const fhMatch = snip.match(/([A-Z][a-zA-Z &'\-]+(?:Funeral\s+Home|Mortuary|Cremation|Memorial|Crematory|Chapel))/);
      if (fhMatch) out.funeral_homes.push({ name: fhMatch[1].trim(), url: it.link });
      const phoneMatch = snip.match(/\b\(?(\d{3})\)?[\s.\-]?(\d{3})[\s.\-]?(\d{4})\b/);
      if (phoneMatch) out.phones.push(`${phoneMatch[1]}-${phoneMatch[2]}-${phoneMatch[3]}`);
      const survMatch = snip.match(/survived\s+by[^.]{0,200}/i);
      if (survMatch) {
        const names = survMatch[0].match(/[A-Z][a-zA-Z'\-]+(?:\s+[A-Z][a-zA-Z'\-]+){1,3}/g) || [];
        out.family_names.push(...names.slice(0, 5));
      }
    }
    return { ok: out.funeral_homes.length > 0, data: out };
  } catch (e) { return { ok: false, error: e.message }; }
}

// 8. Property county
async function srcPropertyCounty(db, person) {
  const st = String(person.state || '').toUpperCase();
  const city = String(person.city || '').toLowerCase();
  let key = null;
  if (st === 'IL' && /chicago|cook/.test(city)) key = 'IL';
  else if (st === 'TX' && /houston|harris/.test(city)) key = 'TX_HARRIS';
  else if (st === 'TX' && /austin|travis/.test(city)) key = 'TX_TRAVIS';
  else if (st === 'CA' && /(los\s*angeles|la|long\s*beach|pasadena)/.test(city)) key = 'CA_LA';
  else if (st === 'GA' && /(atlanta|fulton)/.test(city)) key = 'GA_FULTON';
  if (!key) return { ok: false, error: 'no_county_match' };
  const { last } = splitName(person.full_name);
  if (!last) return { ok: false, error: 'no_last' };
  const cfg = PROPERTY_COUNTIES[key];
  try {
    const url = cfg.url(last);
    const r = await fetch(url, {
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS),
      headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'text/html' }
    });
    if (!r.ok) return { ok: false, status: r.status };
    const html = (await r.text()).slice(0, 80000);
    const out = { phones: [], emails: [], addresses: [], county: cfg.county, source_url: url };
    const addrMatches = html.match(/\b\d{2,5}\s+[A-Z][A-Za-z]{2,}(?:\s+[A-Za-z]{2,}){0,3}\s+(?:St|Ave|Rd|Blvd|Dr|Way|Ln|Ct|Cir|Pl|Hwy|Pkwy)\b/g) || [];
    for (const a of addrMatches.slice(0, 3)) {
      out.addresses.push({ address: a, city: person.city || null, state: st, zip: null });
    }
    return { ok: out.addresses.length > 0, data: out };
  } catch (e) { return { ok: false, error: e.message }; }
}

// 9. PDL
async function srcPdl(db, person) {
  const key = await getCfg(db, 'pdl_api_key', 'PDL_API_KEY');
  if (!key) return { ok: false, error: 'no_key' };
  const { first, last } = splitName(person.full_name);
  if (!first || !last) return { ok: false, error: 'no_name_split' };
  const params = new URLSearchParams();
  params.append('first_name', first);
  params.append('last_name', last);
  if (person.city) params.append('locality', person.city);
  if (person.state) params.append('region', String(person.state).toLowerCase());
  params.append('min_likelihood', '1');
  try {
    const r = await fetch(`${PDL_ENRICH_URL}?${params.toString()}`, {
      headers: { 'X-API-Key': key, 'Accept': 'application/json' },
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS)
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || d.status !== 200 || !d.data) return { ok: false, status: d.status || r.status };
    const out = { phones: [], emails: [], addresses: [] };
    if (d.data.mobile_phone) out.phones.push(d.data.mobile_phone);
    if (Array.isArray(d.data.phone_numbers)) for (const p of d.data.phone_numbers) p && out.phones.push(p);
    if (Array.isArray(d.data.personal_emails)) for (const e of d.data.personal_emails) e && out.emails.push(e);
    if (d.data.work_email) out.emails.push(d.data.work_email);
    if (d.data.location_street_address) {
      out.addresses.push({
        address: d.data.location_street_address,
        city: d.data.location_locality || null,
        state: d.data.location_region || null,
        zip: d.data.location_postal_code || null
      });
    }
    if (d.data.linkedin_url) out.linkedin_url = d.data.linkedin_url;
    if (d.data.job_company_name) out.employer = d.data.job_company_name;
    return { ok: !!(out.phones.length || out.emails.length || out.addresses.length), data: out, likelihood: d.likelihood };
  } catch (e) { return { ok: false, error: e.message }; }
}

// 10. Apollo
async function srcApollo(db, person) {
  const key = await getCfg(db, 'apollo_api_key', 'APOLLO_API_KEY');
  if (!key) return { ok: false, error: 'no_key' };
  const { first, last } = splitName(person.full_name);
  if (!first || !last) return { ok: false, error: 'no_name_split' };
  const body = { first_name: first, last_name: last, reveal_personal_emails: false };
  if (person.city) body.city = person.city;
  if (person.state) body.state = person.state;
  if (person.employer) body.organization_name = person.employer;
  try {
    const r = await fetch(APOLLO_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Api-Key': key, 'Cache-Control': 'no-cache' },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS)
    });
    if (!r.ok) return { ok: false, status: r.status };
    const d = await r.json().catch(() => null);
    const p = d?.person;
    if (!p) return { ok: false, error: 'no_person' };
    const out = { phones: [], emails: [], addresses: [] };
    if (Array.isArray(p.phone_numbers)) for (const ph of p.phone_numbers) {
      const num = ph.sanitized_number || ph.raw_number || ph.number;
      if (num) out.phones.push(num);
    }
    if (p.mobile_phone) out.phones.push(p.mobile_phone);
    if (p.email) out.emails.push(p.email);
    if (p.linkedin_url) out.linkedin_url = p.linkedin_url;
    if (p.organization?.name) out.employer = p.organization.name;
    return { ok: !!(out.phones.length || out.emails.length), data: out };
  } catch (e) { return { ok: false, error: e.message }; }
}

// 11. Hunter
async function srcHunter(db, person) {
  const key = await getCfg(db, 'hunter_api_key', 'HUNTER_API_KEY');
  if (!key) return { ok: false, error: 'no_key' };
  if (!person.employer) return { ok: false, error: 'no_employer' };
  const { first, last } = splitName(person.full_name);
  if (!first || !last) return { ok: false, error: 'no_name_split' };
  try {
    const params = new URLSearchParams({ company: person.employer, first_name: first, last_name: last, api_key: key });
    const r = await fetch(`${HUNTER_URL}?${params.toString()}`, { signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (!r.ok) return { ok: false, status: r.status };
    const d = await r.json().catch(() => null);
    if (!d?.data?.email) return { ok: false, error: 'no_email' };
    return { ok: true, data: { phones: [], emails: [d.data.email], addresses: [], score: d.data.score } };
  } catch (e) { return { ok: false, error: e.message }; }
}

// 12. people-search-multi
async function srcPeopleSearchMulti(db, person) {
  const UAS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
  ];
  const PHONE_RE = /\b\(?(\d{3})\)?[\s.\-]?(\d{3})[\s.\-]?(\d{4})\b/g;
  const EMAIL_RE = /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b/g;
  const slug = n => String(n || '').trim().toLowerCase().replace(/[^a-z0-9\s]/g, '').replace(/\s+/g, '-');
  const { first, last } = splitName(person.full_name);
  const st = (person.state || '').toLowerCase();
  const sources = [
    { src: 'fastpeoplesearch', url: `https://www.fastpeoplesearch.com/name/${slug(person.full_name)}_${st}` },
    { src: 'thatsthem', url: `https://thatsthem.com/name/${slug(first)}-${slug(last)}` },
    { src: 'truepeoplesearch', url: `https://www.truepeoplesearch.com/results?name=${encodeURIComponent(person.full_name)}&citystatezip=${encodeURIComponent(st)}` }
  ];
  const out = { phones: [], emails: [], addresses: [], hits: [] };
  for (const s of sources) {
    try {
      const r = await fetch(s.url, {
        headers: { 'User-Agent': UAS[Math.floor(Math.random() * UAS.length)], 'Accept': 'text/html', 'Accept-Language': 'en-US,en;q=0.9' },
        signal: AbortSignal.timeout(8000), redirect: 'follow'
      });
      if (!r.ok) continue;
      const html = (await r.text()).slice(0, 120000);
      const cleaned = html.replace(/<script[\s\S]*?<\/script>/gi, ' ').replace(/<style[\s\S]*?<\/style>/gi, ' ');
      let m;
      PHONE_RE.lastIndex = 0;
      while ((m = PHONE_RE.exec(cleaned)) !== null) {
        if (['000','111','555'].includes(m[1])) continue;
        out.phones.push(`${m[1]}-${m[2]}-${m[3]}`);
        if (out.phones.length >= 5) break;
      }
      EMAIL_RE.lastIndex = 0;
      while ((m = EMAIL_RE.exec(cleaned)) !== null) {
        const e = m[0].toLowerCase();
        if (e.includes('example.com') || e.endsWith('.png') || e.endsWith('.jpg')) continue;
        out.emails.push(e);
        if (out.emails.length >= 3) break;
      }
      if (out.phones.length || out.emails.length) { out.hits.push(s.src); break; }
    } catch (_) {}
  }
  return { ok: out.hits.length > 0, data: out };
}

async function logEnrichment(db, personId, source, fields, confidence, conflict) {
  try {
    await db('enrichment_logs').insert({
      person_id: personId,
      field_name: 'victim-contact-finder:' + source,
      old_value: null,
      new_value: JSON.stringify(fields).slice(0, 4000),
      source_url: null,
      source: 'victim-contact-finder:' + source,
      confidence: confidence || 60,
      verified: false,
      data: JSON.stringify({ source, fields, cross_engine_conflict: !!conflict, weight: 60 }).slice(0, 4000),
      created_at: new Date()
    });
  } catch (_) {}
}

const SOURCES = [
  { label: 'voter_rolls',       fn: srcVoterRolls,       weight: 90 },
  { label: 'maricopa_property', fn: srcMaricopa,         weight: 85 },
  { label: 'obituary_search',   fn: srcObituarySearch,   weight: 75 },
  { label: 'court_records',     fn: srcCourtListener,    weight: 70 },
  { label: 'gofundme',          fn: srcGoFundMe,         weight: 80 },
  { label: 'news_rescrape',     fn: srcNewsRescrape,     weight: 85 },
  { label: 'funeral_home',      fn: srcFuneralHome,      weight: 75 },
  { label: 'property_county',   fn: srcPropertyCounty,   weight: 80 },
  { label: 'pdl_enrich',        fn: srcPdl,              weight: 95 },
  { label: 'apollo_match',      fn: srcApollo,           weight: 80 },
  { label: 'hunter',            fn: srcHunter,           weight: 50 },
  { label: 'people_search_multi', fn: srcPeopleSearchMulti, weight: 55 }
];

async function resolveOne(db, personId) {
  const start = Date.now();
  const stats = { person_id: personId, sources_tried: [], sources_succeeded: [], fields_filled: 0, conflicts: 0, errors: [], by_source: {} };

  let person = await db('persons').where('id', personId).first();
  if (!person) return { ok: false, error: 'person_not_found', stats };
  if (!person.victim_verified) return { ok: false, error: 'not_verified', stats };

  let incident = null;
  if (person.incident_id) {
    incident = await db('incidents').where('id', person.incident_id).first('city', 'state').catch(() => null);
  }
  const incCity = (person.city || incident?.city || '').toString();
  if (!person.city && incident?.city) person.city = incident.city;
  if (!person.state && incident?.state) person.state = incident.state;

  if (isContactComplete(person)) {
    return { ok: true, fully_resolved: true, stats: { ...stats, skipped: 'already_complete' } };
  }

  const buffers = { phones: [], emails: [], addresses: [], by_source: {} };
  function timeUp() { return (Date.now() - start) > PERSON_TIME_BUDGET_MS; }

  for (const s of SOURCES) {
    if (timeUp()) { stats.errors.push('time_budget_exhausted'); break; }
    try { person = await db('persons').where('id', personId).first(); } catch (_) {}
    if (isContactComplete(person)) break;

    stats.sources_tried.push(s.label);
    let r;
    try { r = await s.fn(db, person); }
    catch (e) { stats.errors.push(`${s.label}:${e.message}`); r = { ok: false, error: e.message }; }
    await trackApiCall(db, 'victim-contact-finder', s.label, 0, 0, !!(r && r.ok)).catch(() => {});

    if (r && r.ok && r.data) {
      stats.sources_succeeded.push(s.label);
      stats.by_source[s.label] = r.data;
      buffers.by_source[s.label] = r.data;
      if (Array.isArray(r.data.phones)) buffers.phones.push(...r.data.phones.map(p => ({ p, src: s.label, w: s.weight })));
      if (Array.isArray(r.data.emails)) buffers.emails.push(...r.data.emails.map(e => ({ e, src: s.label, w: s.weight })));
      if (Array.isArray(r.data.addresses)) buffers.addresses.push(...r.data.addresses.map(a => ({ a, src: s.label, w: s.weight })));
      await logEnrichment(db, personId, s.label, r.data, s.weight);
    }
  }

  try { person = await db('persons').where('id', personId).first(); } catch (_) {}
  const update = {};

  if (!person.phone && buffers.phones.length) {
    const ranked = [...buffers.phones].sort((a, b) => b.w - a.w);
    update.phone = digitsOnly(ranked[0].p);
    const distinct = new Set(buffers.phones.map(x => digitsOnly(x.p)));
    if (distinct.size > 1) {
      stats.conflicts++;
      await logEnrichment(db, personId, 'phone_conflict', { sources: buffers.phones.map(x => ({ src: x.src, p: x.p })) }, 40, true);
    }
  }
  if (!person.email && buffers.emails.length) {
    const ranked = [...buffers.emails].sort((a, b) => b.w - a.w);
    update.email = String(ranked[0].e).toLowerCase();
    const distinct = new Set(buffers.emails.map(x => String(x.e || '').toLowerCase()));
    if (distinct.size > 1) {
      stats.conflicts++;
      await logEnrichment(db, personId, 'email_conflict', { sources: buffers.emails.map(x => ({ src: x.src, e: x.e })) }, 40, true);
    }
  }
  if (!person.address && buffers.addresses.length) {
    const ranked = buffers.addresses
      .map(x => {
        const cityMatch = x.a.city && incCity && normalizeAddr(x.a.city) === normalizeAddr(incCity);
        return { ...x, score: x.w + (cityMatch ? 20 : 0) };
      })
      .sort((a, b) => b.score - a.score);
    const winner = ranked[0].a;
    update.address = winner.address || null;
    if (!person.city && winner.city) update.city = winner.city;
    if (!person.state && winner.state) update.state = winner.state;
    if (!person.zip && winner.zip) update.zip = winner.zip;
  }

  for (const src of Object.values(buffers.by_source)) {
    if (src.linkedin_url && !person.linkedin_url) update.linkedin_url = src.linkedin_url;
    if (src.facebook_url && !person.facebook_url) update.facebook_url = src.facebook_url;
    if (src.twitter_url && !person.twitter_url) update.twitter_url = src.twitter_url;
    if (src.employer && !person.employer) update.employer = src.employer;
  }

  if (Object.keys(update).length) {
    update.updated_at = new Date();
    try {
      await db('persons').where('id', personId).update(update);
      stats.fields_filled = Object.keys(update).filter(k => k !== 'updated_at').length;
    } catch (e) { stats.errors.push('update:' + e.message); }
  }

  try { person = await db('persons').where('id', personId).first(); } catch (_) {}
  const fully_resolved = !!(person.phone && person.email && person.address);

  try {
    await enqueueCascade(db, {
      person_id: personId,
      incident_id: person.incident_id,
      trigger_source: 'victim-contact-finder',
      trigger_field: 'multi',
      trigger_value: stats.sources_succeeded.join(',') || 'none',
      priority: fully_resolved ? 9 : 6
    });
  } catch (_) {}

  return {
    ok: true,
    fully_resolved,
    stats,
    snapshot: { phone: !!person.phone, email: !!person.email, address: !!person.address },
    by_source: stats.by_source,
    final_contact: { phone: person.phone, email: person.email, address: person.address }
  };
}

async function batchResolve(db, { limit = 10 } = {}) {
  const rows = await db('persons')
    .where('victim_verified', true)
    .where(function () {
      this.whereNull('phone').orWhere('phone', '')
        .orWhereNull('email').orWhere('email', '')
        .orWhereNull('address').orWhere('address', '');
    })
    .whereNotNull('full_name')
    .orderBy('updated_at', 'desc')
    .limit(limit)
    .select('id', 'full_name');

  const results = { candidates: rows.length, resolved: 0, fully_resolved: 0, fields_filled: 0, sources_succeeded: {}, samples: [] };
  for (const r of rows) {
    let one;
    try { one = await resolveOne(db, r.id); }
    catch (e) {
      results.samples.push({ person_id: r.id, name: r.full_name, error: e.message });
      continue;
    }
    if (!one || !one.ok) continue;
    results.resolved++;
    if (one.fully_resolved) results.fully_resolved++;
    if (one.stats?.fields_filled) results.fields_filled += one.stats.fields_filled;
    for (const s of (one.stats?.sources_succeeded || [])) {
      results.sources_succeeded[s] = (results.sources_succeeded[s] || 0) + 1;
    }
    if (results.samples.length < 8) {
      results.samples.push({
        person_id: r.id,
        name: r.full_name,
        fields_filled: one.stats?.fields_filled || 0,
        sources: one.stats?.sources_succeeded || [],
        fully_resolved: !!one.fully_resolved,
        snapshot: one.snapshot
      });
    }
  }
  return results;
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const action = (req.query?.action || 'health').toLowerCase();
  let db;
  try { db = getDb(); } catch (e) {
    return res.status(500).json({ success: false, error: 'db_unavailable:' + e.message });
  }

  if (action === 'health') {
    const checks = {};
    for (const k of [
      ['pdl_api_key', 'PDL_API_KEY'],
      ['apollo_api_key', 'APOLLO_API_KEY'],
      ['hunter_api_key', 'HUNTER_API_KEY'],
      ['maricopa_api_token', 'MARICOPA_API_TOKEN'],
      ['google_cse', 'GOOGLE_CSE_API_KEY'],
      ['courtlistener_api_token', 'COURTLISTENER_API_TOKEN']
    ]) {
      checks[k[0]] = !!(await getCfg(db, k[0], k[1]));
    }
    return res.status(200).json({
      success: true,
      service: 'victim-contact-finder',
      sources_count: SOURCES.length,
      sources: SOURCES.map(s => s.label),
      keys_present: checks,
      ts: new Date().toISOString()
    });
  }

  if (action === 'resolve') {
    const personId = req.query?.person_id || req.query?.id;
    if (!personId) return res.status(400).json({ error: 'person_id required' });
    try {
      const r = await resolveOne(db, personId);
      await trackApiCall(db, 'victim-contact-finder', 'resolve_one', 0, 0, !!r.ok).catch(() => {});
      return res.status(200).json({ success: true, ...r });
    } catch (e) {
      await reportError(db, 'victim-contact-finder', null, e.message, { severity: 'error' });
      return res.status(500).json({ error: e.message, success: false });
    }
  }

  if (action === 'batch') {
    const limit = Math.max(1, Math.min(parseInt(req.query?.limit) || 10, 30));
    try {
      const r = await batchResolve(db, { limit });
      await trackApiCall(db, 'victim-contact-finder', 'batch', 0, 0, true).catch(() => {});
      return res.status(200).json({ success: true, ...r });
    } catch (e) {
      await reportError(db, 'victim-contact-finder', null, e.message, { severity: 'error' });
      return res.status(500).json({ error: e.message, success: false });
    }
  }

  return res.status(400).json({ error: 'unknown action: ' + action });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.resolveOne = resolveOne;
module.exports.batchResolve = batchResolve;
module.exports.SOURCES = SOURCES;
