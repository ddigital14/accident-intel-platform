/**
 * FREE OSINT EXTRAS — Phase 43
 *
 * Five additional FREE sources NOT currently in the OSINT miner and NOT
 * IP-blocked from Vercel egress:
 *
 *   1. OpenStates.org           (state legislator lookup, district verification)
 *   2. OpenCorporates           (business registry across 50 states + 130 countries)
 *   3. CollegeScorecard.ed.gov  (federal education-data — verify alma mater)
 *   4. NewsAPI archive deep     (30-day archive search using existing NEWSAPI_ORG_KEY)
 *   5. Federal Election Commission (api.open.fec.gov — donor lookups: address +
 *      employer + occupation, GOLD for older / affluent victims)
 *
 * All endpoints handle:
 *   - GET /api/v1/enrich/free-osint-extras?secret=ingest-now&action=health
 *   - GET ?action=lookup&name=<full_name>&city=&state=&source=fec|opencorp|openstates|cscore|news_archive
 *   - GET ?action=all&name=...&city=&state=    (runs all 5 in parallel)
 *
 * Returns structured `{phone?, email?, address?, employer?, source, confidence}`
 * when applicable.
 *
 * No API key required for OpenStates/OpenCorporates/CollegeScorecard/FEC.
 * NewsAPI uses existing system_config key.
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');
const { enqueueCascade } = require('../system/_cascade');

const SECRET = 'ingest-now';
const HTTP_TIMEOUT_MS = 15000;
const UA = 'AIP/1.0 (accident-intel-platform; +contact via mason@donovandigitalsolutions.com)';

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

function splitName(full) {
  const parts = String(full || '').trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return { first: '', last: '', mid: '' };
  if (parts.length === 1) return { first: parts[0], last: '', mid: '' };
  return { first: parts[0], last: parts[parts.length - 1], mid: parts.slice(1, -1).join(' ') };
}

async function getCfg(db, key, envName) {
  if (envName && process.env[envName]) return process.env[envName];
  try {
    const row = await db('system_config').where({ key }).first();
    if (row?.value) return typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
  } catch (_) {}
  return null;
}

// ---------------------------------------------------------------------------
// 1. OpenStates — https://openstates.org/api/v3/people?name=...
// ---------------------------------------------------------------------------
async function lookupOpenStates(db, { name, state }) {
  if (!name) return { ok: false, source: 'openstates', error: 'no_name' };
  try {
    const params = new URLSearchParams({ name });
    if (state) params.set('jurisdiction', state);
    const url = `https://v3.openstates.org/people?${params.toString()}`;
    const r = await fetch(url, {
      headers: { 'User-Agent': UA, 'Accept': 'application/json' },
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS)
    });
    await trackApiCall(db, 'openstates', '/people', 0, 0, r.ok).catch(() => {});
    if (!r.ok) return { ok: false, source: 'openstates', status: r.status, error: `http_${r.status}` };
    const d = await r.json().catch(() => null);
    const results = d?.results || [];
    if (!results.length) return { ok: false, source: 'openstates', empty: true };
    const top = results[0];
    const offices = top.offices || [];
    const capitol = offices.find(o => o.classification === 'capitol') || offices[0] || {};
    return {
      ok: true,
      source: 'openstates',
      confidence: 70,
      legislator: {
        name: top.name,
        party: top.party,
        district: top.current_role?.district,
        chamber: top.current_role?.org_classification,
        state: top.jurisdiction?.name
      },
      phone: capitol.voice || null,
      email: top.email || null,
      address: capitol.address || null,
      employer: top.current_role?.title || 'State Legislator',
      raw_count: results.length
    };
  } catch (e) {
    return { ok: false, source: 'openstates', error: `exception:${e.message}` };
  }
}

// ---------------------------------------------------------------------------
// 2. OpenCorporates — https://api.opencorporates.com/companies/search?q=
// ---------------------------------------------------------------------------
async function lookupOpenCorporates(db, { name, state }) {
  if (!name) return { ok: false, source: 'opencorporates', error: 'no_name' };
  try {
    const params = new URLSearchParams({ q: name, per_page: '5' });
    if (state) params.set('jurisdiction_code', `us_${String(state).toLowerCase()}`);
    const url = `https://api.opencorporates.com/v0.4/officers/search?${params.toString()}`;
    const r = await fetch(url, {
      headers: { 'User-Agent': UA, 'Accept': 'application/json' },
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS)
    });
    await trackApiCall(db, 'opencorporates', '/officers/search', 0, 0, r.ok).catch(() => {});
    if (!r.ok) return { ok: false, source: 'opencorporates', status: r.status, error: `http_${r.status}` };
    const d = await r.json().catch(() => null);
    const officers = d?.results?.officers || [];
    if (!officers.length) return { ok: false, source: 'opencorporates', empty: true };
    const top = officers[0]?.officer || {};
    const company = top.company || {};
    return {
      ok: true,
      source: 'opencorporates',
      confidence: 65,
      employer: company.name || null,
      address: top.address || null,
      role: top.position || null,
      jurisdiction: company.jurisdiction_code || null,
      company_url: company.opencorporates_url || null,
      all_companies: officers.slice(0, 5).map(o => ({
        company: o.officer?.company?.name,
        position: o.officer?.position,
        date_started: o.officer?.start_date
      }))
    };
  } catch (e) {
    return { ok: false, source: 'opencorporates', error: `exception:${e.message}` };
  }
}

// ---------------------------------------------------------------------------
// 3. CollegeScorecard — used for cross-verifying news article alma-mater claim
// ---------------------------------------------------------------------------
async function lookupCollegeScorecard(db, { schoolName, state }) {
  if (!schoolName) return { ok: false, source: 'collegescorecard', error: 'no_school' };
  try {
    const apiKey = await getCfg(db, 'collegescorecard_api_key', 'COLLEGESCORECARD_API_KEY')
      || 'DEMO_KEY'; // data.gov DEMO_KEY works for low-volume lookups
    const params = new URLSearchParams({
      'school.name': schoolName,
      'fields': 'school.name,school.city,school.state,school.school_url,school.zip',
      'api_key': apiKey,
      'per_page': '5'
    });
    if (state) params.set('school.state', state);
    const url = `https://api.data.gov/ed/collegescorecard/v1/schools?${params.toString()}`;
    const r = await fetch(url, {
      headers: { 'User-Agent': UA, 'Accept': 'application/json' },
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS)
    });
    await trackApiCall(db, 'collegescorecard', '/schools', 0, 0, r.ok).catch(() => {});
    if (!r.ok) return { ok: false, source: 'collegescorecard', status: r.status, error: `http_${r.status}` };
    const d = await r.json().catch(() => null);
    const results = d?.results || [];
    if (!results.length) return { ok: false, source: 'collegescorecard', empty: true };
    const top = results[0];
    return {
      ok: true,
      source: 'collegescorecard',
      confidence: 55,
      school: top['school.name'],
      city: top['school.city'],
      state: top['school.state'],
      url: top['school.school_url'],
      zip: top['school.zip'],
      match_count: results.length
    };
  } catch (e) {
    return { ok: false, source: 'collegescorecard', error: `exception:${e.message}` };
  }
}

// ---------------------------------------------------------------------------
// 4. NewsAPI archive deep search (30 days back, all sources)
// ---------------------------------------------------------------------------
async function lookupNewsArchive(db, { name, city, state, daysBack = 30 }) {
  const apiKey = await getCfg(db, 'newsapi_org_key', 'NEWSAPI_ORG_KEY');
  if (!apiKey) return { ok: false, source: 'news_archive', error: 'no_newsapi_key' };
  if (!name) return { ok: false, source: 'news_archive', error: 'no_name' };
  try {
    const fromDate = new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const queryParts = [`"${name}"`];
    if (city) queryParts.push(`"${city}"`);
    else if (state) queryParts.push(`"${state}"`);
    const params = new URLSearchParams({
      q: queryParts.join(' '),
      from: fromDate,
      sortBy: 'relevancy',
      pageSize: '15',
      language: 'en',
      apiKey
    });
    const url = `https://newsapi.org/v2/everything?${params.toString()}`;
    const r = await fetch(url, {
      headers: { 'User-Agent': UA, 'Accept': 'application/json' },
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS)
    });
    await trackApiCall(db, 'newsapi-archive', '/everything', 0, 0, r.ok).catch(() => {});
    if (!r.ok) return { ok: false, source: 'news_archive', status: r.status, error: `http_${r.status}` };
    const d = await r.json().catch(() => null);
    const articles = d?.articles || [];
    if (!articles.length) return { ok: false, source: 'news_archive', empty: true };
    const PHONE_RE = /\b\(?(\d{3})\)?[\s.\-]?(\d{3})[\s.\-]?(\d{4})\b/g;
    const EMAIL_RE = /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b/g;
    const phones = new Set(), emails = new Set();
    const trimmed = articles.slice(0, 10).map(a => {
      const blob = `${a.title || ''} ${a.description || ''} ${a.content || ''}`;
      let m; PHONE_RE.lastIndex = 0;
      while ((m = PHONE_RE.exec(blob)) !== null) {
        if (!['000','111','555','800','866','877','888'].includes(m[1])) phones.add(`${m[1]}-${m[2]}-${m[3]}`);
      }
      EMAIL_RE.lastIndex = 0;
      while ((m = EMAIL_RE.exec(blob)) !== null) {
        const e = m[0].toLowerCase();
        if (!/^(support|info|contact|hello|admin|noreply|webmaster|press|editor)@/i.test(e)) emails.add(e);
      }
      return { title: a.title, url: a.url, source: a.source?.name, published_at: a.publishedAt };
    });
    return {
      ok: true,
      source: 'news_archive',
      confidence: 60,
      total_articles: articles.length,
      articles: trimmed,
      phones: [...phones],
      emails: [...emails],
      days_back: daysBack
    };
  } catch (e) {
    return { ok: false, source: 'news_archive', error: `exception:${e.message}` };
  }
}

// ---------------------------------------------------------------------------
// 5. FEC donor records — api.open.fec.gov/v1/schedules/schedule_a/?contributor_name=
// ---------------------------------------------------------------------------
async function lookupFEC(db, { name, state, city }) {
  if (!name) return { ok: false, source: 'fec', error: 'no_name' };
  try {
    const apiKey = await getCfg(db, 'fec_api_key', 'FEC_API_KEY') || 'DEMO_KEY';
    const params = new URLSearchParams({
      contributor_name: name,
      per_page: '20',
      sort: '-contribution_receipt_date',
      api_key: apiKey
    });
    if (state) params.set('contributor_state', state);
    if (city) params.set('contributor_city', city);
    const url = `https://api.open.fec.gov/v1/schedules/schedule_a/?${params.toString()}`;
    const r = await fetch(url, {
      headers: { 'User-Agent': UA, 'Accept': 'application/json' },
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS)
    });
    await trackApiCall(db, 'fec', '/schedules/schedule_a', 0, 0, r.ok).catch(() => {});
    if (!r.ok) return { ok: false, source: 'fec', status: r.status, error: `http_${r.status}` };
    const d = await r.json().catch(() => null);
    const results = d?.results || [];
    if (!results.length) return { ok: false, source: 'fec', empty: true };
    // Pick the most recent record (already sorted desc) and aggregate employer freq
    const top = results[0];
    const employerCounts = {};
    const occupationCounts = {};
    const addrSet = new Set();
    for (const rec of results) {
      const emp = (rec.contributor_employer || '').trim();
      const occ = (rec.contributor_occupation || '').trim();
      if (emp) employerCounts[emp] = (employerCounts[emp] || 0) + 1;
      if (occ) occupationCounts[occ] = (occupationCounts[occ] || 0) + 1;
      if (rec.contributor_street_1) {
        addrSet.add([rec.contributor_street_1, rec.contributor_city, rec.contributor_state, rec.contributor_zip].filter(Boolean).join(', '));
      }
    }
    const topEmployer = Object.entries(employerCounts).sort((a, b) => b[1] - a[1])[0]?.[0] || null;
    const topOccupation = Object.entries(occupationCounts).sort((a, b) => b[1] - a[1])[0]?.[0] || null;
    return {
      ok: true,
      source: 'fec',
      confidence: 85, // FEC data is sworn under penalty of perjury — high quality
      name: top.contributor_name,
      address: [top.contributor_street_1, top.contributor_city, top.contributor_state, top.contributor_zip].filter(Boolean).join(', '),
      addresses_seen: [...addrSet].slice(0, 5),
      employer: topEmployer,
      occupation: topOccupation,
      total_donations: results.length,
      latest_donation_date: top.contribution_receipt_date,
      latest_amount: top.contribution_receipt_amount,
      committee: top.committee?.name
    };
  } catch (e) {
    return { ok: false, source: 'fec', error: `exception:${e.message}` };
  }
}

// ---------------------------------------------------------------------------
// Combined runner: all 5 in parallel
// ---------------------------------------------------------------------------
async function lookupAll(db, opts) {
  const { name, city, state, schoolName } = opts;
  const tasks = [
    lookupOpenStates(db, { name, state }),
    lookupOpenCorporates(db, { name, state }),
    lookupNewsArchive(db, { name, city, state }),
    lookupFEC(db, { name, state, city })
  ];
  if (schoolName) tasks.push(lookupCollegeScorecard(db, { schoolName, state }));
  const settled = await Promise.allSettled(tasks);
  const results = settled.map(r => r.status === 'fulfilled' ? r.value : { ok: false, error: r.reason?.message || 'rejected' });
  const successes = results.filter(r => r.ok);
  return {
    ok: successes.length > 0,
    sources_tried: results.length,
    sources_succeeded: successes.length,
    results,
    consolidated: {
      phones: results.flatMap(r => r.phones || (r.phone ? [r.phone] : [])).filter(Boolean),
      emails: results.flatMap(r => r.emails || (r.email ? [r.email] : [])).filter(Boolean),
      addresses: results.flatMap(r => r.addresses_seen || (r.address ? [r.address] : [])).filter(Boolean),
      employer_candidates: results.map(r => r.employer).filter(Boolean)
    }
  };
}

// ---------------------------------------------------------------------------
// Optional: write findings to enrichment_logs and trigger cascade
// ---------------------------------------------------------------------------
async function writeAndCascade(db, personId, payload) {
  if (!personId) return;
  try {
    await db('enrichment_logs').insert({
      person_id: personId,
      field_name: 'free-osint-extras',
      old_value: null,
      new_value: JSON.stringify(payload).slice(0, 4000),
      source: 'free-osint-extras',
      confidence: payload.consolidated?.phones?.length ? 70 : 40,
      verified: false,
      created_at: new Date()
    });
  } catch (_) {}
  try {
    const person = await db('persons').where('id', personId).first();
    await enqueueCascade(db, {
      person_id: personId,
      incident_id: person?.incident_id || null,
      trigger_source: 'free-osint-extras',
      trigger_field: 'multi',
      trigger_value: `succ=${payload.sources_succeeded}/${payload.sources_tried}`,
      priority: payload.sources_succeeded >= 2 ? 7 : 4
    });
  } catch (_) {}
}

async function health(db) {
  return {
    ok: true,
    sources: ['openstates', 'opencorporates', 'collegescorecard', 'news_archive', 'fec'],
    keys_required: {
      openstates: 'none (rate-limited 25/min)',
      opencorporates: 'none for basic queries',
      collegescorecard: 'optional (DEMO_KEY works low-volume); env COLLEGESCORECARD_API_KEY',
      news_archive: 'requires NEWSAPI_ORG_KEY (already configured)',
      fec: 'optional (DEMO_KEY works); env FEC_API_KEY'
    },
    valid_actions: ['health', 'lookup', 'all']
  };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const db = getDb();
  const action = (req.query?.action || 'health').toLowerCase();

  try {
    if (action === 'health') {
      const h = await health(db);
      return res.json({ success: true, action: 'health', ...h, timestamp: new Date().toISOString() });
    }
    const name = req.query?.name;
    const city = req.query?.city;
    const state = req.query?.state;
    const schoolName = req.query?.school || req.query?.school_name;
    const personId = req.query?.person_id;

    if (action === 'lookup') {
      const source = (req.query?.source || '').toLowerCase();
      let r;
      if (source === 'openstates') r = await lookupOpenStates(db, { name, state });
      else if (source === 'opencorporates' || source === 'opencorp') r = await lookupOpenCorporates(db, { name, state });
      else if (source === 'collegescorecard' || source === 'cscore') r = await lookupCollegeScorecard(db, { schoolName, state });
      else if (source === 'news_archive' || source === 'news') r = await lookupNewsArchive(db, { name, city, state });
      else if (source === 'fec') r = await lookupFEC(db, { name, state, city });
      else return res.status(400).json({ error: 'unknown source', valid: ['openstates','opencorporates','collegescorecard','news_archive','fec'] });
      return res.json({ success: !!r.ok, ...r, timestamp: new Date().toISOString() });
    }
    if (action === 'all') {
      if (!name) return res.status(400).json({ error: 'name required' });
      const out = await lookupAll(db, { name, city, state, schoolName });
      if (personId) await writeAndCascade(db, personId, out);
      return res.json({ success: out.ok, ...out, timestamp: new Date().toISOString() });
    }
    return res.status(400).json({ error: 'unknown action', valid: ['health', 'lookup', 'all'] });
  } catch (e) {
    try { await reportError(db, 'free-osint-extras', null, e.message); } catch (_) {}
    return res.status(500).json({ success: false, error: e.message });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.lookupOpenStates = lookupOpenStates;
module.exports.lookupOpenCorporates = lookupOpenCorporates;
module.exports.lookupCollegeScorecard = lookupCollegeScorecard;
module.exports.lookupNewsArchive = lookupNewsArchive;
module.exports.lookupFEC = lookupFEC;
module.exports.lookupAll = lookupAll;
module.exports.health = health;
