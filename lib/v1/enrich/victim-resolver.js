/**
 * VICTIM RESOLVER - Smart cross-engine orchestrator (Phase 38 Wave B)
 *
 * For each verified victim (persons.victim_verified = true), fan out across
 * every contact-discovery enricher in priority order until phone+email+address
 * are all populated, or time/cost budget is exhausted.
 *
 * Priority chain:
 *   1. PDL Person Enrichment (Pro tier - pulls personal phone/email/address)
 *   2. Apollo /v1/people/match
 *   3. Maricopa property (AZ only) - lookup by city, match by last_name
 *   4. Voter rolls (DB-loaded states)
 *   5. people-search-multi (FastPeopleSearch / ThatsThem / Radaris / TPS)
 *   6. Hunter.io email-finder (employer-domain fallback)
 *   7. Google CSE social search
 *   8. Trestle Reverse Phone (only if a phone candidate exists)
 *
 * Cross-validation:
 *   - phone area-code vs. incident state (or known prior city)
 *   - address city vs. incident city (50mi tolerance via simple match)
 *   - source-vs-source phone/email conflict logged with cross_engine_conflict=true
 *
 * HTTP entrypoint:
 *   GET /api/v1/enrich/victim-resolver?secret=ingest-now&action=health
 *   GET /api/v1/enrich/victim-resolver?secret=ingest-now&action=resolve&person_id=<uuid>
 *   GET /api/v1/enrich/victim-resolver?secret=ingest-now&action=batch&limit=10
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');
const { enqueueCascade } = require('../system/_cascade');

const SECRET = 'ingest-now';
const PERSON_TIME_BUDGET_MS = 45000;
const HTTP_TIMEOUT_MS = 15000;

const APOLLO_URL = 'https://api.apollo.io/v1/people/match';
const PDL_ENRICH_URL = 'https://api.peopledatalabs.com/v5/person/enrich';
const HUNTER_URL = 'https://api.hunter.io/v2/email-finder';

// State -> known phone area-code prefixes (sample - used for soft validation only).
// A miss DOES NOT reject the result, only docks confidence.
const STATE_AREA_CODES = {
  AL: ['205','251','256','334','659','938'], AK: ['907'],
  AZ: ['480','520','602','623','928'],
  AR: ['479','501','870'],
  CA: ['209','213','310','323','408','415','424','442','510','530','559','562','619','626','628','650','657','661','669','707','714','747','760','805','818','820','831','858','909','916','925','949','951'],
  CO: ['303','719','720','970'], CT: ['203','475','860','959'],
  DE: ['302'], FL: ['239','305','321','352','386','407','561','727','754','772','786','813','850','863','904','941','954','959'],
  GA: ['229','404','470','478','678','706','762','770','912'], HI: ['808'],
  ID: ['208','986'], IL: ['217','224','309','312','331','447','464','618','630','708','730','773','779','815','847','872'],
  IN: ['219','260','317','463','574','765','812','930'], IA: ['319','515','563','641','712'],
  KS: ['316','620','785','913'], KY: ['270','364','502','606','859'],
  LA: ['225','318','337','504','985'], ME: ['207'], MD: ['240','301','410','443','667'],
  MA: ['339','351','413','508','617','774','781','857','978'], MI: ['231','248','269','313','517','586','616','679','734','810','906','947','989'],
  MN: ['218','320','507','612','651','763','952'], MS: ['228','601','662','769'],
  MO: ['314','417','573','636','660','816'], MT: ['406'], NE: ['308','402','531'],
  NV: ['702','725','775'], NH: ['603'], NJ: ['201','551','609','640','732','848','856','862','908','973'],
  NM: ['505','575'], NY: ['212','315','329','332','347','363','516','518','585','607','631','646','680','716','718','838','845','914','917','929','934'],
  NC: ['252','336','472','704','743','828','910','919','980','984'], ND: ['701'],
  OH: ['216','220','234','283','326','330','380','419','440','513','567','614','740','937'],
  OK: ['405','539','580','918'], OR: ['458','503','541','971'],
  PA: ['215','223','267','272','412','445','484','570','582','610','717','724','814','878'],
  RI: ['401'], SC: ['803','839','843','854','864'], SD: ['605'],
  TN: ['423','615','629','731','865','901','931'],
  TX: ['210','214','254','281','325','346','361','409','430','432','469','512','682','713','726','737','806','817','830','832','903','915','936','940','945','956','972','979'],
  UT: ['385','435','801'], VT: ['802'],
  VA: ['276','434','540','571','703','757','804','826','948'],
  WA: ['206','253','360','425','509','564'], WV: ['304','681'],
  WI: ['262','274','414','534','608','715','920'], WY: ['307'], DC: ['202']
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

function areaCode(phone) {
  const d = digitsOnly(phone);
  if (d.length === 11 && d.startsWith('1')) return d.slice(1, 4);
  if (d.length === 10) return d.slice(0, 3);
  return null;
}

function validatePhoneState(phone, state) {
  const ac = areaCode(phone);
  if (!ac || !state) return { ok: true, score: 50, reason: 'no_state_or_phone' };
  const codes = STATE_AREA_CODES[String(state).toUpperCase()];
  if (!codes) return { ok: true, score: 50, reason: 'state_not_in_table' };
  if (codes.includes(ac)) return { ok: true, score: 90, reason: 'area_code_match' };
  return { ok: true, score: 30, reason: 'area_code_mismatch' };
}

function validateAddressCity(addrCity, incidentCity) {
  if (!addrCity || !incidentCity) return { ok: true, score: 50 };
  const a = normalizeAddr(addrCity), b = normalizeAddr(incidentCity);
  if (a === b) return { ok: true, score: 100 };
  if (a.includes(b) || b.includes(a)) return { ok: true, score: 70 };
  return { ok: true, score: 30 };
}

// ---------------------- PDL ----------------------
async function getPdlKey(db) {
  if (process.env.PDL_API_KEY) return process.env.PDL_API_KEY;
  try {
    const row = await db('system_config').where({ key: 'pdl_api_key' }).first();
    if (row?.value) return typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
  } catch (_) {}
  return null;
}

async function callPdlEnrich(person, key) {
  const { first, last } = splitName(person.full_name);
  if (!first || !last) return { ok: false, error: 'no_name_split' };
  const params = new URLSearchParams();
  params.append('first_name', first);
  params.append('last_name', last);
  if (person.city) params.append('locality', person.city);
  if (person.state) params.append('region', String(person.state).toLowerCase());
  params.append('min_likelihood', '1'); // Phase 38: lowered for accident-victim partial-data matching
  try {
    const r = await fetch(`${PDL_ENRICH_URL}?${params.toString()}`, {
      headers: { 'X-API-Key': key, 'Accept': 'application/json' },
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS)
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || d.status !== 200 || !d.data) return { ok: false, status: d.status || r.status };
    return { ok: true, data: d.data, likelihood: d.likelihood || 3 };
  } catch (e) { return { ok: false, error: e.message }; }
}

function pdlExtract(d) {
  const out = { phones: [], emails: [], addresses: [] };
  if (!d) return out;
  if (d.mobile_phone) out.phones.push(d.mobile_phone);
  if (Array.isArray(d.phone_numbers)) for (const p of d.phone_numbers) if (p) out.phones.push(p);
  if (Array.isArray(d.personal_emails)) for (const e of d.personal_emails) if (e) out.emails.push(e);
  if (d.work_email) out.emails.push(d.work_email);
  if (d.location_street_address) {
    out.addresses.push({
      address: d.location_street_address,
      city: d.location_locality || null,
      state: d.location_region || null,
      zip: d.location_postal_code || null
    });
  }
  if (d.linkedin_url) out.linkedin_url = d.linkedin_url;
  if (d.facebook_url) out.facebook_url = d.facebook_url;
  if (d.twitter_url) out.twitter_url = d.twitter_url;
  if (d.job_company_name) out.employer = d.job_company_name;
  if (d.job_title) out.occupation = d.job_title;
  return out;
}

// ---------------------- Apollo ----------------------
async function getApolloKey(db) {
  if (process.env.APOLLO_API_KEY) return process.env.APOLLO_API_KEY;
  try {
    const row = await db('system_config').where({ key: 'apollo_api_key' }).first();
    if (row?.value) return typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
  } catch (_) {}
  return null;
}

async function callApolloMatch(person, key) {
  const { first, last } = splitName(person.full_name);
  if (!first || !last) return { ok: false, error: 'no_name_split' };
  const body = { first_name: first, last_name: last, reveal_personal_emails: false };
  if (person.city) body.city = person.city;
  if (person.state) body.state = person.state;
  if (person.employer) body.organization_name = person.employer;
  try {
    const r = await fetch(APOLLO_URL, {
      method: 'POST',
      headers: { 'Cache-Control': 'no-cache', 'Content-Type': 'application/json', 'X-Api-Key': key },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS)
    });
    if (!r.ok) return { ok: false, status: r.status };
    const d = await r.json().catch(() => null);
    return { ok: true, data: d };
  } catch (e) { return { ok: false, error: e.message }; }
}

function apolloExtract(data) {
  const out = { phones: [], emails: [], addresses: [] };
  const p = data && data.person;
  if (!p) return out;
  if (Array.isArray(p.phone_numbers)) {
    for (const ph of p.phone_numbers) {
      const num = ph.sanitized_number || ph.raw_number || ph.number;
      if (num) out.phones.push(num);
    }
  }
  if (p.mobile_phone) out.phones.push(p.mobile_phone);
  if (p.email) out.emails.push(p.email);
  if (p.linkedin_url) out.linkedin_url = p.linkedin_url;
  if (p.organization?.name) out.employer = p.organization.name;
  if (p.title) out.occupation = p.title;
  return out;
}

// ---------------------- Maricopa ----------------------
async function lookupMaricopaByLastName(db, person) {
  if (String(person.state || '').toUpperCase() !== 'AZ') return { ok: false, error: 'not_az' };
  if (!person.city) return { ok: false, error: 'no_city' };
  const { last } = splitName(person.full_name);
  if (!last) return { ok: false, error: 'no_last_name' };
  let token = process.env.MARICOPA_API_TOKEN;
  if (!token) {
    try {
      const row = await db('system_config').where({ key: 'maricopa_api_token' }).first();
      if (row?.value) token = typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
    } catch (_) {}
  }
  if (!token) return { ok: false, error: 'no_maricopa_token', deferred: true };
  try {
    const url = `https://mcassessor.maricopa.gov/search/property/?q=${encodeURIComponent(last)}`;
    const r = await fetch(url, {
      headers: { 'AUTHORIZATION': token, 'Accept': 'application/json', 'User-Agent': '' },
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS)
    });
    if (!r.ok) return { ok: false, status: r.status };
    const d = await r.json().catch(() => null);
    const results = (d && (d.RealPropertyResults || d.Real || d.results)) || [];
    if (!Array.isArray(results) || !results.length) return { ok: true, data: [] };
    const matches = results.filter(rr => {
      const city = (rr.City || rr.PropertyAddressCity || '').toString();
      return normalizeAddr(city).includes(normalizeAddr(person.city || ''));
    }).slice(0, 3);
    return { ok: true, data: matches };
  } catch (e) { return { ok: false, error: e.message }; }
}

function maricopaExtract(rows, person) {
  const out = { phones: [], emails: [], addresses: [] };
  if (!Array.isArray(rows)) return out;
  const { last } = splitName(person.full_name);
  for (const r of rows) {
    const owner = (r.Ownership || r.Owner || '').toString();
    if (last && owner && !owner.toUpperCase().includes(last.toUpperCase())) continue;
    const street = r.PropertyAddress || r.SitusAddress || r.StreetAddr || null;
    const city = r.PropertyAddressCity || r.City || null;
    if (street) out.addresses.push({ address: street, city, state: 'AZ', zip: r.PropertyAddressZip || r.Zip || null });
  }
  return out;
}

// ---------------------- Voter rolls ----------------------
async function lookupVoterRoll(db, person) {
  try {
    const { lookupVoter } = require('./voter-rolls');
    const { first, last } = splitName(person.full_name);
    if (!last) return { ok: false, error: 'no_last' };
    const rows = await lookupVoter(db, first, last, person.state);
    return { ok: true, data: rows || [] };
  } catch (e) { return { ok: false, error: e.message }; }
}

function voterExtract(rows) {
  const out = { phones: [], emails: [], addresses: [] };
  if (!Array.isArray(rows)) return out;
  for (const r of rows) {
    if (r.residence_address) {
      out.addresses.push({
        address: r.residence_address,
        city: r.residence_city || null,
        state: r.state || null,
        zip: r.residence_zip || null
      });
    }
  }
  return out;
}

// ---------------------- people-search-multi ----------------------
async function lookupPeopleSearchMulti(db, person) {
  const UAS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
  ];
  const PHONE_RE = /\b\(?(\d{3})\)?[\s.-]?(\d{3})[\s.-]?(\d{4})\b/g;
  const EMAIL_RE = /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b/g;
  function slug(n) { return String(n || '').trim().toLowerCase().replace(/[^a-z0-9\s]/g, '').replace(/\s+/g, '-'); }
  const { first, last } = splitName(person.full_name);
  const st = (person.state || '').toLowerCase();
  const sources = [
    { src: 'fastpeoplesearch', url: `https://www.fastpeoplesearch.com/name/${slug(person.full_name)}_${st}` },
    { src: 'thatsthem', url: `https://thatsthem.com/name/${slug(first)}-${slug(last)}` }
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

// ---------------------- Hunter ----------------------
async function getHunterKey(db) {
  if (process.env.HUNTER_API_KEY) return process.env.HUNTER_API_KEY;
  try {
    const row = await db('system_config').where({ key: 'hunter_api_key' }).first();
    if (row?.value) return typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
  } catch (_) {}
  return null;
}

async function callHunter(person, key) {
  const { first, last } = splitName(person.full_name);
  if (!first || !last || !person.employer) return { ok: false, error: 'need_first_last_employer' };
  const company = String(person.employer).trim();
  const params = new URLSearchParams({
    company,
    first_name: first,
    last_name: last,
    api_key: key
  });
  try {
    const r = await fetch(`${HUNTER_URL}?${params.toString()}`, { signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (!r.ok) return { ok: false, status: r.status };
    const d = await r.json().catch(() => null);
    if (!d?.data?.email) return { ok: false, error: 'no_email' };
    return { ok: true, data: { email: d.data.email, score: d.data.score } };
  } catch (e) { return { ok: false, error: e.message }; }
}

// ---------------------- Google CSE ----------------------
async function callCseSocial(db, person) {
  try {
    const { searchSocial } = require('./social-search');
    const cfgRow = await db('system_config').where('key', 'google_cse').first();
    let cfg = { key: process.env.GOOGLE_CSE_API_KEY, cx: process.env.GOOGLE_CSE_ID };
    if (cfgRow?.value) {
      const v = typeof cfgRow.value === 'string' ? JSON.parse(cfgRow.value) : cfgRow.value;
      cfg = { key: v.api_key || cfg.key, cx: v.cse_id || cfg.cx };
    }
    if (!cfg.key || !cfg.cx) return { ok: false, error: 'no_cse_config' };
    const results = await searchSocial(person.full_name, person.city, person.state, cfg);
    return { ok: !!(results && results.length), data: results || [] };
  } catch (e) { return { ok: false, error: e.message }; }
}

function cseExtract(items) {
  const out = { phones: [], emails: [], addresses: [] };
  if (!Array.isArray(items)) return out;
  for (const i of items) {
    if (i.platform === 'facebook') out.facebook_url = i.link;
    if (i.platform === 'linkedin') out.linkedin_url = i.link;
    if (i.platform === 'twitter') out.twitter_url = i.link;
    if (i.platform === 'instagram') out.instagram_url = i.link;
  }
  return out;
}

// ---------------------- Trestle ----------------------
async function callTrestlePhone(db, phoneCandidate) {
  if (!phoneCandidate) return { ok: false, error: 'no_phone' };
  try {
    const trestle = require('./trestle');
    if (!(await trestle.isConfigured(db))) return { ok: false, error: 'trestle_not_configured' };
    const r = await trestle.reversePhone({ phone: digitsOnly(phoneCandidate) }, db);
    if (r?.error) return { ok: false, error: r.error };
    return { ok: true, data: r };
  } catch (e) { return { ok: false, error: e.message }; }
}

function trestleExtract(d) {
  const out = { phones: [], emails: [], addresses: [] };
  if (!d || !Array.isArray(d.owners)) return out;
  for (const o of d.owners) {
    if (Array.isArray(o.alternate_phones)) for (const p of o.alternate_phones) if (p?.phone_number) out.phones.push(p.phone_number);
    if (Array.isArray(o.emails)) for (const e of o.emails) if (e?.email_address) out.emails.push(e.email_address);
    if (Array.isArray(o.current_addresses)) {
      for (const a of o.current_addresses) {
        if (a.street_line_1 || a.full_address) {
          out.addresses.push({
            address: a.street_line_1 || a.full_address,
            city: a.city,
            state: a.state_code,
            zip: a.postal_code
          });
        }
      }
    }
  }
  return out;
}

// ---------------------- Cross-validate + apply ----------------------
function isContactComplete(p) { return !!(p.phone && p.email && p.address); }

async function logEnrichment(db, personId, sourceLabel, fields, confidence, conflict) {
  try {
    await db('enrichment_logs').insert({
      person_id: personId,
      field_name: 'victim_resolver:' + sourceLabel,
      old_value: null,
      new_value: JSON.stringify(fields).slice(0, 4000),
      source_url: null,
      source: sourceLabel,
      confidence: confidence || 60,
      verified: false,
      data: JSON.stringify({ source: sourceLabel, fields, cross_engine_conflict: !!conflict, weight: 100 }).slice(0, 4000),
      created_at: new Date()
    });
  } catch (_) {}
}

async function resolveOne(db, personId) {
  const start = Date.now();
  const stats = {
    person_id: personId,
    sources_tried: [],
    sources_succeeded: [],
    fields_filled: 0,
    conflicts: 0,
    errors: []
  };

  let person = await db('persons').where('id', personId).first();
  if (!person) return { ok: false, error: 'person_not_found', stats };
  if (!person.victim_verified) return { ok: false, error: 'not_verified', stats };
  let incident = null;
  if (person.incident_id) {
    incident = await db('incidents').where('id', person.incident_id).first('city', 'state').catch(() => null);
  }
  const incCity = (person.city || incident?.city || '').toString();
  const incState = (person.state || incident?.state || '').toString();

  if (person.phone && person.email && person.address) {
    return { ok: true, stats: { ...stats, skipped: 'already_complete' }, fully_resolved: true };
  }

  const buffers = { phones: [], emails: [], addresses: [], by_source: {} };

  function timeUp() { return (Date.now() - start) > PERSON_TIME_BUDGET_MS; }
  let consecutiveFails = 0;

  async function refreshPerson() {
    try { person = await db('persons').where('id', personId).first(); } catch (_) {}
  }

  async function runSource(label, fn) {
    if (timeUp()) return null;
    if (consecutiveFails >= 3) return null;
    stats.sources_tried.push(label);
    let result;
    try { result = await fn(); }
    catch (e) {
      stats.errors.push(`${label}:${e.message}`);
      consecutiveFails++;
      return null;
    }
    await trackApiCall(db, 'victim-resolver', label, 0, 0, !!(result && result.ok)).catch(() => {});
    if (!result || !result.ok) { consecutiveFails++; return null; }
    consecutiveFails = 0;
    stats.sources_succeeded.push(label);
    return result;
  }

  // 1. PDL
  const pdlKey = await getPdlKey(db);
  if (pdlKey) {
    const pdlR = await runSource('pdl_enrich', () => callPdlEnrich(person, pdlKey));
    if (pdlR) {
      const ext = pdlExtract(pdlR.data);
      buffers.by_source.pdl = ext;
      buffers.phones.push(...ext.phones.map(p => ({ p, src: 'pdl', w: 95 })));
      buffers.emails.push(...ext.emails.map(e => ({ e, src: 'pdl', w: 95 })));
      buffers.addresses.push(...ext.addresses.map(a => ({ a, src: 'pdl', w: 95 })));
      await logEnrichment(db, personId, 'pdl_enrich', ext, 90);
    }
  }

  // 2. Apollo
  await refreshPerson();
  if (!isContactComplete(person)) {
    const apolloKey = await getApolloKey(db);
    if (apolloKey) {
      const r = await runSource('apollo_match', () => callApolloMatch(person, apolloKey));
      if (r) {
        const ext = apolloExtract(r.data);
        buffers.by_source.apollo = ext;
        buffers.phones.push(...ext.phones.map(p => ({ p, src: 'apollo', w: 80 })));
        buffers.emails.push(...ext.emails.map(e => ({ e, src: 'apollo', w: 80 })));
        await logEnrichment(db, personId, 'apollo_match', ext, 75);
      }
    }
  }

  // 3. Maricopa
  await refreshPerson();
  if (!isContactComplete(person) && String(person.state || '').toUpperCase() === 'AZ') {
    const r = await runSource('maricopa_property', () => lookupMaricopaByLastName(db, person));
    if (r && r.data) {
      const ext = maricopaExtract(r.data, person);
      buffers.by_source.maricopa = ext;
      buffers.addresses.push(...ext.addresses.map(a => ({ a, src: 'maricopa', w: 70 })));
      await logEnrichment(db, personId, 'maricopa_property', ext, 70);
    }
  }

  // 4. Voter rolls
  await refreshPerson();
  if (!isContactComplete(person)) {
    const r = await runSource('voter_rolls', () => lookupVoterRoll(db, person));
    if (r && r.data) {
      const ext = voterExtract(r.data);
      buffers.by_source.voter = ext;
      buffers.addresses.push(...ext.addresses.map(a => ({ a, src: 'voter', w: 65 })));
      await logEnrichment(db, personId, 'voter_rolls', ext, 65);
    }
  }

  // 5. people-search-multi
  await refreshPerson();
  if (!isContactComplete(person)) {
    const r = await runSource('people_search_multi', () => lookupPeopleSearchMulti(db, person));
    if (r && r.data) {
      buffers.by_source.psm = r.data;
      buffers.phones.push(...(r.data.phones || []).map(p => ({ p, src: 'psm', w: 55 })));
      buffers.emails.push(...(r.data.emails || []).map(e => ({ e, src: 'psm', w: 55 })));
      await logEnrichment(db, personId, 'people_search_multi', r.data, 55);
    }
  }

  // 6. Hunter
  await refreshPerson();
  if (!person.email && person.employer) {
    const hKey = await getHunterKey(db);
    if (hKey) {
      const r = await runSource('hunter', () => callHunter(person, hKey));
      if (r && r.data) {
        buffers.by_source.hunter = r.data;
        buffers.emails.push({ e: r.data.email, src: 'hunter', w: 50 });
        await logEnrichment(db, personId, 'hunter', r.data, 50);
      }
    }
  }

  // 7. Google CSE social
  await refreshPerson();
  if (!isContactComplete(person)) {
    const r = await runSource('google_cse', () => callCseSocial(db, person));
    if (r && r.data) {
      const ext = cseExtract(r.data);
      buffers.by_source.cse = ext;
      const upd = {};
      if (ext.facebook_url && !person.facebook_url) upd.facebook_url = ext.facebook_url;
      if (ext.linkedin_url && !person.linkedin_url) upd.linkedin_url = ext.linkedin_url;
      if (ext.twitter_url && !person.twitter_url) upd.twitter_url = ext.twitter_url;
      if (Object.keys(upd).length) {
        upd.updated_at = new Date();
        try { await db('persons').where('id', personId).update(upd); stats.fields_filled += Object.keys(upd).length - 1; } catch (_) {}
      }
      await logEnrichment(db, personId, 'google_cse', ext, 30);
    }
  }

  // 8. Trestle
  await refreshPerson();
  let phoneCandidate = person.phone;
  if (!phoneCandidate && buffers.phones.length) {
    phoneCandidate = buffers.phones[0].p;
  }
  if (phoneCandidate && !isContactComplete(person)) {
    const r = await runSource('trestle_phone', () => callTrestlePhone(db, phoneCandidate));
    if (r && r.data) {
      const ext = trestleExtract(r.data);
      buffers.by_source.trestle = ext;
      buffers.phones.push(...ext.phones.map(p => ({ p, src: 'trestle', w: 75 })));
      buffers.emails.push(...ext.emails.map(e => ({ e, src: 'trestle', w: 75 })));
      buffers.addresses.push(...ext.addresses.map(a => ({ a, src: 'trestle', w: 75 })));
      await logEnrichment(db, personId, 'trestle_phone', ext, 75);
    }
  }

  // ---- Cross-validate + pick best across all collected buffers ----
  await refreshPerson();
  const update = {};

  if (!person.phone && buffers.phones.length) {
    const ranked = buffers.phones
      .map(x => ({ ...x, vScore: validatePhoneState(x.p, incState).score }))
      .sort((a, b) => (b.w + b.vScore) - (a.w + a.vScore));
    const winner = ranked[0];
    update.phone = digitsOnly(winner.p);
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
      .map(x => ({ ...x, vScore: validateAddressCity(x.a.city, incCity).score }))
      .sort((a, b) => (b.w + b.vScore) - (a.w + a.vScore));
    const winner = ranked[0].a;
    update.address = winner.address || null;
    if (!person.city && winner.city) update.city = winner.city;
    if (!person.state && winner.state) update.state = winner.state;
    if (!person.zip && winner.zip) update.zip = winner.zip;
  }

  if (Object.keys(update).length) {
    update.updated_at = new Date();
    try {
      await db('persons').where('id', personId).update(update);
      stats.fields_filled += Object.keys(update).filter(k => k !== 'updated_at').length;
    } catch (e) { stats.errors.push('update:' + e.message); }
  }

  await refreshPerson();
  const fully_resolved = !!(person.phone && person.email && person.address);

  try {
    await enqueueCascade(db, {
      person_id: personId,
      incident_id: person.incident_id,
      trigger_source: 'victim-resolver',
      trigger_field: 'multi',
      trigger_value: stats.sources_succeeded.join(',') || 'none',
      priority: fully_resolved ? 9 : 5
    });
  } catch (_) {}

  return {
    ok: true,
    fully_resolved,
    stats,
    snapshot: { phone: !!person.phone, email: !!person.email, address: !!person.address }
  };
}

async function batchResolve(db, { limit = 10 } = {}) {
  const rows = await db('persons')
    .where('victim_verified', true)
    .where(function () { this.whereNull('phone').orWhereNull('email').orWhereNull('address'); })
    .whereNotNull('full_name')
    .orderBy('updated_at', 'desc')
    .limit(limit)
    .select('id', 'full_name', 'phone', 'email', 'address');

  const results = {
    candidates: rows.length,
    resolved: 0,
    fully_resolved: 0,
    fields_filled: 0,
    sources_succeeded: {},
    samples: []
  };

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
        fully_resolved: !!one.fully_resolved
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
    const apollo = !!(process.env.APOLLO_API_KEY || (await db('system_config').where({ key: 'apollo_api_key' }).first().catch(() => null)));
    const pdl = !!(process.env.PDL_API_KEY || (await db('system_config').where({ key: 'pdl_api_key' }).first().catch(() => null)));
    const trestleCfg = !!(process.env.TRESTLE_API_KEY || (await db('system_config').where({ key: 'trestle' }).first().catch(() => null)));
    const cse = !!(process.env.GOOGLE_CSE_API_KEY || (await db('system_config').where({ key: 'google_cse' }).first().catch(() => null)));
    const hunter = !!(process.env.HUNTER_API_KEY || (await db('system_config').where({ key: 'hunter_api_key' }).first().catch(() => null)));
    return res.status(200).json({
      success: true,
      service: 'victim-resolver',
      sources: { pdl, apollo, trestle: trestleCfg, cse, hunter, voter_rolls: true, maricopa: !!(process.env.MARICOPA_API_TOKEN), people_search_multi: true },
      ts: new Date().toISOString()
    });
  }

  if (action === 'resolve') {
    const personId = req.query?.person_id || req.query?.id;
    if (!personId) return res.status(400).json({ error: 'person_id required' });
    try {
      const r = await resolveOne(db, personId);
      await trackApiCall(db, 'victim-resolver', 'resolve_one', 0, 0, !!r.ok).catch(() => {});
      return res.status(200).json({ success: true, ...r });
    } catch (e) {
      await reportError(db, 'victim-resolver', null, e.message, { severity: 'error' });
      return res.status(500).json({ error: e.message, success: false });
    }
  }

  if (action === 'batch') {
    const limit = Math.max(1, Math.min(parseInt(req.query?.limit) || 10, 30));
    try {
      const r = await batchResolve(db, { limit });
      await trackApiCall(db, 'victim-resolver', 'batch', 0, 0, true).catch(() => {});
      return res.status(200).json({ success: true, ...r });
    } catch (e) {
      await reportError(db, 'victim-resolver', null, e.message, { severity: 'error' });
      return res.status(500).json({ error: e.message, success: false });
    }
  }

  return res.status(400).json({ error: 'unknown action: ' + action });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.resolveOne = resolveOne;
module.exports.batchResolve = batchResolve;
