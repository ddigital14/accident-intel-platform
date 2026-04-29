/**
 * DEEP PHONE RESEARCH — Phase 45
 *
 * The reasoning brain that finds phone + email for accident victims when
 * everything else came up empty.  Uses Claude Opus 4.7 as a multi-step OSINT
 * analyst.  Loops up to 4 cycles.  90s budget per person.
 *
 * Loop:
 *   1. gather()        — pull every known fact about the victim
 *   2. opusHypothesize — Opus reads facts and emits ranked search hypotheses
 *   3. executeTop3()   — fan-out top 3 hypotheses to live engines
 *   4. opusSynthesize  — Opus reads results, decides confidence + next iter
 *   5. crossValidate() — area-code/state, address/city, email/employer
 *
 * GET /api/v1/enrich/deep-phone-research?secret=ingest-now&action=health
 * GET /api/v1/enrich/deep-phone-research?secret=ingest-now&action=research&person_id=<uuid>
 * GET /api/v1/enrich/deep-phone-research?secret=ingest-now&action=research&victim_name=Heather%20Avery&city=Houston&state=TX
 * GET /api/v1/enrich/deep-phone-research?secret=ingest-now&action=batch&limit=5
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');
const { enqueueCascade } = require('../system/_cascade');
const { extract } = require('./_ai_router');

const SECRET = 'ingest-now';
const PERSON_BUDGET_MS = 90000;
const ITERATION_CAP = 4;
const HTTP_TIMEOUT_MS = 12000;
const MIN_STORE_CONF = 60;
const STOP_CONF = 75;

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}
function splitName(full) {
  const parts = String(full || '').trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return { first: '', last: '' };
  if (parts.length === 1) return { first: parts[0], last: '' };
  return { first: parts[0], last: parts[parts.length - 1] };
}
function digitsOnly(s) { return String(s || '').replace(/\D+/g, ''); }
function clamp(n, lo, hi) { return Math.max(lo, Math.min(hi, Number(n) || 0)); }

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
    const row = await db('system_config').where('key', 'google_cse').first();
    let key = process.env.GOOGLE_CSE_API_KEY;
    let cx = process.env.GOOGLE_CSE_ID;
    if (row?.value) {
      const v = typeof row.value === 'string' ? JSON.parse(row.value) : row.value;
      key = v.api_key || key;
      cx = v.cse_id || cx;
    }
    if (!key || !cx) return null;
    return { key, cx };
  } catch (_) { return null; }
}

const STATE_AREA_CODES = {
  AL: ['205','251','256','334','659','938'], AK: ['907'],
  AZ: ['480','520','602','623','928'], AR: ['479','501','870'],
  CA: ['209','213','279','310','323','341','408','415','424','442','510','530','559','562','619','626','628','650','657','661','669','707','714','747','760','805','818','820','831','840','858','909','916','925','949','951'],
  CO: ['303','719','720','970','983'], CT: ['203','475','860','959'], DE: ['302'],
  FL: ['239','305','321','352','386','407','448','561','656','689','727','754','772','786','813','850','863','904','941','954'],
  GA: ['229','404','470','478','678','706','762','770','912'], HI: ['808'],
  ID: ['208','986'], IL: ['217','224','309','312','331','447','464','618','630','708','730','773','779','815','847','872'],
  IN: ['219','260','317','463','574','765','812','930'], IA: ['319','515','563','641','712'],
  KS: ['316','620','785','913'], KY: ['270','364','502','606','859'],
  LA: ['225','318','337','504','985'], ME: ['207'], MD: ['227','240','301','410','443','667'],
  MA: ['339','351','413','508','617','774','781','857','978'],
  MI: ['231','248','269','313','517','586','616','679','734','810','906','947','989'],
  MN: ['218','320','507','612','651','763','952'], MS: ['228','601','662','769'],
  MO: ['314','417','557','573','636','660','816','975'], MT: ['406'],
  NE: ['308','402','531'], NV: ['702','725','775'], NH: ['603'],
  NJ: ['201','551','609','640','732','848','856','862','908','973'], NM: ['505','575'],
  NY: ['212','315','329','332','347','363','516','518','585','607','631','646','680','716','718','838','845','914','917','929','934'],
  NC: ['252','336','472','704','743','828','910','919','980','984'], ND: ['701'],
  OH: ['216','220','234','283','326','330','380','419','436','440','513','567','614','740','937'],
  OK: ['405','539','572','580','918'], OR: ['458','503','541','971'],
  PA: ['215','223','267','272','412','445','484','570','582','610','717','724','814','835','878'],
  RI: ['401'], SC: ['803','821','839','843','854','864'], SD: ['605'],
  TN: ['423','615','629','731','865','901','931'],
  TX: ['210','214','254','281','325','346','361','409','430','432','469','512','682','713','726','737','806','817','830','832','903','915','936','940','945','956','972','979'],
  UT: ['385','435','801'], VT: ['802'],
  VA: ['276','434','540','571','703','757','804','826','948'], WA: ['206','253','360','425','509','564'],
  WV: ['304','681'], WI: ['262','274','414','534','608','715','920'], WY: ['307'], DC: ['202']
};
function areaCodeMatchesState(phone, state) {
  if (!phone || !state) return null;
  const d = digitsOnly(phone);
  const ac = d.length === 11 ? d.slice(1, 4) : d.slice(0, 3);
  const list = STATE_AREA_CODES[String(state).toUpperCase()];
  if (!list) return null;
  return list.includes(ac);
}

async function gatherFacts(db, opts) {
  const facts = { person: null, incident: null, related_persons: [], enrichment_logs: [], source_reports: [], partials: {} };
  let person = null;
  if (opts.person_id) {
    person = await db('persons').where('id', opts.person_id).first().catch(() => null);
  } else if (opts.victim_name) {
    const q = db('persons').whereRaw('LOWER(full_name) = ?', [opts.victim_name.toLowerCase()]);
    if (opts.state) q.andWhere('state', opts.state);
    if (opts.city)  q.andWhereRaw('LOWER(city) = ?', [String(opts.city).toLowerCase()]);
    person = await q.orderBy('updated_at', 'desc').first().catch(() => null);
  }
  if (!person && (opts.victim_name || opts.full_name)) {
    person = { id: null, full_name: opts.victim_name || opts.full_name, city: opts.city || null, state: opts.state || null };
  }
  if (!person) return { ok: false, error: 'no_person' };
  facts.person = person;

  if (person.incident_id) {
    facts.incident = await db('incidents').where('id', person.incident_id).first().catch(() => null);
    facts.related_persons = await db('persons')
      .where('incident_id', person.incident_id)
      .andWhereNot('id', person.id || '00000000-0000-0000-0000-000000000000')
      .limit(8)
      .select('id','full_name','role','phone','email','address','city','state','age','employer')
      .catch(() => []);
    facts.source_reports = await db('source_reports')
      .where('incident_id', person.incident_id)
      .orderBy('created_at', 'desc')
      .limit(6)
      .select('source','source_url','title','content_text','created_at')
      .catch(() => []);
  }
  if (person.id) {
    facts.enrichment_logs = await db('enrichment_logs')
      .where('person_id', person.id)
      .orderBy('created_at', 'desc')
      .limit(40)
      .select('source','field_name','new_value','confidence','data','created_at')
      .catch(() => []);
  }
  facts.partials = {
    full_name: person.full_name,
    first_name: splitName(person.full_name).first,
    last_name:  splitName(person.full_name).last,
    age: person.age || null, role: person.role || null,
    employer: person.employer || null, occupation: person.occupation || null,
    phone: person.phone || null, email: person.email || null,
    address: person.address || null,
    city: person.city || facts.incident?.city || null,
    state: person.state || facts.incident?.state || null,
    accident_address: facts.incident?.accident_address || null,
    accident_date: person.accident_date || facts.incident?.accident_date || null,
    severity: facts.incident?.severity || null,
    fatal: !!facts.incident?.fatal
  };
  return { ok: true, facts };
}

function compactFactsForPrompt(facts, maxLen = 14000) {
  const obj = {
    subject: facts.partials,
    incident: facts.incident ? {
      id: facts.incident.id, city: facts.incident.city, state: facts.incident.state,
      accident_address: facts.incident.accident_address,
      accident_date: facts.incident.accident_date,
      severity: facts.incident.severity, fatal: facts.incident.fatal,
      vehicle_info: facts.incident.vehicle_info || facts.incident.vehicles || null,
      time: facts.incident.time || facts.incident.accident_time || null
    } : null,
    related_persons: (facts.related_persons || []).map(p => ({
      name: p.full_name, role: p.role, age: p.age,
      city: p.city, state: p.state, employer: p.employer,
      has_phone: !!p.phone, has_email: !!p.email
    })),
    enrichment_log_summary: (facts.enrichment_logs || []).map(l => ({
      source: l.source, field: l.field_name, conf: l.confidence,
      val: typeof l.new_value === 'string' ? l.new_value.slice(0, 200) : l.new_value
    })).slice(0, 25),
    source_reports: (facts.source_reports || []).map(r => ({
      source: r.source, url: r.source_url, title: r.title,
      excerpt: String(r.content_text || '').slice(0, 800)
    }))
  };
  let s = JSON.stringify(obj, null, 2);
  if (s.length > maxLen) s = s.slice(0, maxLen) + '\n...truncated';
  return s;
}

const HYPOTHESIZE_SYSTEM = `You are an expert OSINT analyst tasked with finding the phone number and email of a personal-injury accident victim. You think step-by-step like a private investigator. Return JSON only.

Output schema:
{
  "reasoning": "2-3 sentences of thinking",
  "age_estimate": {"low": int|null, "high": int|null, "based_on": "..."},
  "likely_employer": {"name": "..."|null, "based_on": "..."},
  "family_signals": [{"name": "...", "relationship": "..."}],
  "social_likely": ["facebook","linkedin","instagram","twitter","reddit","gofundme"],
  "vehicle_plate_hint": null|"...",
  "lawsuit_or_court_hint": null|"...",
  "community_hint": null|"...",
  "hypotheses": [
    {"id": int, "engine": "cse|brave|wayback|courtlistener|gofundme|reddit|github|fec|opencorporates|hunter|property|voter|funeral|family|news_archive",
     "query": "specific search string", "expected_yield": "phone|email|address|family|social|other",
     "confidence_if_hit": int(0-100), "rationale": "..."}
  ],
  "priority_order": [int, int, int, ...]
}
Return 5-8 hypotheses ordered by tractability. Prefer free public records first.`;

async function opusHypothesize(db, facts, prevAttempts) {
  const factBlob = compactFactsForPrompt(facts);
  const prevBlob = prevAttempts && prevAttempts.length
    ? `\n\nPrior iterations (do NOT repeat queries that already failed):\n${JSON.stringify(prevAttempts.slice(-2), null, 2).slice(0, 3000)}`
    : '';
  const userPrompt = `KNOWN FACTS:\n${factBlob}\n${prevBlob}\n\nReason step-by-step about who this victim is and where their phone/email is most likely findable. Return JSON.`;
  const r = await extract(db, {
    pipeline: 'deep-phone-research:hypothesize',
    systemPrompt: HYPOTHESIZE_SYSTEM, userPrompt,
    provider: 'auto', tier: 'premium',
    timeoutMs: 35000, responseFormat: 'json', temperature: 0.2
  });
  if (!r.ok) return { ok: false, error: r.error, attempts: r.attempts };
  return { ok: true, parsed: r.parsed || {}, raw: r.content, model: r.model_used,
           tokens_in: r.tokens_in, tokens_out: r.tokens_out };
}

// ============== ENGINE EXECUTORS ==============
async function runCse(db, q) {
  const cfg = await loadCseCfg(db);
  if (!cfg) return { ok: false, error: 'no_cse_key' };
  try {
    const url = `https://www.googleapis.com/customsearch/v1?key=${cfg.key}&cx=${cfg.cx}&q=${encodeURIComponent(q)}&num=5`;
    const resp = await fetch(url, { signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (db) await trackApiCall(db, 'deep-phone-research', 'cse', 0, 0, resp.ok).catch(() => {});
    if (!resp.ok) return { ok: false, error: `http_${resp.status}` };
    const data = await resp.json();
    const items = (data.items || []).map(i => ({ title: i.title, link: i.link, snippet: i.snippet }));
    return { ok: items.length > 0, items };
  } catch (e) { return { ok: false, error: e.message }; }
}
async function runBrave(db, q) {
  try {
    const { searchBrave } = require('./brave-search');
    const r = await searchBrave(q, { db, count: 6 });
    return { ok: !!r.ok, items: r.results || [], error: r.error };
  } catch (e) { return { ok: false, error: e.message }; }
}
async function runWayback(db, q) {
  try {
    const url = `https://archive.org/wayback/available?url=${encodeURIComponent(q)}`;
    const resp = await fetch(url, { signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (db) await trackApiCall(db, 'deep-phone-research', 'wayback', 0, 0, resp.ok).catch(() => {});
    if (!resp.ok) return { ok: false, error: `http_${resp.status}` };
    const data = await resp.json();
    const snap = data?.archived_snapshots?.closest;
    return snap?.url ? { ok: true, items: [{ title: 'wayback_snapshot', link: snap.url, snippet: snap.timestamp }] } : { ok: false, error: 'no_snapshot' };
  } catch (e) { return { ok: false, error: e.message }; }
}
async function runCourtListener(db, q) {
  try {
    const tok = await getCfg(db, 'courtlistener_api_token', 'COURTLISTENER_API_TOKEN');
    const headers = tok ? { Authorization: `Token ${tok}` } : {};
    const url = `https://www.courtlistener.com/api/rest/v3/search/?q=${encodeURIComponent(q)}&type=r&order_by=dateFiled+desc`;
    const resp = await fetch(url, { headers, signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (db) await trackApiCall(db, 'deep-phone-research', 'courtlistener', 0, 0, resp.ok).catch(() => {});
    if (!resp.ok) return { ok: false, error: `http_${resp.status}` };
    const data = await resp.json();
    const items = (data.results || []).slice(0, 5).map(d => ({
      title: d.caseName || d.case_name || 'case',
      link: d.absolute_url ? `https://www.courtlistener.com${d.absolute_url}` : null,
      snippet: d.snippet || d.description || ''
    }));
    return { ok: items.length > 0, items };
  } catch (e) { return { ok: false, error: e.message }; }
}
async function runGoFundMe(db, q) { return runCse(db, `${q} site:gofundme.com`); }
async function runReddit(db, q) {
  try {
    const url = `https://www.reddit.com/search.json?q=${encodeURIComponent(q)}&limit=10`;
    const resp = await fetch(url, { headers: { 'User-Agent': 'aip-deep-research/1.0' }, signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (db) await trackApiCall(db, 'deep-phone-research', 'reddit', 0, 0, resp.ok).catch(() => {});
    if (!resp.ok) return { ok: false, error: `http_${resp.status}` };
    const data = await resp.json();
    const items = (data?.data?.children || []).slice(0, 6).map(c => ({
      title: c.data?.title, link: 'https://reddit.com' + (c.data?.permalink || ''),
      snippet: (c.data?.selftext || '').slice(0, 240)
    }));
    return { ok: items.length > 0, items };
  } catch (e) { return { ok: false, error: e.message }; }
}
async function runGithub(db, q) {
  try {
    const url = `https://api.github.com/search/users?q=${encodeURIComponent(q)}&per_page=5`;
    const resp = await fetch(url, { signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (db) await trackApiCall(db, 'deep-phone-research', 'github', 0, 0, resp.ok).catch(() => {});
    if (!resp.ok) return { ok: false, error: `http_${resp.status}` };
    const data = await resp.json();
    const items = (data.items || []).slice(0, 5).map(u => ({ title: u.login, link: u.html_url, snippet: u.type }));
    return { ok: items.length > 0, items };
  } catch (e) { return { ok: false, error: e.message }; }
}
async function runFEC(db, person) {
  try {
    const { lookupFEC } = require('./free-osint-extras');
    const r = await lookupFEC(db, { name: person.full_name, state: person.state, city: person.city });
    return { ok: !!r?.ok, items: r?.donations || [], summary: r?.summary || null };
  } catch (e) { return { ok: false, error: e.message }; }
}
async function runOpenCorporates(db, person) {
  try {
    const { lookupOpenCorporates } = require('./free-osint-extras');
    const r = await lookupOpenCorporates(db, { name: person.full_name, state: person.state });
    return { ok: !!r?.ok, items: r?.companies || [], summary: r?.summary || null };
  } catch (e) { return { ok: false, error: e.message }; }
}
async function runHunter(db, person) {
  try {
    const apiKey = await getCfg(db, 'hunter_api_key', 'HUNTER_API_KEY');
    if (!apiKey || !person.employer) return { ok: false, error: 'missing_key_or_employer' };
    const { first, last } = splitName(person.full_name);
    const dom = String(person.employer).toLowerCase().replace(/[^a-z0-9]/g, '') + '.com';
    const url = `https://api.hunter.io/v2/email-finder?domain=${dom}&first_name=${first}&last_name=${last}&api_key=${apiKey}`;
    const resp = await fetch(url, { signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (db) await trackApiCall(db, 'deep-phone-research', 'hunter', 0, 0, resp.ok).catch(() => {});
    if (!resp.ok) return { ok: false, error: `http_${resp.status}` };
    const data = await resp.json();
    return data?.data?.email
      ? { ok: true, items: [{ title: data.data.email, snippet: `score:${data.data.score}` }] }
      : { ok: false, error: 'no_match' };
  } catch (e) { return { ok: false, error: e.message }; }
}
async function runProperty(db, person) {
  try {
    const { lookupOwner } = require('./property-records');
    if (!person.address && !person.city) return { ok: false, error: 'no_address' };
    const r = await lookupOwner({ address: person.address, city: person.city, state: person.state }, db);
    return { ok: !!r?.ok, items: r?.records || [], owner_name: r?.owner_name || null };
  } catch (e) { return { ok: false, error: e.message }; }
}
async function runVoter(db, person) {
  try {
    const { lookupVoter } = require('./voter-rolls');
    const { first, last } = splitName(person.full_name);
    const rows = await lookupVoter(db, first, last, person.state);
    const items = (rows || []).slice(0, 5).map(r => ({
      title: `${r.first_name || ''} ${r.last_name || ''}`.trim(),
      snippet: `${r.residence_address || ''}, ${r.residence_city || ''} ${r.residence_zip || ''}`.trim(),
      address: r.residence_address, city: r.residence_city, zip: r.residence_zip,
      dob: r.dob, party: r.party
    }));
    return { ok: items.length > 0, items };
  } catch (e) { return { ok: false, error: e.message }; }
}
async function runFuneral(db, person) {
  try {
    const { resolveOne } = require('./funeral-home-survivors');
    const r = await resolveOne(db, { full_name: person.full_name, city: person.city, state: person.state });
    return { ok: !!r?.ok, items: r?.survivors || [], obit_url: r?.obit_url || null };
  } catch (e) { return { ok: false, error: e.message }; }
}
async function runFamily(db, person) {
  try {
    if (!person.id) return { ok: false, error: 'no_person_id' };
    const { processDeceased } = require('./family-tree');
    const r = await processDeceased(db, person);
    return { ok: !!r?.ok, items: r?.relatives || [] };
  } catch (e) { return { ok: false, error: e.message }; }
}
async function runNewsArchive(db, person) {
  try {
    const { lookupNewsArchive } = require('./free-osint-extras');
    const r = await lookupNewsArchive(db, { name: person.full_name, city: person.city, state: person.state });
    return { ok: !!r?.ok, items: r?.articles || [] };
  } catch (e) { return { ok: false, error: e.message }; }
}

const ENGINE_DISPATCH = {
  cse:            (db, h, p) => runCse(db, h.query),
  brave:          (db, h, p) => runBrave(db, h.query),
  wayback:        (db, h, p) => runWayback(db, h.query),
  courtlistener:  (db, h, p) => runCourtListener(db, h.query),
  gofundme:       (db, h, p) => runGoFundMe(db, h.query),
  reddit:         (db, h, p) => runReddit(db, h.query),
  github:         (db, h, p) => runGithub(db, h.query),
  fec:            (db, h, p) => runFEC(db, p),
  opencorporates: (db, h, p) => runOpenCorporates(db, p),
  hunter:         (db, h, p) => runHunter(db, p),
  property:       (db, h, p) => runProperty(db, p),
  voter:          (db, h, p) => runVoter(db, p),
  funeral:        (db, h, p) => runFuneral(db, p),
  family:         (db, h, p) => runFamily(db, p),
  news_archive:   (db, h, p) => runNewsArchive(db, p)
};

// Engine aliases — Opus may suggest "facebook" / "linkedin" / "twitter" etc.
// Route them to CSE with site:-prefixed queries.
const ENGINE_ALIASES = {
  facebook:  (q) => `${q} site:facebook.com`,
  linkedin:  (q) => `${q} site:linkedin.com`,
  twitter:   (q) => `${q} site:twitter.com OR site:x.com`,
  instagram: (q) => `${q} site:instagram.com`,
  obituary:  (q) => `${q} obituary`,
  truepeoplesearch: (q) => `${q} site:truepeoplesearch.com`,
  fastpeoplesearch: (q) => `${q} site:fastpeoplesearch.com`,
  spokeo: (q) => `${q} site:spokeo.com`
};
for (const [alias, mapQuery] of Object.entries(ENGINE_ALIASES)) {
  ENGINE_DISPATCH[alias] = (db, h, p) => runCse(db, mapQuery(h.query));
}

async function executeHypotheses(db, person, ranked, budgetMs = 60000) {
  const top = ranked.slice(0, 3);
  const start = Date.now();
  const out = [];
  await Promise.all(top.map(async h => {
    const dispatch = ENGINE_DISPATCH[(h.engine || '').toLowerCase()];
    if (!dispatch) { out.push({ ...h, ok: false, error: 'unknown_engine' }); return; }
    try {
      const left = budgetMs - (Date.now() - start);
      if (left <= 0) { out.push({ ...h, ok: false, error: 'budget_exhausted' }); return; }
      const guarded = await Promise.race([
        dispatch(db, h, person),
        new Promise(resolve => setTimeout(() => resolve({ ok: false, error: 'soft_timeout' }), Math.min(left, 25000)))
      ]);
      out.push({ ...h, ok: !!guarded.ok, error: guarded.error || null, data: guarded });
    } catch (e) { out.push({ ...h, ok: false, error: e.message }); }
  }));
  return out;
}

const SYNTHESIZE_SYSTEM = `You are the same OSINT analyst. You ran 3 searches. Reason whether ANY result is a confident contact-info hit for the SAME person (not a namesake).

Cross-validation rules:
- If result city != incident city != adjacent metro -> likely wrong person
- If phone area code != victim state -> flag
- If email domain matches employer -> boost
- If 2+ independent results agree on same value -> +20 confidence
- Different age/race/ethnicity -> reject
- Prefer family contacts over fake-looking phone strings

Return JSON only:
{
  "reasoning": "step-by-step interpretation",
  "found_phone": null | { "value": "+1XXX...", "confidence_pct": int, "source": "engine+url", "cross_validated_by": ["..."], "reasoning": "..." },
  "found_email": null | { "value": "...", "confidence_pct": int, "source": "...", "reasoning": "..." },
  "found_address": null | { "street": "...", "city": "...", "state": "...", "zip": "...", "confidence_pct": int, "source": "..." },
  "family_contacts": [ { "name":"...", "relationship":"...", "phone":null|"...", "email":null|"...", "source":"..." } ],
  "next_iteration": { "continue": bool, "hypotheses": [ ... ] },
  "cumulative_confidence": int(0-100),
  "stop_reason": null | "found"|"plateau"|"budget"|"no_more_hypotheses"
}`;

async function opusSynthesize(db, facts, executed, history) {
  const compact = executed.map(e => ({
    id: e.id, engine: e.engine, query: e.query, expected: e.expected_yield,
    ok: e.ok, error: e.error,
    items: (e.data?.items || []).slice(0, 6),
    summary: e.data?.summary || null,
    obit_url: e.data?.obit_url || null,
    owner_name: e.data?.owner_name || null
  }));
  const userPrompt = `Original known facts:\n${compactFactsForPrompt(facts, 8000)}

Search results from 3 hypotheses (this iteration):
${JSON.stringify(compact, null, 2).slice(0, 14000)}

Iteration history (cumulative confidence trend): ${JSON.stringify(history.map(h => ({ iter: h.iter, conf: h.cumulative_confidence })))}

Reason about each result. Did anything surface a phone/email/address CONSISTENT with the known incident? Is it THIS victim or a namesake? Are 2 results agreeing? What should I search NEXT? Return JSON.`;
  const r = await extract(db, {
    pipeline: 'deep-phone-research:synthesize',
    systemPrompt: SYNTHESIZE_SYSTEM, userPrompt,
    provider: 'auto', tier: 'premium',
    timeoutMs: 35000, responseFormat: 'json', temperature: 0.1
  });
  if (!r.ok) return { ok: false, error: r.error, attempts: r.attempts };
  return { ok: true, parsed: r.parsed || {}, raw: r.content, model: r.model_used,
           tokens_in: r.tokens_in, tokens_out: r.tokens_out };
}

function crossValidate(synth, facts) {
  const out = { phone: null, email: null, address: null, conflicts: [], boosts: [] };
  const partials = facts.partials;
  if (synth.found_phone?.value) {
    let conf = clamp(synth.found_phone.confidence_pct, 0, 100);
    const acMatch = areaCodeMatchesState(synth.found_phone.value, partials.state);
    if (acMatch === true)  { conf = clamp(conf + 10, 0, 100); out.boosts.push('area_code_state_match'); }
    if (acMatch === false) { conf = clamp(conf - 15, 0, 100); out.conflicts.push('area_code_state_mismatch'); }
    if ((synth.found_phone.cross_validated_by || []).length >= 2) {
      conf = clamp(conf + 20, 0, 100); out.boosts.push('two_source_agreement_phone');
    }
    out.phone = { ...synth.found_phone, confidence_pct: conf };
  }
  if (synth.found_email?.value) {
    let conf = clamp(synth.found_email.confidence_pct, 0, 100);
    const dom = String(synth.found_email.value).split('@')[1] || '';
    if (partials.employer && dom && new RegExp(String(partials.employer).split(/\s+/)[0], 'i').test(dom)) {
      conf = clamp(conf + 15, 0, 100); out.boosts.push('email_domain_employer_match');
    }
    out.email = { ...synth.found_email, confidence_pct: conf };
  }
  if (synth.found_address?.street) {
    let conf = clamp(synth.found_address.confidence_pct, 0, 100);
    if (partials.city && synth.found_address.city &&
        String(partials.city).toLowerCase() !== String(synth.found_address.city).toLowerCase()) {
      conf = clamp(conf - 10, 0, 100); out.conflicts.push('address_city_mismatch');
    }
    out.address = { ...synth.found_address, confidence_pct: conf };
  }
  return out;
}

async function persistFinding(db, facts, validated, trace) {
  const personId = facts.person.id;
  const update = {};
  if (personId && validated.phone && validated.phone.confidence_pct >= MIN_STORE_CONF && !facts.partials.phone) {
    update.phone = digitsOnly(validated.phone.value);
  }
  if (personId && validated.email && validated.email.confidence_pct >= MIN_STORE_CONF && !facts.partials.email) {
    update.email = String(validated.email.value).toLowerCase();
  }
  if (personId && validated.address && validated.address.confidence_pct >= MIN_STORE_CONF && !facts.partials.address) {
    update.address = validated.address.street;
    if (validated.address.city)  update.city  = validated.address.city;
    if (validated.address.state) update.state = validated.address.state;
    if (validated.address.zip)   update.zip   = validated.address.zip;
  }
  let applied = false;
  if (personId && Object.keys(update).length) {
    update.updated_at = new Date();
    try { await db('persons').where('id', personId).update(update); applied = true; }
    catch (e) { trace.errors.push('persist_persons:' + e.message); }
  }
  if (personId) {
    try {
      await db('enrichment_logs').insert({
        person_id: personId,
        field_name: 'deep-phone-research:trace',
        old_value: null,
        new_value: JSON.stringify({
          phone: validated.phone?.value || null,
          email: validated.email?.value || null,
          address: validated.address?.street || null,
          conflicts: validated.conflicts, boosts: validated.boosts
        }).slice(0, 4000),
        source: 'deep-phone-research',
        confidence: Math.max(
          validated.phone?.confidence_pct || 0,
          validated.email?.confidence_pct || 0,
          validated.address?.confidence_pct || 0
        ),
        verified: false,
        data: JSON.stringify({ deep_research_trace: trace, validated, applied }).slice(0, 12000),
        created_at: new Date()
      });
    } catch (e) { trace.errors.push('persist_log:' + e.message); }
  }
  if (applied && personId) {
    try {
      await enqueueCascade(db, {
        person_id: personId, incident_id: facts.person.incident_id,
        trigger_source: 'deep-phone-research',
        trigger_field: Object.keys(update).filter(k => k !== 'updated_at').join(','),
        trigger_value: '1', priority: 9
      });
    } catch (_) {}
  }
  return { applied, fields: Object.keys(update).filter(k => k !== 'updated_at') };
}

function orderHypotheses(list, priorityIds) {
  const byId = new Map(list.map(h => [h.id, h]));
  const out = [];
  for (const id of priorityIds || []) if (byId.has(id)) { out.push(byId.get(id)); byId.delete(id); }
  const rest = Array.from(byId.values()).sort((a, b) => (b.confidence_if_hit || 0) - (a.confidence_if_hit || 0));
  return [...out, ...rest];
}

async function researchOne(db, opts) {
  const t0 = Date.now();
  const trace = { started_at: new Date().toISOString(), iterations: [], errors: [], final: null };
  const g = await gatherFacts(db, opts);
  if (!g.ok) return { ok: false, error: g.error, trace };
  const facts = g.facts;
  trace.subject = facts.partials;

  const history = [];
  let prevConf = 0;
  let lastSynth = null, lastValidated = null, lastExecuted = [], lastHypoth = null;
  let stopReason = null;

  for (let iter = 1; iter <= ITERATION_CAP; iter++) {
    if (Date.now() - t0 > PERSON_BUDGET_MS) { stopReason = 'budget'; break; }
    const itTrace = { iter, started_at: new Date().toISOString() };
    const hypoth = await opusHypothesize(db, facts, history);
    lastHypoth = hypoth;
    if (!hypoth.ok) {
      itTrace.error = 'hypothesize_failed:' + hypoth.error; itTrace.attempts = hypoth.attempts;
      trace.iterations.push(itTrace); stopReason = 'hypothesize_error'; break;
    }
    itTrace.hypotheses = (hypoth.parsed.hypotheses || []).slice(0, 8);
    itTrace.priority_order = hypoth.parsed.priority_order || [];
    itTrace.opus_reasoning = hypoth.parsed.reasoning || '';
    itTrace.tokens_in = hypoth.tokens_in; itTrace.tokens_out = hypoth.tokens_out;

    const ordered = orderHypotheses(hypoth.parsed.hypotheses || [], hypoth.parsed.priority_order || []);
    const remainingBudget = Math.max(15000, PERSON_BUDGET_MS - (Date.now() - t0) - 25000);
    const executed = await executeHypotheses(db, facts.person, ordered, Math.min(remainingBudget, 60000));
    lastExecuted = executed;
    itTrace.executed = executed.map(e => ({
      id: e.id, engine: e.engine, query: e.query, ok: e.ok, error: e.error,
      preview: (e.data?.items || []).slice(0, 3)
    }));

    const synth = await opusSynthesize(db, facts, executed, history);
    lastSynth = synth;
    if (!synth.ok) {
      itTrace.error = (itTrace.error ? itTrace.error + ';' : '') + 'synthesize_failed:' + synth.error;
      trace.iterations.push(itTrace); stopReason = 'synthesize_error'; break;
    }
    const parsed = synth.parsed || {};
    itTrace.synth_reasoning = parsed.reasoning || '';
    itTrace.found_phone = parsed.found_phone || null;
    itTrace.found_email = parsed.found_email || null;
    itTrace.found_address = parsed.found_address || null;
    itTrace.family_contacts = parsed.family_contacts || [];
    itTrace.cumulative_confidence = parsed.cumulative_confidence || 0;
    itTrace.tokens_in_synth = synth.tokens_in; itTrace.tokens_out_synth = synth.tokens_out;

    const validated = crossValidate(parsed, facts);
    lastValidated = validated;
    itTrace.validated = validated;

    history.push({ iter, cumulative_confidence: parsed.cumulative_confidence || 0 });
    trace.iterations.push(itTrace);

    const phoneConf = validated.phone?.confidence_pct || 0;
    const emailConf = validated.email?.confidence_pct || 0;
    if (phoneConf >= STOP_CONF || emailConf >= STOP_CONF) { stopReason = 'found_high_conf'; break; }
    const cumConf = parsed.cumulative_confidence || 0;
    if (iter >= 2 && Math.abs(cumConf - prevConf) < 5) { stopReason = 'plateau'; break; }
    if (parsed.next_iteration && parsed.next_iteration.continue === false) { stopReason = 'opus_says_stop'; break; }
    prevConf = cumConf;
  }
  if (!stopReason) stopReason = 'iteration_cap';

  let persistResult = { applied: false, fields: [] };
  if (lastValidated && facts.person.id) {
    persistResult = await persistFinding(db, facts, lastValidated, trace);
  }

  trace.final = {
    stop_reason: stopReason, duration_ms: Date.now() - t0,
    persist: persistResult, validated: lastValidated,
    last_synthesizer_reasoning: lastSynth?.parsed?.reasoning || null
  };
  return {
    ok: true, person_id: facts.person.id, full_name: facts.person.full_name,
    found: {
      phone: lastValidated?.phone || null,
      email: lastValidated?.email || null,
      address: lastValidated?.address || null,
      family_contacts: lastSynth?.parsed?.family_contacts || []
    },
    iterations_run: trace.iterations.length,
    stop_reason: stopReason,
    persisted: persistResult,
    duration_ms: Date.now() - t0,
    trace
  };
}

async function batchResearch(db, { limit = 3 } = {}) {
  const lim = Math.max(1, Math.min(parseInt(limit) || 3, 10));
  const candidates = await db('persons')
    .where(function () { this.whereNull('phone').orWhere('phone', ''); })
    .andWhere(function () { this.whereNull('email').orWhere('email', ''); })
    .andWhereNotNull('full_name')
    .andWhere(function () { this.where('victim_verified', true).orWhereNotNull('role'); })
    .orderBy('updated_at', 'desc').limit(lim)
    .select('id','full_name','city','state').catch(() => []);
  const results = [];
  for (const c of candidates) {
    try {
      const r = await researchOne(db, { person_id: c.id });
      results.push({
        person_id: c.id, name: c.full_name, ok: !!r.ok,
        applied: r.persisted?.applied, fields: r.persisted?.fields || [],
        phone: r.found?.phone?.value || null, email: r.found?.email?.value || null,
        stop_reason: r.stop_reason, duration_ms: r.duration_ms
      });
    } catch (e) {
      results.push({ person_id: c.id, name: c.full_name, ok: false, error: e.message });
    }
  }
  return { count: results.length, results };
}

async function health(db) {
  const checks = {};
  for (const k of [
    ['anthropic_api_key', 'ANTHROPIC_API_KEY'],
    ['google_cse', 'GOOGLE_CSE_API_KEY'],
    ['brave_api_key', 'BRAVE_API_KEY'],
    ['hunter_api_key', 'HUNTER_API_KEY'],
    ['courtlistener_api_token', 'COURTLISTENER_API_TOKEN']
  ]) checks[k[0]] = !!(await getCfg(db, k[0], k[1]));
  return {
    ok: true, service: 'deep-phone-research',
    iteration_cap: ITERATION_CAP, person_budget_ms: PERSON_BUDGET_MS,
    min_store_conf: MIN_STORE_CONF, stop_conf: STOP_CONF,
    engines: Object.keys(ENGINE_DISPATCH), keys_present: checks
  };
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
  try {
    if (action === 'health') {
      const h = await health(db);
      return res.status(200).json({ success: true, ...h });
    }
    if (action === 'research') {
      const opts = {
        person_id: req.query?.person_id || req.query?.id || null,
        victim_name: req.query?.victim_name || req.query?.name || null,
        full_name: req.query?.full_name || null,
        city: req.query?.city || null, state: req.query?.state || null
      };
      if (!opts.person_id && !opts.victim_name && !opts.full_name) {
        return res.status(400).json({ error: 'person_id or victim_name required' });
      }
      const r = await researchOne(db, opts);
      await trackApiCall(db, 'deep-phone-research', 'research', 0, 0, !!r.ok).catch(() => {});
      return res.status(200).json({ success: !!r.ok, ...r });
    }
    if (action === 'batch') {
      const limit = req.query?.limit || 3;
      const r = await batchResearch(db, { limit });
      await trackApiCall(db, 'deep-phone-research', 'batch', 0, 0, true).catch(() => {});
      return res.status(200).json({ success: true, ...r });
    }
    return res.status(400).json({ error: 'unknown_action', valid: ['health','research','batch'] });
  } catch (e) {
    try { await reportError(db, 'deep-phone-research', null, e.message, { severity: 'error' }); } catch (_) {}
    return res.status(500).json({ success: false, error: e.message });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.researchOne = researchOne;
module.exports.batchResearch = batchResearch;
module.exports.health = health;
module.exports.gatherFacts = gatherFacts;
module.exports.crossValidate = crossValidate;
module.exports.ENGINE_DISPATCH = ENGINE_DISPATCH;
