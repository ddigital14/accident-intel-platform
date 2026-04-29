/**
 * HOMEGROWN OSINT MINER — Phase 42
 *
 * The single composite engine that uses the WHOLE PUBLIC WEB + AI reasoning
 * to find what off-the-shelf APIs (Trestle/Apollo/PDL) missed for accident
 * victims. Trestle denied AIP. Apollo + PDL Pro both return empty for
 * accident victims (B2B databases). This engine substitutes for them by
 * fanning out across 12 free OSINT signals + Claude Opus synthesis.
 *
 * GET /api/v1/enrich/homegrown-osint-miner?secret=ingest-now&action=health
 * GET /api/v1/enrich/homegrown-osint-miner?secret=ingest-now&action=mine&person_id=<uuid>
 * GET /api/v1/enrich/homegrown-osint-miner?secret=ingest-now&action=batch&limit=3
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');
const { enqueueCascade } = require('../system/_cascade');
const { extract, MODELS } = require('./_ai_router');

const SECRET = 'ingest-now';
const HTTP_TIMEOUT_MS = 15000;
const PERSON_BUDGET_MS = 50000;
const MAX_BATCH = 2;

const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

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

function digitsOnly(s) { return String(s || '').replace(/\D+/g, ''); }

const PHONE_RE = /\b\(?(\d{3})\)?[\s.\-]?(\d{3})[\s.\-]?(\d{4})\b/g;
const EMAIL_RE = /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b/g;
const ADDR_RE = /\b\d{2,5}\s+[A-Z][A-Za-z]{1,}(?:\s+[A-Za-z]{1,}){0,3}\s+(?:St|Ave|Rd|Blvd|Dr|Way|Ln|Ct|Cir|Pl|Hwy|Pkwy|Pike|Trail|Terrace|Ter)\b\.?/g;

function extractContacts(text) {
  const t = String(text || '');
  const phones = new Set();
  const emails = new Set();
  const addresses = [];
  PHONE_RE.lastIndex = 0;
  let m;
  while ((m = PHONE_RE.exec(t)) !== null) {
    if (['000', '111', '555', '888', '877', '866', '800'].includes(m[1])) continue;
    phones.add(`${m[1]}-${m[2]}-${m[3]}`);
    if (phones.size >= 8) break;
  }
  EMAIL_RE.lastIndex = 0;
  while ((m = EMAIL_RE.exec(t)) !== null) {
    const e = m[0].toLowerCase();
    if (e.includes('example.com') || e.endsWith('.png') || e.endsWith('.jpg')) continue;
    if (/^(support|info|contact|hello|admin|noreply|no-reply|webmaster|press|editor|abuse|legal|privacy)@/i.test(e)) continue;
    emails.add(e);
    if (emails.size >= 6) break;
  }
  ADDR_RE.lastIndex = 0;
  while ((m = ADDR_RE.exec(t)) !== null) {
    addresses.push(m[0].trim());
    if (addresses.length >= 5) break;
  }
  return { phones: [...phones], emails: [...emails], addresses };
}

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

async function fetchWithRetry(url, opts = {}, retries = 1) {
  const o = { ...opts };
  delete o.timeoutMs;
  o.signal = AbortSignal.timeout(opts.timeoutMs || HTTP_TIMEOUT_MS);
  o.headers = { 'User-Agent': UA, ...(opts.headers || {}) };
  try {
    const r = await fetch(url, o);
    if ((r.status === 503 || r.status === 429) && retries > 0) {
      await new Promise(res => setTimeout(res, 1500));
      return fetchWithRetry(url, opts, retries - 1);
    }
    return r;
  } catch (e) {
    if (retries > 0) {
      await new Promise(res => setTimeout(res, 1000));
      return fetchWithRetry(url, opts, retries - 1);
    }
    throw e;
  }
}

// ============================================================================
// SIGNAL 1: Google CSE deep search (9 targeted queries)
// ============================================================================
async function srcGoogleCseDeep(db, person) {
  const cfg = await loadCseCfg(db);
  if (!cfg) return { ok: false, error: 'no_cse' };
  const name = person.full_name;
  const city = person.city || '';
  const state = person.state || '';
  const queries = [
    { tag: 'obituary', q: `"${name}" "${city || state}" obituary` },
    { tag: 'linkedin', q: `"${name}" "${city || state}" linkedin` },
    { tag: 'facebook', q: `"${name}" "${city || state}" facebook` },
    { tag: 'twitter',  q: `"${name}" "${city || state}" twitter OR x.com` },
    { tag: 'address',  q: `"${name}" "${state}" address` },
    { tag: 'phone',    q: `"${name}" "${city || state}" phone OR contact` },
    { tag: 'gofundme', q: `"${name}" "${city || state}" gofundme OR caringbridge` },
    { tag: 'attorney', q: `"${name}" "${city || state}" attorney OR lawsuit` },
    { tag: 'court',    q: `"${name}" "${city || state}" court records` }
  ];
  const out = { hits_by_tag: {}, all_phones: [], all_emails: [], snippets: [], links: [], queries_run: 0, queries_ok: 0 };
  const cseRuns = await Promise.all(queries.map(async Q => {
    try {
      const url = `https://www.googleapis.com/customsearch/v1?key=${cfg.key}&cx=${cfg.cx}&q=${encodeURIComponent(Q.q)}&num=4`;
      const r = await fetchWithRetry(url, { timeoutMs: 6000 });
      if (!r.ok) return { tag: Q.tag, ok: false, status: r.status };
      const d = await r.json().catch(() => null);
      return { tag: Q.tag, ok: true, items: d?.items || [] };
    } catch (e) { return { tag: Q.tag, ok: false, err: e.message }; }
  }));
  for (const run of cseRuns) {
    out.queries_run++;
    if (!run.ok) { out.hits_by_tag[run.tag] = { error: run.err || `http_${run.status}` }; continue; }
    out.queries_ok++;
    const tagHits = [];
    for (const it of run.items) {
      const blob = `${it.title || ''} ${it.snippet || ''}`;
      const c = extractContacts(blob);
      out.all_phones.push(...c.phones);
      out.all_emails.push(...c.emails);
      out.snippets.push({ tag: run.tag, title: it.title, snippet: it.snippet, link: it.link });
      out.links.push(it.link);
      tagHits.push({ link: it.link, title: it.title, snippet: it.snippet });
    }
    out.hits_by_tag[run.tag] = { count: run.items.length, items: tagHits.slice(0, 3) };
  }
  return { ok: out.queries_ok > 0, data: out };
}

// ============================================================================
// SIGNAL 2: Wayback Machine
// ============================================================================
async function srcWayback(db, person) {
  const out = { archived_urls: [], extracted: { phones: [], emails: [], addresses: [] } };
  const slug = person.full_name.replace(/\s+/g, '').toLowerCase();
  const dotted = person.full_name.replace(/\s+/g, '.').toLowerCase();
  const dashed = person.full_name.replace(/\s+/g, '-').toLowerCase();
  const candidates = [
    `https://www.facebook.com/${dotted}`,
    `https://twitter.com/${slug}`,
    `https://www.linkedin.com/in/${dashed}`,
    `https://${slug}.com`
  ];
  for (const u of candidates) {
    try {
      const apiUrl = `https://archive.org/wayback/available?url=${encodeURIComponent(u)}`;
      const r = await fetchWithRetry(apiUrl, { timeoutMs: 6000 });
      if (!r.ok) continue;
      const d = await r.json().catch(() => null);
      const snap = d?.archived_snapshots?.closest;
      if (snap?.url) {
        out.archived_urls.push({ original: u, archived: snap.url, ts: snap.timestamp });
        try {
          const a = await fetchWithRetry(snap.url, { timeoutMs: 8000 });
          if (a.ok) {
            const html = (await a.text()).slice(0, 80000);
            const text = html.replace(/<script[\s\S]*?<\/script>/gi, ' ').replace(/<style[\s\S]*?<\/style>/gi, ' ').replace(/<[^>]+>/g, ' ');
            const c = extractContacts(text);
            out.extracted.phones.push(...c.phones);
            out.extracted.emails.push(...c.emails);
            out.extracted.addresses.push(...c.addresses);
          }
        } catch (_) {}
      }
    } catch (_) {}
  }
  return { ok: out.archived_urls.length > 0, data: out };
}

// ============================================================================
// SIGNAL 3: DuckDuckGo HTML fallback
// ============================================================================
async function srcDuckDuckGo(db, person) {
  const out = { results: [], snippets: [], extracted: { phones: [], emails: [] } };
  const q = `"${person.full_name}" "${person.city || person.state || ''}"`.trim();
  try {
    const url = `https://html.duckduckgo.com/html/?q=${encodeURIComponent(q)}`;
    const r = await fetchWithRetry(url, { timeoutMs: 8000, headers: { 'Accept': 'text/html' } });
    if (!r.ok) return { ok: false, status: r.status };
    const html = (await r.text()).slice(0, 120000);
    const linkRx = /<a[^>]+class="result__a"[^>]+href="([^"]+)"[^>]*>([^<]+)<\/a>/g;
    const snipRx = /<a[^>]+class="result__snippet"[^>]*>([\s\S]*?)<\/a>/g;
    let m;
    while ((m = linkRx.exec(html)) !== null && out.results.length < 8) {
      out.results.push({ url: m[1], title: m[2].replace(/<[^>]+>/g, '').trim() });
    }
    const snippets = [];
    while ((m = snipRx.exec(html)) !== null) {
      const s = m[1].replace(/<[^>]+>/g, '').trim();
      if (s) snippets.push(s);
    }
    const blob = snippets.join(' ');
    const c = extractContacts(blob);
    out.extracted.phones = c.phones;
    out.extracted.emails = c.emails;
    out.snippets = snippets.slice(0, 8);
    return { ok: out.results.length > 0, data: out };
  } catch (e) { return { ok: false, error: e.message }; }
}

// ============================================================================
// SIGNAL 4: Email permutator + Hunter verify
// ============================================================================
function permuteEmails(first, last, domain) {
  if (!first || !last || !domain) return [];
  const f = first.toLowerCase().replace(/[^a-z]/g, '');
  const l = last.toLowerCase().replace(/[^a-z]/g, '');
  const d = domain.toLowerCase().replace(/^https?:\/\//, '').replace(/^www\./, '').replace(/\/.*$/, '');
  return [
    `${f}.${l}@${d}`, `${f}${l}@${d}`, `${f[0]}${l}@${d}`, `${f}@${d}`, `${l}@${d}`,
    `${f[0]}.${l}@${d}`, `${l}.${f}@${d}`, `${f}_${l}@${d}`, `${f}-${l}@${d}`
  ];
}

async function hunterVerify(apiKey, email) {
  try {
    const url = `https://api.hunter.io/v2/email-verifier?email=${encodeURIComponent(email)}&api_key=${apiKey}`;
    const r = await fetchWithRetry(url, { timeoutMs: 8000 });
    if (!r.ok) return { verified: false, status: r.status };
    const d = await r.json().catch(() => null);
    const data = d?.data || {};
    return {
      verified: data.status === 'valid' || data.status === 'accept_all',
      status: data.status, score: data.score, smtp_check: data.smtp_check, result: data.result
    };
  } catch (_) { return { verified: false }; }
}

async function srcEmailPermutator(db, person) {
  const { first, last } = splitName(person.full_name);
  if (!first || !last) return { ok: false, error: 'no_name_split' };
  const domain = person.employer_domain || (person.employer ? `${String(person.employer).toLowerCase().replace(/\s+/g, '')}.com` : null);
  if (!domain) return { ok: false, error: 'no_employer_domain' };
  const candidates = permuteEmails(first, last, domain);
  const apiKey = await getCfg(db, 'hunter_api_key', 'HUNTER_API_KEY');
  const out = { domain, candidates, verified: [] };
  if (!apiKey) return { ok: true, data: { ...out, note: 'unverified_no_hunter_key' } };
  for (const email of candidates.slice(0, 6)) {
    const v = await hunterVerify(apiKey, email);
    if (v.verified || (v.score || 0) >= 50) out.verified.push({ email, ...v });
  }
  return { ok: out.verified.length > 0, data: out };
}

// ============================================================================
// SIGNAL 5: Reddit
// ============================================================================
async function srcReddit(db, person) {
  const q = `${person.full_name} ${person.city || ''}`.trim();
  try {
    const url = `https://www.reddit.com/search.json?q=${encodeURIComponent(q)}&limit=10&sort=relevance`;
    const r = await fetchWithRetry(url, { timeoutMs: 8000, headers: { 'Accept': 'application/json' } });
    if (!r.ok) return { ok: false, status: r.status };
    const d = await r.json().catch(() => null);
    const posts = d?.data?.children || [];
    const out = { posts: [], usernames: [], extracted: { phones: [], emails: [] } };
    const u = new Set();
    for (const p of posts.slice(0, 10)) {
      const dat = p.data || {};
      out.posts.push({
        title: dat.title, author: dat.author, subreddit: dat.subreddit,
        url: `https://reddit.com${dat.permalink || ''}`,
        snippet: (dat.selftext || '').slice(0, 200)
      });
      if (dat.author && dat.author !== '[deleted]') u.add(dat.author);
      const c = extractContacts(`${dat.title || ''} ${dat.selftext || ''}`);
      out.extracted.phones.push(...c.phones);
      out.extracted.emails.push(...c.emails);
    }
    out.usernames = [...u];
    return { ok: out.posts.length > 0, data: out };
  } catch (e) { return { ok: false, error: e.message }; }
}

// ============================================================================
// SIGNAL 6: GitHub
// ============================================================================
async function srcGitHub(db, person) {
  const { first, last } = splitName(person.full_name);
  if (!first || !last) return { ok: false, error: 'no_name_split' };
  const loc = person.city || person.state || '';
  const q = `"${first} ${last}"${loc ? ` location:"${loc}"` : ''}`;
  try {
    const url = `https://api.github.com/search/users?q=${encodeURIComponent(q)}&per_page=5`;
    const r = await fetchWithRetry(url, { timeoutMs: 8000, headers: { 'Accept': 'application/vnd.github+json' } });
    if (!r.ok) return { ok: false, status: r.status };
    const d = await r.json().catch(() => null);
    const items = d?.items || [];
    const out = { users: [] };
    for (const u of items.slice(0, 3)) {
      try {
        const profileR = await fetchWithRetry(`https://api.github.com/users/${u.login}`, { timeoutMs: 6000 });
        if (profileR.ok) {
          const profile = await profileR.json().catch(() => ({}));
          out.users.push({
            login: u.login, name: profile.name, email: profile.email,
            location: profile.location, company: profile.company,
            blog: profile.blog, html_url: profile.html_url
          });
        }
      } catch (_) {}
    }
    return { ok: out.users.length > 0, data: out };
  } catch (e) { return { ok: false, error: e.message }; }
}

// ============================================================================
// SIGNAL 7: LinkedIn URL inference + public preview
// ============================================================================
async function srcLinkedIn(db, person) {
  const cfg = await loadCseCfg(db);
  const out = { profiles: [], extracted: { snippets: [] } };
  if (cfg) {
    try {
      const q = `site:linkedin.com/in "${person.full_name}" ${person.city || person.state || ''}`;
      const url = `https://www.googleapis.com/customsearch/v1?key=${cfg.key}&cx=${cfg.cx}&q=${encodeURIComponent(q)}&num=3`;
      const r = await fetchWithRetry(url, { timeoutMs: 8000 });
      if (r.ok) {
        const d = await r.json().catch(() => null);
        for (const it of (d?.items || [])) {
          out.profiles.push({ url: it.link, title: it.title, snippet: it.snippet });
          out.extracted.snippets.push(it.snippet || '');
        }
      }
    } catch (_) {}
  }
  if (out.profiles.length) {
    try {
      const r = await fetchWithRetry(out.profiles[0].url, { timeoutMs: 8000, headers: { 'Accept': 'text/html' } });
      if (r.ok) {
        const html = (await r.text()).slice(0, 60000);
        const title = (html.match(/<title>([^<]+)<\/title>/i) || [])[1];
        const desc = (html.match(/<meta\s+name="description"\s+content="([^"]+)"/i) || [])[1];
        const og = (html.match(/<meta\s+property="og:description"\s+content="([^"]+)"/i) || [])[1];
        out.preview = { title, description: desc, og_description: og };
      }
    } catch (_) {}
  }
  return { ok: out.profiles.length > 0, data: out };
}

// ============================================================================
// SIGNAL 8: Bankruptcy via CSE
// ============================================================================
async function srcBankruptcy(db, person) {
  const cfg = await loadCseCfg(db);
  if (!cfg) return { ok: false, error: 'no_cse' };
  const q = `"${person.full_name}" bankruptcy ${person.city || person.state || ''}`;
  try {
    const url = `https://www.googleapis.com/customsearch/v1?key=${cfg.key}&cx=${cfg.cx}&q=${encodeURIComponent(q)}&num=3`;
    const r = await fetchWithRetry(url, { timeoutMs: 8000 });
    if (!r.ok) return { ok: false, status: r.status };
    const d = await r.json().catch(() => null);
    const items = d?.items || [];
    const out = { filings: items.map(it => ({ link: it.link, title: it.title, snippet: it.snippet })) };
    return { ok: out.filings.length > 0, data: out };
  } catch (e) { return { ok: false, error: e.message }; }
}

// ============================================================================
// SIGNAL 9: CourtListener
// ============================================================================
async function srcCourtListener(db, person) {
  const token = await getCfg(db, 'courtlistener_api_token', 'COURTLISTENER_API_TOKEN');
  try {
    const params = new URLSearchParams({ q: `"${person.full_name}"`, type: 'r', order_by: 'dateFiled desc' });
    const headers = { 'Accept': 'application/json' };
    if (token) headers['Authorization'] = `Token ${token}`;
    const r = await fetchWithRetry(`https://www.courtlistener.com/api/rest/v3/search/?${params.toString()}`, { headers, timeoutMs: 10000 });
    if (!r.ok) return { ok: false, status: r.status };
    const d = await r.json().catch(() => null);
    const results = d?.results || [];
    const out = { cases: [], extracted: { addresses: [] } };
    for (const c of results.slice(0, 5)) {
      out.cases.push({ caseName: c.caseName, court: c.court, dateFiled: c.dateFiled, snippet: c.snippet });
      const c2 = extractContacts(`${c.caseName || ''} ${c.snippet || ''}`);
      out.extracted.addresses.push(...c2.addresses);
    }
    return { ok: results.length > 0, data: out };
  } catch (e) { return { ok: false, error: e.message }; }
}

// ============================================================================
// SIGNAL 10: News archive deep search
// ============================================================================
async function srcNewsArchive(db, person) {
  const cfg = await loadCseCfg(db);
  if (!cfg) return { ok: false, error: 'no_cse' };
  const after = person.accident_date ? new Date(person.accident_date).toISOString().slice(0, 10) : null;
  const q = `"${person.full_name}" ${person.city || person.state || ''}${after ? ` after:${after}` : ''} -site:obituaries -site:legacy.com`;
  try {
    const url = `https://www.googleapis.com/customsearch/v1?key=${cfg.key}&cx=${cfg.cx}&q=${encodeURIComponent(q)}&num=5`;
    const r = await fetchWithRetry(url, { timeoutMs: 8000 });
    if (!r.ok) return { ok: false, status: r.status };
    const d = await r.json().catch(() => null);
    const items = d?.items || [];
    const out = { articles: [], extracted: { phones: [], emails: [], family: [] } };
    for (const it of items) {
      out.articles.push({ link: it.link, title: it.title, snippet: it.snippet });
      const blob = `${it.title || ''} ${it.snippet || ''}`;
      const c = extractContacts(blob);
      out.extracted.phones.push(...c.phones);
      out.extracted.emails.push(...c.emails);
      const fam = blob.match(/(?:husband|wife|son|daughter|father|mother|brother|sister|widow)\s+([A-Z][a-zA-Z'\-]+\s+[A-Z][a-zA-Z'\-]+)/g);
      if (fam) out.extracted.family.push(...fam);
    }
    return { ok: items.length > 0, data: out };
  } catch (e) { return { ok: false, error: e.message }; }
}

// ============================================================================
// SIGNAL 11: State SOS business filings
// ============================================================================
async function srcBusinessFilings(db, person) {
  const cfg = await loadCseCfg(db);
  if (!cfg) return { ok: false, error: 'no_cse' };
  const queries = [
    `"${person.full_name}" site:bizfileonline.sos.ca.gov OR site:businesssearch.sos.ca.gov`,
    `"${person.full_name}" site:mycpa.cpa.state.tx.us OR site:sos.state.tx.us`,
    `"${person.full_name}" site:sunbiz.org`
  ];
  const out = { filings: [], states_checked: [] };
  for (const q of queries) {
    try {
      const url = `https://www.googleapis.com/customsearch/v1?key=${cfg.key}&cx=${cfg.cx}&q=${encodeURIComponent(q)}&num=2`;
      const r = await fetchWithRetry(url, { timeoutMs: 8000 });
      if (!r.ok) continue;
      const d = await r.json().catch(() => null);
      const items = d?.items || [];
      out.states_checked.push((q.match(/site:([^\s]+)/) || [])[1] || 'unknown');
      for (const it of items) out.filings.push({ link: it.link, title: it.title, snippet: it.snippet });
    } catch (_) {}
  }
  return { ok: out.filings.length > 0, data: out };
}

// ============================================================================
// SIGNAL 12: Photo OCR / vision
// ============================================================================
async function srcPhotoVision(db, person) {
  if (!person.incident_id) return { ok: false, error: 'no_incident' };
  let images = [];
  try {
    const reports = await db('reports')
      .where('incident_id', person.incident_id)
      .whereNotNull('parsed_data')
      .limit(5)
      .select('source_reference', 'parsed_data');
    for (const rep of reports) {
      let pd = rep.parsed_data;
      if (typeof pd === 'string') { try { pd = JSON.parse(pd); } catch (_) { pd = {}; } }
      pd = pd || {};
      if (Array.isArray(pd.image_urls)) images.push(...pd.image_urls);
      if (pd.image_url) images.push(pd.image_url);
      if (pd.thumbnail) images.push(pd.thumbnail);
    }
  } catch (_) {}
  images = [...new Set(images)].slice(0, 2);
  if (!images.length) return { ok: false, error: 'no_images' };
  const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
  if (!ANTHROPIC_API_KEY) return { ok: false, error: 'no_claude_key' };
  const out = { images_processed: [], findings: [] };
  for (const imgUrl of images) {
    try {
      const body = {
        model: MODELS.cheap_anth || 'claude-haiku-4-5',
        max_tokens: 600,
        messages: [{
          role: 'user',
          content: [
            { type: 'image', source: { type: 'url', url: imgUrl } },
            { type: 'text', text: 'You are reading an accident scene photo for an OSINT investigator. Extract any visible: license plate(s), vehicle make/model/color, location signs (street name, business name), road conditions. Return JSON only: {plates: [], vehicles: [], signs: [], conditions: ""}. If nothing legible, return empty arrays.' }
          ]
        }]
      };
      const r = await fetchWithRetry('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'x-api-key': ANTHROPIC_API_KEY,
          'anthropic-version': '2023-06-01',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(body),
        timeoutMs: 20000
      });
      if (!r.ok) { out.images_processed.push({ url: imgUrl, error: `http_${r.status}` }); continue; }
      const d = await r.json().catch(() => null);
      const content = d?.content?.[0]?.text || '';
      let parsed = null;
      try {
        const cleaned = content.replace(/^```(?:json)?\s*/i, '').replace(/\s*```\s*$/i, '');
        const i = cleaned.indexOf('{'); const j = cleaned.lastIndexOf('}');
        if (i >= 0 && j > i) parsed = JSON.parse(cleaned.slice(i, j + 1));
      } catch (_) {}
      out.images_processed.push({ url: imgUrl, ok: true });
      if (parsed) out.findings.push({ url: imgUrl, ...parsed });
      await trackApiCall(db, 'homegrown-osint-miner:vision', body.model, d?.usage?.input_tokens || 0, d?.usage?.output_tokens || 0, true).catch(()=>{});
    } catch (e) {
      out.images_processed.push({ url: imgUrl, error: e.message });
    }
  }
  return { ok: out.findings.length > 0, data: out };
}

// ============================================================================
// SOURCE REGISTRY
// ============================================================================
const SOURCES = [
  { label: 'google_cse_deep',   fn: srcGoogleCseDeep,    weight: 90 },
  { label: 'wayback',           fn: srcWayback,          weight: 60 },
  { label: 'duckduckgo',        fn: srcDuckDuckGo,       weight: 55 },
  { label: 'email_permutator',  fn: srcEmailPermutator,  weight: 70 },
  { label: 'reddit',            fn: srcReddit,           weight: 65 },
  { label: 'github',            fn: srcGitHub,           weight: 60 },
  { label: 'linkedin',          fn: srcLinkedIn,         weight: 80 },
  { label: 'bankruptcy',        fn: srcBankruptcy,       weight: 50 },
  { label: 'court_records',     fn: srcCourtListener,    weight: 75 },
  { label: 'news_archive',      fn: srcNewsArchive,      weight: 80 },
  { label: 'business_filings',  fn: srcBusinessFilings,  weight: 70 },
  { label: 'photo_vision',      fn: srcPhotoVision,      weight: 65 }
];

// ============================================================================
// AI SYNTHESIS — Claude Opus
// ============================================================================
const SYNTH_SYSTEM = `You are an expert OSINT analyst synthesizing public records on an accident victim. You will receive 12 signals from different free public web searches. Your job is to cross-reference them like a human investigator and extract the highest-confidence contact information.

RULES:
- Cite a source for every claim (use the source label like "google_cse_deep" or "reddit").
- Reason carefully about conflicting signals. Recency + authority wins.
- A phone in 3 sources beats a phone in 1.
- Reject obvious noise: scraper-website emails (support@thatsthem, info@radaris), 555 numbers, example.com, news-org generic emails.
- If signals are weak, say so honestly; confidence < 40 means "not enough".
- next_step: CALL if phone is verified high-confidence; EMAIL if email + employer match; CERTIFIED_LETTER if address-only; NEEDS_MORE_RESEARCH otherwise.

Return JSON only with this exact shape:
{
  "best_phone": {"value": "...", "confidence": 0-100, "source": "...", "reasoning": "..."},
  "best_email": {"value": "...", "confidence": 0-100, "source": "...", "reasoning": "...", "verified_via_hunter": false},
  "best_address": {"street": "...", "city": "...", "state": "...", "zip": "...", "confidence": 0-100, "source": "...", "reasoning": "..."},
  "employer": {"name": "...", "domain": "...", "confidence": 0-100, "source": "..."},
  "family_members": [{"name": "...", "relationship": "...", "contact_hint": "..."}],
  "social_handles": {"linkedin": "", "twitter": "", "facebook": "", "reddit": "", "github": "", "instagram": ""},
  "case_value_hints": ["..."],
  "next_step": "CALL|EMAIL|CERTIFIED_LETTER|NEEDS_MORE_RESEARCH",
  "confidence_overall": 0-100
}`;

async function synthesizeWithOpus(db, person, signals) {
  const userPrompt = `Subject:
- full_name: ${person.full_name}
- city: ${person.city || 'unknown'}
- state: ${person.state || 'unknown'}
- accident_date: ${person.accident_date || 'unknown'}
- known_phone: ${person.phone || 'none'}
- known_email: ${person.email || 'none'}
- known_address: ${person.address || 'none'}
- known_employer: ${person.employer || 'none'}

12 OSINT signals (truncated to relevant fields):
${JSON.stringify(signals, null, 2).slice(0, 18000)}

Synthesize. Return JSON only.`;
  const r = await extract(db, {
    pipeline: 'homegrown-osint-miner:synth',
    systemPrompt: SYNTH_SYSTEM,
    userPrompt,
    provider: 'auto',
    tier: 'premium',
    timeoutMs: 55000,
    responseFormat: 'json',
    temperature: 0
  });
  if (!r.ok) return { ok: false, error: r.error, attempts: r.attempts };
  return {
    ok: true, parsed: r.parsed || {}, raw: r.content,
    model: r.model_used, tokens_in: r.tokens_in, tokens_out: r.tokens_out
  };
}

// ============================================================================
// PERSIST
// ============================================================================
async function logEnrichment(db, personId, source, payload, confidence) {
  try {
    await db('enrichment_logs').insert({
      person_id: personId,
      field_name: 'homegrown-osint-miner:' + source,
      old_value: null,
      new_value: JSON.stringify(payload).slice(0, 4000),
      source_url: null,
      source: 'homegrown-osint-miner:' + source,
      confidence: confidence || 60,
      verified: false,
      data: JSON.stringify({ source, payload, weight: confidence || 60 }).slice(0, 4000),
      created_at: new Date()
    });
  } catch (_) {}
}

async function applySynthesisToPerson(db, person, synth) {
  const update = {};
  if (!person.phone && synth.best_phone?.value && (synth.best_phone.confidence || 0) >= 60) {
    update.phone = digitsOnly(synth.best_phone.value);
  }
  if (!person.email && synth.best_email?.value && (synth.best_email.confidence || 0) >= 60) {
    update.email = String(synth.best_email.value).toLowerCase();
  }
  if (!person.address && synth.best_address?.street && (synth.best_address.confidence || 0) >= 55) {
    update.address = synth.best_address.street;
    if (!person.city && synth.best_address.city) update.city = synth.best_address.city;
    if (!person.state && synth.best_address.state) update.state = synth.best_address.state;
    if (!person.zip && synth.best_address.zip) update.zip = synth.best_address.zip;
  }
  if (!person.employer && synth.employer?.name && (synth.employer.confidence || 0) >= 50) {
    update.employer = synth.employer.name;
  }
  if (synth.social_handles?.linkedin && !person.linkedin_url) {
    update.linkedin_url = synth.social_handles.linkedin;
  }
  if (Object.keys(update).length === 0) return { fields_filled: 0 };
  update.updated_at = new Date();
  try {
    await db('persons').where('id', person.id).update(update);
    return { fields_filled: Object.keys(update).length - 1, applied: update };
  } catch (e) {
    return { fields_filled: 0, error: e.message };
  }
}

// ============================================================================
// MINE ONE
// ============================================================================
async function mineOne(db, personId) {
  const start = Date.now();
  const stats = {
    person_id: personId, sources_tried: [], sources_succeeded: [],
    signals_count: 0, synthesis_confidence: 0, fields_filled: 0,
    tokens_in: 0, tokens_out: 0, errors: []
  };
  let person = await db('persons').where('id', personId).first();
  if (!person) return { ok: false, error: 'person_not_found', stats };
  let incident = null;
  if (person.incident_id) {
    incident = await db('incidents').where('id', person.incident_id).first().catch(() => null);
    if (incident) {
      if (!person.city && incident.city) person.city = incident.city;
      if (!person.state && incident.state) person.state = incident.state;
      if (!person.accident_date && incident.accident_date) person.accident_date = incident.accident_date;
    }
  }
  const signals = {};
  const sourceTimeoutMs = 18000;
  // Run all 12 sources in parallel with a per-source soft timeout so a single
  // hung source doesn't blow the Vercel 60s function cap.
  const fanOut = SOURCES.map(async src => {
    stats.sources_tried.push(src.label);
    try {
      const guarded = await Promise.race([
        src.fn(db, person),
        new Promise(resolve => setTimeout(() => resolve({ ok: false, error: 'soft_timeout' }), sourceTimeoutMs))
      ]);
      await trackApiCall(db, 'homegrown-osint-miner', src.label, 0, 0, !!(guarded && guarded.ok)).catch(() => {});
      if (guarded && guarded.ok && guarded.data) {
        stats.sources_succeeded.push(src.label);
        stats.signals_count++;
        signals[src.label] = guarded.data;
        logEnrichment(db, personId, src.label, guarded.data, src.weight).catch(() => {});
      } else if (guarded && guarded.error) {
        signals[src.label] = { ok: false, error: guarded.error };
      }
    } catch (e) {
      stats.errors.push(`${src.label}:${e.message}`);
      signals[src.label] = { ok: false, error: e.message };
    }
  });
  const overallTimeout = new Promise(resolve => setTimeout(resolve, 25000));
  await Promise.race([Promise.all(fanOut), overallTimeout]);
  let synthOut = null;
  if (stats.signals_count >= 1) {
    const s = await synthesizeWithOpus(db, person, signals);
    if (s.ok) {
      synthOut = s.parsed;
      stats.tokens_in = s.tokens_in || 0;
      stats.tokens_out = s.tokens_out || 0;
      stats.synthesis_confidence = synthOut.confidence_overall || 0;
      stats.model_used = s.model;
    } else {
      stats.errors.push('synthesis:' + s.error);
    }
  }
  let applyResult = { fields_filled: 0 };
  if (synthOut) {
    applyResult = await applySynthesisToPerson(db, person, synthOut);
    stats.fields_filled = applyResult.fields_filled || 0;
  }
  try {
    await db('enrichment_logs').insert({
      person_id: personId,
      field_name: 'homegrown-osint-miner:synthesis',
      old_value: null,
      new_value: JSON.stringify(synthOut || {}).slice(0, 4000),
      source: 'homegrown-osint-miner',
      confidence: stats.synthesis_confidence || 0,
      verified: false,
      data: JSON.stringify({
        engine: 'homegrown-osint-miner',
        signals_count: stats.signals_count,
        synthesis_confidence: stats.synthesis_confidence,
        sources_succeeded: stats.sources_succeeded,
        applied: applyResult.applied || null
      }).slice(0, 4000),
      created_at: new Date()
    });
  } catch (_) {}
  try {
    await enqueueCascade(db, {
      person_id: personId,
      incident_id: person.incident_id,
      trigger_source: 'homegrown-osint-miner',
      trigger_field: 'multi',
      trigger_value: `signals=${stats.signals_count},conf=${stats.synthesis_confidence}`,
      priority: stats.fields_filled > 0 ? 8 : 4
    });
  } catch (_) {}
  return {
    ok: true, stats, synthesis: synthOut,
    signals_summary: Object.fromEntries(
      Object.entries(signals).map(([k, v]) => [k, v?.ok === false ? { error: v.error } : { ok: true, keys: Object.keys(v || {}) }])
    ),
    latency_ms: Date.now() - start
  };
}

async function batchMine(db, { limit = 3 } = {}) {
  const cap = Math.min(parseInt(limit, 10) || 3, MAX_BATCH);
  const rows = await db('persons')
    .where('victim_verified', true)
    .where(function () {
      this.whereNull('phone').orWhere('phone', '')
        .orWhereNull('email').orWhere('email', '')
        .orWhereNull('address').orWhere('address', '');
    })
    .whereNotNull('full_name')
    .orderBy('updated_at', 'desc')
    .limit(cap)
    .select('id', 'full_name');
  const results = {
    candidates: rows.length, mined: 0, fields_filled_total: 0,
    signals_total: 0, tokens_in_total: 0, tokens_out_total: 0, samples: []
  };
  for (const r of rows) {
    let one;
    try { one = await mineOne(db, r.id); }
    catch (e) {
      results.samples.push({ person_id: r.id, name: r.full_name, error: e.message });
      continue;
    }
    if (!one || !one.ok) continue;
    results.mined++;
    results.fields_filled_total += one.stats?.fields_filled || 0;
    results.signals_total += one.stats?.signals_count || 0;
    results.tokens_in_total += one.stats?.tokens_in || 0;
    results.tokens_out_total += one.stats?.tokens_out || 0;
    results.samples.push({
      person_id: r.id, name: r.full_name,
      signals_count: one.stats?.signals_count || 0,
      sources_succeeded: one.stats?.sources_succeeded || [],
      synthesis_confidence: one.stats?.synthesis_confidence || 0,
      fields_filled: one.stats?.fields_filled || 0,
      next_step: one.synthesis?.next_step,
      synthesis: one.synthesis,
      latency_ms: one.latency_ms
    });
  }
  return results;
}

async function health(db) {
  const checks = {};
  const cse = await loadCseCfg(db);
  checks.google_cse = !!cse ? 'configured' : 'missing';
  checks.hunter = !!(await getCfg(db, 'hunter_api_key', 'HUNTER_API_KEY')) ? 'configured' : 'missing';
  checks.courtlistener = !!(await getCfg(db, 'courtlistener_api_token', 'COURTLISTENER_API_TOKEN')) ? 'configured' : 'public_only';
  checks.anthropic = !!process.env.ANTHROPIC_API_KEY ? 'configured' : 'missing';
  try {
    const r = await fetchWithRetry('https://archive.org/wayback/available?url=example.com', { timeoutMs: 5000 });
    checks.wayback_reachable = r.ok ? 'ok' : `http_${r.status}`;
  } catch (e) { checks.wayback_reachable = 'error:' + e.message; }
  try {
    const r = await fetchWithRetry('https://www.reddit.com/search.json?q=hello&limit=1', { timeoutMs: 5000 });
    checks.reddit_reachable = r.ok ? 'ok' : `http_${r.status}`;
  } catch (e) { checks.reddit_reachable = 'error:' + e.message; }
  try {
    const r = await fetchWithRetry('https://api.github.com', { timeoutMs: 5000 });
    checks.github_reachable = r.ok ? 'ok' : `http_${r.status}`;
  } catch (e) { checks.github_reachable = 'error:' + e.message; }
  const candidates = await db('persons')
    .where('victim_verified', true)
    .where(function () {
      this.whereNull('phone').orWhere('phone', '')
        .orWhereNull('email').orWhere('email', '')
        .orWhereNull('address').orWhere('address', '');
    })
    .count('* as c').first().catch(() => ({ c: 0 }));
  return {
    ok: true, engine: 'homegrown-osint-miner', phase: 42, checks,
    sources_registered: SOURCES.map(s => s.label),
    pending_candidates: parseInt(candidates.c, 10) || 0,
    max_batch: MAX_BATCH
  };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  const action = (req.query?.action || 'health').toLowerCase();
  let db;
  try { db = getDb(); }
  catch (e) { return res.status(500).json({ success: false, error: 'db_unavailable:' + e.message }); }
  try {
    if (action === 'health') {
      const h = await health(db);
      return res.status(200).json(h);
    }
    if (action === 'mine') {
      const personId = req.query?.person_id;
      if (!personId) return res.status(400).json({ error: 'person_id_required' });
      const r = await mineOne(db, personId);
      return res.status(200).json({ success: true, ...r });
    }
    if (action === 'batch') {
      const limit = req.query?.limit || 3;
      const r = await batchMine(db, { limit });
      return res.status(200).json({ success: true, ...r });
    }
    return res.status(400).json({ error: 'unknown_action', valid: ['health', 'mine', 'batch'] });
  } catch (e) {
    await reportError(db, 'homegrown-osint-miner', null, e.message, { stack: e.stack?.slice(0, 1000) }).catch(() => {});
    return res.status(500).json({ success: false, error: e.message });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.mineOne = mineOne;
module.exports.batchMine = batchMine;
module.exports.health = health;
