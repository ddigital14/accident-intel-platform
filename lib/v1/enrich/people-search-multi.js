/**
 * Phase 37 Wave B - Hardened Multi-Source People-Search Scraper
 *
 * Existing people-search.js + Whitepages free wall mostly returns 0 due to
 * 403/429. This adds 4 fresh sources with rotating UAs + retry-on-block +
 * targeted phone/email regex parsing (no LLM hop - faster + free).
 *
 * Sources (cascade):
 *   1. fastpeoplesearch.com/name/<first>-<last>_<state>
 *   2. thatsthem.com/name/<first>-<last>
 *   3. radaris.com/p/<First>/<Last>
 *   4. truepeoplesearch.com/results?name=...&citystatezip=<state>
 *
 * GET /api/v1/enrich/people-search-multi?secret=ingest-now&action=batch&limit=N
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');
const { enqueueCascade } = require('../system/_cascade');

const UAS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:124.0) Gecko/20100101 Firefox/124.0'
];
const pickUA = () => UAS[Math.floor(Math.random() * UAS.length)];

const PHONE_RE = /\b\(?(\d{3})\)?[\s.-]?(\d{3})[\s.-]?(\d{4})\b/g;
const EMAIL_RE = /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b/g;

function slugName(name) {
  return String(name || '').trim().toLowerCase().replace(/[^a-z0-9\s]/g, '').replace(/\s+/g, '-');
}
function splitFirstLast(name) {
  const parts = String(name || '').trim().split(/\s+/);
  return { first: parts[0] || '', last: parts[parts.length - 1] || '' };
}

const SOURCES = [
  { src: 'fastpeoplesearch', url: (n, st) => `https://www.fastpeoplesearch.com/name/${slugName(n)}_${(st || '').toLowerCase()}` },
  { src: 'thatsthem', url: (n) => { const { first, last } = splitFirstLast(n); return `https://thatsthem.com/name/${slugName(first)}-${slugName(last)}`; } },
  { src: 'radaris', url: (n) => { const { first, last } = splitFirstLast(n); const f = first.charAt(0).toUpperCase() + first.slice(1).toLowerCase(); const l = last.charAt(0).toUpperCase() + last.slice(1).toLowerCase(); return `https://radaris.com/p/${encodeURIComponent(f)}/${encodeURIComponent(l)}`; } },
  { src: 'truepeoplesearch', url: (n, st) => { const { first, last } = splitFirstLast(n); return `https://www.truepeoplesearch.com/results?name=${encodeURIComponent(first + ' ' + last)}&citystatezip=${encodeURIComponent(st || '')}`; } }
];

async function fetchWithRetry(url, attempt = 0) {
  try {
    const resp = await fetch(url, {
      headers: {
        'User-Agent': pickUA(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache'
      },
      signal: AbortSignal.timeout(8000),
      redirect: 'follow'
    });
    if ((resp.status === 403 || resp.status === 429) && attempt < 1) {
      await new Promise(r => setTimeout(r, 800 + Math.random() * 800));
      return fetchWithRetry(url, attempt + 1);
    }
    if (!resp.ok) return { ok: false, status: resp.status, html: null };
    const html = await resp.text();
    return { ok: true, status: 200, html: html.substring(0, 120000) };
  } catch (e) {
    return { ok: false, status: 0, html: null, err: e.message };
  }
}

function parseContacts(html) {
  if (!html) return { phones: [], emails: [] };
  const cleaned = html.replace(/<script[\s\S]*?<\/script>/gi, ' ').replace(/<style[\s\S]*?<\/style>/gi, ' ');
  const phones = new Set();
  let m;
  PHONE_RE.lastIndex = 0;
  while ((m = PHONE_RE.exec(cleaned)) !== null) {
    if (m[1] === '555' || m[1] === '000' || m[1] === '111' || /^(\d)\1{2}$/.test(m[1])) continue;
    if (m[2] === '000' || m[2] === '111') continue;
    phones.add(`${m[1]}-${m[2]}-${m[3]}`);
    if (phones.size >= 8) break;
  }
  const emails = new Set();
  EMAIL_RE.lastIndex = 0;
  while ((m = EMAIL_RE.exec(cleaned)) !== null) {
    const e = m[0].toLowerCase();
    if (e.includes('example.com') || e.includes('sentry.io') || e.includes('cloudflare')) continue;
    if (e.endsWith('.png') || e.endsWith('.jpg') || e.endsWith('.svg')) continue;
    emails.add(e);
    if (emails.size >= 5) break;
  }
  return { phones: [...phones], emails: [...emails] };
}

async function enrichOne(db, p, results) {
  const lookupState = p.state || p.incident_state;
  let chosen = null;
  for (const site of SOURCES) {
    const url = site.url(p.full_name, lookupState);
    const r = await fetchWithRetry(url);
    await trackApiCall(db, 'people-search-multi', site.src, 0, 0, r.ok).catch(() => {});
    results.scraped++;
    if (!r.ok) continue;
    const { phones, emails } = parseContacts(r.html);
    if (phones.length || emails.length) { chosen = { src: site.src, url, phones, emails }; break; }
  }
  if (!chosen) return false;
  results.matches++;
  const update = { updated_at: new Date() };
  if (!p.phone && chosen.phones.length) update.phone = chosen.phones[0];
  if (!p.email && chosen.emails.length) update.email = chosen.emails[0];
  if (Object.keys(update).length === 1) return false;
  await db('persons').where('id', p.id).update(update);
  results.updated++;
  await db('enrichment_logs').insert({
    person_id: p.id,
    field_name: 'people_search_multi',
    old_value: null,
    new_value: JSON.stringify({ phones: chosen.phones, emails: chosen.emails, source: chosen.src }),
    source_url: chosen.url,
    source: chosen.src,
    confidence: 65,
    verified: false,
    created_at: new Date()
  }).catch(() => {});
  if (update.phone) {
    await enqueueCascade(db, { person_id: p.id, trigger_source: 'people-search-multi', trigger_field: 'phone', trigger_value: update.phone, weight: 60 }).catch(() => {});
  }
  return true;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  const db = getDb();
  const limit = Math.min(Number(req.query.limit) || 10, 25);
  const results = { candidates: 0, scraped: 0, matches: 0, updated: 0, errors: [] };
  const start = Date.now();
  try {
    const candidates = await db('persons as p')
      .leftJoin('incidents as i', 'p.incident_id', 'i.id')
      .whereNotNull('p.full_name')
      .where(function () { this.whereNull('p.phone').orWhereNull('p.email'); })
      .where(function () { this.where('i.qualification_state', 'pending_named').orWhere('i.qualification_state', 'pending').orWhereNull('i.qualification_state'); })
      .where('p.created_at', '>', new Date(Date.now() - 30 * 86400000))
      .select('p.id', 'p.full_name', 'p.state', 'p.city', 'p.phone', 'p.email', 'i.city as incident_city', 'i.state as incident_state')
      .limit(limit);
    results.candidates = candidates.length;
    for (const p of candidates) {
      if (Date.now() - start > 50000) break;
      try { await enrichOne(db, p, results); }
      catch (e) { results.errors.push(`${p.full_name}: ${e.message}`); await reportError(db, 'people-search-multi', p.id, e.message).catch(() => {}); }
    }
    res.json({ success: true, message: `people-search-multi: ${results.scraped} scrapes, ${results.matches} matches, ${results.updated} updated`, ...results, timestamp: new Date().toISOString() });
  } catch (err) {
    await reportError(db, 'people-search-multi', null, err.message).catch(() => {});
    res.status(500).json({ error: err.message, results });
  }
};
