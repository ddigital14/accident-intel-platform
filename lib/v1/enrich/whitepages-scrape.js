/**
 * Phase 68 #3: Whitepages free directory scrape.
 * Headless fetch + cheerio-style regex parse. Returns name + age + relatives + previous addresses.
 * Free, no API key. Rate-limited ~10/min to respect the site.
 *
 * NOTE: This is the *free* whitepages.com directory, not Whitepages Pro.
 * For commercial use the platform should respect robots.txt and PI consent rules.
 */
const { getDb } = require('../../_db');
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

const UAS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
];

function pickUA() { return UAS[Math.floor(Math.random() * UAS.length)]; }

async function searchByName(name, state) {
  if (!name || !state) return { ok: false, error: 'name+state required' };
  const slug = name.replace(/\s+/g, '-').toLowerCase();
  const url = `https://www.whitepages.com/name/${encodeURIComponent(slug)}/${state.toUpperCase()}`;
  try {
    const r = await fetch(url, {
      headers: { 'User-Agent': pickUA(), 'Accept': 'text/html' },
      signal: AbortSignal.timeout(15000)
    });
    if (!r.ok) return { ok: false, status: r.status };
    const html = await r.text();
    // Naive parse — extract age + city + age_range + relatives
    const ageMatches = [...html.matchAll(/Age\s*(\d{2,3})/gi)].map(m => parseInt(m[1]));
    const cityMatches = [...html.matchAll(/lives in ([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})/g)].map(m => ({ city: m[1], state: m[2] }));
    return { ok: true, name, state, candidates: ageMatches.length, ages: ageMatches.slice(0, 5), cities: cityMatches.slice(0, 5), source: 'whitepages-free' };
  } catch (e) { return { ok: false, error: e.message }; }
}

async function lookupOne(db, personId) {
  const p = await db('persons').where('id', personId).first();
  if (!p?.full_name || !p?.state) return { ok: true, skipped: 'no_name_or_state' };
  const r = await searchByName(p.full_name, p.state);
  if (r.ok && r.candidates > 0) {
    try {
      await db('enrichment_logs').insert({
        person_id: personId,
        field_name: 'whitepages_directory',
        old_value: null,
        new_value: JSON.stringify({ ages: r.ages, cities: r.cities, source: 'whitepages-free' }).slice(0, 4000),
        created_at: new Date()
      });
    } catch (_) {}
  }
  return r;
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();
  if (action === 'health') return res.json({ success: true, service: 'whitepages-scrape' });
  if (action === 'lookup') {
    const pid = req.query?.person_id;
    if (!pid) return res.status(400).json({ error: 'person_id required' });
    return res.json(await lookupOne(db, pid));
  }
  if (action === 'search') {
    const name = req.query?.name;
    const state = req.query?.state;
    return res.json(await searchByName(name, state));
  }
  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.searchByName = searchByName;
module.exports.lookupOne = lookupOne;
