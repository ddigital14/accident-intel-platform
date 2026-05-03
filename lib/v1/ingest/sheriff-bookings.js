/**
 * Phase 94: Sheriff DUI/Vehicular Booking Log Scraper (Brave-pivot)
 *
 * Each county sheriff has a different portal (ASPX postbacks, JS SPAs, custom
 * APIs). Rather than scrape each one directly, we use Brave Search to find
 * recent booking pages across ALL sheriff domains in a single sweep. Brave
 * indexes these public booking pages within hours.
 */
const { getDb } = require('../../_db');

const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function getBraveKey(db) {
  if (process.env.BRAVE_API_KEY) return process.env.BRAVE_API_KEY;
  try {
    const row = await db('system_config').where({ key: 'brave_api_key' }).first();
    if (row?.value) return typeof row.value === 'string' ? row.value : (row.value.api_key || row.value.key);
  } catch { /* table missing */ }
  return null;
}

const VEHICULAR_CHARGE_RE = /\b(DUI|DWI|OVI|OUI|drunk\s*driv|hit[\s-]and[\s-]run|hit\s+and\s+run|vehicular\s*(?:manslaughter|homicide|assault)|reckless\s*driv|fleeing|leaving\s*scene|aggravated\s*assault\s*with\s*motor|negligent\s*homicide|involuntary\s*manslaughter|fatal\s*crash|crash\s*killed)/i;

const SEARCH_TERMS = [
  '(site:mcso.org OR site:hcso.tampa.fl.us OR site:tcsheriff.org) DUI booking',
  '(site:hennepinsheriff.org OR site:harriscountyso.org OR site:lvmpd.com) "DUI" arrest',
  '(site:sacsheriff.com OR site:lvmpd.com OR site:cuyahogacounty.gov) "vehicular" booking',
  '"hit and run" sheriff arrest booking',
  '"vehicular manslaughter" booking arrested',
  'DUI fatal crash arrested name booking',
  '"reckless driving" arrest booking name',
  'sheriff "intoxicated driver" arrested name'
];

async function searchBrave(key, q) {
  try {
    const r = await fetch(`https://api.search.brave.com/res/v1/web/search?q=${encodeURIComponent(q)}&count=10&freshness=pw`, {
      headers: { 'Accept': 'application/json', 'X-Subscription-Token': key },
      signal: AbortSignal.timeout(10000)
    });
    if (!r.ok) return [];
    const j = await r.json();
    return (j.web?.results || []).map(x => ({ title: x.title, url: x.url, snippet: x.description, age: x.age }));
  } catch { return []; }
}

function extractFromBooking(item) {
  const blob = `${item.title} ${item.snippet}`;
  if (!VEHICULAR_CHARGE_RE.test(blob)) return null;
  const nameMatch = blob.match(/\b([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:-[A-Z][a-z]+)?)/);
  const name = nameMatch ? nameMatch[1].trim() : null;
  if (!name || name.length < 5 || name.split(/\s+/).length < 2) return null;
  const ageMatch = blob.match(/(?:age\s+)?(\d{2}),?\s*(?:of|from)\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)?)/);
  const cityMatch = blob.match(/([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)?),\s*([A-Z]{2})/);
  return {
    name,
    age: ageMatch ? parseInt(ageMatch[1]) : null,
    city: cityMatch ? cityMatch[1] : (ageMatch ? ageMatch[2] : null),
    state: cityMatch ? cityMatch[2] : null,
    url: item.url,
    snippet: item.snippet,
    title: item.title,
    age_age: item.age
  };
}

async function ingestRun(db) {
  const braveKey = await getBraveKey(db);
  if (!braveKey) return { ok: false, error: 'no_brave_key' };
  const seen = new Set();
  const candidates = [];
  for (const term of SEARCH_TERMS) {
    const results = await searchBrave(braveKey, term);
    for (const r of results) {
      if (seen.has(r.url)) continue;
      seen.add(r.url);
      const c = extractFromBooking(r);
      if (c) candidates.push(c);
    }
    await new Promise(r => setTimeout(r, 1100));
  }
  const { v4: uuid } = require('uuid');
  let inserted = 0, persons_inserted = 0, skipped = 0;
  for (const c of candidates) {
    const ref = `sheriff:brave:${c.name.replace(/\s+/g, '')}:${c.url.slice(0, 60)}`.slice(0, 100);
    const exists = await db('incidents').where('incident_number', ref).first();
    let incidentId;
    if (!exists) {
      incidentId = uuid();
      try {
        await db('incidents').insert({
          id: incidentId, incident_number: ref, state: c.state, city: c.city,
          severity: 'unknown', incident_type: 'car_accident', fatalities_count: 0,
          description: `Sheriff booking (Brave): ${c.title}`.slice(0, 500),
          raw_description: JSON.stringify(c).slice(0, 4000),
          occurred_at: new Date(),
          discovered_at: new Date(),
          qualification_state: 'pending', lead_score: 50, source_count: 1
        });
        inserted++;
      } catch { skipped++; continue; }
    } else {
      incidentId = exists.id;
    }
    const dup = await db('persons').where({ incident_id: incidentId, full_name: c.name }).first();
    if (!dup) {
      try {
        await db('persons').insert({
          id: uuid(), incident_id: incidentId, full_name: c.name, role: 'driver',
          age: c.age, city: c.city, state: c.state,
          victim_verified: false, lead_tier: 'pending',
          source: 'sheriff-brave', created_at: new Date()
        });
        persons_inserted++;
      } catch { /* skip */ }
    }
  }
  return { ok: true, queries_run: SEARCH_TERMS.length, candidates_found: candidates.length, inserted, persons_inserted, skipped };
}

module.exports = async function handler(req, res) {
  if (!authed(req)) return res.status(401).json({ error: 'unauthorized' });
  const action = req.query?.action || 'health';
  const db = getDb();
  if (action === 'health') {
    const has = !!await getBraveKey(db);
    return res.status(200).json({ ok: true, engine: 'sheriff-bookings', strategy: 'Brave-search-across-sheriff-domains', brave_configured: has, queries: SEARCH_TERMS.length });
  }
  if (action === 'run') {
    const r = await ingestRun(db);
    return res.status(200).json({ ok: true, ...r });
  }
  return res.status(400).json({ error: 'unknown action', valid: ['health','run'] });
};
