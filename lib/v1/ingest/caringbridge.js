/**
 * Phase 93: Caringbridge.org Public CarePages Scraper
 *
 * Caringbridge runs hundreds of thousands of public "CarePages" where families
 * post recovery updates for hospitalized loved ones. When someone is critically
 * injured in a car accident, family members often start a Caringbridge page
 * with full name + city + accident details.
 *
 * Strategy: Brave Search the site for crash-related posts, then fetch the
 * landing page to extract name + city + injury context.
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

const SEARCH_TERMS = [
  '"car accident" survivor recovery',
  '"hit by car" recovery hospital',
  '"car crash" recovery ICU',
  '"motorcycle accident" recovery',
  '"pedestrian struck" recovery',
  '"truck accident" recovery hospital',
  '"DUI accident" recovery'
];

async function braveSiteSearch(key, term) {
  const q = `site:caringbridge.org ${term}`;
  try {
    const r = await fetch(`https://api.search.brave.com/res/v1/web/search?q=${encodeURIComponent(q)}&count=10`, {
      headers: { 'Accept': 'application/json', 'X-Subscription-Token': key },
      signal: AbortSignal.timeout(10000)
    });
    if (!r.ok) return [];
    const j = await r.json();
    return (j.web?.results || []).map(x => ({ title: x.title, url: x.url, snippet: x.description }));
  } catch { return []; }
}

function extractFromCB(item) {
  // Caringbridge URLs: caringbridge.org/visit/<slug>
  const m = item.url.match(/caringbridge\.org\/(?:visit|site|public)\/([^\/?#]+)/i);
  if (!m) return null;
  const slug = m[1];
  // Title format often "Page name | CaringBridge" or "First Last | CaringBridge"
  const t = item.title || '';
  const titleMatch = t.match(/^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)/);
  const name = titleMatch ? titleMatch[1] : null;
  // Extract city from snippet
  const cityMatch = (item.snippet || '').match(/([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)?),\s*([A-Z]{2})/);
  return {
    slug, name, url: item.url,
    city: cityMatch ? cityMatch[1] : null,
    state: cityMatch ? cityMatch[2] : null,
    snippet: item.snippet
  };
}

async function ingestRun(db) {
  const braveKey = await getBraveKey(db);
  if (!braveKey) return { ok: false, error: 'no_brave_key' };
  const seen = new Set();
  const candidates = [];
  for (const term of SEARCH_TERMS) {
    const results = await braveSiteSearch(braveKey, term);
    for (const r of results) {
      if (seen.has(r.url)) continue;
      seen.add(r.url);
      const c = extractFromCB(r);
      if (c && c.name) candidates.push(c);
    }
    await new Promise(r => setTimeout(r, 1100));
  }
  const { v4: uuid } = require('uuid');
  let inserted = 0, persons_inserted = 0, skipped = 0;
  for (const c of candidates) {
    const ref = `caringbridge:${c.slug}`;
    const exists = await db('incidents').where('incident_number', ref).first();
    let incidentId;
    if (!exists) {
      incidentId = uuid();
      try {
        await db('incidents').insert({
          id: incidentId, incident_number: ref,
          state: c.state || null, city: c.city || null,
          severity: 'critical', incident_type: 'car_accident', fatalities_count: 0,
          description: `Caringbridge: ${c.name || 'unnamed'} - ${(c.snippet || '').slice(0, 350)}`.slice(0, 500),
          raw_description: JSON.stringify(c).slice(0, 4000),
          occurred_at: new Date(),
          discovered_at: new Date(),
          qualification_state: 'pending', lead_score: 55, source_count: 1
        });
        inserted++;
      } catch { skipped++; continue; }
    } else {
      incidentId = exists.id;
    }
    if (c.name) {
      const dup = await db('persons').where({ incident_id: incidentId, full_name: c.name }).first();
      if (!dup) {
        try {
          await db('persons').insert({
            id: uuid(), incident_id: incidentId, full_name: c.name, role: 'victim',
            city: c.city, state: c.state,
            victim_verified: false, lead_tier: 'pending',
            source: 'caringbridge', created_at: new Date()
          });
          persons_inserted++;
        } catch { /* skip */ }
      }
    }
  }
  return { ok: true, candidates_found: candidates.length, inserted, persons_inserted, skipped };
}

module.exports = async function handler(req, res) {
  if (!authed(req)) return res.status(401).json({ error: 'unauthorized' });
  const action = req.query?.action || 'health';
  const db = getDb();
  if (action === 'health') {
    const has = !!await getBraveKey(db);
    return res.status(200).json({ ok: true, engine: 'caringbridge', brave_configured: has, search_terms: SEARCH_TERMS.length });
  }
  if (action === 'run') {
    const r = await ingestRun(db);
    return res.status(200).json({ ok: true, ...r });
  }
  return res.status(400).json({ error: 'unknown action', valid: ['health','run'] });
};
