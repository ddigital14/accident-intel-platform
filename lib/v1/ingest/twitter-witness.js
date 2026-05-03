/**
 * Phase 93: Twitter/X Witness Monitor (via Brave site:twitter.com search)
 *
 * Brave doesn't have a paid Twitter scraper, but Brave search indexes public
 * tweets and Twitter SERP cards. We search for "witnessed crash on" /
 * "saw accident" / "praying for [name]" tweets that often surface victim
 * names + locations + family contacts.
 *
 * Strategy: rotate through a curated set of high-signal queries and ingest
 * the result snippets as proto-incidents with raw tweet text. The
 * ai-news-extractor cron will fan out and pull names.
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

const QUERIES = [
  '"just witnessed" "crash" OR "accident"',
  '"praying for" "car accident" OR "crash"',
  '"family of" "killed in crash"',
  '"struck and killed" pedestrian',
  '"hit-and-run" died',
  '"motorcycle accident" tribute',
  '"GoFundMe" "car accident" survivor',
  'crash victim identified police',
  'fatal crash named'
];

async function searchBrave(key, query) {
  const q = `(site:twitter.com OR site:x.com OR site:facebook.com) ${query}`;
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

function extractCandidate(result) {
  const blob = `${result.title} ${result.snippet}`;
  // Try to extract a quoted name
  const nameMatch = blob.match(/"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})"/) ||
                    blob.match(/(?:victim|deceased|killed|named|identified as)[\s:]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})/i) ||
                    blob.match(/([A-Z][a-z]+\s+[A-Z][a-z]+),\s*\d{1,3}/);
  const name = nameMatch ? nameMatch[1] : null;
  const cityMatch = blob.match(/([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)?),\s*([A-Z]{2})/);
  return {
    url: result.url,
    name,
    city: cityMatch ? cityMatch[1] : null,
    state: cityMatch ? cityMatch[2] : null,
    snippet: result.snippet,
    title: result.title
  };
}

async function ingestRun(db) {
  const braveKey = await getBraveKey(db);
  if (!braveKey) return { ok: false, error: 'no_brave_key' };
  const seen = new Set();
  const candidates = [];
  for (const q of QUERIES) {
    const results = await searchBrave(braveKey, q);
    for (const r of results) {
      if (seen.has(r.url)) continue;
      seen.add(r.url);
      const c = extractCandidate(r);
      candidates.push(c);
    }
    await new Promise(r => setTimeout(r, 1100));
  }
  const { v4: uuid } = require('uuid');
  let inserted = 0, persons_inserted = 0, skipped = 0;
  for (const c of candidates) {
    const ref = `social:${c.url.slice(0, 90)}`;
    const exists = await db('incidents').where('incident_number', ref).first();
    let incidentId;
    if (!exists) {
      incidentId = uuid();
      try {
        await db('incidents').insert({
          id: incidentId, incident_number: ref,
          state: c.state || null, city: c.city || null,
          severity: 'unknown', incident_type: 'car_accident', fatalities_count: 0,
          description: `Social witness: ${c.title || ''} - ${(c.snippet || '').slice(0, 300)}`.slice(0, 500),
          raw_description: JSON.stringify(c).slice(0, 4000),
          occurred_at: new Date(),
          discovered_at: new Date(),
          qualification_state: 'pending', lead_score: 30, source_count: 1
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
            source: 'twitter-witness', created_at: new Date()
          });
          persons_inserted++;
        } catch { /* skip */ }
      }
    }
  }
  return { ok: true, queries_run: QUERIES.length, candidates_found: candidates.length, inserted, persons_inserted, skipped };
}

module.exports = async function handler(req, res) {
  if (!authed(req)) return res.status(401).json({ error: 'unauthorized' });
  const action = req.query?.action || 'health';
  const db = getDb();
  if (action === 'health') {
    const has = !!await getBraveKey(db);
    return res.status(200).json({ ok: true, engine: 'twitter-witness', brave_configured: has, queries: QUERIES.length });
  }
  if (action === 'run') {
    const r = await ingestRun(db);
    return res.status(200).json({ ok: true, ...r });
  }
  return res.status(400).json({ error: 'unknown action', valid: ['health','run'] });
};
