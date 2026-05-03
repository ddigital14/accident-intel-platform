/**
 * Phase 102: Nameless-Incident Research Engine
 *
 * For incidents that have NO person attached (1,051 of them — mostly Socrata
 * crash records with location+date+severity but no names), this engine:
 *   1. Builds a search query from incident description + location + date
 *   2. brave_answers + brave_search to find related news articles
 *   3. fetch_url on top results
 *   4. Claude Opus 4.7 extracts victim name(s) from article text
 *   5. Inserts person rows (which trigger cascade enrichment)
 *
 * Endpoints:
 *   GET ?action=health
 *   POST ?action=run&limit=5  -- nameless fatal Socrata incidents
 *   POST ?action=research&incident_id=X
 */
const { getDb } = require('../../_db');

const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
const MODEL = 'claude-opus-4-7';

async function getKey(db, key, configKey) {
  if (process.env[key]) return process.env[key];
  try {
    const row = await db('system_config').where({ key: configKey }).first();
    if (row?.value) return typeof row.value === 'string' ? row.value : (row.value.api_key || row.value.key);
  } catch {}
  return null;
}

async function braveSearch(db, q) {
  const key = await getKey(db, 'BRAVE_API_KEY', 'brave_api_key');
  if (!key) return [];
  try {
    const r = await fetch(`https://api.search.brave.com/res/v1/web/search?q=${encodeURIComponent(q)}&count=10&country=us`, {
      headers: { 'Accept': 'application/json', 'X-Subscription-Token': key },
      signal: AbortSignal.timeout(8000)
    });
    if (!r.ok) return [];
    const j = await r.json();
    const out = [];
    for (const arr of [j.web?.results, j.news?.results]) {
      if (!Array.isArray(arr)) continue;
      for (const x of arr) if (x.url) out.push({ title: x.title || '', url: x.url, snippet: x.description || '' });
    }
    return out.slice(0, 8);
  } catch { return []; }
}

async function fetchPage(url) {
  try {
    const r = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; AccidentCommandCenter/1.0)' },
      signal: AbortSignal.timeout(7000)
    });
    if (!r.ok) return null;
    const html = await r.text();
    const text = html
      .replace(/<script[\s\S]*?<\/script>/gi, ' ')
      .replace(/<style[\s\S]*?<\/style>/gi, ' ')
      .replace(/<[^>]+>/g, ' ')
      .replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/&quot;/g, '"').replace(/&#39;/g, "'")
      .replace(/\s+/g, ' ').trim().slice(0, 5000);
    return text;
  } catch { return null; }
}

function buildSearchQueries(incident) {
  const date = incident.occurred_at ? new Date(incident.occurred_at).toISOString().split('T')[0] : '';
  const desc = (incident.description || incident.raw_description || '').slice(0, 200);
  const city = incident.city || '';
  const state = incident.state || '';
  const sev = incident.severity === 'fatal' ? 'fatal crash killed' : 'crash injured';
  const queries = [];
  if (city && state && date) queries.push(`${sev} ${city} ${state} ${date} victim identified`);
  if (city && date) queries.push(`fatal accident ${city} ${date} police identified`);
  if (state && date) queries.push(`${sev} ${state} ${date} obituary OR victim`);
  // Pull street name from description
  const streetMatch = desc.match(/([A-Z][a-z]+\s+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Highway|Hwy|Pkwy))/);
  if (streetMatch && state) queries.push(`${streetMatch[1]} ${city||state} ${date} crash victim`);
  return queries.slice(0, 3);
}

async function claudeExtract(incident, articleText) {
  if (!ANTHROPIC_KEY) return null;
  const prompt = `An accident occurred:
- Date: ${incident.occurred_at}
- Location: ${incident.city || ''}, ${incident.state || ''}
- Severity: ${incident.severity}
- Description: ${(incident.description || '').slice(0, 300)}

Below is text from one or more news articles that may describe this accident. Extract the victim name(s) IF AND ONLY IF the article clearly describes the same accident (matching date, location, vehicle type).

ARTICLE TEXT:
"""
${articleText.slice(0, 5000)}
"""

Return JSON only:
{
  "is_match": <boolean>,
  "match_confidence": <0-1>,
  "victims": [{"name": "<full name>", "role": "<driver|passenger|pedestrian|cyclist|victim>", "age": <int|null>, "city": "<string|null>", "state": "<2-letter|null>"}],
  "reasoning": "<brief>"
}

If not a match, return {"is_match": false, "victims": [], "reasoning": "..."}.
Names must be FULL first+last, NOT descriptors like "the driver" or "a teenager". No guessing.`;
  try {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': ANTHROPIC_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
      body: JSON.stringify({ model: MODEL, max_tokens: 1500, messages: [{ role: 'user', content: prompt }] }),
      signal: AbortSignal.timeout(20000)
    });
    if (!r.ok) return null;
    const j = await r.json();
    const text = j.content?.[0]?.text || '';
    const m = text.match(/\{[\s\S]*\}/);
    if (!m) return null;
    return JSON.parse(m[0]);
  } catch { return null; }
}

async function processIncident(db, incident) {
  const queries = buildSearchQueries(incident);
  if (queries.length === 0) return { incident_id: incident.id, status: 'no_queries' };
  const allResults = [];
  for (const q of queries) {
    const results = await braveSearch(db, q);
    for (const r of results) {
      if (allResults.find(x => x.url === r.url)) continue;
      allResults.push(r);
    }
    await new Promise(r => setTimeout(r, 100));
  }
  if (allResults.length === 0) return { incident_id: incident.id, status: 'no_search_results', queries };
  // Filter: prioritize news/obit URLs
  const NEWS = /(news|tribune|times|herald|post|gazette|chronicle|register|telegram|sun|star|wsbtv|wcpo|nbc|abc|cbs|fox\d|kstp|kgw|kcra|kgo|kron|kpix|legacy|funeralhome|obituar|memorial|ktla|wibc|wthr|fox59|wsbradio|11alive)/i;
  const sorted = allResults.sort((a, b) => (NEWS.test(b.url) ? 1 : 0) - (NEWS.test(a.url) ? 1 : 0));
  const top = sorted.slice(0, 4);
  // Fetch top 4 in parallel
  const fetched = await Promise.all(top.map(async t => {
    const text = await fetchPage(t.url);
    return text ? { url: t.url, title: t.title, text } : null;
  }));
  const corpus = fetched.filter(Boolean).map(f => `=== ${f.title} ===\nURL: ${f.url}\n${f.text}`).join('\n\n---\n\n');
  if (!corpus) return { incident_id: incident.id, status: 'no_corpus', search_results: allResults.length };

  const result = await claudeExtract(incident, corpus);
  if (!result || !result.is_match || !Array.isArray(result.victims) || result.victims.length === 0) {
    return { incident_id: incident.id, status: 'no_extract', search_results: allResults.length, fetched: fetched.filter(Boolean).length, reasoning: result?.reasoning || 'claude_returned_nothing' };
  }

  const { v4: uuid } = require('uuid');
  let inserted = 0;
  const insertedNames = [];
  for (const v of result.victims) {
    if (!v.name || v.name.length < 5 || !/\s/.test(v.name)) continue;
    const exists = await db('persons').where({ incident_id: incident.id, full_name: v.name }).first();
    if (exists) continue;
    try {
      await db('persons').insert({
        id: uuid(),
        incident_id: incident.id,
        full_name: v.name,
        role: v.role || 'victim',
        age: v.age || null,
        city: v.city || incident.city,
        state: v.state || incident.state,
        victim_verified: false,
        lead_tier: 'pending',
        source: 'nameless-incident-research',
        created_at: new Date()
      });
      inserted++;
      insertedNames.push(v.name);
      // Log
      await db('enrichment_logs').insert({
        person_id: null, field_name: 'nameless_research_extraction',
        old_value: null,
        new_value: JSON.stringify({ source: 'nameless-incident-research', incident: incident.incident_number, victim: v, confidence: result.match_confidence, evidence_articles: top.map(t => t.url) }).slice(0, 4000),
        created_at: new Date()
      }).catch(() => {});
    } catch { /* skip */ }
  }
  return {
    incident_id: incident.id,
    incident_number: incident.incident_number,
    status: 'success',
    persons_inserted: inserted,
    names: insertedNames,
    confidence: result.match_confidence,
    search_results: allResults.length,
    articles_read: fetched.filter(Boolean).length
  };
}

async function findNamelessTargets(db, limit) {
  return (await db.raw(`
    SELECT i.* FROM incidents i
    LEFT JOIN persons p ON p.incident_id = i.id
    WHERE p.id IS NULL
      AND i.severity IN ('fatal', 'critical')
      AND i.occurred_at > NOW() - INTERVAL '21 days'
      AND i.state IS NOT NULL
    ORDER BY
      CASE i.severity WHEN 'fatal' THEN 1 ELSE 2 END,
      i.occurred_at DESC
    LIMIT ${parseInt(limit) || 5}
  `)).rows;
}

module.exports = async function handler(req, res) {
  if (!authed(req)) return res.status(401).json({ error: 'unauthorized' });
  const action = req.query?.action || 'health';
  const db = getDb();

  if (action === 'health') {
    return res.status(200).json({
      ok: true, engine: 'nameless-incident-research', model: MODEL,
      strategy: 'brave_search + fetch_url + Claude Opus 4.7 extraction'
    });
  }
  if (action === 'research') {
    const id = req.query?.incident_id;
    if (!id) return res.status(400).json({ error: 'incident_id required' });
    const inc = await db('incidents').where('id', id).first();
    if (!inc) return res.status(404).json({ error: 'incident_not_found' });
    const result = await processIncident(db, inc);
    return res.status(200).json({ ok: true, result });
  }
  if (action === 'run') {
    const limit = Math.min(parseInt(req.query?.limit) || 3, 5);
    const targets = await findNamelessTargets(db, limit);
    const results = [];
    let total_inserted = 0;
    for (const inc of targets) {
      try {
        const r = await processIncident(db, inc);
        results.push(r);
        total_inserted += r.persons_inserted || 0;
      } catch (e) {
        results.push({ incident_id: inc.id, error: e.message });
      }
    }
    return res.status(200).json({ ok: true, processed: results.length, total_persons_inserted: total_inserted, results });
  }
  return res.status(400).json({ error: 'unknown action', valid: ['health','run','research'] });
};
