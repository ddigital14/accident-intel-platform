/**
 * Funeral home announcement scraper. Funeral notices typically post within 24h
 * of death — earlier than newspaper obituaries by 1-3 days. Each funeral home
 * has a "Recent Obituaries" page on their site. We aggregate from a list.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');

const FUNERAL_HOMES = [
  { name: 'Anthony-Anderson', city: 'Akron', state: 'OH', url: 'https://www.anthony-anderson.com/recent-obituaries' },
  { name: 'Cox-McNulty', city: 'Akron', state: 'OH', url: 'https://www.coxmcnultyfuneralhome.com/obituaries' },
  { name: 'Brown-Forward', city: 'Cleveland', state: 'OH', url: 'https://www.brownforward.com/obituaries' },
  { name: 'Geo-H-Lewis', city: 'Houston', state: 'TX', url: 'https://www.lewisfuneralhouston.com/obituaries' },
  { name: 'HM Patterson', city: 'Atlanta', state: 'GA', url: 'https://www.hmpatterson.com/obituaries' }
];

async function fetchFuneralHome(home, db) {
  let html = null, ok = false;
  try { const r = await fetch(home.url, { timeout: 10000, headers: { 'User-Agent': 'Mozilla/5.0 AIP' } }); if (r.ok) { html = await r.text(); ok = html.length > 500; } } catch (_) {}
  await trackApiCall(db, 'ingest-funeral-homes', home.name, 0, 0, ok).catch(() => {});
  if (!html) return [];
  const names = [];
  // Try common pattern: <h2|h3|h4> Firstname Lastname </h2>
  const hMatches = [...html.matchAll(/<h[2-4][^>]*>\s*([A-Z][a-zA-Z'.-]+\s+(?:[A-Z]\.?\s+)?[A-Z][a-zA-Z'.-]+(?:\s+(?:Jr|Sr|III|II|IV))?)\s*<\/h[2-4]>/g)];
  for (const m of hMatches) names.push({ name: m[1].trim(), source: home.name, city: home.city, state: home.state, url: home.url });
  return names.slice(0, 25);
}

async function run(db) {
  let total = 0, inserted = 0;
  for (const home of FUNERAL_HOMES) {
    const names = await fetchFuneralHome(home, db);
    total += names.length;
    for (const n of names) {
      try {
        const sourceId = `funeral-${home.name}-${n.name.replace(/\s+/g, '-')}`;
        await db('source_reports').insert({
          source_type: 'funeral_home',
          source_reference: sourceId,
          parsed_data: JSON.stringify({ full_name: n.name, city: n.city, state: n.state, source: n.source }),
          created_at: new Date()
        }).onConflict('source_reference').ignore();
        inserted++;
      } catch (_) {}
    }
  }
  return { homes: FUNERAL_HOMES.length, found: total, inserted };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'funeral-homes', homes: FUNERAL_HOMES.length, cost: 0 });
    const out = await run(db);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'funeral-homes', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
