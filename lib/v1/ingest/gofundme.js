/**
 * GoFundMe accident campaign scraper.
 * Families start campaigns within 24-48h of fatal/severe accidents.
 * Search yields full names, family names, hospital, employer, photos.
 * Free public site search.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');

async function search(city, db) {
  const q = `accident OR crash OR injured OR funeral ${city || ''}`.trim();
  const url = `https://www.gofundme.com/mvc.php?route=homepage_norma/search&term=${encodeURIComponent(q)}&country=US`;
  let body = null, ok = false;
  try { const r = await fetch(url, { timeout: 10000, headers: { 'User-Agent': 'Mozilla/5.0 AIP', 'Accept': 'application/json' } }); if (r.ok) { body = await r.json(); ok = true; } } catch (_) {}
  await trackApiCall(db, 'ingest-gofundme', 'search', 0, 0, ok).catch(() => {});
  if (!body) return [];
  return (body.campaigns || body.results || []).slice(0, 25).map(c => ({
    title: c.title, url: c.url, location: c.location, raised: c.raised_amount, goal: c.goal_amount, created: c.created_at, summary: c.summary
  }));
}

async function run(db) {
  const cities = ['Akron OH', 'Cleveland OH', 'Houston TX', 'Atlanta GA', 'Tampa FL', 'Phoenix AZ', 'Cincinnati OH', 'Columbus OH', 'Dallas TX', 'Miami FL'];
  let total = 0, inserted = 0;
  for (const c of cities) {
    const items = await search(c, db);
    total += items.length;
    for (const it of items) {
      try {
        const sourceId = `gofundme-${(it.url || it.title || '').slice(-100)}`;
        await db('incidents').insert({
          source: 'gofundme',
          source_id: sourceId,
          description: `GoFundMe: ${it.title}`,
          accident_type: 'unknown',
          severity: /memorial|funeral|fatal|killed|loss/i.test(it.title || it.summary || '') ? 'fatal' : 'serious',
          city: c.split(' ')[0], state: c.split(' ')[1],
          occurred_at: new Date(),
          created_at: new Date(),
          raw_payload: JSON.stringify(it)
        }).onConflict('source_id').ignore();
        inserted++;
      } catch (_) {}
    }
  }
  return { cities: cities.length, fetched: total, inserted };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { city, action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'gofundme', cost: 0 });
    if (city) { const r = await search(city, db); return res.json({ count: r.length, results: r.slice(0, 5) }); }
    const out = await run(db);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'gofundme', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
