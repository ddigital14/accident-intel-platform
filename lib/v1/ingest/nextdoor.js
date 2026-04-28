/**
 * NextDoor public-feed scraper. Neighbors post about accidents constantly:
 * "saw a crash on Howard St, ambulance + police on scene". NextDoor's public
 * neighborhood feeds are crawlable for accident keywords.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');

const NEIGHBORHOODS = [
  'akron-oh', 'cleveland-oh', 'houston-tx', 'atlanta-ga', 'tampa-fl',
  'phoenix-az', 'cincinnati-oh', 'dallas-tx', 'columbus-oh', 'miami-fl'
];

const ACCIDENT_KEYWORDS = /accident|crash|collision|wreck|hit|injured|ambulance|fire trucks|police on scene|fatal|killed|hospital/i;

async function fetchHood(slug, db) {
  const url = `https://nextdoor.com/news_feed/?init_source=org_homepage&neighborhood=${slug}`;
  let html = null, ok = false;
  try { const r = await fetch(url, { timeout: 10000, headers: { 'User-Agent': 'Mozilla/5.0 AIP' } }); if (r.ok) { html = await r.text(); ok = html.length > 500; } } catch (_) {}
  await trackApiCall(db, 'ingest-nextdoor', slug, 0, 0, ok).catch(() => {});
  if (!html) return [];
  const posts = [];
  // Try __NEXT_DATA__ JSON extraction
  const nextDataMatch = html.match(/<script[^>]+id=["']__NEXT_DATA__["'][^>]*>([\s\S]*?)<\/script>/);
  if (nextDataMatch) {
    try {
      const data = JSON.parse(nextDataMatch[1]);
      const items = data?.props?.pageProps?.feed?.items || data?.props?.pageProps?.posts || [];
      for (const it of items.slice(0, 25)) {
        const text = it.body || it.message || it.content || '';
        if (ACCIDENT_KEYWORDS.test(text)) posts.push({ text: text.slice(0, 500), author: it.author?.display_name, neighborhood: slug, posted_at: it.created_at });
      }
    } catch (_) {}
  }
  return posts;
}

async function run(db) {
  let total = 0, inserted = 0;
  for (const slug of NEIGHBORHOODS) {
    const posts = await fetchHood(slug, db);
    total += posts.length;
    for (const p of posts) {
      try {
        const sourceId = `nextdoor-${slug}-${(p.text || '').slice(0, 40).replace(/\W+/g, '-')}`;
        await db('source_reports').insert({
          source_type: 'nextdoor',
          source_reference: sourceId,
          parsed_data: JSON.stringify(p),
          created_at: new Date()
        }).onConflict('source_reference').ignore();
        inserted++;
      } catch (_) {}
    }
  }
  return { hoods: NEIGHBORHOODS.length, found: total, inserted };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'nextdoor', neighborhoods: NEIGHBORHOODS.length });
    const out = await run(db);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'nextdoor', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
