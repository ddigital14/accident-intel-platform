/**
 * Expanded hospital admission RSS feeds (beyond trauma board).
 * Many hospital systems publish weekly news/admission RSS that mention serious cases.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');

const FEEDS = [
  { name: 'Cleveland Clinic news', url: 'https://newsroom.clevelandclinic.org/feed/', state: 'OH' },
  { name: 'University Hospitals Cleveland', url: 'https://www.uhhospitals.org/about-uh/news.rss', state: 'OH' },
  { name: 'Akron Childrens', url: 'https://www.akronchildrens.org/News/RSS', state: 'OH' },
  { name: 'Memorial Hermann Houston', url: 'https://www.memorialhermann.org/about-us/news/feed', state: 'TX' },
  { name: 'Houston Methodist', url: 'https://www.houstonmethodist.org/blog/feed/', state: 'TX' },
  { name: 'Emory Health Atlanta', url: 'https://news.emory.edu/feeds/health.xml', state: 'GA' },
  { name: 'Tampa General', url: 'https://www.tgh.org/news/feed', state: 'FL' },
  { name: 'Banner Health Phoenix', url: 'https://www.bannerhealth.com/about-us/feeds/news', state: 'AZ' }
];

const KEYWORDS = /trauma|ICU|critical|fatality|fatal|airlifted|life-flight|life flight|crash|collision|injured/i;

async function fetchFeed(feed, db) {
  let xml = null, ok = false;
  try { const r = await fetch(feed.url, { timeout: 10000, headers: { 'User-Agent': 'AIP-AccidentIntel/1.0' } }); if (r.ok) { xml = await r.text(); ok = xml.length > 200; } } catch (_) {}
  await trackApiCall(db, 'ingest-hospital-rss', feed.name, 0, 0, ok).catch(() => {});
  if (!xml) return [];
  const items = [];
  const matches = [...xml.matchAll(/<item>([\s\S]*?)<\/item>/g)].slice(0, 20);
  for (const m of matches) {
    const title = (m[1].match(/<title>(?:<!\[CDATA\[)?([\s\S]*?)(?:\]\]>)?<\/title>/) || [])[1] || '';
    const link = (m[1].match(/<link>([\s\S]*?)<\/link>/) || [])[1] || '';
    if (KEYWORDS.test(title)) items.push({ title: title.trim(), link: link.trim(), state: feed.state, source: feed.name });
  }
  return items;
}

async function run(db) {
  let total = 0, inserted = 0;
  for (const feed of FEEDS) {
    const items = await fetchFeed(feed, db);
    total += items.length;
    for (const it of items) {
      try {
        const sourceId = `hosp-rss-${feed.name.replace(/\W+/g, '-')}-${it.title.slice(0, 50).replace(/\W+/g, '-')}`;
        await db('source_reports').insert({
          source_type: 'hospital_rss',
          source_reference: sourceId,
          parsed_data: JSON.stringify(it),
          created_at: new Date()
        }).onConflict('source_reference').ignore();
        inserted++;
      } catch (_) {}
    }
  }
  return { feeds: FEEDS.length, found: total, inserted };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'hospital-rss', feeds: FEEDS.length });
    const out = await run(db);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'hospital-rss', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
