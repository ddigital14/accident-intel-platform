/**
 * Phase 92: Patch.com Hyperlocal News Ingester
 * 50 markets, RSS-based ingest, filter for crash/accident/fatal keywords.
 */
const { getDb } = require('../../_db');

const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

const KEYWORDS = /(crash|accident|collision|killed|fatal|injured|hospitalized|dui|drunk\s*driver|hit[\s-]and[\s-]run|pedestrian struck|motorcycle|tractor[\s-]trailer|rollover|t-bone|head[\s-]on)/i;
const NON_VEHICLE = /(stabbing|shooting|burglary|theft|robbery|fire|drowning|overdose|missing person|wanted)/i;

const MARKETS = [
  'patch.com/new-jersey/jerseycity', 'patch.com/new-jersey/hoboken', 'patch.com/new-jersey/montclair',
  'patch.com/new-jersey/newark', 'patch.com/new-jersey/edison', 'patch.com/new-jersey/clifton',
  'patch.com/new-york/new-york-city', 'patch.com/new-york/midtown-nyc', 'patch.com/new-york/brooklyn',
  'patch.com/new-york/queens', 'patch.com/new-york/bronx', 'patch.com/new-york/staten-island',
  'patch.com/new-york/yonkers', 'patch.com/new-york/whiteplains',
  'patch.com/connecticut/stamford', 'patch.com/connecticut/norwalk', 'patch.com/connecticut/bridgeport',
  'patch.com/connecticut/newhaven', 'patch.com/connecticut/hartford',
  'patch.com/massachusetts/boston', 'patch.com/massachusetts/cambridge', 'patch.com/massachusetts/quincy',
  'patch.com/massachusetts/worcester', 'patch.com/massachusetts/springfield',
  'patch.com/pennsylvania/philadelphia', 'patch.com/pennsylvania/pittsburgh',
  'patch.com/illinois/chicago', 'patch.com/illinois/naperville', 'patch.com/illinois/aurora',
  'patch.com/california/losangeles', 'patch.com/california/sanjose', 'patch.com/california/sandiego',
  'patch.com/california/oakland', 'patch.com/california/sacramento',
  'patch.com/florida/miami', 'patch.com/florida/tampa', 'patch.com/florida/orlando',
  'patch.com/florida/jacksonville', 'patch.com/florida/fortlauderdale',
  'patch.com/georgia/atlanta', 'patch.com/georgia/marietta', 'patch.com/georgia/decatur',
  'patch.com/georgia/sandy-springs',
  'patch.com/texas/houston', 'patch.com/texas/dallas', 'patch.com/texas/austin',
  'patch.com/texas/sanantonio', 'patch.com/texas/fortworth',
  'patch.com/ohio/akron', 'patch.com/ohio/cleveland', 'patch.com/ohio/columbus', 'patch.com/ohio/cincinnati',
  'patch.com/virginia/arlingtonva', 'patch.com/virginia/alexandria', 'patch.com/dc/washington',
  'patch.com/maryland/baltimore'
];

function marketToFeedUrl(slug) {
  const m = slug.match(/^patch\.com\/([^\/]+)\/(.+)$/);
  if (!m) return null;
  return `https://patch.com/${m[1]}/${m[2]}`;
}

function parseStateFromMarket(slug) {
  const m = slug.match(/^patch\.com\/([a-z\-]+)\//);
  if (!m) return null;
  const map = { 'new-jersey': 'NJ', 'new-york': 'NY', 'connecticut': 'CT', 'massachusetts': 'MA',
    'pennsylvania': 'PA', 'illinois': 'IL', 'california': 'CA', 'florida': 'FL',
    'georgia': 'GA', 'texas': 'TX', 'ohio': 'OH', 'virginia': 'VA', 'maryland': 'MD', 'dc': 'DC' };
  return map[m[1]] || null;
}

async function fetchMarket(slug) {
  const url = marketToFeedUrl(slug);
  if (!url) return [];
  const tries = [`${url}/recent.xml`, `${url}/feed.xml`, url];
  for (const u of tries) {
    try {
      const r = await fetch(u, {
        headers: { 'User-Agent': 'AccidentCommandCenter/1.0', 'Accept': 'application/xml,text/html' },
        signal: AbortSignal.timeout(8000)
      });
      if (!r.ok) continue;
      const txt = await r.text();
      if (txt.length < 500) continue;
      const items = [];
      const itemRe = /<item[\s\S]*?<\/item>/gi;
      const titleRe = /<title>([\s\S]*?)<\/title>/i;
      const linkRe = /<link>([\s\S]*?)<\/link>/i;
      const descRe = /<description>([\s\S]*?)<\/description>/i;
      const pubRe = /<pubDate>([\s\S]*?)<\/pubDate>/i;
      let m;
      while ((m = itemRe.exec(txt)) !== null) {
        const block = m[0];
        const title = (titleRe.exec(block) || [])[1] || '';
        const link = (linkRe.exec(block) || [])[1] || '';
        const desc = (descRe.exec(block) || [])[1] || '';
        const pub = (pubRe.exec(block) || [])[1] || '';
        items.push({
          title: title.replace(/<!\[CDATA\[|\]\]>/g, '').trim(),
          link: link.trim(),
          desc: desc.replace(/<!\[CDATA\[|\]\]>/g, '').replace(/<[^>]+>/g, '').trim(),
          pub
        });
      }
      if (items.length === 0) {
        const entryRe = /<entry[\s\S]*?<\/entry>/gi;
        while ((m = entryRe.exec(txt)) !== null) {
          const block = m[0];
          const title = (titleRe.exec(block) || [])[1] || '';
          const linkM = block.match(/<link[^>]*href=["']([^"']+)["']/i);
          const summaryM = block.match(/<summary[^>]*>([\s\S]*?)<\/summary>/i);
          const pubM = block.match(/<published>([\s\S]*?)<\/published>/i);
          items.push({
            title: title.replace(/<!\[CDATA\[|\]\]>/g, '').trim(),
            link: (linkM || [])[1] || '',
            desc: ((summaryM || [])[1] || '').replace(/<!\[CDATA\[|\]\]>/g, '').replace(/<[^>]+>/g, '').trim(),
            pub: (pubM || [])[1] || ''
          });
        }
      }
      return items;
    } catch { continue; }
  }
  return [];
}

function isRelevant(item) {
  const blob = `${item.title} ${item.desc}`;
  if (NON_VEHICLE.test(blob) && !KEYWORDS.test(blob)) return false;
  return KEYWORDS.test(blob);
}

async function ingestMarket(db, slug) {
  const state = parseStateFromMarket(slug);
  if (!state) return { slug, ok: false, error: 'unknown_state' };
  const items = await fetchMarket(slug);
  if (items.length === 0) return { slug, state, ok: true, items: 0, inserted: 0 };
  const relevant = items.filter(isRelevant);
  let inserted = 0, skipped = 0;
  const { v4: uuid } = require('uuid');
  for (const it of relevant) {
    const ref = `patch:${slug}:${(it.link || it.title).slice(0, 80)}`;
    const exists = await db('incidents').where('incident_number', ref).first();
    if (exists) { skipped++; continue; }
    const blob = `${it.title} ${it.desc}`;
    const isFatal = /killed|fatal|dies|died|deceased/i.test(blob);
    const isInjured = /injured|hospitalized|critical condition/i.test(blob);
    const severity = isFatal ? 'fatal' : (isInjured ? 'critical' : 'minor');
    const score = isFatal ? 65 : (isInjured ? 45 : 25);
    try {
      await db('incidents').insert({
        id: uuid(), incident_number: ref, state,
        city: slug.split('/').pop().replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
        severity, incident_type: 'car_accident', fatalities_count: isFatal ? 1 : 0,
        description: it.title.slice(0, 500),
        raw_description: `${it.title}\n\n${it.desc}\n\n${it.link}`.slice(0, 4000),
        occurred_at: it.pub ? new Date(it.pub) : new Date(),
        discovered_at: new Date(), qualification_state: 'pending', lead_score: score, source_count: 1
      });
      inserted++;
    } catch { skipped++; }
  }
  return { slug, state, ok: true, items: items.length, relevant: relevant.length, inserted, skipped };
}

module.exports = async function handler(req, res) {
  if (!authed(req)) return res.status(401).json({ error: 'unauthorized' });
  const action = req.query?.action || 'health';
  const db = getDb();
  if (action === 'health') {
    return res.status(200).json({ ok: true, engine: 'patch-news', markets_configured: MARKETS.length, markets_sample: MARKETS.slice(0, 5) });
  }
  if (action === 'run_market') {
    const slug = req.query?.market;
    if (!slug) return res.status(400).json({ error: 'market required' });
    const matched = MARKETS.find(m => m.endsWith(slug) || m === slug);
    if (!matched) return res.status(404).json({ error: 'unknown market' });
    const r = await ingestMarket(db, matched);
    return res.status(200).json({ ok: true, ...r });
  }
  if (action === 'run') {
    const limit = parseInt(req.query?.limit) || MARKETS.length;
    const markets = MARKETS.slice(0, limit);
    const results = [];
    let totalInserted = 0, totalRelevant = 0;
    for (let i = 0; i < markets.length; i += 6) {
      const batch = markets.slice(i, i + 6);
      const batchResults = await Promise.all(batch.map(m => ingestMarket(db, m).catch(e => ({ slug: m, ok: false, error: e.message }))));
      for (const r of batchResults) {
        results.push(r);
        if (r.inserted) totalInserted += r.inserted;
        if (r.relevant) totalRelevant += r.relevant;
      }
      await new Promise(r => setTimeout(r, 500));
    }
    return res.status(200).json({ ok: true, markets_processed: results.length, total_relevant: totalRelevant, total_inserted: totalInserted, results: results.slice(0, 20) });
  }
  return res.status(400).json({ error: 'unknown action', valid: ['health', 'run', 'run_market'] });
};
