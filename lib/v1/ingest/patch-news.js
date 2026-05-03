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
  // Patch killed RSS in 2024. Scrape market homepage for article URLs, then
  // fetch each article body (in parallel, capped) to get real titles + descriptions.
  const url = marketToFeedUrl(slug);
  if (!url) return [];
  let html;
  try {
    const r = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; AccidentCommandCenter/1.0)',
        'Accept': 'text/html'
      },
      signal: AbortSignal.timeout(8000)
    });
    if (!r.ok) return [];
    html = await r.text();
    if (html.length < 1000) return [];
  } catch { return []; }
  const m = url.match(/patch\.com\/([^\/]+)\/([^\/]+)/);
  if (!m) return [];
  const stateSlug = m[1], citySlug = m[2];
  const articleRe = new RegExp(`href="(/${stateSlug}/${citySlug}/[a-z0-9][a-z0-9-]{20,})"`, 'gi');
  const seen = new Set();
  const candidates = [];
  let mm;
  while ((mm = articleRe.exec(html)) !== null) {
    const path = mm[1];
    if (seen.has(path)) continue;
    seen.add(path);
    if (/\/(calendar|advertise|business|around-town|arts-entertainment|best-of|bulletinboard|community-corner)\//.test(path)) continue;
    const slug = path.split('/').pop();
    candidates.push({ path, slug });
    if (candidates.length >= 12) break;
  }
  if (candidates.length === 0) return [];
  // Pre-filter on slug — if slug clearly contains crash terms, skip the slow body fetch
  // and use the slug as the title. Otherwise, fetch the article body for description+title.
  const items = [];
  const SLUG_FAST_PATH = /(crash|accident|killed|fatal|dies|hit-and-run|dui|drunk|collision|injured|hospitalized|pedestrian|motorcycle|tractor-trailer|rollover)/i;
  for (const c of candidates) {
    if (SLUG_FAST_PATH.test(c.slug)) {
      const title = c.slug.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
      items.push({ title, link: `https://patch.com${c.path}`, desc: title, pub: '' });
    }
  }
  // For non-fast-path slugs, fetch the article body to get a real description (one batch of 6 in parallel)
  const slow = candidates.filter(c => !SLUG_FAST_PATH.test(c.slug)).slice(0, 6);
  if (slow.length > 0) {
    const articles = await Promise.all(slow.map(async c => {
      try {
        const r = await fetch(`https://patch.com${c.path}`, {
          headers: { 'User-Agent': 'Mozilla/5.0 (compatible; AccidentCommandCenter/1.0)' },
          signal: AbortSignal.timeout(6000)
        });
        if (!r.ok) return null;
        const body = await r.text();
        // Extract og:title and og:description from meta tags
        const ogTitle = (body.match(/<meta\s+property="og:title"\s+content="([^"]+)"/i) || [])[1];
        const ogDesc = (body.match(/<meta\s+property="og:description"\s+content="([^"]+)"/i) || [])[1];
        const ogTime = (body.match(/<meta\s+property="article:published_time"\s+content="([^"]+)"/i) || [])[1];
        return {
          title: ogTitle || c.slug.replace(/-/g, ' '),
          link: `https://patch.com${c.path}`,
          desc: ogDesc || '',
          pub: ogTime || ''
        };
      } catch { return null; }
    }));
    for (const a of articles) if (a) items.push(a);
  }
  return items;
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
