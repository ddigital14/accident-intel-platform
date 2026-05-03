/**
 * Phase 97b: Local-News Direct Fetcher
 *
 * When a victim's city is known, hit the local newspaper's site search directly
 * rather than relying on Brave (which has sparse indexing of small-market news).
 * Each metro maps to a list of local-paper search-URL templates. We fetch the
 * search results page, parse article links, fetch top articles, and return
 * combined text for downstream Claude extraction.
 */
const { getDb } = require('../../_db');

const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

// City/metro → array of search-URL templates. {q} = url-encoded query.
const METRO_PAPERS = {
  'Indianapolis': [
    'https://www.indystar.com/search/?q={q}',
    'https://www.wthr.com/search?q={q}',
    'https://fox59.com/?s={q}'
  ],
  'New York': [
    'https://www.nydailynews.com/search/?q={q}',
    'https://nypost.com/search/{q}/',
    'https://www.amny.com/?s={q}',
    'https://abc7ny.com/search/?searchterm={q}'
  ],
  'Los Angeles': [
    'https://www.latimes.com/search?q={q}',
    'https://ktla.com/?s={q}',
    'https://abc7.com/search/?searchterm={q}'
  ],
  'Chicago': [
    'https://www.chicagotribune.com/search/?q={q}',
    'https://www.suntimes.com/search?q={q}',
    'https://abc7chicago.com/search/?searchterm={q}'
  ],
  'Houston': [
    'https://www.houstonchronicle.com/search/?action=search&searchindex=solr&query={q}',
    'https://abc13.com/search/?searchterm={q}',
    'https://www.click2houston.com/search/?searchTerm={q}'
  ],
  'Atlanta': [
    'https://www.ajc.com/search/?q={q}',
    'https://www.fox5atlanta.com/search?q={q}',
    'https://www.wsbtv.com/search/?searchTerm={q}'
  ],
  'Miami': [
    'https://www.miamiherald.com/search?q={q}',
    'https://www.local10.com/search/?searchTerm={q}'
  ],
  'Tampa': [
    'https://www.tampabay.com/?s={q}',
    'https://www.abcactionnews.com/search?q={q}'
  ],
  'Orlando': [
    'https://www.orlandosentinel.com/search/?q={q}',
    'https://www.clickorlando.com/search/?searchTerm={q}'
  ],
  'Phoenix': [
    'https://www.azcentral.com/search/?q={q}',
    'https://www.abc15.com/search?q={q}'
  ],
  'Las Vegas': [
    'https://www.reviewjournal.com/?s={q}',
    'https://news3lv.com/?s={q}',
    'https://www.fox5vegas.com/search?q={q}'
  ],
  'Seattle': [
    'https://www.seattletimes.com/?s={q}',
    'https://komonews.com/?s={q}',
    'https://www.king5.com/search/?searchterm={q}'
  ],
  'Boston': [
    'https://www.bostonglobe.com/search/?q={q}',
    'https://whdh.com/?s={q}'
  ],
  'Philadelphia': [
    'https://www.inquirer.com/search.html?q={q}',
    'https://6abc.com/search/?searchterm={q}'
  ],
  'Detroit': [
    'https://www.freep.com/search/?q={q}',
    'https://www.detroitnews.com/search/?q={q}'
  ],
  'Denver': [
    'https://www.denverpost.com/?s={q}',
    'https://denver.cbslocal.com/?s={q}'
  ],
  'Dallas': [
    'https://www.dallasnews.com/search/?q={q}',
    'https://www.fox4news.com/search?q={q}'
  ],
  'Newark': [
    'https://www.nj.com/search/?q={q}',
    'https://abc7ny.com/search/?searchterm={q}'
  ],
  'Cleveland': [
    'https://www.cleveland.com/search/?q={q}',
    'https://fox8.com/?s={q}'
  ],
  'Akron': [
    'https://www.beaconjournal.com/search/?q={q}',
    'https://www.cleveland19.com/search?q={q}'
  ],
  'Cincinnati': [
    'https://www.cincinnati.com/search/?q={q}',
    'https://www.wcpo.com/search?q={q}'
  ],
  'St. Louis': [
    'https://www.stltoday.com/search/?q={q}',
    'https://www.ksdk.com/search/?searchterm={q}'
  ],
  'Pittsburgh': [
    'https://www.post-gazette.com/search/?q={q}',
    'https://www.wtae.com/searchresults?searchterm={q}'
  ],
  'Charlotte': [
    'https://www.charlotteobserver.com/search?q={q}',
    'https://www.wcnc.com/search/?searchterm={q}'
  ],
  'Nashville': [
    'https://www.tennessean.com/search/?q={q}',
    'https://www.newschannel5.com/search?q={q}'
  ],
  'Minneapolis': [
    'https://www.startribune.com/search/?q={q}',
    'https://kstp.com/?s={q}'
  ],
  'Portland': [
    'https://www.oregonlive.com/search/?q={q}',
    'https://www.kgw.com/search/?searchterm={q}'
  ],
  'San Francisco': [
    'https://www.sfchronicle.com/search/?action=search&searchindex=solr&query={q}',
    'https://abc7news.com/search/?searchterm={q}'
  ],
  'San Diego': [
    'https://www.sandiegouniontribune.com/?s={q}'
  ],
  'Sacramento': [
    'https://www.sacbee.com/search?q={q}',
    'https://www.kcra.com/searchresults?searchterm={q}'
  ]
};

function citykey(s) {
  if (!s) return null;
  const c = s.trim();
  for (const k of Object.keys(METRO_PAPERS)) {
    if (k.toLowerCase() === c.toLowerCase()) return k;
  }
  // partial match
  for (const k of Object.keys(METRO_PAPERS)) {
    if (c.toLowerCase().includes(k.toLowerCase()) || k.toLowerCase().includes(c.toLowerCase())) return k;
  }
  return null;
}

async function fetchPage(url) {
  try {
    const r = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9'
      },
      signal: AbortSignal.timeout(8000),
      redirect: 'follow'
    });
    if (!r.ok) return null;
    const html = await r.text();
    return html;
  } catch { return null; }
}

function stripHtml(html) {
  if (!html) return '';
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\s+/g, ' ')
    .trim();
}

function extractArticleLinks(html, baseUrl) {
  if (!html) return [];
  const out = [];
  const re = /<a[^>]+href="([^"]+)"[^>]*>([^<]{8,200})<\/a>/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    let href = m[1];
    const text = m[2].trim();
    if (!text || text.length < 12) continue;
    if (!href) continue;
    if (href.startsWith('//')) href = 'https:' + href;
    if (href.startsWith('/')) {
      try { href = new URL(href, baseUrl).href; } catch { continue; }
    }
    if (!/^https?:/.test(href)) continue;
    // Skip non-article paths
    if (/\/(tag|category|author|video|live|search|page)\//.test(href.toLowerCase())) continue;
    if (/(\.jpg|\.png|\.gif|\.css|\.js|\.ico)$/i.test(href)) continue;
    out.push({ href, text });
    if (out.length >= 30) break;
  }
  return out;
}

async function searchCity(city, name, options = {}) {
  const limit = options.limit || 4;
  const key = citykey(city);
  if (!key) return { ok: false, error: 'unknown_city', city };
  const tries = METRO_PAPERS[key];
  const queryEnc = encodeURIComponent(name);
  const allLinks = [];
  let papers_hit = 0, papers_responded = 0;
  for (const tmpl of tries) {
    papers_hit++;
    const url = tmpl.replace('{q}', queryEnc);
    const html = await fetchPage(url);
    if (!html) continue;
    papers_responded++;
    const baseUrl = url.split('?')[0].split('#')[0];
    const links = extractArticleLinks(html, baseUrl);
    // Filter: link text must contain at least one name token
    const tokens = name.toLowerCase().split(/\s+/).filter(t => t.length > 2);
    const filtered = links.filter(l => {
      const txt = l.text.toLowerCase();
      return tokens.some(t => txt.includes(t));
    });
    for (const l of filtered.slice(0, 6)) {
      allLinks.push({ ...l, source_url: url, paper: new URL(baseUrl).hostname });
    }
    await new Promise(r => setTimeout(r, 600)); // be polite
  }
  // Dedup by href
  const seen = new Set();
  const unique = allLinks.filter(l => {
    if (seen.has(l.href)) return false;
    seen.add(l.href);
    return true;
  }).slice(0, limit);

  // Fetch the actual article bodies in parallel
  const articles = await Promise.all(unique.map(async l => {
    const html = await fetchPage(l.href);
    if (!html) return null;
    // Extract og:title and prefer article body via class hints
    const ogTitle = (html.match(/<meta\s+(?:name|property)="og:title"\s+content="([^"]+)"/i) || [])[1];
    const ogDesc = (html.match(/<meta\s+(?:name|property)="og:description"\s+content="([^"]+)"/i) || [])[1];
    const text = stripHtml(html).slice(0, 4500);
    return { url: l.href, paper: l.paper, link_text: l.text, title: ogTitle, desc: ogDesc, body: text };
  }));
  return {
    ok: true,
    city: key,
    papers_hit,
    papers_responded,
    candidate_links: unique.length,
    articles_fetched: articles.filter(Boolean).length,
    articles: articles.filter(Boolean)
  };
}

module.exports = async function handler(req, res) {
  if (!authed(req)) return res.status(401).json({ error: 'unauthorized' });
  const action = req.query?.action || 'health';
  const db = getDb();
  if (action === 'health') {
    return res.status(200).json({
      ok: true, engine: 'local-news-fetcher',
      metros_supported: Object.keys(METRO_PAPERS).length,
      total_papers: Object.values(METRO_PAPERS).reduce((s, a) => s + a.length, 0)
    });
  }
  if (action === 'search') {
    const name = req.query?.name;
    const city = req.query?.city;
    if (!name || !city) return res.status(400).json({ error: 'name and city required' });
    const r = await searchCity(city, name, { limit: parseInt(req.query?.limit) || 4 });
    return res.status(200).json(r);
  }
  return res.status(400).json({ error: 'unknown action', valid: ['health','search'] });
};
module.exports.searchCity = searchCity;
module.exports.METRO_PAPERS = METRO_PAPERS;
