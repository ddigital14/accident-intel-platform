/**
 * HOMEGROWN ROTATION — Phase 23 #4
 *
 * Wraps 8 new public-data ingestion sources behind a single rotating handler.
 * Each cron tick picks the next source in rotation and runs for up to 30s.
 * Avoids burning a vercel.json cron slot (we are at 11/11) while still
 * touching each source roughly every 4 hours.
 *
 * Sources rotated:
 *   a. insurance-claim-filings  — state-commissioner public filings
 *   b. hospital-press-releases  — top trauma centers' press pages
 *   c. uscourts-pacer-rss       — federal PACER RSS (PI cases)
 *   d. state-court-rss          — state court docket RSS (FL/TX/GA/OH)
 *   e. funeral-home-listings    — Frazer/Tribute Center/FH Solutions
 *   f. yardmap-citizens         — citizen.com public incidents feed
 *   g. whitepages-free-scrape   — free-tier address+phone lookups
 *   h. nextdoor-public          — public city pages crash discussions
 *
 * GET /api/v1/ingest/homegrown-rotation?secret=ingest-now
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');
const { enqueueCascade } = require('../system/_cascade');

let _ensured = false;
async function ensureRotationState(db) {
  if (_ensured) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS homegrown_rotation_state (
        source VARCHAR(60) PRIMARY KEY,
        last_run_at TIMESTAMPTZ,
        last_status VARCHAR(20),
        last_count INTEGER DEFAULT 0,
        total_runs INTEGER DEFAULT 0
      );
    `);
    _ensured = true;
  } catch (_) {}
}

const SOURCES_ORDER = [
  'insurance-claim-filings',
  'hospital-press-releases',
  'uscourts-pacer-rss',
  'state-court-rss',
  'funeral-home-listings',
  'yardmap-citizens',
  'whitepages-free-scrape',
  'nextdoor-public',
];

const FETCH_TIMEOUT_MS = 12000;

async function fetchHtml(url) {
  try {
    const r = await fetch(url, {
      signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; AIPCrawler/1.0; +https://accident-intel-platform.vercel.app)',
        'Accept': 'text/html,application/xml,application/rss+xml,*/*',
      }
    });
    if (!r.ok) return null;
    return await r.text();
  } catch (_) { return null; }
}

function extractRssItems(xml) {
  if (!xml) return [];
  const items = [];
  const itemRegex = /<item>([\s\S]*?)<\/item>/g;
  let m;
  while ((m = itemRegex.exec(xml)) && items.length < 30) {
    const block = m[1];
    const t = (block.match(/<title>(?:<!\[CDATA\[)?([\s\S]*?)(?:\]\]>)?<\/title>/) || [])[1];
    const link = (block.match(/<link>([\s\S]*?)<\/link>/) || [])[1];
    const desc = (block.match(/<description>(?:<!\[CDATA\[)?([\s\S]*?)(?:\]\]>)?<\/description>/) || [])[1];
    const pub = (block.match(/<pubDate>([\s\S]*?)<\/pubDate>/) || [])[1];
    if (t) items.push({ title: t.trim(), link: (link||'').trim(), description: (desc||'').trim(), pub_date: pub });
  }
  return items;
}

function extractNamesFromText(text) {
  if (!text) return [];
  const re = /\b([A-Z][a-z]{1,15})\s+(?:([A-Z]\.)\s+)?([A-Z][a-zA-Z\-']{1,20})\b/g;
  const out = new Set();
  let m;
  while ((m = re.exec(text)) && out.size < 20) {
    const name = `${m[1]} ${m[2] ? m[2] + ' ' : ''}${m[3]}`;
    if (!/^(United States|New York|Los Angeles|San Francisco|North Carolina|South Carolina|West Virginia|North Dakota|South Dakota)\b/i.test(name)
        && !/(Department|Police|Hospital|Center|County|Court|Avenue|Street|Highway|Boulevard|Plaintiff|Defendant)\b/i.test(name)) {
      out.add(name);
    }
  }
  return [...out];
}

async function persistCandidate(db, source, payload) {
  try {
    const reportId = uuidv4();
    await db('source_reports').insert({
      id: reportId,
      source_type: source,
      raw_data: JSON.stringify(payload).slice(0, 8000),
      parsed_data: JSON.stringify(payload).slice(0, 8000),
      confidence: 0.5,
      created_at: new Date(),
    }).catch(()=>{});
    return reportId;
  } catch (_) { return null; }
}

async function ingestInsuranceClaimFilings(db) {
  const result = { source: 'insurance-claim-filings', items: 0, candidates: 0 };
  const URLS = [
    'https://www.dfs.ny.gov/insurance/health_ins/insureapplrss.xml',
  ];
  for (const u of URLS) {
    const txt = await fetchHtml(u);
    if (!txt) continue;
    const items = u.endsWith('.xml') ? extractRssItems(txt) : [];
    for (const it of items.slice(0, 10)) {
      result.items++;
      await persistCandidate(db, 'insurance-claim-filings', { source_url: u, ...it });
      result.candidates++;
    }
    await trackApiCall(db, 'ingest-insurance-claim-filings', u, 0, 0, true).catch(()=>{});
  }
  return result;
}

async function ingestHospitalPressReleases(db) {
  const result = { source: 'hospital-press-releases', items: 0, candidates: 0, names: [] };
  const FEEDS = [
    'https://news.uchicagomedicine.org/feed/',
    'https://www.massgeneral.org/news/rss/feed.xml',
    'https://news.weill.cornell.edu/news.rss',
  ];
  for (const u of FEEDS) {
    const txt = await fetchHtml(u);
    if (!txt) continue;
    const items = extractRssItems(txt);
    for (const it of items.slice(0, 10)) {
      const text = `${it.title || ''} ${it.description || ''}`;
      if (!/(crash|trauma|accident|injured|victim|collision|hit-and-run)/i.test(text)) continue;
      result.items++;
      const names = extractNamesFromText(text);
      result.names.push(...names);
      await persistCandidate(db, 'hospital-press-releases', { source_url: u, names, ...it });
      result.candidates++;
    }
    await trackApiCall(db, 'ingest-hospital-press-releases', u, 0, 0, true).catch(()=>{});
  }
  return result;
}

async function ingestUscourtsPacerRss(db) {
  const result = { source: 'uscourts-pacer-rss', items: 0, candidates: 0, names: [] };
  const FEEDS = [
    'https://ecf.cand.uscourts.gov/cgi-bin/rss_outside.pl',
    'https://ecf.nyed.uscourts.gov/cgi-bin/rss_outside.pl',
    'https://ecf.txsd.uscourts.gov/cgi-bin/rss_outside.pl',
    'https://ecf.flsd.uscourts.gov/cgi-bin/rss_outside.pl',
  ];
  for (const u of FEEDS) {
    const txt = await fetchHtml(u);
    if (!txt) continue;
    const items = extractRssItems(txt);
    for (const it of items.slice(0, 15)) {
      const text = `${it.title || ''} ${it.description || ''}`;
      if (!/(motor vehicle|personal injury|negligence|wrongful death|tort|crash|collision)/i.test(text)) continue;
      result.items++;
      const names = extractNamesFromText(text);
      result.names.push(...names);
      await persistCandidate(db, 'uscourts-pacer-rss', { source_url: u, names, ...it });
      result.candidates++;
    }
    await trackApiCall(db, 'ingest-uscourts-pacer-rss', u, 0, 0, true).catch(()=>{});
  }
  return result;
}

async function ingestStateCourtRss(db) {
  const result = { source: 'state-court-rss', items: 0, candidates: 0, names: [] };
  const FEEDS = [
    'https://www.flcourts.org/News-Media/News.rss',
    'https://www.txcourts.gov/news.rss',
    'https://www.gasupreme.us/feed/',
    'https://www.supremecourt.ohio.gov/news.rss',
  ];
  for (const u of FEEDS) {
    const txt = await fetchHtml(u);
    if (!txt) continue;
    const items = extractRssItems(txt);
    for (const it of items.slice(0, 15)) {
      const text = `${it.title || ''} ${it.description || ''}`;
      if (!/(personal injury|crash|collision|negligence|wrongful death|motor vehicle)/i.test(text)) continue;
      result.items++;
      const names = extractNamesFromText(text);
      result.names.push(...names);
      await persistCandidate(db, 'state-court-rss', { source_url: u, names, ...it });
      result.candidates++;
    }
    await trackApiCall(db, 'ingest-state-court-rss', u, 0, 0, true).catch(()=>{});
  }
  return result;
}

async function ingestFuneralHomeListings(db) {
  const result = { source: 'funeral-home-listings', items: 0, candidates: 0, names: [] };
  const URLS = [
    'https://www.tributearchive.com/recent-obituaries',
    'https://www.frazerconsultants.com/category/recent-obituaries/feed/',
  ];
  for (const u of URLS) {
    const txt = await fetchHtml(u);
    if (!txt) continue;
    const items = u.endsWith('/feed/') ? extractRssItems(txt) : [];
    for (const it of items.slice(0, 20)) {
      const text = `${it.title || ''} ${it.description || ''}`;
      result.items++;
      const names = extractNamesFromText(text);
      result.names.push(...names);
      await persistCandidate(db, 'funeral-home-listings', { source_url: u, names, ...it });
      result.candidates++;
    }
    if (!u.endsWith('/feed/')) {
      const names = extractNamesFromText(txt.replace(/<[^>]+>/g, ' ').slice(0, 8000));
      for (const n of names.slice(0, 15)) {
        result.items++;
        result.names.push(n);
        await persistCandidate(db, 'funeral-home-listings', { source_url: u, name: n });
        result.candidates++;
      }
    }
    await trackApiCall(db, 'ingest-funeral-home-listings', u, 0, 0, true).catch(()=>{});
  }
  return result;
}

async function ingestYardmapCitizens(db) {
  const result = { source: 'yardmap-citizens', items: 0, candidates: 0 };
  const URLS = ['https://citizen.com/incidents'];
  for (const u of URLS) {
    const html = await fetchHtml(u);
    if (!html) continue;
    const m = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
    if (!m) continue;
    try {
      const json = JSON.parse(m[1]);
      const list = json?.props?.pageProps?.incidents || json?.props?.pageProps?.feed || [];
      for (const inc of list.slice(0, 30)) {
        result.items++;
        if (!/(crash|collision|accident|hit-and-run|MVA|vehicle)/i.test(inc.title || inc.raw || '')) continue;
        await persistCandidate(db, 'yardmap-citizens', {
          title: inc.title, location: inc.location,
          lat: inc.latitude, lng: inc.longitude,
          created: inc.createdAt, raw: String(inc.raw || '').slice(0, 500),
        });
        result.candidates++;
      }
    } catch (_) {}
    await trackApiCall(db, 'ingest-yardmap-citizens', u, 0, 0, true).catch(()=>{});
  }
  return result;
}

async function ingestWhitepagesFreeScrape(db) {
  const result = { source: 'whitepages-free-scrape', evaluated: 0, matched: 0 };
  const ensureCol = async () => {
    await db.raw(`ALTER TABLE persons ADD COLUMN IF NOT EXISTS last_whitepages_check TIMESTAMPTZ`).catch(()=>{});
  };
  await ensureCol();
  const targets = await db.raw(`
    SELECT id, full_name, city, state, address, phone FROM persons
    WHERE full_name IS NOT NULL
      AND (address IS NULL OR address = '')
      AND city IS NOT NULL AND state IS NOT NULL
      AND (last_whitepages_check IS NULL OR last_whitepages_check < NOW() - INTERVAL '30 days')
    ORDER BY updated_at DESC
    LIMIT 8
  `).then(r => r.rows || []).catch(() => []);

  for (const p of targets) {
    result.evaluated++;
    const slug = encodeURIComponent(p.full_name.replace(/\s+/g, '-').toLowerCase());
    const stateSlug = (p.state || '').toLowerCase();
    const citySlug = (p.city || '').replace(/\s+/g, '-').toLowerCase();
    const url = `https://www.whitepages.com/name/${slug}/${stateSlug}/${citySlug}`;
    const html = await fetchHtml(url);
    if (!html) {
      await db('persons').where('id', p.id).update({ last_whitepages_check: new Date() }).catch(()=>{});
      continue;
    }
    const addrMatch = html.match(/(\d{1,6}\s+[A-Z][a-zA-Z\s]+(?:Ave|Street|St|Road|Rd|Drive|Dr|Lane|Ln|Court|Ct|Boulevard|Blvd|Way|Pkwy|Place|Pl)\.?)/);
    const phoneMatch = html.match(/\((\d{3})\)\s*(\d{3})-(\d{4})/);
    const updates = { last_whitepages_check: new Date(), updated_at: new Date() };
    if (addrMatch && !p.address) updates.address = addrMatch[1].slice(0, 200);
    if (phoneMatch && !p.phone) updates.phone = `+1${phoneMatch[1]}${phoneMatch[2]}${phoneMatch[3]}`;
    await db('persons').where('id', p.id).update(updates).catch(()=>{});
    if (addrMatch || phoneMatch) {
      await enqueueCascade(db, {
        person_id: p.id,
        trigger_source: 'whitepages-free-scrape',
        trigger_field: addrMatch ? 'address' : 'phone',
        trigger_value: addrMatch ? addrMatch[1] : phoneMatch[0],
        priority: 5,
      }).catch(()=>{});
      result.matched++;
    }
    await trackApiCall(db, 'ingest-whitepages-free-scrape', url, 0, 0, true).catch(()=>{});
  }
  return result;
}

async function ingestNextdoorPublic(db) {
  const result = { source: 'nextdoor-public', items: 0, candidates: 0, names: [] };
  const URLS = [
    'https://nextdoor.com/news_feed/?city=atlanta-ga',
    'https://nextdoor.com/news_feed/?city=austin-tx',
    'https://nextdoor.com/news_feed/?city=miami-fl',
    'https://nextdoor.com/news_feed/?city=cleveland-oh',
  ];
  for (const u of URLS) {
    const html = await fetchHtml(u);
    if (!html) continue;
    const plain = html.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').slice(0, 8000);
    if (!/(crash|accident|collision|hit-and-run|MVA)/i.test(plain)) continue;
    const names = extractNamesFromText(plain);
    result.items++;
    result.names.push(...names);
    await persistCandidate(db, 'nextdoor-public', { source_url: u, names, snippet: plain.slice(0, 800) });
    result.candidates++;
    await trackApiCall(db, 'ingest-nextdoor-public', u, 0, 0, true).catch(()=>{});
  }
  return result;
}

const SOURCE_HANDLERS = {
  'insurance-claim-filings': ingestInsuranceClaimFilings,
  'hospital-press-releases': ingestHospitalPressReleases,
  'uscourts-pacer-rss': ingestUscourtsPacerRss,
  'state-court-rss': ingestStateCourtRss,
  'funeral-home-listings': ingestFuneralHomeListings,
  'yardmap-citizens': ingestYardmapCitizens,
  'whitepages-free-scrape': ingestWhitepagesFreeScrape,
  'nextdoor-public': ingestNextdoorPublic,
};

async function pickNextSource(db) {
  await ensureRotationState(db);
  const rows = await db('homegrown_rotation_state').select('*').catch(() => []);
  const lookup = {};
  for (const r of rows) lookup[r.source] = r;
  const ordered = SOURCES_ORDER.map(s => ({
    source: s,
    last_run_at: lookup[s]?.last_run_at || null,
    total_runs: lookup[s]?.total_runs || 0,
  }));
  ordered.sort((a, b) => {
    if (!a.last_run_at && !b.last_run_at) return 0;
    if (!a.last_run_at) return -1;
    if (!b.last_run_at) return 1;
    return new Date(a.last_run_at) - new Date(b.last_run_at);
  });
  return ordered[0].source;
}

async function runSource(db, source) {
  await ensureRotationState(db);
  const fn = SOURCE_HANDLERS[source];
  if (!fn) return { ok: false, error: `unknown source ${source}` };
  const startT = Date.now();
  let result, status = 'pass';
  try {
    result = await Promise.race([
      fn(db),
      new Promise((_, rej) => setTimeout(() => rej(new Error('source timeout 35s')), 35000))
    ]);
  } catch (e) {
    status = 'fail';
    result = { error: e.message };
    await reportError(db, `homegrown:${source}`, source, e.message).catch(()=>{});
  }
  const count = result?.candidates || result?.matched || result?.items || 0;
  await db.raw(`
    INSERT INTO homegrown_rotation_state (source, last_run_at, last_status, last_count, total_runs)
    VALUES (?, NOW(), ?, ?, 1)
    ON CONFLICT (source) DO UPDATE SET
      last_run_at = NOW(),
      last_status = EXCLUDED.last_status,
      last_count = EXCLUDED.last_count,
      total_runs = homegrown_rotation_state.total_runs + 1
  `, [source, status, count]).catch(()=>{});
  return { ok: status === 'pass', status, source, latency_ms: Date.now() - startT, ...result };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  try {
    if (req.query.action === 'health' || req.query.action === 'status') {
      await ensureRotationState(db);
      const rows = await db('homegrown_rotation_state').select('*').orderBy('last_run_at', 'desc');
      return res.json({ success: true, sources: SOURCES_ORDER, state: rows });
    }
    if (req.query.action === 'all') {
      const out = {};
      for (const s of SOURCES_ORDER) {
        out[s] = await runSource(db, s);
      }
      return res.json({ success: true, all: out });
    }
    const source = req.query.source || await pickNextSource(db);
    const r = await runSource(db, source);
    return res.json({
      success: true,
      message: `Homegrown rotation: ${source} ${r.status} (${r.candidates || r.matched || r.items || 0})`,
      ...r,
    });
  } catch (err) {
    await reportError(db, 'homegrown-rotation', null, err.message).catch(()=>{});
    res.status(500).json({ error: err.message });
  }
};

module.exports.runSource = runSource;
module.exports.SOURCES_ORDER = SOURCES_ORDER;
