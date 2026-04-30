/**
 * Phase 50b: Multi-community news ingest.
 *
 * Polls RSS feeds from publishers serving underrepresented U.S. communities.
 * Each item is run through the multi-language detector -> translated to
 * English (if foreign-lang) -> stored in source_reports for downstream
 * victim-verifier and standard pipelines.
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('../system/_errors');
const { dedupCache } = require('../../_cache');
const { trackApiCall } = require('../system/cost');
const { bumpCounter } = require('../system/_cei_telemetry');
const { detect, translateAndExtract } = require('../enrich/multilang-detector');
const { PI_KEYWORD_REGEX } = require('./_pi_keywords');

const ENGINE = 'multicommunity-news';

const COMMUNITY_SOURCES = [
  // Hispanic/Latino
  { name: 'Univision',         url: 'https://www.univision.com/feeds/rss',           lang: 'es', community: 'hispanic' },
  { name: 'Telemundo',         url: 'https://feeds.telemundo.com/Telemundo/local',   lang: 'es', community: 'hispanic' },
  { name: 'La Opinión',        url: 'https://laopinion.com/feed/',                   lang: 'es', community: 'hispanic' },
  { name: 'El Tiempo Latino',  url: 'https://eltiempolatino.com/feed/',              lang: 'es', community: 'hispanic' },
  { name: 'Mundo Hispánico',   url: 'https://mundohispanico.com/feed/',              lang: 'es', community: 'hispanic' },
  // Black/African American
  { name: 'Atlanta Black Star',url: 'https://atlantablackstar.com/feed/',            lang: 'en', community: 'black' },
  { name: 'NewsOne',           url: 'https://newsone.com/feed/',                     lang: 'en', community: 'black' },
  // Asian American
  { name: 'World Journal',     url: 'https://www.worldjournal.com/rss',              lang: 'zh', community: 'chinese' },
  { name: 'Asian Journal',     url: 'https://asianjournal.com/feed/',                lang: 'en', community: 'filipino' },
  { name: 'Korea Daily',       url: 'https://www.koreadaily.com/rss',                lang: 'ko', community: 'korean' },
  // Haitian/French
  { name: 'Le Floridien',      url: 'https://lefloridien.com/feed/',                 lang: 'fr', community: 'haitian' },
  // Native American
  { name: 'Indian Country Today', url: 'https://ictnews.org/feed',                   lang: 'en', community: 'native' },
  // Vietnamese
  { name: 'Nguoi Viet',        url: 'https://www.nguoi-viet.com/feed/',              lang: 'vi', community: 'vietnamese' },
];

function parseRssXml(xml) {
  const items = [];
  const itemMatches = xml.match(/<item[\s\S]*?<\/item>|<entry[\s\S]*?<\/entry>/gi) || [];
  for (const it of itemMatches) {
    const title = (it.match(/<title[^>]*>([\s\S]*?)<\/title>/i)?.[1] || '').replace(/<!\[CDATA\[|\]\]>/g, '').trim();
    const link  = (it.match(/<link[^>]*>([\s\S]*?)<\/link>/i)?.[1] || it.match(/<link[^>]*href="([^"]+)"/i)?.[1] || '').trim();
    const desc  = (it.match(/<description[^>]*>([\s\S]*?)<\/description>/i)?.[1] || it.match(/<summary[^>]*>([\s\S]*?)<\/summary>/i)?.[1] || '').replace(/<!\[CDATA\[|\]\]>/g, '').trim();
    const pubDate = (it.match(/<pubDate[^>]*>([\s\S]*?)<\/pubDate>/i)?.[1] || it.match(/<published[^>]*>([\s\S]*?)<\/published>/i)?.[1] || '').trim();
    if (title && link) items.push({ title, link, description: desc, publishedAt: pubDate });
  }
  return items;
}

async function ensureDataSource(db) {
  let ds = await db('data_sources').where('name', 'Multi-Community News').first().catch(() => null);
  if (ds) return ds;
  const id = uuidv4();
  try {
    await db('data_sources').insert({
      id, name: 'Multi-Community News', type: 'news',
      provider: 'rss-multicommunity',
      api_endpoint: `${COMMUNITY_SOURCES.length} community-focused outlets`,
      is_active: true, last_polled_at: new Date(),
      created_at: new Date(), updated_at: new Date()
    });
    return { id };
  } catch (_) { return { id }; }
}

async function processFeed(db, feed, ds, results, deadlineMs) {
  if (Date.now() > deadlineMs) return;
  const t0 = Date.now();
  let resp;
  try {
    resp = await fetch(feed.url, {
      headers: { 'User-Agent': 'AIP/1.0 (multi-community)', 'Accept': 'application/rss+xml, application/xml, text/xml' },
      signal: AbortSignal.timeout(15000),
    });
  } catch (e) {
    results.errors.push(`${feed.name}: fetch ${e.message}`);
    return;
  }
  if (!resp || !resp.ok) {
    results.errors.push(`${feed.name}: status ${resp?.status}`);
    return;
  }
  const xml = await resp.text();
  const items = parseRssXml(xml);
  results.feeds_polled++;
  results.items_total += items.length;

  const isLatin = ['en', 'es', 'fr', 'pt', 'tl', 'ht'].includes(feed.lang);
  const candidates = items.filter(it => {
    const txt = (it.title || '') + ' ' + (it.description || '');
    if (!isLatin) return true;
    return PI_KEYWORD_REGEX.test(txt);
  });
  results.crash_candidates += candidates.length;

  for (const it of candidates.slice(0, 5)) {
    if (Date.now() > deadlineMs) break;
    try {
      const cacheKey = `mc:${it.link}`;
      if (dedupCache.has(cacheKey)) continue;
      const exists = await db('source_reports').where('source_reference', it.link).first().catch(() => null);
      if (exists) { dedupCache.set(cacheKey, 1); continue; }
      dedupCache.set(cacheKey, 1);

      const text = `${it.title}\n${it.description || ''}`;
      const det = detect(text);

      if (det.lang) {
        results.foreign_detected++;
        results.by_lang[det.lang] = (results.by_lang[det.lang] || 0) + 1;
        const r = await translateAndExtract(db, text, it.link);
        if (r.ok) {
          results.translated++;
          results.victims_extracted += r.victims_extracted_count || 0;
        } else {
          results.errors.push(`${feed.name}: translate ${r.error || r.skipped || '?'}`);
        }
      } else {
        try {
          await db('source_reports').insert({
            id: uuidv4(),
            source_type: 'rss_community',
            source_reference: it.link,
            raw_data: JSON.stringify({ feed: feed.name, community: feed.community, item: it }),
            parsed_data: JSON.stringify({ source: feed.name, community: feed.community }),
            contributed_fields: ['source'],
            confidence: 50,
            is_verified: false,
            fetched_at: new Date(), processed_at: new Date(), created_at: new Date(),
            meta: JSON.stringify({ engine: ENGINE, community: feed.community, lang: 'en' })
          }).onConflict('source_reference').ignore();
          results.english_stored++;
        } catch (e) {
          results.errors.push(`${feed.name}: store ${e.message}`);
        }
      }
    } catch (e) {
      results.errors.push(`${feed.name}: item ${e.message}`);
    }
  }

  await trackApiCall(db, ENGINE, feed.url, true, Date.now() - t0).catch(() => {});
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const action = req.query?.action || 'run';

  if (action === 'health') {
    return res.json({
      success: true,
      engine: ENGINE,
      message: 'Multi-community news ingest online',
      sources: COMMUNITY_SOURCES.map(s => ({ name: s.name, lang: s.lang, community: s.community })),
      timestamp: new Date().toISOString()
    });
  }

  const startT = Date.now();
  const TIME_BUDGET_MS = 50000;
  const deadline = startT + TIME_BUDGET_MS;
  const results = {
    feeds_polled: 0, items_total: 0, crash_candidates: 0,
    foreign_detected: 0, translated: 0, english_stored: 0, victims_extracted: 0,
    by_lang: {}, errors: []
  };

  try {
    const ds = await ensureDataSource(db);
    for (const feed of COMMUNITY_SOURCES) {
      if (Date.now() > deadline) break;
      await processFeed(db, feed, ds, results, deadline);
    }
    await db('data_sources').where('id', ds.id).update({ last_polled_at: new Date(), updated_at: new Date() }).catch(() => {});
    await bumpCounter(db, ENGINE, true, Date.now() - startT).catch(() => {});

    return res.json({
      success: true,
      message: `multi-community: ${results.feeds_polled} feeds, ${results.translated} translated (${Object.keys(results.by_lang).length} langs), ${results.victims_extracted} victims, ${results.english_stored} English-stored`,
      ...results,
      total_latency_ms: Date.now() - startT,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, ENGINE, null, err.message).catch(() => {});
    await bumpCounter(db, ENGINE, false, Date.now() - startT).catch(() => {});
    return res.status(500).json({ error: err.message, results });
  }
};

module.exports.COMMUNITY_SOURCES = COMMUNITY_SOURCES;
