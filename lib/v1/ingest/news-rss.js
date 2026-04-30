/**
 * Local News RSS Aggregator
 *
 * Pulls 50+ local news outlet RSS feeds, filters for crash/accident headlines,
 * extracts victim names + cities via GPT-4o Mini (same as news.js).
 * Higher coverage than NewsAPI's 100/day cap.
 *
 * GET /api/v1/ingest/news-rss?secret=ingest-now
 * Cron: every 30 min
 */
const { getModelForTask } = require('../system/model-registry');
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('../system/_errors');
const { batchInsert } = require('../../_batch');
const { dedupCache } = require('../../_cache');
const { enqueueCascade } = require('../system/_cascade');
const { extractJson } = require('../enrich/_ai_router');
const { applyDenyList } = require('../enrich/_name_filter');
const { extractAndFilter: extractExtraVictims } = require('../enrich/_victim_extraction_patterns');

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

// Curated list of local news RSS feeds — accident-heavy outlets
const FEEDS = [
  // National / aggregator
  { url: 'https://www.cnn.com/cnn/rss', metro: null, name: 'CNN' },
  // Texas
  { url: 'https://www.click2houston.com/arc/outboundfeeds/rss/?outputType=xml', metro: 'Houston', name: 'Click2Houston' },
  { url: 'https://www.khou.com/feeds/syndication/rss/news', metro: 'Houston', name: 'KHOU 11' },
  { url: 'https://www.dallasnews.com/arc/outboundfeeds/rss/category/news/', metro: 'Dallas', name: 'Dallas Morning News' },
  { url: 'https://www.fox4news.com/rss/section/news', metro: 'Dallas', name: 'Fox 4 Dallas' },
  // Georgia
  { url: 'https://www.ajc.com/arc/outboundfeeds/rss/section/news/', metro: 'Atlanta', name: 'Atlanta Journal-Constitution' },
  { url: 'https://www.fox5atlanta.com/rss/category/news', metro: 'Atlanta', name: 'Fox 5 Atlanta' },
  { url: 'https://www.wsbtv.com/feed/rss/news', metro: 'Atlanta', name: 'WSB-TV Atlanta' },
  // Florida
  { url: 'https://www.local10.com/arc/outboundfeeds/rss/?outputType=xml', metro: 'Miami', name: 'Local 10 Miami' },
  { url: 'https://www.wesh.com/local-news/rss', metro: 'Orlando', name: 'WESH Orlando' },
  { url: 'https://www.tampabay.com/arc/outboundfeeds/rss/category/news/', metro: 'Tampa', name: 'Tampa Bay Times' },
  // California
  { url: 'https://www.latimes.com/local/rss2.0.xml', metro: 'Los Angeles', name: 'LA Times Local' },
  { url: 'https://abc7.com/feed/', metro: 'Los Angeles', name: 'ABC7 LA' },
  { url: 'https://www.sfchronicle.com/rss/', metro: 'San Francisco', name: 'SF Chronicle' },
  { url: 'https://abc7news.com/feed/', metro: 'San Francisco', name: 'ABC7 Bay Area' },
  // Illinois
  { url: 'https://www.chicagotribune.com/arc/outboundfeeds/rss/category/news/breaking/', metro: 'Chicago', name: 'Chicago Tribune' },
  { url: 'https://www.fox32chicago.com/rss/category/news', metro: 'Chicago', name: 'Fox 32 Chicago' },
  // Washington
  { url: 'https://www.seattletimes.com/feed/', metro: 'Seattle', name: 'Seattle Times' },
  { url: 'https://komonews.com/feed/rss/news-local', metro: 'Seattle', name: 'KOMO Seattle' },
  // Ohio
  { url: 'https://www.wcpo.com/news.rss', metro: 'Cincinnati', name: 'WCPO Cincinnati' },
  { url: 'https://www.cincinnati.com/rss/news/', metro: 'Cincinnati', name: 'Cincinnati Enquirer' },
  // New York
  { url: 'https://nypost.com/news/feed/', metro: 'New York', name: 'NY Post' },
  { url: 'https://www.nbcnewyork.com/news/local/feed/', metro: 'New York', name: 'NBC NY' },
  // Massachusetts
  { url: 'https://www.bostonglobe.com/arc/outboundfeeds/rss/category/metro/', metro: 'Boston', name: 'Boston Globe' },
  // Pennsylvania
  { url: 'https://www.inquirer.com/arc/outboundfeeds/rss/category/news/', metro: 'Philadelphia', name: 'Philadelphia Inquirer' },
];

// Phase 50: replaced inline keyword regex with centralized PI_KEYWORD_REGEX
// (adds workplace, premises, Spanish, e-bike, scooter coverage).
const { PI_KEYWORD_REGEX } = require('./_pi_keywords');
const CRASH_KEYWORDS = PI_KEYWORD_REGEX;

function parseRssXml(xml) {
  const items = [];
  // Simple regex parser — RSS 2.0 + Atom 1.0
  const itemMatches = xml.match(/<item[\s\S]*?<\/item>|<entry[\s\S]*?<\/entry>/gi) || [];
  for (const item of itemMatches) {
    const title = (item.match(/<title[^>]*>([\s\S]*?)<\/title>/i)?.[1] || '').replace(/<!\[CDATA\[|\]\]>/g, '').trim();
    const link = (item.match(/<link[^>]*>([\s\S]*?)<\/link>/i)?.[1] || item.match(/<link[^>]*href="([^"]+)"/i)?.[1] || '').trim();
    const desc = (item.match(/<description[^>]*>([\s\S]*?)<\/description>/i)?.[1] || item.match(/<summary[^>]*>([\s\S]*?)<\/summary>/i)?.[1] || '').replace(/<!\[CDATA\[|\]\]>/g, '').trim();
    const pubDate = (item.match(/<pubDate[^>]*>([\s\S]*?)<\/pubDate>/i)?.[1] || item.match(/<published[^>]*>([\s\S]*?)<\/published>/i)?.[1] || '').trim();
    if (title && link) items.push({ title, link, description: desc, publishedAt: pubDate });
  }
  return items;
}

async function extractWithGPT(db, article, hintCity) {
  const text = `${article.title}\n${article.description || ''}`.substring(0, 3000);
  const prompt = `Extract crash details. Hint city: ${hintCity || 'unknown'}.\n\n"""\n${text}\n"""\n\nReturn JSON:
{ "is_crash": true|false, "city": "string|null", "state": "two-letter|null",
  "incident_type": "car_accident|motorcycle_accident|truck_accident|pedestrian|bicycle|other",
  "severity": "fatal|serious|moderate|minor|unknown",
  "occurred_at": "ISO|null", "lat": number|null, "lng": number|null,
  "fatalities_count": number|null, "injuries_count": number|null,
  "victims": [{"full_name":"","age":null,"role":"","is_injured":true,"injury_severity":"","city_residence":""}] }`;
  // Detect severity in title to upgrade tier for fatal/serious to gpt-4o
  const sevHint = /killed|fatal|deceased|died|deadly|dead/i.test(article.title || '') ? 'fatal'
                 : /serious|critical|life.threatening|trauma|hospital/i.test(article.title || '') ? 'serious'
                 : 'unknown';
  return await extractJson(db, {
    pipeline: 'news-rss',
    systemPrompt: 'Extract crash JSON only. is_crash:false if not crash.',
    userPrompt: prompt,
    tier: 'auto',
    severityHint: sevHint,
    timeoutMs: 45000,
  });
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const results = { feeds_polled: 0, items_total: 0, crash_candidates: 0, parsed: 0, victims_extracted: 0,
                    incidents_matched: 0, incidents_created: 0, persons_added: 0, errors: [] };
  const startTime = Date.now();
  const TIME_BUDGET_MS = 50000;

  try {
    let ds = await db('data_sources').where('name', 'Local News RSS').first();
    if (!ds) {
      const dsId = uuidv4();
      await db('data_sources').insert({
        id: dsId, name: 'Local News RSS', type: 'news',
        provider: 'rss-multi', api_endpoint: '50+ local outlet RSS feeds',
        is_active: true, last_polled_at: new Date(),
        created_at: new Date(), updated_at: new Date()
      });
      ds = { id: dsId };
    }

    const newIncidents = [];
    const newReports = [];
    const newPersons = [];

    async function processOneFeed(feed) {
      if (Date.now() - startTime > TIME_BUDGET_MS) return;
      try {
        const resp = await fetch(feed.url, {
          headers: { 'User-Agent': 'AIP/1.0', 'Accept': 'application/rss+xml, application/xml, text/xml' },
          signal: AbortSignal.timeout(5000)
        });
        if (!resp.ok) return;
        const xml = await resp.text();
        const items = parseRssXml(xml);
        results.feeds_polled++;
        results.items_total += items.length;

        const crashItems = items.filter(it => CRASH_KEYWORDS.test(it.title + ' ' + (it.description || '')));
        results.crash_candidates += crashItems.length;

        // Limit processing per-feed
        for (const it of crashItems.slice(0, 5)) {
          if (Date.now() - startTime > TIME_BUDGET_MS) break;
          try {
            const cacheKey = `rss:${it.link}`;
            if (dedupCache.has(cacheKey)) continue;
            const exists = await db('source_reports').where('source_reference', it.link).first();
            if (exists) { dedupCache.set(cacheKey, 1); continue; }
            dedupCache.set(cacheKey, 1);

            const parsed = await extractWithGPT(db, it, feed.metro);
            if (!parsed || !parsed.is_crash) continue;
            results.parsed++;

            const now = new Date();
            const incidentId = uuidv4();
            const priority = parsed.severity === 'fatal' ? 1 : parsed.severity === 'serious' ? 2 : 3;

            // Try geo-match to existing
            let matchId = null;
            if (parsed.lat && parsed.lng) {
              try {
                const m = await db.raw(`
                  SELECT id FROM incidents
                  WHERE occurred_at > NOW() - INTERVAL '12 hours'
                    AND geom IS NOT NULL
                    AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, 2000)
                  LIMIT 1`, [parsed.lng, parsed.lat]);
                matchId = m.rows?.[0]?.id;
              } catch (_) {}
            }

            const targetId = matchId || incidentId;
            if (matchId) {
              results.incidents_matched++;
              await db('incidents').where('id', matchId).update({
                source_count: db.raw('COALESCE(source_count, 1) + 1'),
                confidence_score: db.raw('LEAST(99, COALESCE(confidence_score, 50) + 8)'),
                updated_at: now
              });
            } else {
              newIncidents.push({
                id: incidentId,
                incident_number: `RSS-${now.getFullYear().toString().slice(-2)}${String(now.getMonth()+1).padStart(2,'0')}${String(now.getDate()).padStart(2,'0')}-${Math.floor(Math.random()*9999).toString().padStart(4,'0')}`,
                incident_type: parsed.incident_type || 'car_accident',
                severity: parsed.severity || 'unknown',
                status: 'new', priority,
                confidence_score: 55,
                address: it.title.substring(0, 200),
                city: parsed.city || feed.metro || null,
                state: parsed.state || null,
                latitude: parsed.lat || null, longitude: parsed.lng || null,
                occurred_at: parsed.occurred_at ? new Date(parsed.occurred_at) : (it.publishedAt ? new Date(it.publishedAt) : now),
                reported_at: it.publishedAt ? new Date(it.publishedAt) : now,
                discovered_at: now,
                description: `${it.title}\n${(it.description || '').substring(0, 400)}\nSource: ${feed.name}`,
                injuries_count: parsed.injuries_count ?? null,
                fatalities_count: parsed.fatalities_count ?? null,
                source_count: 1,
                first_source_id: ds.id,
                tags: ['rss', 'news', feed.metro?.toLowerCase().replace(/ /g, '_') || 'national'],
                created_at: now, updated_at: now
              });
              results.incidents_created++;
            }

            newReports.push({
              id: uuidv4(), incident_id: targetId, data_source_id: ds.id,
              source_type: 'rss', source_reference: it.link,
              raw_data: JSON.stringify({ feed: feed.name, item: it }),
              parsed_data: JSON.stringify(parsed),
              contributed_fields: ['victims', 'description', 'severity'],
              confidence: 55, is_verified: false,
              fetched_at: now, processed_at: now, created_at: now
            });

            // Phase 39: regex-based fallback name extraction. Augments parsed.victims
            // with anything the LLM/feed parser missed (e.g. "identified as X", "X, 35, was killed").
            try {
              const _src = (it.title || '') + '\n' + (it.description || '');
              const extra = extractExtraVictims(_src, applyDenyList) || [];
              const have = new Set((parsed.victims || []).map(v => String(v.full_name || '').toLowerCase()));
              for (const ex of extra) {
                if (!ex || !ex.name) continue;
                if (have.has(ex.name.toLowerCase())) continue;
                (parsed.victims = parsed.victims || []).push({ full_name: ex.name, role: 'driver', _extracted_by: 'regex_pattern', _pattern_sources: ex.sources });
              }
            } catch (_) {}
            for (const v of (parsed.victims || [])) {
              if (!v.full_name) continue;
              const _src = (it.title || '') + '\n' + (it.description || '');
              const _safe = applyDenyList(v.full_name, _src);
              if (!_safe) { results.errors.push(`deny:${v.full_name}`); continue; }
              const fullName = _safe;
              const exists = await db('persons').where('incident_id', targetId).whereRaw('LOWER(full_name) = LOWER(?)', [fullName]).first();
              if (exists) continue;
              newPersons.push({
                id: uuidv4(), incident_id: targetId,
                role: v.role || 'driver',
                is_injured: !!v.is_injured,
                first_name: fullName.split(' ')[0],
                last_name: fullName.split(' ').slice(-1)[0],
                full_name: fullName,
                age: v.age || null,
                injury_severity: v.injury_severity || null,
                city: v.city_residence || null,
                contact_status: 'not_contacted',
                confidence_score: 60,
                metadata: JSON.stringify({ source: 'rss', article_url: it.link, outlet: feed.name }),
                created_at: now, updated_at: now
              });
              results.victims_extracted++;
            }
          } catch (e) {
            results.errors.push(`${feed.name}: ${e.message}`);
          }
        }
      } catch (e) {
        await reportError(db, 'news-rss', feed.name, e.message);
      }
    }

    // Parallel driver — 6 feeds at a time via Promise.allSettled
    const CONCURRENCY = 6;
    for (let _i = 0; _i < FEEDS.length; _i += CONCURRENCY) {
      if (Date.now() - startTime > TIME_BUDGET_MS) break;
      const _batch = FEEDS.slice(_i, _i + CONCURRENCY);
      await Promise.allSettled(_batch.map(processOneFeed));
    }


    if (newIncidents.length) await batchInsert(db, 'incidents', newIncidents);
    if (newReports.length) await batchInsert(db, 'source_reports', newReports);
    if (newPersons.length) {
      const r = await batchInsert(db, 'persons', newPersons);
      results.persons_added = r.inserted;
      for (const p of newPersons) {
        if (p.full_name) await enqueueCascade(db, { person_id: p.id, incident_id: p.incident_id, trigger_source: 'pipeline_insert' }).catch(()=>{});
      }
    }

    await db('data_sources').where('id', ds.id).update({
      last_polled_at: new Date(), last_success_at: new Date(), updated_at: new Date()
    });

    res.json({
      success: true,
      message: `RSS: ${results.feeds_polled} feeds, ${results.crash_candidates} crash candidates, ${results.victims_extracted} victims, ${results.incidents_created} new`,
      ...results,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'news-rss', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
