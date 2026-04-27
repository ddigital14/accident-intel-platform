/**
 * Police Department Social Media Aggregator
 *
 * Police PIO accounts post accident details (often with names) on Twitter/X
 * and Facebook hours before press release. We use Nitter (self-hostable
 * Twitter mirror) and direct RSS feeds where available.
 *
 * Departments tracked:
 *   - HoustonPolice, AtlantaPD, ChicagoCAPS, SeattlePD, SFPD, NYPDnews, etc.
 *
 * GET /api/v1/ingest/police-social?secret=ingest-now
 * Cron: every 20 min
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('../system/_errors');
const { batchInsert } = require('../../_batch');
const { dedupCache } = require('../../_cache');
const { enqueueCascade } = require('../system/_cascade');

const { extractJson } = require('../enrich/_ai_router');

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

// Twitter/X via Nitter mirrors (free, no auth)
// Multiple mirrors for redundancy — picked dynamically based on availability
const NITTER_HOSTS = [
  'nitter.net',
  'nitter.poast.org',
  'nitter.privacydev.net',
  'nitter.cz'
];

const PD_ACCOUNTS = [
  { handle: 'HoustonPolice', metro: 'Houston', state: 'TX' },
  { handle: 'AtlantaPD',     metro: 'Atlanta', state: 'GA' },
  { handle: 'CPD_News',      metro: 'Chicago', state: 'IL' }, // Chicago Police News
  { handle: 'SeattlePD',     metro: 'Seattle', state: 'WA' },
  { handle: 'SFPD',          metro: 'San Francisco', state: 'CA' },
  { handle: 'NYPDnews',      metro: 'New York', state: 'NY' },
  { handle: 'DallasPD',      metro: 'Dallas', state: 'TX' },
  { handle: 'LAPDHQ',        metro: 'Los Angeles', state: 'CA' },
  { handle: 'CincinnatiPD',  metro: 'Cincinnati', state: 'OH' },
  { handle: 'PhillyPolice',  metro: 'Philadelphia', state: 'PA' },
  { handle: 'bostonpolice',  metro: 'Boston', state: 'MA' },
  { handle: 'MiamiPD',       metro: 'Miami', state: 'FL' },
];

async function fetchNitterRss(handle) {
  for (const host of NITTER_HOSTS) {
    try {
      const url = `https://${host}/${handle}/rss`;
      const resp = await fetch(url, {
        headers: { 'User-Agent': 'AIP/1.0' },
        signal: AbortSignal.timeout(8000)
      });
      if (resp.ok) {
        const xml = await resp.text();
        if (xml.includes('<rss') || xml.includes('<feed')) return xml;
      }
    } catch (_) {}
  }
  return null;
}

function parseRss(xml) {
  const items = [];
  const matches = xml.match(/<item[\s\S]*?<\/item>/gi) || [];
  for (const m of matches) {
    const title = (m.match(/<title[^>]*>([\s\S]*?)<\/title>/i)?.[1] || '').replace(/<!\[CDATA\[|\]\]>/g, '').trim();
    const link = (m.match(/<link[^>]*>([\s\S]*?)<\/link>/i)?.[1] || '').trim();
    const desc = (m.match(/<description[^>]*>([\s\S]*?)<\/description>/i)?.[1] || '').replace(/<!\[CDATA\[|\]\]>/g, '').replace(/<[^>]+>/g, ' ').trim();
    const pubDate = (m.match(/<pubDate[^>]*>([\s\S]*?)<\/pubDate>/i)?.[1] || '').trim();
    if (title) items.push({ title, link, description: desc, publishedAt: pubDate });
  }
  return items;
}

async function extractCrashFromTweet(tweet, pd) {
  if (!OPENAI_API_KEY) return null;
  const text = `${tweet.title}\n${tweet.description || ''}`.substring(0, 1500);
  const prompt = `Extract crash details from this police PIO tweet. Hint: ${pd.metro}, ${pd.state}.
"""
${text}
"""
JSON only (set is_crash:false for non-accident tweets like community events, recruitment, weather):
{ "is_crash": true|false, "city": "string|null", "state": "two-letter|null",
  "incident_type": "car_accident|motorcycle_accident|truck_accident|pedestrian|bicycle|other",
  "severity": "fatal|serious|moderate|minor|unknown",
  "occurred_at": "ISO|null", "lat": number|null, "lng": number|null,
  "fatalities_count": number|null, "injuries_count": number|null,
  "highway": "string|null", "address": "string|null",
  "police_report_number": "string|null",
  "victims": [{"full_name":"","age":null,"role":"","is_injured":true,"injury_severity":""}],
  "summary": "1 sentence" }`;
  try {
    const resp = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: [
          { role: 'system', content: 'Extract crash JSON from police tweets. is_crash:false if not crash.' },
          { role: 'user', content: prompt }
        ],
        temperature: 0,
        response_format: { type: 'json_object' }
      }),
      signal: AbortSignal.timeout(12000)
    });
    if (!resp.ok) return null;
    const data = await resp.json();
    return JSON.parse(data.choices?.[0]?.message?.content || '{}');
  } catch (_) { return null; }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const results = { accounts_polled: 0, tweets_total: 0, crash_tweets: 0, parsed: 0, victims_extracted: 0,
                    incidents_matched: 0, incidents_created: 0, persons_added: 0, errors: [] };
  const startTime = Date.now();
  const TIME_BUDGET = 50000;

  try {
    let ds = await db('data_sources').where('name', 'Police Social Media').first();
    if (!ds) {
      const dsId = uuidv4();
      await db('data_sources').insert({
        id: dsId, name: 'Police Social Media', type: 'social_media',
        provider: 'twitter-via-nitter', api_endpoint: 'nitter.net + mirrors',
        is_active: true, last_polled_at: new Date(),
        created_at: new Date(), updated_at: new Date()
      });
      ds = { id: dsId };
    }

    const newIncidents = [];
    const newReports = [];
    const newPersons = [];

    for (const pd of PD_ACCOUNTS) {
      if (Date.now() - startTime > TIME_BUDGET) break;
      try {
        const xml = await fetchNitterRss(pd.handle);
        if (!xml) {
          results.errors.push(`${pd.handle}: all Nitter mirrors failed`);
          continue;
        }
        const tweets = parseRss(xml);
        results.accounts_polled++;
        results.tweets_total += tweets.length;

        // Filter tweets that mention crash/accident keywords
        const candidates = tweets.filter(t =>
          /crash|accident|collision|fatal|killed|injured|struck|hit.?run|trooper/i.test(t.title + ' ' + (t.description || ''))
        );

        for (const tweet of candidates.slice(0, 4)) {
          if (Date.now() - startTime > TIME_BUDGET) break;
          try {
            const ref = `pd-tweet:${pd.handle}:${tweet.link}`;
            if (dedupCache.has(ref)) continue;
            const exists = await db('source_reports').where('source_reference', ref).first();
            if (exists) { dedupCache.set(ref, 1); continue; }
            dedupCache.set(ref, 1);

            const parsed = await extractCrashFromTweet(tweet, pd);
            if (!parsed?.is_crash) continue;
            results.crash_tweets++;
            results.parsed++;

            const now = new Date();
            const incidentId = uuidv4();
            const priority = parsed.severity === 'fatal' ? 1 : parsed.severity === 'serious' ? 2 : 3;

            let matchId = null;
            if (parsed.lat && parsed.lng) {
              try {
                const m = await db.raw(`
                  SELECT id FROM incidents WHERE occurred_at > NOW() - INTERVAL '12 hours'
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
                confidence_score: db.raw('LEAST(99, COALESCE(confidence_score, 50) + 12)'), // PD = high trust
                police_department: db.raw(`COALESCE(police_department, ?)`, [pd.handle]),
                updated_at: now
              });
            } else {
              newIncidents.push({
                id: incidentId,
                incident_number: `PDS-${now.getFullYear().toString().slice(-2)}${String(now.getMonth()+1).padStart(2,'0')}${String(now.getDate()).padStart(2,'0')}-${Math.floor(Math.random()*9999).toString().padStart(4,'0')}`,
                incident_type: parsed.incident_type || 'car_accident',
                severity: parsed.severity || 'unknown',
                status: 'new', priority,
                confidence_score: 80,  // PD = high confidence
                address: parsed.address || parsed.summary?.substring(0, 200) || tweet.title.substring(0, 200),
                city: parsed.city || pd.metro || null,
                state: parsed.state || pd.state || null,
                highway: parsed.highway || null,
                latitude: parsed.lat || null, longitude: parsed.lng || null,
                occurred_at: parsed.occurred_at ? new Date(parsed.occurred_at) : (tweet.publishedAt ? new Date(tweet.publishedAt) : now),
                reported_at: tweet.publishedAt ? new Date(tweet.publishedAt) : now,
                discovered_at: now,
                description: `Police PIO @${pd.handle}: ${tweet.title}\n${(tweet.description || '').substring(0, 300)}`,
                injuries_count: parsed.injuries_count ?? null,
                fatalities_count: parsed.fatalities_count ?? null,
                police_department: pd.handle,
                police_report_number: parsed.police_report_number || null,
                source_count: 1, first_source_id: ds.id,
                tags: ['police_social', 'twitter', pd.handle.toLowerCase()],
                created_at: now, updated_at: now
              });
              results.incidents_created++;
            }

            newReports.push({
              id: uuidv4(), incident_id: targetId, data_source_id: ds.id,
              source_type: 'police_social', source_reference: ref,
              raw_data: JSON.stringify({ pd, tweet, parsed }),
              parsed_data: JSON.stringify(parsed),
              contributed_fields: ['description', 'severity', 'police_department', 'victims'],
              confidence: 80, is_verified: true,
              fetched_at: now, processed_at: now, created_at: now
            });

            for (const v of (parsed.victims || [])) {
              if (!v.full_name) continue;
              const fn = v.full_name.trim();
              const exists = await db('persons').where('incident_id', targetId).whereRaw('LOWER(full_name) = LOWER(?)', [fn]).first();
              if (exists) continue;
              newPersons.push({
                id: uuidv4(), incident_id: targetId,
                role: v.role || 'driver',
                is_injured: !!v.is_injured,
                first_name: fn.split(' ')[0],
                last_name: fn.split(' ').slice(-1)[0],
                full_name: fn,
                age: v.age || null,
                injury_severity: v.injury_severity || null,
                contact_status: 'not_contacted',
                confidence_score: 80,
                metadata: JSON.stringify({ source: 'police_pio', handle: pd.handle, tweet_url: tweet.link }),
                created_at: now, updated_at: now
              });
              results.victims_extracted++;
            }
          } catch (e) {
            results.errors.push(`${pd.handle}: ${e.message}`);
          }
        }
      } catch (e) {
        await reportError(db, 'police-social', pd.handle, e.message);
      }
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
      message: `Police social: ${results.accounts_polled} accounts, ${results.crash_tweets} crash tweets, ${results.victims_extracted} victims, ${results.incidents_created} new`,
      ...results,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'police-social', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
