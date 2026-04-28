
// Phase 29: Reddit OAuth (uses REDDIT_CLIENT_ID + REDDIT_CLIENT_SECRET when set).
let _redditToken = null, _redditTokenExpiry = 0;
async function getRedditToken() {
  if (_redditToken && Date.now() < _redditTokenExpiry) return _redditToken;
  const cid = process.env.REDDIT_CLIENT_ID; const sec = process.env.REDDIT_CLIENT_SECRET;
  if (!cid || !sec) return null;
  const auth = Buffer.from(cid + ':' + sec).toString('base64');
  try {
    const r = await fetch('https://www.reddit.com/api/v1/access_token', {
      method: 'POST', headers: { Authorization: 'Basic ' + auth, 'Content-Type': 'application/x-www-form-urlencoded', 'User-Agent': 'AIP-AccidentIntel/1.0' },
      body: 'grant_type=client_credentials'
    });
    if (r.ok) { const j = await r.json(); _redditToken = j.access_token; _redditTokenExpiry = Date.now() + (j.expires_in - 60) * 1000; return _redditToken; }
  } catch (_) {}
  return null;
}
async function authedFetch(url, opts = {}) {
  const tok = await getRedditToken();
  const headers = Object.assign({ 'User-Agent': 'AIP-AccidentIntel/1.0' }, opts.headers || {});
  if (tok) { headers.Authorization = 'Bearer ' + tok; url = url.replace('https://www.reddit.com', 'https://oauth.reddit.com'); }
  return fetch(url, Object.assign({}, opts, { headers }));
}
/**
 * Reddit City-Subreddit Aggregator
 *
 * Searches city + topic subreddits for accident/crash mentions.
 * Reddit's free JSON API requires no auth — just a User-Agent.
 *
 * Examples of high-value subs:
 *   r/houston, r/atlanta, r/Chicago, r/sanfrancisco, r/seattle, r/Boston
 *   r/news (national breaking)
 *   r/nyc, r/LosAngeles, r/Dallas, r/Philadelphia, r/Cincinnati
 *
 * GET /api/v1/ingest/reddit?secret=ingest-now
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

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

const SUBREDDITS = [
  { sub: 'houston', metro: 'Houston', state: 'TX' },
  { sub: 'atlanta', metro: 'Atlanta', state: 'GA' },
  { sub: 'Chicago', metro: 'Chicago', state: 'IL' },
  { sub: 'sanfrancisco', metro: 'San Francisco', state: 'CA' },
  { sub: 'LosAngeles', metro: 'Los Angeles', state: 'CA' },
  { sub: 'Seattle', metro: 'Seattle', state: 'WA' },
  { sub: 'Dallas', metro: 'Dallas', state: 'TX' },
  { sub: 'nyc', metro: 'New York', state: 'NY' },
  { sub: 'philadelphia', metro: 'Philadelphia', state: 'PA' },
  { sub: 'Cincinnati', metro: 'Cincinnati', state: 'OH' },
  { sub: 'Boston', metro: 'Boston', state: 'MA' },
  { sub: 'Miami', metro: 'Miami', state: 'FL' },
  { sub: 'orlando', metro: 'Orlando', state: 'FL' },
];

const SEARCH_TERMS = ['accident OR crash', 'fatal'];

async function fetchSubreddit(sub, query) {
  try {
    const url = `https://www.reddit.com/r/${sub}/search.json?q=${encodeURIComponent(query)}&restrict_sr=on&sort=new&limit=10&t=day`;
    const resp = await fetch(url, {
      headers: { 'User-Agent': 'AIP/1.0 (Accident Intelligence Platform; +https://accident-intel-platform.vercel.app)' },
      signal: AbortSignal.timeout(10000)
    });
    if (!resp.ok) return [];
    const data = await resp.json();
    return (data.data?.children || []).map(c => c.data);
  } catch (_) { return []; }
}

async function extractCrashFromPost(post, sub) {
  if (!OPENAI_API_KEY) return null;
  const text = `Title: ${post.title}\nBody: ${(post.selftext || '').substring(0, 1500)}\nSub: r/${sub.sub}`;
  const prompt = `Extract crash details from this Reddit post. Hint: ${sub.metro}, ${sub.state}.
"""
${text}
"""
JSON only:
{ "is_crash": true|false, "city": "string|null", "state": "two-letter|null",
  "incident_type": "car_accident|motorcycle_accident|truck_accident|pedestrian|bicycle|other",
  "severity": "fatal|serious|moderate|minor|unknown",
  "occurred_at": "ISO|null", "lat": number|null, "lng": number|null,
  "fatalities_count": number|null, "injuries_count": number|null,
  "victims": [{"full_name":"","age":null,"role":"","is_injured":true,"injury_severity":""}],
  "summary": "1 sentence" }
Set is_crash=false for off-topic posts (memes, equipment talk, etc).`;
  try {
    const resp = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: [
          { role: 'system', content: 'Extract crash info from Reddit posts as JSON. is_crash:false if not real crash.' },
          { role: 'user', content: prompt }
        ],
        temperature: 0,
        response_format: { type: 'json_object' }
      }),
      signal: AbortSignal.timeout(15000)
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
  const results = { subs_polled: 0, posts_found: 0, crash_posts: 0, parsed: 0, victims_extracted: 0,
                    incidents_matched: 0, incidents_created: 0, persons_added: 0, errors: [] };
  const startTime = Date.now();
  const TIME_BUDGET = 50000;

  try {
    let ds = await db('data_sources').where('name', 'Reddit Aggregator').first();
    if (!ds) {
      const dsId = uuidv4();
      await db('data_sources').insert({
        id: dsId, name: 'Reddit Aggregator', type: 'social_media',
        provider: 'reddit', api_endpoint: 'reddit.com/r/*/search.json',
        is_active: true, last_polled_at: new Date(),
        created_at: new Date(), updated_at: new Date()
      });
      ds = { id: dsId };
    }

    const newIncidents = [];
    const newReports = [];
    const newPersons = [];

    for (const sub of SUBREDDITS) {
      if (Date.now() - startTime > TIME_BUDGET) break;
      try {
        let posts = [];
        for (const term of SEARCH_TERMS) {
          posts = posts.concat(await fetchSubreddit(sub.sub, term));
          if (Date.now() - startTime > TIME_BUDGET) break;
        }
        // Dedup by url
        const seenUrls = new Set();
        posts = posts.filter(p => {
          if (!p.url || seenUrls.has(p.url)) return false;
          seenUrls.add(p.url);
          return true;
        });
        results.subs_polled++;
        results.posts_found += posts.length;

        for (const post of posts.slice(0, 4)) {
          if (Date.now() - startTime > TIME_BUDGET) break;
          try {
            const cacheKey = `reddit:${post.url}`;
            if (dedupCache.has(cacheKey)) continue;
            const exists = await db('source_reports').where('source_reference', post.url).first();
            if (exists) { dedupCache.set(cacheKey, 1); continue; }
            dedupCache.set(cacheKey, 1);

            const parsed = await extractCrashFromPost(post, sub);
            if (!parsed?.is_crash) continue;
            results.crash_posts++;
            results.parsed++;

            const now = new Date();
            const incidentId = uuidv4();
            const priority = parsed.severity === 'fatal' ? 1 : parsed.severity === 'serious' ? 2 : 3;

            // Geo match attempt
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
                confidence_score: db.raw('LEAST(99, COALESCE(confidence_score, 50) + 6)'),
                updated_at: now
              });
            } else {
              newIncidents.push({
                id: incidentId,
                incident_number: `RED-${now.getFullYear().toString().slice(-2)}${String(now.getMonth()+1).padStart(2,'0')}${String(now.getDate()).padStart(2,'0')}-${Math.floor(Math.random()*9999).toString().padStart(4,'0')}`,
                incident_type: parsed.incident_type || 'car_accident',
                severity: parsed.severity || 'unknown',
                status: 'new', priority,
                confidence_score: 45, // Reddit lower trust
                address: parsed.summary?.substring(0, 200) || post.title.substring(0, 200),
                city: parsed.city || sub.metro || null,
                state: parsed.state || sub.state || null,
                latitude: parsed.lat || null, longitude: parsed.lng || null,
                occurred_at: parsed.occurred_at ? new Date(parsed.occurred_at) : new Date(post.created_utc * 1000),
                reported_at: new Date(post.created_utc * 1000),
                discovered_at: now,
                description: `Reddit r/${sub.sub}: ${post.title}\n${(post.selftext || '').substring(0, 300)}\nURL: ${post.url}`,
                injuries_count: parsed.injuries_count ?? null,
                fatalities_count: parsed.fatalities_count ?? null,
                source_count: 1, first_source_id: ds.id,
                tags: ['reddit', sub.sub.toLowerCase()],
                created_at: now, updated_at: now
              });
              results.incidents_created++;
            }

            newReports.push({
              id: uuidv4(), incident_id: targetId, data_source_id: ds.id,
              source_type: 'reddit', source_reference: post.url,
              raw_data: JSON.stringify({ post, parsed }),
              parsed_data: JSON.stringify(parsed),
              contributed_fields: ['description', 'severity', 'victims'],
              confidence: 45, is_verified: false,
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
                confidence_score: 45,
                metadata: JSON.stringify({ source: 'reddit', subreddit: sub.sub, post_url: post.url }),
                created_at: now, updated_at: now
              });
              results.victims_extracted++;
            }
          } catch (e) {
            results.errors.push(e.message);
          }
        }
      } catch (e) {
        await reportError(db, 'reddit', sub.sub, e.message);
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
      message: `Reddit: ${results.subs_polled} subs, ${results.crash_posts} crash posts, ${results.victims_extracted} victims, ${results.incidents_created} new`,
      ...results,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'reddit', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
