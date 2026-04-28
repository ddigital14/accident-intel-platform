/**
 * News Scraper for Victim Name Extraction
 * Cron: every 30 minutes
 * GET /api/v1/ingest/news?secret=ingest-now
 */
const { getModelForTask } = require('../system/model-registry');
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('../system/_errors');
const { batchInsert } = require('../../_batch');
const { dedupCache } = require('../../_cache');
const { enqueueCascade } = require('../system/_cascade');
const { extractVehiclesFromText, decodeVin } = require('../enrich/vehicle');
const { extractJson } = require('../enrich/_ai_router');

const NEWSAPI_KEY = process.env.NEWSAPI_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

const SEARCH_QUERIES = [
  'fatal crash',
  'car accident victim identified',
  'pedestrian struck killed',
  'motorcycle crash injured',
  'multi-vehicle collision'
];

async function fetchArticles() {
  if (!NEWSAPI_KEY) return [];
  const articles = [];
  for (const q of SEARCH_QUERIES) {
    try {
      const url = `https://newsapi.org/v2/everything?q=${encodeURIComponent(q)}&language=en&sortBy=publishedAt&pageSize=5&apiKey=${NEWSAPI_KEY}`;
      const resp = await fetch(url, { signal: AbortSignal.timeout(15000) });
      if (!resp.ok) continue;
      const data = await resp.json();
      for (const a of data.articles || []) {
        if (a.url && !articles.find(x => x.url === a.url)) articles.push(a);
      }
    } catch (_) {}
  }
  return articles;
}

async function extractVictims(db, article) {
  const text = `${article.title}\n\n${article.description || ''}\n\n${article.content || ''}`.substring(0, 4000);
  const prompt = `Extract structured information from this crash news article. Return ONLY valid JSON, no other text.

Article:
"""
${text}
"""

Schema:
{
  "is_crash_article": true|false,
  "city": "string|null",
  "state": "two-letter|null",
  "occurred_at": "ISO 8601 datetime|null",
  "incident_type": "car_accident|motorcycle_accident|truck_accident|pedestrian|bicycle|other",
  "severity": "fatal|critical|serious|moderate|minor|unknown",
  "lat": number|null,
  "lng": number|null,
  "fatalities_count": number|null,
  "injuries_count": number|null,
  "vehicles_involved": number|null,
  "victims": [
    {
      "full_name": "string",
      "first_name": "string|null",
      "last_name": "string|null",
      "age": number|null,
      "role": "driver|passenger|pedestrian|cyclist|other",
      "is_injured": true|false,
      "injury_severity": "fatal|incapacitating|non_incapacitating|possible|none|unknown",
      "transported_to": "hospital name|null",
      "city_residence": "string|null"
    }
  ],
  "police_department": "string|null",
  "highway": "string|null",
  "summary": "1-sentence summary"
}`;
  // Detect severity in headline to upgrade tier to gpt-4o for fatal cases
  const sevHint = /killed|fatal|deceased|died|deadly|dead/i.test(article.title || '') ? 'fatal'
                 : /serious|critical|life.threatening/i.test(article.title || '') ? 'serious'
                 : 'unknown';
  return await extractJson(db, {
    pipeline: 'news',
    systemPrompt: 'You are a forensic news analyst. Extract crash data into structured JSON only. If the article is not about a crash/accident, set is_crash_article to false and return minimal fields.',
    userPrompt: prompt,
    tier: 'auto',
    severityHint: sevHint,
    timeoutMs: 22000,
  });
}

async function findMatchingIncident(db, parsed, articleDate) {
  if (!parsed.is_crash_article) return null;
  const targetTime = parsed.occurred_at ? new Date(parsed.occurred_at) : new Date(articleDate);
  const windowStart = new Date(targetTime.getTime() - 6 * 60 * 60 * 1000);
  const windowEnd = new Date(targetTime.getTime() + 6 * 60 * 60 * 1000);
  if (parsed.lat && parsed.lng) {
    try {
      const r = await db.raw(`
        SELECT id FROM incidents
        WHERE occurred_at BETWEEN $1 AND $2
          AND geom IS NOT NULL
          AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography, 2000)
        ORDER BY ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography) ASC
        LIMIT 1
      `, [windowStart, windowEnd, parsed.lng, parsed.lat]);
      if (r.rows?.[0]) return r.rows[0];
    } catch (_) {}
  }
  if (parsed.city && parsed.state) {
    return db('incidents')
      .whereRaw('LOWER(city) = LOWER(?)', [parsed.city])
      .whereRaw('LOWER(state) = LOWER(?)', [parsed.state])
      .where('occurred_at', '>=', windowStart)
      .where('occurred_at', '<=', windowEnd)
      .orderBy('occurred_at', 'desc')
      .first();
  }
  return null;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  if (!NEWSAPI_KEY) return res.status(400).json({ error: 'NEWSAPI_KEY not configured' });

  const db = getDb();
  const results = { articles_fetched: 0, articles_parsed: 0, victims_extracted: 0,
                    incidents_matched: 0, incidents_created: 0, persons_added: 0,
                    skipped: 0, errors: [] };
  try {
    let newsDs = await db('data_sources').where('name', 'NewsAPI Crash Scraper').first();
    if (!newsDs) {
      const dsId = uuidv4();
      await db('data_sources').insert({
        id: dsId, name: 'NewsAPI Crash Scraper', type: 'news', provider: 'newsapi',
        api_endpoint: 'https://newsapi.org/v2/everything',
        is_active: true, last_polled_at: new Date(),
        created_at: new Date(), updated_at: new Date()
      });
      newsDs = { id: dsId };
    }

    const articles = (await fetchArticles()).slice(0, 12);
    results.articles_fetched = articles.length;
    const startTime = Date.now();
    const TIME_BUDGET_MS = 22000;

    const newPersons = [];
    const newIncidents = [];
    const newReports = [];

    for (const article of articles) {
      if (Date.now() - startTime > TIME_BUDGET_MS) {
        results.errors.push('time budget exhausted, deferring remaining articles');
        break;
      }
      try {
        const articleKey = `news:${article.url}`;
        if (dedupCache.has(articleKey)) { results.skipped++; continue; }
        const existing = await db('source_reports').where('source_reference', article.url).first();
        if (existing) { dedupCache.set(articleKey, 1); results.skipped++; continue; }
        dedupCache.set(articleKey, 1);

        const parsed = await extractVictims(db, article);
        if (!parsed) continue;
        results.articles_parsed++;
        if (!parsed.is_crash_article) continue;
        if ((!parsed.victims || parsed.victims.length === 0) && (!parsed.city || !parsed.state)) continue;

        const matched = await findMatchingIncident(db, parsed, article.publishedAt);
        const now = new Date();

        let incidentId;
        if (matched) {
          incidentId = matched.id;
          results.incidents_matched++;
          await db('incidents').where('id', incidentId).update({
            source_count: db.raw('COALESCE(source_count, 1) + 1'),
            confidence_score: db.raw('LEAST(99, COALESCE(confidence_score, 50) + 10)'),
            updated_at: now
          });
        } else {
          incidentId = uuidv4();
          const priority = parsed.severity === 'fatal' ? 1 :
                           parsed.severity === 'critical' || parsed.severity === 'serious' ? 2 :
                           parsed.severity === 'moderate' ? 3 : 4;
          newIncidents.push({
            id: incidentId,
            incident_number: `NEWS-${now.getFullYear().toString().slice(-2)}${String(now.getMonth()+1).padStart(2,'0')}${String(now.getDate()).padStart(2,'0')}-${Math.floor(Math.random()*9999).toString().padStart(4,'0')}`,
            incident_type: parsed.incident_type || 'car_accident',
            severity: parsed.severity || 'unknown',
            status: 'new', priority, confidence_score: 60,
            address: parsed.summary || article.title,
            city: parsed.city || null, state: parsed.state || null,
            highway: parsed.highway || null,
            latitude: parsed.lat || null, longitude: parsed.lng || null,
            occurred_at: parsed.occurred_at ? new Date(parsed.occurred_at) : new Date(article.publishedAt),
            reported_at: new Date(article.publishedAt),
            discovered_at: now,
            description: `${article.title}\n${parsed.summary || ''}\nSource: ${article.source?.name || 'unknown'}`,
            injuries_count: parsed.injuries_count ?? null,
            fatalities_count: parsed.fatalities_count ?? null,
            vehicles_involved: parsed.vehicles_involved ?? null,
            police_department: parsed.police_department || null,
            source_count: 1,
            first_source_id: newsDs.id,
            tags: ['news', 'newsapi'],
            created_at: now, updated_at: now
          });
          results.incidents_created++;
        }

        newReports.push({
          id: uuidv4(), incident_id: incidentId, data_source_id: newsDs.id,
          source_type: 'newsapi', source_reference: article.url,
          raw_data: JSON.stringify({ article, parsed }),
          parsed_data: JSON.stringify(parsed),
          contributed_fields: ['victims', 'severity', 'description', 'persons'],
          confidence: 60, is_verified: false,
          fetched_at: now, processed_at: now, created_at: now
        });

        // ── Extract vehicles from article ──
        try {
          const articleText = `${article.title}\n${article.description || ''}\n${article.content || ''}`;
          const vehiclesParsed = await extractVehiclesFromText(articleText);
          const vehicles = vehiclesParsed?.vehicles || [];
          for (const v of vehicles) {
            if (!v.year && !v.make && !v.license_plate && !v.vin) continue;
            // Decode VIN if available (free NHTSA API)
            let vinData = null;
            if (v.vin && v.vin.length === 17) {
              vinData = await decodeVin(v.vin);
            }
            await db('vehicles').insert({
              id: uuidv4(),
              incident_id: incidentId,
              year: v.year || vinData?.year || null,
              make: v.make || vinData?.make || null,
              model: v.model || vinData?.model || null,
              color: v.color || null,
              body_type: v.body_type || vinData?.body_type || null,
              license_plate: v.license_plate || null,
              license_state: v.license_state || null,
              vin: v.vin || null,
              damage_severity: v.damage_severity || null,
              is_commercial: !!(v.is_commercial || vinData?.is_commercial),
              dot_number: v.dot_number || null,
              carrier_name: v.carrier_name || null,
              metadata: JSON.stringify({ source: 'news', article_url: article.url, vin_decoded: !!vinData }),
              created_at: now, updated_at: now
            }).catch(() => {});
          }
        } catch (vErr) { results.errors.push(`vehicle: ${vErr.message}`); }

        if (parsed.victims && parsed.victims.length) {
          for (const v of parsed.victims) {
            if (!v.full_name && !v.last_name) continue;
            const fullName = v.full_name || `${v.first_name || ''} ${v.last_name || ''}`.trim();
            const existingPerson = await db('persons')
              .where('incident_id', incidentId)
              .whereRaw('LOWER(full_name) = LOWER(?)', [fullName])
              .first();
            if (existingPerson) {
              await db('persons').where('id', existingPerson.id).update({
                age: v.age || existingPerson.age,
                injury_severity: v.injury_severity || existingPerson.injury_severity,
                is_injured: v.is_injured ?? existingPerson.is_injured,
                transported_to: v.transported_to || existingPerson.transported_to,
                updated_at: now
              });
              continue;
            }
            newPersons.push({
              id: uuidv4(), incident_id: incidentId,
              role: v.role || 'driver',
              is_injured: v.is_injured ?? (v.injury_severity && v.injury_severity !== 'none'),
              first_name: v.first_name || (fullName.split(' ')[0] || null),
              last_name: v.last_name || (fullName.split(' ').slice(-1)[0] || null),
              full_name: fullName,
              age: v.age || null,
              injury_severity: v.injury_severity || null,
              transported_to: v.transported_to || null,
              city: v.city_residence || null,
              contact_status: 'not_contacted',
              confidence_score: 65,
              metadata: JSON.stringify({ source: 'news_extraction', article_url: article.url }),
              created_at: now, updated_at: now
            });
            results.victims_extracted++;
          }
        }
      } catch (e) {
        results.errors.push(e.message);
        await reportError(db, 'news', article.url, e.message);
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

    await db('data_sources').where('id', newsDs.id).update({
      last_polled_at: new Date(), last_success_at: new Date(), updated_at: new Date()
    });

    res.json({
      success: true,
      message: `News scraper: ${results.articles_fetched} fetched, ${results.victims_extracted} victims, ${results.incidents_created} new, ${results.incidents_matched} matched`,
      ...results,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'news', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
