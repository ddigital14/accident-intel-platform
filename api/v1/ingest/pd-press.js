/**
 * Police Department Press Release Scraper
 *
 * Direct scrape of each PD's news/press page (more reliable than Nitter).
 * Each PD posts crash details, sometimes with names, on their public news page
 * within hours of an incident.
 *
 * GET /api/v1/ingest/pd-press?secret=ingest-now
 * Cron: every 30 min
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('../system/_errors');
const { batchInsert } = require('../../_batch');
const { dedupCache } = require('../../_cache');
const { normalizeIncident, normalizePerson } = require('../../_schema');

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

// Police department news URLs (publicly accessible, no auth)
const PD_PAGES = [
  { name: 'NYPD News',         url: 'https://www.nyc.gov/site/nypd/news/news.page', metro: 'New York', state: 'NY' },
  { name: 'LAPD News',         url: 'https://www.lapdonline.org/newsroom/', metro: 'Los Angeles', state: 'CA' },
  { name: 'Chicago Police',    url: 'https://home.chicagopolice.org/news/', metro: 'Chicago', state: 'IL' },
  { name: 'Houston Police',    url: 'https://www.houstontx.gov/police/nr/index.htm', metro: 'Houston', state: 'TX' },
  { name: 'Atlanta PD',        url: 'https://www.atlantapd.org/Home/Components/News/News?', metro: 'Atlanta', state: 'GA' },
  { name: 'Dallas PD',         url: 'https://dallaspolice.net/News/Pages/default.aspx', metro: 'Dallas', state: 'TX' },
  { name: 'Seattle PD blotter',url: 'https://spdblotter.seattle.gov/', metro: 'Seattle', state: 'WA' },
  { name: 'SFPD News',         url: 'https://www.sanfranciscopolice.org/news', metro: 'San Francisco', state: 'CA' },
  { name: 'Cincinnati PD',     url: 'https://www.cincinnati-oh.gov/police/news/', metro: 'Cincinnati', state: 'OH' },
  { name: 'Boston PD News',    url: 'https://www.boston.gov/news?topic=43396', metro: 'Boston', state: 'MA' },
  { name: 'Miami PD',          url: 'https://www.miami-police.org/news.html', metro: 'Miami', state: 'FL' },
  { name: 'Philly PD',         url: 'https://www.phillypolice.com/news/', metro: 'Philadelphia', state: 'PA' }
];

async function fetchPage(url) {
  try {
    const r = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml'
      },
      signal: AbortSignal.timeout(12000)
    });
    if (!r.ok) return null;
    return (await r.text()).substring(0, 100000);
  } catch (_) { return null; }
}

async function extractCrashes(html, pd) {
  if (!OPENAI_API_KEY || !html) return null;
  const text = html
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .substring(0, 6000);

  const prompt = `Extract recent (last 7 days) crash/accident press releases from this police news page.
Department: ${pd.name}, ${pd.metro}, ${pd.state}.

"""
${text}
"""

JSON only:
{
  "crashes": [
    {
      "title": "press release headline",
      "summary": "1-2 sentence",
      "occurred_at": "ISO|null",
      "address": "string|null",
      "highway": "string|null",
      "lat": number|null, "lng": number|null,
      "incident_type": "car_accident|motorcycle_accident|truck_accident|pedestrian|bicycle|other",
      "severity": "fatal|serious|moderate|minor|unknown",
      "fatalities_count": number|null,
      "injuries_count": number|null,
      "police_report_number": "string|null",
      "victims": [
        { "full_name": "", "age": null, "role": "driver|passenger|pedestrian|cyclist",
          "is_injured": true, "injury_severity": "fatal|incapacitating|non_incapacitating|none",
          "city_residence": "" }
      ],
      "press_release_url": "string|null"
    }
  ]
}
ONLY recent crashes (within ~7 days of today). Empty crashes:[] if none found.`;

  try {
    const r = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: [
          { role: 'system', content: 'Extract crash data from police press releases as JSON. Only recent crashes.' },
          { role: 'user', content: prompt }
        ],
        temperature: 0,
        response_format: { type: 'json_object' }
      }),
      signal: AbortSignal.timeout(20000)
    });
    if (!r.ok) return null;
    const d = await r.json();
    return JSON.parse(d.choices?.[0]?.message?.content || '{}');
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
  const results = { pds_polled: 0, crashes_found: 0, incidents_created: 0, incidents_matched: 0,
                    persons_added: 0, errors: [] };
  const startTime = Date.now();
  const TIME_BUDGET = 50000;

  try {
    let ds = await db('data_sources').where('name', 'PD Press Releases').first();
    if (!ds) {
      const dsId = uuidv4();
      await db('data_sources').insert({
        id: dsId, name: 'PD Press Releases', type: 'police_report',
        provider: 'direct-scrape', api_endpoint: '12 PD news pages',
        is_active: true, last_polled_at: new Date(),
        created_at: new Date(), updated_at: new Date()
      });
      ds = { id: dsId };
    }

    const newIncidents = [];
    const newReports = [];
    const newPersons = [];

    for (const pd of PD_PAGES) {
      if (Date.now() - startTime > TIME_BUDGET) break;
      try {
        const html = await fetchPage(pd.url);
        if (!html) {
          results.errors.push(`${pd.name}: fetch failed`);
          continue;
        }
        results.pds_polled++;

        const parsed = await extractCrashes(html, pd);
        const crashes = parsed?.crashes || [];
        results.crashes_found += crashes.length;

        for (const c of crashes) {
          if (Date.now() - startTime > TIME_BUDGET) break;
          try {
            const ref = `pd-press:${pd.name}:${c.title || ''}:${c.occurred_at || ''}`.substring(0, 400);
            if (dedupCache.has(ref)) continue;
            const exists = await db('source_reports').where('source_reference', ref).first();
            if (exists) { dedupCache.set(ref, 1); continue; }
            dedupCache.set(ref, 1);

            const now = new Date();

            // Geo match
            let matchId = null;
            if (c.lat && c.lng) {
              try {
                const m = await db.raw(`
                  SELECT id FROM incidents WHERE occurred_at > NOW() - INTERVAL '7 days'
                    AND geom IS NOT NULL
                    AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, 1500)
                  LIMIT 1`, [c.lng, c.lat]);
                matchId = m.rows?.[0]?.id;
              } catch (_) {}
            }

            const incidentId = uuidv4();
            const targetId = matchId || incidentId;

            if (matchId) {
              results.incidents_matched++;
              await db('incidents').where('id', matchId).update({
                source_count: db.raw('COALESCE(source_count, 1) + 1'),
                confidence_score: db.raw('LEAST(99, COALESCE(confidence_score, 50) + 15)'),
                police_department: db.raw(`COALESCE(police_department, ?)`, [pd.name]),
                police_report_number: db.raw(`COALESCE(police_report_number, ?)`, [c.police_report_number || null]),
                updated_at: now
              });
            } else {
              const inc = normalizeIncident({
                id: incidentId,
                incident_number: `PRESS-${now.getFullYear().toString().slice(-2)}${String(now.getMonth()+1).padStart(2,'0')}${String(now.getDate()).padStart(2,'0')}-${Math.floor(Math.random()*9999).toString().padStart(4,'0')}`,
                incident_type: c.incident_type,
                severity: c.severity,
                status: 'verified',
                confidence_score: 88,
                address: c.address || c.summary || c.title,
                city: pd.metro, state: pd.state,
                highway: c.highway,
                latitude: c.lat, longitude: c.lng,
                occurred_at: c.occurred_at,
                description: `${pd.name} press release: ${c.title}\n${c.summary || ''}`,
                injuries_count: c.injuries_count, fatalities_count: c.fatalities_count,
                police_department: pd.name, police_report_number: c.police_report_number,
                source_count: 1, first_source_id: ds.id,
                tags: ['pd_press', pd.metro.toLowerCase().replace(/ /g, '_')]
              });
              newIncidents.push(inc);
              results.incidents_created++;
            }

            newReports.push({
              id: uuidv4(), incident_id: targetId, data_source_id: ds.id,
              source_type: 'police_report', source_reference: ref,
              raw_data: JSON.stringify({ pd: pd.name, crash: c }),
              parsed_data: JSON.stringify(c),
              contributed_fields: ['victims', 'severity', 'description', 'police_department'],
              confidence: 88, is_verified: true,
              fetched_at: now, processed_at: now, created_at: now
            });

            for (const v of (c.victims || [])) {
              if (!v.full_name) continue;
              const exists = await db('persons').where('incident_id', targetId).whereRaw('LOWER(full_name) = LOWER(?)', [v.full_name.trim()]).first();
              if (exists) continue;
              const person = normalizePerson({
                incident_id: targetId,
                full_name: v.full_name,
                age: v.age, role: v.role,
                is_injured: !!v.is_injured,
                injury_severity: v.injury_severity,
                city: v.city_residence,
                state: pd.state,
                contact_status: 'not_contacted',
                confidence_score: 88,
                metadata: { source: 'pd_press', pd: pd.name, press_url: c.press_release_url }
              });
              person.id = uuidv4();
              newPersons.push(person);
            }
          } catch (e) {
            results.errors.push(`${pd.name} crash: ${e.message}`);
          }
        }
      } catch (e) {
        await reportError(db, 'pd-press', pd.name, e.message);
      }
    }

    if (newIncidents.length) await batchInsert(db, 'incidents', newIncidents);
    if (newReports.length) await batchInsert(db, 'source_reports', newReports);
    if (newPersons.length) {
      const r = await batchInsert(db, 'persons', newPersons);
      results.persons_added = r.inserted;
    }

    await db('data_sources').where('id', ds.id).update({
      last_polled_at: new Date(), last_success_at: new Date(), updated_at: new Date()
    });

    res.json({
      success: true,
      message: `PD Press: ${results.pds_polled} pds polled, ${results.crashes_found} crashes, ${results.incidents_created} new, ${results.persons_added} persons`,
      ...results,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'pd-press', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
