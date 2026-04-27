/**
 * Hospital Trauma Board / Patient Logs Scraper
 *
 * Many Level-I trauma centers publish daily patient transport logs and
 * helicopter dispatch records (HEMS — Helicopter Emergency Medical Services).
 * These contain incident lat/lng, patient age/condition, and trauma category
 * even before police reports are filed.
 *
 * We scrape:
 *   - SkyHealth (CT) https://www.ynhh.org/services/skyhealth/dispatch
 *   - LifeFlight feeds where public
 *   - State EMS logs (TX iSTOP, FL EMSTARS, GA OEMS) when JSON available
 *
 * Most are HTML — use GPT-4o to extract structured data.
 *
 * GET /api/v1/ingest/trauma?secret=ingest-now
 * Cron: every 1 hour
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('../system/_errors');
const { batchInsert } = require('../../_batch');
const { dedupCache } = require('../../_cache');

const { extractJson } = require('../enrich/_ai_router');

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

// Public trauma / EMS / HEMS log sources (best-effort — many require manual updates)
const TRAUMA_SOURCES = [
  // STATracker / HEMS feeds — most are non-public; these are placeholders.
  // The real value is in scraping local fire/EMS Twitter accounts which post live
  // HEMS launches. We poll those via NewsAPI/RSS too.
  // For now, this pipeline is scaffolded — add real endpoints as found.
  {
    name: 'SkyHealth (Yale) Daily Log',
    url: 'https://www.ynhh.org/services/skyhealth/dispatch.aspx',
    state: 'CT',
    parser: 'gpt'
  },
  // CMC LifeStar (NC)
  {
    name: 'CMC LifeStar Dispatch',
    url: 'https://www.atriumhealth.org/dailyflight',
    state: 'NC',
    parser: 'gpt'
  }
];

async function scrape(url) {
  try {
    const resp = await fetch(url, {
      headers: { 'User-Agent': 'AIP/1.0 (Mozilla compatible)', 'Accept': 'text/html' },
      signal: AbortSignal.timeout(12000)
    });
    if (!resp.ok) return null;
    return (await resp.text()).substring(0, 80000);
  } catch (_) { return null; }
}

async function extractTraumaEvents(html, source) {
  if (!OPENAI_API_KEY || !html) return null;
  const text = html
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .substring(0, 6000);

  const prompt = `Extract HEMS / trauma dispatch events from this page text. Source: ${source.name}, State: ${source.state}.

"""
${text}
"""

Return JSON only:
{
  "events": [
    {
      "occurred_at": "ISO datetime|null",
      "city": "string|null",
      "address_or_location": "string|null",
      "lat": number|null,
      "lng": number|null,
      "incident_type": "car_accident|motorcycle_accident|truck_accident|pedestrian|fall|other|null",
      "severity": "fatal|critical|serious|moderate|unknown",
      "patient_age": number|null,
      "patient_condition": "string|null",
      "transported_to": "hospital name|null",
      "agency": "EMS/Fire agency|null",
      "is_recent_24h": true|false
    }
  ]
}
Only events where is_recent_24h=true.`;

  try {
    const resp = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: [
          { role: 'system', content: 'Extract HEMS / trauma dispatch events as JSON. Empty events array if none found.' },
          { role: 'user', content: prompt }
        ],
        temperature: 0,
        response_format: { type: 'json_object' }
      }),
      signal: AbortSignal.timeout(20000)
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
  const results = { sources_polled: 0, events: 0, inserted: 0, corroborated: 0, errors: [] };

  try {
    let ds = await db('data_sources').where('name', 'Trauma / HEMS Logs').first();
    if (!ds) {
      const dsId = uuidv4();
      await db('data_sources').insert({
        id: dsId, name: 'Trauma / HEMS Logs', type: 'hospital_ems',
        api_endpoint: 'multiple HEMS dispatch boards',
        is_active: true, last_polled_at: new Date(),
        created_at: new Date(), updated_at: new Date()
      });
      ds = { id: dsId };
    }

    const newIncidents = [];
    const newReports = [];

    for (const src of TRAUMA_SOURCES) {
      try {
        const html = await scrape(src.url);
        if (!html) {
          results.errors.push(`${src.name}: fetch failed`);
          continue;
        }
        results.sources_polled++;

        const parsed = await extractTraumaEvents(html, src);
        if (!parsed?.events?.length) continue;

        for (const ev of parsed.events) {
          if (!ev.is_recent_24h) continue;
          results.events++;

          const refKey = `TRAUMA-${src.name}-${ev.occurred_at || Date.now()}-${ev.address_or_location || ''}`.substring(0, 200);
          if (dedupCache.has(refKey)) continue;
          const exists = await db('source_reports').where('source_reference', refKey).first();
          if (exists) { dedupCache.set(refKey, 1); continue; }
          dedupCache.set(refKey, 1);

          // Geo match to existing
          let matchId = null;
          if (ev.lat && ev.lng) {
            try {
              const m = await db.raw(`
                SELECT id FROM incidents
                WHERE occurred_at > NOW() - INTERVAL '6 hours'
                  AND geom IS NOT NULL
                  AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, 1500)
                LIMIT 1`, [ev.lng, ev.lat]);
              matchId = m.rows?.[0]?.id;
            } catch (_) {}
          }

          const incidentId = matchId || uuidv4();
          const now = new Date();
          if (matchId) {
            results.corroborated++;
            await db('incidents').where('id', matchId).update({
              source_count: db.raw('COALESCE(source_count, 1) + 1'),
              confidence_score: db.raw('LEAST(99, COALESCE(confidence_score, 50) + 15)'),
              ems_dispatched: true,
              helicopter_dispatched: true,
              updated_at: now
            });
          } else {
            newIncidents.push({
              id: incidentId,
              incident_number: `HEMS-${now.getFullYear().toString().slice(-2)}${String(now.getMonth()+1).padStart(2,'0')}${String(now.getDate()).padStart(2,'0')}-${Math.floor(Math.random()*9999).toString().padStart(4,'0')}`,
              incident_type: ev.incident_type || 'car_accident',
              severity: ev.severity || 'critical',
              status: 'new',
              priority: ev.severity === 'fatal' ? 1 : 2,
              confidence_score: 80,
              address: ev.address_or_location || `${ev.city || src.state}`,
              city: ev.city || null, state: src.state,
              latitude: ev.lat || null, longitude: ev.lng || null,
              occurred_at: ev.occurred_at ? new Date(ev.occurred_at) : now,
              reported_at: now, discovered_at: now,
              description: `HEMS dispatch (${src.name}): ${ev.patient_condition || 'trauma'}. Agency: ${ev.agency || 'unknown'}. To: ${ev.transported_to || 'unknown'}.`,
              ems_dispatched: true, helicopter_dispatched: true,
              source_count: 1, first_source_id: ds.id,
              tags: ['trauma', 'hems', src.state.toLowerCase()],
              created_at: now, updated_at: now
            });
            results.inserted++;
          }

          newReports.push({
            id: uuidv4(), incident_id: incidentId, data_source_id: ds.id,
            source_type: 'trauma_hems', source_reference: refKey,
            raw_data: JSON.stringify({ source: src.name, event: ev }),
            parsed_data: JSON.stringify(ev),
            contributed_fields: ['hems_dispatch', 'severity', 'patient_condition', 'transported_to'],
            confidence: 80, is_verified: true,
            fetched_at: now, processed_at: now, created_at: now
          });
        }
      } catch (e) {
        results.errors.push(`${src.name}: ${e.message}`);
        await reportError(db, 'trauma', src.name, e.message);
      }
    }

    if (newIncidents.length) await batchInsert(db, 'incidents', newIncidents);
    if (newReports.length) await batchInsert(db, 'source_reports', newReports);

    await db('data_sources').where('id', ds.id).update({
      last_polled_at: new Date(), last_success_at: new Date(), updated_at: new Date()
    });

    res.json({
      success: true,
      message: `Trauma: ${results.sources_polled} sources, ${results.events} events, ${results.inserted} new, ${results.corroborated} corroborated`,
      ...results,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'trauma', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
