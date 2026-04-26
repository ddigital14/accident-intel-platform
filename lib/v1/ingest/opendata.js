/**
 * City Open Data 911 Dispatch Feed Integrations
 * Active feeds (verified 2026-04-25):
 *   Seattle (kzjm-xkqj), SF (nuek-vuh3), Dallas (9fxf-t2tr),
 *   Chicago Traffic Crashes (85ca-t3if) [NEW],
 *   Cincinnati Police CAD (gexm-h6bt)  [NEW]
 * GET /api/v1/ingest/opendata?secret=ingest-now
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('../system/_errors');
const { batchInsert } = require('../../_batch');
const { dedupCache } = require('../../_cache');

const CITY_FEEDS = [
  { name: 'Seattle', state: 'WA',
    url: 'https://data.seattle.gov/resource/kzjm-xkqj.json?$where=type%20like%20%27%25MVA%25%27%20OR%20type%20like%20%27%25Collision%25%27%20OR%20type%20like%20%27%25Vehicle%25%27&$limit=25&$order=datetime%20DESC',
    parser: 'seattle' },
  { name: 'San Francisco', state: 'CA',
    url: 'https://data.sfgov.org/resource/nuek-vuh3.json?$where=call_type%20like%20%27%25Traffic%25%27%20OR%20call_type%20like%20%27%25Vehicle%25%27&$limit=25&$order=call_date%20DESC',
    parser: 'sf' },
  { name: 'Dallas', state: 'TX',
    url: 'https://www.dallasopendata.com/resource/9fxf-t2tr.json?$limit=50',
    parser: 'dallas' },
  { name: 'Chicago', state: 'IL',
    url: 'https://data.cityofchicago.org/resource/85ca-t3if.json?$limit=75&$order=crash_date%20DESC',
    parser: 'chicago' },
  { name: 'Cincinnati', state: 'OH',
    url: "https://data.cincinnati-oh.gov/resource/gexm-h6bt.json?$where=incident_type_id%20like%20'%25CRASH%25'%20OR%20incident_type_id%20like%20'%25ACC%25'%20OR%20incident_type_id%20like%20'%25MVA%25'&$limit=50&$order=create_time_incident%20DESC",
    parser: 'cincinnati' },
];

function parseSeattle(records) {
  return records.map(r => ({
    source: 'opendata_seattle',
    source_reference: `SEA91-${r.incident_number || r.report_number || Date.now()}`,
    title: `${r.type || 'MVA'} - ${r.address || 'Seattle'}`,
    description: `Seattle 911: ${r.type || 'Motor vehicle accident'} at ${r.address || 'unknown'}.`,
    incident_type: classifyType(r.type),
    severity: classifySeverityFromDispatch(r.type),
    city: 'Seattle', state: 'WA',
    lat: parseFloat(r.latitude) || 47.606,
    lng: parseFloat(r.longitude) || -122.332,
    occurred_at: r.datetime || new Date().toISOString(),
    confidence: 85, raw: r
  }));
}
function parseSF(records) {
  return records.map(r => ({
    source: 'opendata_sf',
    source_reference: `SFCA-${r.cad_number || r.incident_number || Date.now()}`,
    title: `${r.call_type_original_desc || 'Traffic Collision'} - ${r.intersection_name || r.address || 'San Francisco'}`,
    description: `SF 911: ${r.call_type_original_desc || 'Collision'} at ${r.intersection_name || r.address || 'unknown'}. Priority: ${r.priority || 'unknown'}.`,
    incident_type: classifyType(r.call_type_original_desc),
    severity: classifySeverityFromDispatch(r.call_type_original_desc),
    city: 'San Francisco', state: 'CA',
    lat: parseFloat(r.latitude) || parseFloat(r.point?.coordinates?.[1]) || 37.774,
    lng: parseFloat(r.longitude) || parseFloat(r.point?.coordinates?.[0]) || -122.419,
    occurred_at: r.call_datetime || r.received_datetime || new Date().toISOString(),
    confidence: 85, raw: r
  }));
}
function parseDallas(records) {
  return records.map(r => ({
    source: 'opendata_dallas',
    source_reference: `DALCA-${r.incident_number || r.servnumid || Date.now()}`,
    title: `${r.nature_of_call || 'Accident'} - ${r.block || r.location || 'Dallas'}`,
    description: `Dallas 911: ${r.nature_of_call || 'Traffic accident'} at ${r.block || r.location || 'unknown'}.`,
    incident_type: classifyType(r.nature_of_call),
    severity: classifySeverityFromDispatch(r.nature_of_call),
    city: 'Dallas', state: 'TX',
    lat: parseFloat(r.geocoded_column?.latitude) || 32.777,
    lng: parseFloat(r.geocoded_column?.longitude) || -96.797,
    occurred_at: r.date1 || r.call_received_date_time || new Date().toISOString(),
    confidence: 80, raw: r
  }));
}
function parseChicago(records) {
  return records.map(r => {
    const street = [r.street_no, r.street_direction, r.street_name].filter(Boolean).join(' ');
    const injuriesTotal = parseInt(r.injuries_total) || 0;
    const fatalities = parseInt(r.injuries_fatal) || 0;
    const incapacitating = parseInt(r.injuries_incapacitating) || 0;
    let severity = 'minor';
    if (fatalities > 0) severity = 'fatal';
    else if (incapacitating > 0) severity = 'serious';
    else if (injuriesTotal > 0) severity = 'moderate';
    else if (r.crash_type === 'INJURY AND / OR TOW DUE TO CRASH') severity = 'moderate';
    const incidentType = /motorcycle/i.test(r.first_crash_type) ? 'motorcycle_accident'
      : /pedestrian/i.test(r.first_crash_type) ? 'pedestrian'
      : /pedalcyclist|bicycle/i.test(r.first_crash_type) ? 'bicycle'
      : 'car_accident';
    return {
      source: 'opendata_chicago',
      source_reference: `CHI-${(r.crash_record_id || '').substring(0,16) || Date.now()}`,
      title: `${r.first_crash_type || 'Crash'} - ${street || 'Chicago'}`,
      description: `Chicago Crash: ${r.first_crash_type || 'Vehicle crash'} at ${street || 'unknown'}. ${injuriesTotal} injured, ${fatalities} fatal. Weather: ${r.weather_condition || 'unknown'}. Hit-and-run: ${r.hit_and_run_i === 'Y' ? 'YES' : 'no'}. Damage: ${r.damage || 'unknown'}.`,
      incident_type: incidentType, severity,
      city: 'Chicago', state: 'IL',
      lat: parseFloat(r.latitude) || 41.8781,
      lng: parseFloat(r.longitude) || -87.6298,
      occurred_at: r.crash_date || new Date().toISOString(),
      confidence: 90,
      vehicles_involved: parseInt(r.num_units) || null,
      injuries_count: injuriesTotal, fatalities_count: fatalities,
      weather_conditions: r.weather_condition,
      lighting_conditions: r.lighting_condition,
      road_conditions: r.roadway_surface_cond,
      hit_and_run: r.hit_and_run_i === 'Y',
      raw: r
    };
  });
}
function parseCincinnati(records) {
  return records.map(r => ({
    source: 'opendata_cincinnati',
    source_reference: `CINCI-${r.event_number || Date.now()}`,
    title: `${r.incident_type_id || 'Accident'} - ${r.address_x || 'Cincinnati'}`,
    description: `Cincinnati CAD: ${r.incident_type_id || 'Crash'} at ${r.address_x || 'unknown'}. Disposition: ${r.disposition_text || 'pending'}. Beat: ${r.beat || 'unknown'}.`,
    incident_type: classifyType(r.incident_type_id),
    severity: classifySeverityFromDispatch(r.incident_type_id + ' ' + (r.disposition_text || '')),
    city: 'Cincinnati', state: 'OH',
    lat: parseFloat(r.latitude_x) || 39.1031,
    lng: parseFloat(r.longitude_x) || -84.5120,
    occurred_at: r.create_time_incident || r.closed_time_incident || new Date().toISOString(),
    confidence: 85, raw: r
  }));
}

const PARSERS = { seattle: parseSeattle, sf: parseSF, dallas: parseDallas, chicago: parseChicago, cincinnati: parseCincinnati };

function classifyType(text) {
  const lower = (text || '').toLowerCase();
  if (/motorcycle/i.test(lower)) return 'motorcycle_accident';
  if (/truck|semi|commercial/i.test(lower)) return 'truck_accident';
  if (/pedestrian/i.test(lower)) return 'pedestrian';
  if (/bicycl|cyclist|pedalcycl/i.test(lower)) return 'bicycle';
  return 'car_accident';
}
function classifySeverityFromDispatch(text) {
  const lower = (text || '').toLowerCase();
  if (/fatal|death|killed|code\s*0|signal\s*0/i.test(lower)) return 'fatal';
  if (/major|serious|entrap|pin|rollover|code\s*3|critical/i.test(lower)) return 'serious';
  if (/injury|injur|code\s*2|with\s*injuries/i.test(lower)) return 'moderate';
  if (/hit.?run|leaving/i.test(lower)) return 'moderate';
  if (/minor|fender|non.?injury|property/i.test(lower)) return 'minor';
  return 'moderate';
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const results = { inserted: 0, skipped: 0, corroborated: 0, errors: [], sources: {} };

  try {
    let openDs = await db('data_sources').where('name', 'like', '%Open Data%').first();
    if (!openDs) {
      const dsId = uuidv4();
      await db('data_sources').insert({
        id: dsId, name: 'City Open Data 911', type: 'cad_dispatch',
        api_endpoint: 'multiple city portals', is_active: true,
        last_polled_at: new Date(), created_at: new Date(), updated_at: new Date()
      });
      openDs = { id: dsId };
    }
    const metroAreas = await db('metro_areas').select('id', 'name');
    const metroMap = {};
    for (const ma of metroAreas) {
      for (const feed of CITY_FEEDS) {
        if (ma.name.toLowerCase().includes(feed.name.toLowerCase())) metroMap[feed.name] = ma.id;
      }
    }

    const existingRefs = await db('source_reports')
      .where('source_type', 'like', 'opendata_%')
      .where('created_at', '>', new Date(Date.now() - 24 * 60 * 60 * 1000))
      .select('source_reference');
    const seenRefs = new Set(existingRefs.map(r => r.source_reference));

    const newIncidents = [];
    const newReports = [];
    const corroborations = [];

    for (const feed of CITY_FEEDS) {
      try {
        const resp = await fetch(feed.url, {
          headers: { 'Accept': 'application/json', 'X-App-Token': process.env.SOCRATA_APP_TOKEN || '' },
          signal: AbortSignal.timeout(15000)
        });
        if (!resp.ok) {
          results.sources[feed.name] = `Error: HTTP ${resp.status}`;
          await reportError(db, 'opendata', feed.name, `HTTP ${resp.status}`, { url: feed.url });
          continue;
        }
        const data = await resp.json();
        if (!Array.isArray(data) || data.length === 0) { results.sources[feed.name] = 0; continue; }
        const parser = PARSERS[feed.parser];
        if (!parser) continue;
        const records = parser(data);
        results.sources[feed.name] = records.length;

        for (const record of records) {
          try {
            if (seenRefs.has(record.source_reference) || dedupCache.has(record.source_reference)) {
              results.skipped++; continue;
            }
            seenRefs.add(record.source_reference);
            dedupCache.set(record.source_reference, 1);

            if (record.lat && record.lng) {
              let nearby;
              try {
                const r = await db.raw(`
                  SELECT id FROM incidents
                  WHERE occurred_at > NOW() - INTERVAL '20 minutes'
                    AND geom IS NOT NULL
                    AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, 300)
                  LIMIT 1
                `, [record.lng, record.lat]);
                nearby = r.rows?.[0];
              } catch (postgisErr) {
                nearby = await db('incidents')
                  .where('occurred_at', '>', new Date(Date.now() - 20 * 60 * 1000))
                  .whereNotNull('latitude')
                  .whereNotNull('longitude')
                  .whereRaw(`
                    (6371000 * acos(LEAST(1.0, GREATEST(-1.0,
                      cos(radians(?)) * cos(radians(latitude)) *
                      cos(radians(longitude) - radians(?)) +
                      sin(radians(?)) * sin(radians(latitude))
                    )))) < 300
                  `, [record.lat, record.lng, record.lat])
                  .first();
              }
              if (nearby) {
                corroborations.push({ id: nearby.id, record, openDsId: openDs.id });
                results.corroborated++;
                continue;
              }
            }

            const incidentId = uuidv4();
            const now = new Date();
            const priority = record.severity === 'fatal' ? 1 : record.severity === 'serious' ? 2 : record.severity === 'moderate' ? 3 : 4;
            newIncidents.push({
              id: incidentId,
              incident_number: `OD-${now.getFullYear().toString().slice(-2)}${String(now.getMonth()+1).padStart(2,'0')}${String(now.getDate()).padStart(2,'0')}-${Math.floor(Math.random()*9999).toString().padStart(4,'0')}`,
              incident_type: record.incident_type,
              severity: record.severity, status: 'new', priority,
              confidence_score: record.confidence,
              address: record.title,
              city: record.city || feed.name, state: record.state || feed.state,
              latitude: record.lat, longitude: record.lng,
              occurred_at: record.occurred_at ? new Date(record.occurred_at) : now,
              reported_at: now, discovered_at: now,
              description: record.description,
              injuries_count: record.injuries_count ?? null,
              fatalities_count: record.fatalities_count ?? null,
              vehicles_involved: record.vehicles_involved ?? null,
              weather_conditions: record.weather_conditions || null,
              lighting_conditions: record.lighting_conditions || null,
              road_conditions: record.road_conditions || null,
              metro_area_id: metroMap[feed.name] || null,
              source_count: 1, first_source_id: openDs.id,
              tags: ['opendata', feed.name.toLowerCase().replace(/ /g, '_'), ...(record.hit_and_run ? ['hit_and_run'] : [])],
              created_at: now, updated_at: now
            });
            newReports.push({
              id: uuidv4(), incident_id: incidentId, data_source_id: openDs.id,
              source_type: record.source, source_reference: record.source_reference,
              raw_data: JSON.stringify(record.raw),
              parsed_data: JSON.stringify({ title: record.title, description: record.description, type: record.incident_type, severity: record.severity }),
              contributed_fields: ['description','incident_type','severity','location','dispatch_time'],
              confidence: record.confidence, is_verified: true,
              fetched_at: now, processed_at: now, created_at: now
            });
            results.inserted++;
          } catch (e) {
            results.errors.push(`${feed.name}: ${e.message}`);
            await reportError(db, 'opendata', feed.name, e.message, { record_ref: record.source_reference });
          }
        }
      } catch (e) {
        results.errors.push(`${feed.name} feed: ${e.message}`);
        results.sources[feed.name] = `Error: ${e.message}`;
        await reportError(db, 'opendata', feed.name, e.message, { feed_url: feed.url });
      }
    }

    if (newIncidents.length) await batchInsert(db, 'incidents', newIncidents);
    if (newReports.length) await batchInsert(db, 'source_reports', newReports);

    for (const corr of corroborations) {
      try {
        await db('incidents').where('id', corr.id).update({
          source_count: db.raw('COALESCE(source_count, 1) + 1'),
          confidence_score: db.raw('LEAST(99, COALESCE(confidence_score, 50) + 15)'),
          updated_at: new Date()
        });
        await db('source_reports').insert({
          id: uuidv4(), incident_id: corr.id, data_source_id: corr.openDsId,
          source_type: corr.record.source, source_reference: corr.record.source_reference,
          raw_data: JSON.stringify(corr.record.raw),
          parsed_data: JSON.stringify({ title: corr.record.title, description: corr.record.description }),
          contributed_fields: ['corroboration', 'location', 'dispatch_details'],
          confidence: corr.record.confidence, is_verified: true,
          fetched_at: new Date(), processed_at: new Date(), created_at: new Date()
        });
      } catch (e) { results.errors.push(`corroborate: ${e.message}`); }
    }

    await db('data_sources').where('id', openDs.id).update({
      last_polled_at: new Date(), last_success_at: new Date(), updated_at: new Date()
    });

    res.json({
      success: true,
      message: `Open Data 911: ${results.inserted} new, ${results.corroborated} corroborated, ${results.skipped} duplicates`,
      ...results,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'opendata', 'main', err.message, { stack: (err.stack||'').substring(0,1000) });
    res.status(500).json({ error: err.message, results });
  }
};
