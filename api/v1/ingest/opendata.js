/**
 * City Open Data 911 Dispatch Feed Integrations
 *
 * Pulls real-time 911 dispatch / incident data from city open data portals.
 * These are FREE, government-published JSON APIs updated every 5-10 minutes.
 *
 * Supported cities:
 *   - Atlanta (via Atlanta PD open data)
 *   - Houston (via Houston Emergency CAD)
 *   - Seattle (Real-Time Fire 911 Calls)
 *   - San Francisco (Law Enforcement Dispatched Calls)
 *   - Dallas (Dallas PD Active Calls)
 *
 * GET /api/v1/ingest/opendata?secret=ingest-now
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');

// ── City Feed Configurations ──────────────────────────────────────────
const CITY_FEEDS = [
  {
    name: 'Houston',
    state: 'TX',
    url: 'https://data.houstontx.gov/resource/bsng-ej6s.json?$where=call_type_desc%20like%20%27%25ACCIDENT%25%27%20OR%20call_type_desc%20like%20%27%25CRASH%25%27%20OR%20call_type_desc%20like%20%27%25COLLISION%25%27&$limit=25&$order=call_date%20DESC',
    parser: 'houston',
  },
  {
    name: 'Seattle',
    state: 'WA',
    url: 'https://data.seattle.gov/resource/kzjm-xkqj.json?$where=type%20like%20%27%25MVA%25%27%20OR%20type%20like%20%27%25Collision%25%27%20OR%20type%20like%20%27%25Vehicle%25%27&$limit=25&$order=datetime%20DESC',
    parser: 'seattle',
  },
  {
    name: 'San Francisco',
    state: 'CA',
    url: 'https://data.sfgov.org/resource/gnap-fj3t.json?$where=call_type_original_desc%20like%20%27%25ACCIDENT%25%27%20OR%20call_type_original_desc%20like%20%27%25COLLISION%25%27%20OR%20call_type_original_desc%20like%20%27%25VEHICLE%25%27&$limit=25&$order=call_datetime%20DESC',
    parser: 'sf',
  },
  {
    name: 'Dallas',
    state: 'TX',
    url: 'https://www.dallasopendata.com/resource/are8-xahz.json?$where=nature_of_call%20like%20%27%25ACCIDENT%25%27%20OR%20nature_of_call%20like%20%27%25CRASH%25%27&$limit=25&$order=date1%20DESC',
    parser: 'dallas',
  },
  {
    name: 'Atlanta',
    state: 'GA',
    // Atlanta PD uses Socrata open data — this endpoint varies; fallback to manual
    url: 'https://opendata.atlantapd.org/resource/jb7d-dhz3.json?$where=type%20like%20%27%25ACCIDENT%25%27%20OR%20type%20like%20%27%25COLLISION%25%27&$limit=25&$order=report_date%20DESC',
    parser: 'atlanta',
  },
];

// ── Parsers for each city's data format ───────────────────────────────
function parseHouston(records) {
  return records.map(r => ({
    source: 'opendata_houston',
    source_reference: `HOUCA-${r.incident_number || r.call_no || Date.now()}`,
    title: `${r.call_type_desc || 'Accident'} - ${r.block_address || 'Houston'}`,
    description: `Houston 911 dispatch: ${r.call_type_desc || 'Traffic incident'} at ${r.block_address || 'unknown location'}. Agency: ${r.agency || 'HPD'}`,
    incident_type: classifyType(r.call_type_desc),
    severity: classifySeverityFromDispatch(r.call_type_desc),
    city: 'Houston',
    state: 'TX',
    lat: parseFloat(r.latitude) || parseFloat(r.combined_location?.latitude) || 29.760,
    lng: parseFloat(r.longitude) || parseFloat(r.combined_location?.longitude) || -95.370,
    occurred_at: r.call_date || r.call_datetime || new Date().toISOString(),
    confidence: 80,
    raw: r
  }));
}

function parseSeattle(records) {
  return records.map(r => ({
    source: 'opendata_seattle',
    source_reference: `SEA91-${r.incident_number || r.report_number || Date.now()}`,
    title: `${r.type || 'MVA'} - ${r.address || 'Seattle'}`,
    description: `Seattle 911: ${r.type || 'Motor vehicle accident'} at ${r.address || 'unknown'}. Reported: ${r.datetime || 'now'}`,
    incident_type: classifyType(r.type),
    severity: classifySeverityFromDispatch(r.type),
    city: 'Seattle',
    state: 'WA',
    lat: parseFloat(r.latitude) || 47.606,
    lng: parseFloat(r.longitude) || -122.332,
    occurred_at: r.datetime || new Date().toISOString(),
    confidence: 85,
    raw: r
  }));
}

function parseSF(records) {
  return records.map(r => ({
    source: 'opendata_sf',
    source_reference: `SFCA-${r.cad_number || r.incident_number || Date.now()}`,
    title: `${r.call_type_original_desc || 'Traffic Collision'} - ${r.intersection_name || r.address || 'San Francisco'}`,
    description: `SF 911: ${r.call_type_original_desc || 'Collision'} at ${r.intersection_name || r.address || 'unknown'}. Priority: ${r.priority || 'unknown'}. Disposition: ${r.disposition || 'pending'}`,
    incident_type: classifyType(r.call_type_original_desc),
    severity: classifySeverityFromDispatch(r.call_type_original_desc),
    city: 'San Francisco',
    state: 'CA',
    lat: parseFloat(r.latitude) || parseFloat(r.point?.coordinates?.[1]) || 37.774,
    lng: parseFloat(r.longitude) || parseFloat(r.point?.coordinates?.[0]) || -122.419,
    occurred_at: r.call_datetime || r.received_datetime || new Date().toISOString(),
    confidence: 85,
    raw: r
  }));
}

function parseDallas(records) {
  return records.map(r => ({
    source: 'opendata_dallas',
    source_reference: `DALCA-${r.incident_number || r.servnumid || Date.now()}`,
    title: `${r.nature_of_call || 'Accident'} - ${r.block || r.location || 'Dallas'}`,
    description: `Dallas 911: ${r.nature_of_call || 'Traffic accident'} at ${r.block || r.location || 'unknown'}. Division: ${r.division || 'unknown'}`,
    incident_type: classifyType(r.nature_of_call),
    severity: classifySeverityFromDispatch(r.nature_of_call),
    city: 'Dallas',
    state: 'TX',
    lat: parseFloat(r.geocoded_column?.latitude) || 32.777,
    lng: parseFloat(r.geocoded_column?.longitude) || -96.797,
    occurred_at: r.date1 || r.call_received_date_time || new Date().toISOString(),
    confidence: 80,
    raw: r
  }));
}

function parseAtlanta(records) {
  return records.map(r => ({
    source: 'opendata_atlanta',
    source_reference: `ATLCA-${r.report_number || r.crime_id || Date.now()}`,
    title: `${r.type || r.crime || 'Accident'} - ${r.location || 'Atlanta'}`,
    description: `Atlanta 911: ${r.type || r.crime || 'Traffic incident'} at ${r.location || 'unknown'}. Beat: ${r.beat || 'unknown'}. Zone: ${r.zone || 'unknown'}`,
    incident_type: classifyType(r.type || r.crime),
    severity: classifySeverityFromDispatch(r.type || r.crime),
    city: 'Atlanta',
    state: 'GA',
    lat: parseFloat(r.latitude) || parseFloat(r.lat) || 33.749,
    lng: parseFloat(r.longitude) || parseFloat(r.long) || -84.388,
    occurred_at: r.report_date || r.occur_date || new Date().toISOString(),
    confidence: 80,
    raw: r
  }));
}

const PARSERS = {
  houston: parseHouston,
  seattle: parseSeattle,
  sf: parseSF,
  dallas: parseDallas,
  atlanta: parseAtlanta,
};

// ── Shared classifiers ────────────────────────────────────────────────
function classifyType(text) {
  const lower = (text || '').toLowerCase();
  if (/motorcycle/i.test(lower)) return 'motorcycle_accident';
  if (/truck|semi|commercial/i.test(lower)) return 'truck_accident';
  if (/pedestrian/i.test(lower)) return 'pedestrian';
  if (/bicycl|cyclist/i.test(lower)) return 'bicycle';
  return 'car_accident';
}

function classifySeverityFromDispatch(text) {
  const lower = (text || '').toLowerCase();
  if (/fatal|death|killed|code\s*0/i.test(lower)) return 'fatal';
  if (/major|serious|entrap|pin|rollover|code\s*3/i.test(lower)) return 'serious';
  if (/injury|injur|code\s*2/i.test(lower)) return 'moderate';
  if (/hit.?run|leaving/i.test(lower)) return 'moderate';
  if (/minor|fender|non.?injury|property/i.test(lower)) return 'minor';
  return 'moderate';
}

// ── Main Handler ──────────────────────────────────────────────────────
module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const db = getDb();
  const results = { inserted: 0, skipped: 0, errors: [], sources: {} };

  // Find or create opendata data source
  let openDs = await db('data_sources').where('name', 'like', '%Open Data%').first();
  if (!openDs) {
    const dsId = uuidv4();
    await db('data_sources').insert({
      id: dsId,
      name: 'City Open Data 911',
      source_type: 'api',
      api_endpoint: 'multiple city portals',
      is_active: true,
      poll_interval_minutes: 10,
      last_polled_at: new Date(),
      created_at: new Date(),
      updated_at: new Date()
    });
    openDs = { id: dsId };
  }

  // Metro area lookup
  const metroAreas = await db('metro_areas').select('id', 'name');
  const metroMap = {};
  for (const ma of metroAreas) {
    for (const feed of CITY_FEEDS) {
      if (ma.name.toLowerCase().includes(feed.name.toLowerCase())) {
        metroMap[feed.name] = ma.id;
      }
    }
  }

  for (const feed of CITY_FEEDS) {
    try {
      const resp = await fetch(feed.url, {
        headers: { 'Accept': 'application/json', 'X-App-Token': process.env.SOCRATA_APP_TOKEN || '' },
        signal: AbortSignal.timeout(15000)
      });

      if (!resp.ok) {
        results.sources[feed.name] = `Error: HTTP ${resp.status}`;
        continue;
      }

      const data = await resp.json();
      if (!Array.isArray(data) || data.length === 0) {
        results.sources[feed.name] = 0;
        continue;
      }

      const parser = PARSERS[feed.parser];
      if (!parser) continue;

      const records = parser(data);
      results.sources[feed.name] = records.length;

      for (const record of records) {
        try {
          // Dedup check
          const existing = await db('source_reports')
            .where('source_reference', record.source_reference)
            .first();
          if (existing) { results.skipped++; continue; }

          // Proximity dedup — check within 300m and 20 min
          if (record.lat && record.lng) {
            const nearby = await db('incidents')
              .where('occurred_at', '>', new Date(Date.now() - 20 * 60 * 1000))
              .whereNotNull('latitude')
              .whereNotNull('longitude')
              .whereRaw(`
                (6371000 * acos(
                  cos(radians(?)) * cos(radians(latitude)) *
                  cos(radians(longitude) - radians(?)) +
                  sin(radians(?)) * sin(radians(latitude))
                )) < 300
              `, [record.lat, record.lng, record.lat])
              .first();

            if (nearby) {
              // Corroborate
              await db('incidents').where('id', nearby.id).update({
                source_count: db.raw('COALESCE(source_count, 1) + 1'),
                confidence_score: db.raw('LEAST(99, COALESCE(confidence_score, 50) + 15)'),
                updated_at: new Date()
              });
              await db('source_reports').insert({
                id: uuidv4(),
                incident_id: nearby.id,
                data_source_id: openDs.id,
                source_type: record.source,
                source_reference: record.source_reference,
                raw_data: JSON.stringify(record.raw),
                parsed_data: JSON.stringify({ title: record.title, description: record.description }),
                contributed_fields: ['corroboration', 'location', 'dispatch_details'],
                confidence: record.confidence,
                is_verified: true,
                fetched_at: new Date(),
                processed_at: new Date(),
                created_at: new Date()
              });
              results.skipped++;
              continue;
            }
          }

          const incidentId = uuidv4();
          const now = new Date();
          const priority = record.severity === 'fatal' ? 1 : record.severity === 'serious' ? 2 : record.severity === 'moderate' ? 3 : 4;

          await db('incidents').insert({
            id: incidentId,
            incident_number: `OD-${now.getFullYear().toString().slice(-2)}${String(now.getMonth()+1).padStart(2,'0')}${String(now.getDate()).padStart(2,'0')}-${Math.floor(Math.random()*9999).toString().padStart(4,'0')}`,
            incident_type: record.incident_type,
            severity: record.severity,
            status: 'new',
            priority: priority,
            confidence_score: record.confidence,
            address: record.title,
            city: record.city || feed.name,
            state: record.state || feed.state,
            latitude: record.lat,
            longitude: record.lng,
            occurred_at: record.occurred_at ? new Date(record.occurred_at) : now,
            reported_at: now,
            discovered_at: now,
            description: record.description,
            injuries_count: null,
            fatalities_count: null,
            vehicles_involved: null,
            metro_area_id: metroMap[feed.name] || null,
            source_count: 1,
            first_source_id: openDs.id,
            tags: ['opendata', feed.name.toLowerCase().replace(/ /g, '_')],
            created_at: now,
            updated_at: now
          });

          await db('source_reports').insert({
            id: uuidv4(),
            incident_id: incidentId,
            data_source_id: openDs.id,
            source_type: record.source,
            source_reference: record.source_reference,
            raw_data: JSON.stringify(record.raw),
            parsed_data: JSON.stringify({ title: record.title, description: record.description, type: record.incident_type, severity: record.severity }),
            contributed_fields: ['description', 'incident_type', 'severity', 'location', 'dispatch_time'],
            confidence: record.confidence,
            is_verified: true,
            fetched_at: now,
            processed_at: now,
            created_at: now
          });

          results.inserted++;
        } catch (e) {
          results.errors.push(`${feed.name}: ${e.message}`);
        }
      }
    } catch (e) {
      results.errors.push(`${feed.name} feed: ${e.message}`);
      results.sources[feed.name] = `Error: ${e.message}`;
    }
  }

  // Update data source
  await db('data_sources').where('id', openDs.id).update({
    last_polled_at: new Date(),
    last_success_at: new Date(),
    updated_at: new Date()
  });

  res.json({
    success: true,
    message: `Open Data 911: ${results.inserted} new, ${results.skipped} corroborated/skipped`,
    ...results,
    timestamp: new Date().toISOString()
  });
};
