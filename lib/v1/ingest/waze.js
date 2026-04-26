/**
 * Waze Live Traffic Accident Scraper
 *
 * Scrapes Waze's public-facing live map data for real-time accident reports.
 * Waze exposes GeoRSS/JSON data via their map tile endpoints that contain
 * user-reported incidents including accidents, hazards, and road closures.
 *
 * No API key required â uses the same public data Waze shows on their web map.
 *
 * GET /api/v1/ingest/waze?secret=ingest-now
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { dedupCache } = require('../../_cache');
const { v4: uuidv4 } = require('uuid');

// Metro bounding boxes for Waze queries
const WAZE_METROS = [
  { name: 'Atlanta', state: 'GA', bbox: { bottom: 33.55, left: -84.65, top: 34.05, right: -84.15 } },
  { name: 'Houston', state: 'TX', bbox: { bottom: 29.50, left: -95.65, top: 30.00, right: -95.10 } },
  { name: 'Dallas', state: 'TX', bbox: { bottom: 32.55, left: -97.05, top: 33.05, right: -96.50 } },
  { name: 'Miami', state: 'FL', bbox: { bottom: 25.60, left: -80.45, top: 26.00, right: -80.05 } },
  { name: 'Tampa', state: 'FL', bbox: { bottom: 27.70, left: -82.65, top: 28.15, right: -82.20 } },
  { name: 'Charlotte', state: 'NC', bbox: { bottom: 35.05, left: -81.00, top: 35.45, right: -80.60 } },
  { name: 'Orlando', state: 'FL', bbox: { bottom: 28.30, left: -81.60, top: 28.75, right: -81.15 } },
  { name: 'Jacksonville', state: 'FL', bbox: { bottom: 30.10, left: -81.90, top: 30.55, right: -81.40 } },
  { name: 'Nashville', state: 'TN', bbox: { bottom: 35.95, left: -87.00, top: 36.35, right: -86.55 } },
  { name: 'Birmingham', state: 'AL', bbox: { bottom: 33.35, left: -87.00, top: 33.70, right: -86.60 } },
  { name: 'Chicago', state: 'IL', bbox: { bottom: 41.65, left: -87.85, top: 42.05, right: -87.40 } },
  { name: 'Los Angeles', state: 'CA', bbox: { bottom: 33.80, left: -118.50, top: 34.20, right: -118.00 } },
  { name: 'Phoenix', state: 'AZ', bbox: { bottom: 33.25, left: -112.30, top: 33.65, right: -111.80 } },
  { name: 'Denver', state: 'CO', bbox: { bottom: 39.55, left: -105.15, top: 39.95, right: -104.75 } },
];

function classifySeverity(alert) {
  const subtype = (alert.subtype || alert.type || '').toUpperCase();
  const reportRating = alert.reportRating || 0;
  const reliability = alert.reliability || 0;
  const nComments = alert.nComments || 0;

  if (/MAJOR|HAZARD_ON_ROAD_CAR_STOPPED/i.test(subtype)) return 'serious';
  if (reportRating >= 4 || reliability >= 8) return 'serious';
  if (reportRating >= 2 || nComments >= 3) return 'moderate';
  return 'minor';
}

function classifyType(alert) {
  const subtype = (alert.subtype || '').toLowerCase();
  const type = (alert.type || '').toLowerCase();

  if (/truck/i.test(subtype)) return 'truck_accident';
  if (/pedestrian/i.test(subtype)) return 'pedestrian';
  if (/bicycle|cyclist/i.test(subtype)) return 'bicycle';
  if (/accident|crash/i.test(type)) return 'car_accident';
  if (/hazard/i.test(type)) return 'hazard';
  return 'car_accident';
}

async function fetchWazeAlerts(metro) {
  const { bbox } = metro;

  // Waze's public GeoJSON feed endpoint
  // This is the same data shown on the Waze Live Map (waze.com/livemap)
  const url = `https://www.waze.com/live-map/api/georss?bottom=${bbox.bottom}&left=${bbox.left}&top=${bbox.top}&right=${bbox.right}&env=row&types=alerts`;

  try {
    const resp = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; AIP-DataCollector/1.0)',
        'Accept': 'application/json',
        'Referer': 'https://www.waze.com/live-map'
      },
      signal: AbortSignal.timeout(15000)
    });

    if (!resp.ok) {
      console.error(`Waze ${metro.name}: HTTP ${resp.status}`);
      return [];
    }

    const data = await resp.json();
    const alerts = data.alerts || [];

    // Filter to accident-related alerts only
    return alerts
      .filter(a => {
        const type = (a.type || '').toUpperCase();
        return type === 'ACCIDENT' ||
               (type === 'HAZARD' && /ACCIDENT|CRASH|CAR_STOPPED/i.test(a.subtype || ''));
      })
      .map(alert => {
        const severity = classifySeverity(alert);
        const type = classifyType(alert);
        const street = alert.street || alert.nearBy || 'Unknown road';

        return {
          source: 'waze',
          source_reference: `WAZE-${alert.uuid || alert.id || Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
          title: `${alert.subtype ? alert.subtype.replace(/_/g, ' ') : 'Accident'} on ${street} near ${metro.name}`,
          description: [
            `Waze user-reported ${(alert.subtype || 'ACCIDENT').replace(/_/g, ' ').toLowerCase()}`,
            `on ${street}`,
            alert.city ? `in ${alert.city}` : '',
            `${alert.reportRating || 0} thumbs up, ${alert.reliability || 0} reliability`,
            alert.nComments ? `${alert.nComments} comments` : ''
          ].filter(Boolean).join('. '),
          incident_type: type,
          severity: severity,
          priority: severity === 'serious' ? 3 : severity === 'moderate' ? 4 : 5,
          city: metro.name,
          state: metro.state,
          lat: alert.location?.y || null,
          lng: alert.location?.x || null,
          injuries_count: null,
          fatalities_count: null,
          vehicles_involved: null,
          occurred_at: alert.pubMillis ? new Date(alert.pubMillis).toISOString() : new Date().toISOString(),
          confidence: Math.min(95, 50 + (alert.reliability || 0) * 5 + (alert.reportRating || 0) * 3),
          raw: alert
        };
      });
  } catch (e) {
    console.error(`Waze fetch error for ${metro.name}:`, e.message);
    return [];
  }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const db = getDb();
  const results = { inserted: 0, skipped: 0, errors: [], sources: {} };

  try {
    const selectedMetros = WAZE_METROS.sort(() => Math.random() - 0.5).slice(0, 4);
    let allAlerts = [];
    for (const metro of selectedMetros) {
      const alerts = await fetchWazeAlerts(metro);
      results.sources[metro.name] = alerts.length;
      allAlerts = allAlerts.concat(alerts);
      await new Promise(r => setTimeout(r, 500));
    }
    const metroAreas = await db('metro_areas').select('id', 'name');
    const metroMap = {};
    for (const ma of metroAreas) {
      for (const key of Object.keys(WAZE_METROS.reduce((acc, m) => ({ ...acc, [m.name]: true }), {}))) {
        if (ma.name.toLowerCase().includes(key.toLowerCase())) metroMap[key] = ma.id;
      }
    }
    let wazeDs = await db('data_sources').where('name', 'like', '%Waze%').first();
    if (!wazeDs) {
      const dsId = uuidv4();
      await db('data_sources').insert({ id: dsId, name: 'Waze Live Map', type: 'api', api_endpoint: 'https://www.waze.com/live-map/api/georss', is_active: true, last_polled_at: new Date(), created_at: new Date(), updated_at: new Date() });
      wazeDs = { id: dsId };
    }
    for (const record of allAlerts) {
      try {
        const existing = await db('source_reports').where('source_reference', record.source_reference).first();
        if (!existing && record.lat && record.lng) {
          const nearby = await db('incidents').where('source_count', '>=', 0).where('occurred_at', '>', new Date(Date.now() - 30 * 60 * 1000)).whereNotNull('latitude').whereNotNull('longitude').whereRaw(`(6371000 * acos(cos(radians(?)) * cos(radians(latitude)) * cos(radians(longitude) - radians(?)) + sin(radians(?)) * sin(radians(latitude)))) < 500`, [record.lat, record.lng, record.lat]).first();
          if (nearby) {
            await db('incidents').where('id', nearby.id).update({ source_count: db.raw('COALESCE(source_count, 1) + 1'), confidence_score: db.raw('LEAST(99, COALESCE(confidence_score, 50) + 10)'), updated_at: new Date() });
            await db('source_reports').insert({ id: uuidv4(), incident_id: nearby.id, data_source_id: wazeDs.id, source_type: 'waze', source_reference: record.source_reference, raw_data: JSON.stringify(record.raw), parsed_data: JSON.stringify({ title: record.title, description: record.description }), contributed_fields: ['corroboration', 'severity', 'location'], confidence: record.confidence, is_verified: false, fetched_at: new Date(), processed_at: new Date(), created_at: new Date() });
            results.skipped++; continue;
          }
        }
        if (existing) { results.skipped++; continue; }
        const incidentId = uuidv4();
        const now = new Date();
        await db('incidents').insert({ id: incidentId, incident_number: `WAZE-${now.getFullYear().toString().slice(-2)}${String(now.getMonth()+1).padStart(2,'0')}${String(now.getDate()).padStart(2,'0')}-${Math.floor(Math.random()*9999).toString().padStart(4,'0')}`, incident_type: record.incident_type, severity: record.severity, status: 'new', priority: record.priority, confidence_score: record.confidence, address: record.title, city: record.city, state: record.state, latitude: record.lat, longitude: record.lng, occurred_at: record.occurred_at ? new Date(record.occurred_at) : now, reported_at: now, discovered_at: now, description: record.description, injuries_count: record.injuries_count, fatalities_count: record.fatalities_count, vehicles_involved: record.vehicles_involved, metro_area_id: metroMap[record.city] || null, source_count: 1, first_source_id: wazeDs.id, tags: ['waze', 'crowdsourced'], created_at: now, updated_at: now });
        await db('source_reports').insert({ id: uuidv4(), incident_id: incidentId, data_source_id: wazeDs.id, source_type: 'waze', source_reference: record.source_reference, raw_data: JSON.stringify(record.raw), parsed_data: JSON.stringify({ title: record.title, description: record.description, type: record.incident_type, severity: record.severity }), contributed_fields: ['description', 'incident_type', 'severity', 'location'], confidence: record.confidence, is_verified: false, fetched_at: now, processed_at: now, created_at: now });
        results.inserted++;
      } catch (e) { results.errors.push(`waze: ${e.message}`); }
    }
    await db('data_sources').where('id', wazeDs.id).update({ last_polled_at: new Date(), last_success_at: new Date(), updated_at: new Date() });
    res.json({ success: true, message: `Waze: Ingested ${results.inserted} accidents, corroborated/skipped ${results.skipped}`, total_alerts: allAlerts.length, ...results, timestamp: new Date().toISOString() });
  } catch (err) {
    await reportError(db, 'waze', null, err.message);
        res.status(500).json({ error: err.message, results });
  }
};
