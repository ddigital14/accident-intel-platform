/**
 * State Crash Report Pipeline
 * Cron: every 2 hours
 * GET /api/v1/ingest/state-crash?secret=ingest-now
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('../system/_errors');
const { batchInsert } = require('../../_batch');
const { dedupCache } = require('../../_cache');

const STATES = [
  { code: 'TX', name: 'Texas',  feedUrl: 'https://its.txdot.gov/its/api/Incident', parser: 'txdot' },
  { code: 'GA', name: 'Georgia', feedUrl: 'https://www.511ga.org/api/v2/get_events?format=json&key=public', parser: 'ga511' },
  { code: 'FL', name: 'Florida', feedUrl: 'https://fl511.com/List/Events/Crashes', parser: 'fl511' }
];

function parseTxDOT(records) {
  const arr = Array.isArray(records) ? records : (records?.Incidents || records?.incidents || []);
  return (arr || []).filter(r =>
    /crash|accident|collision/i.test(r.IncidentType || r.eventType || r.Type || '')
  ).map(r => ({
    source: 'state_txdot',
    source_reference: `TXDOT-${r.IncidentId || r.IncidentID || r.id || Date.now()}`,
    incident_type: 'car_accident',
    severity: /major|fatal|serious/i.test(r.severity || r.Severity || '') ? 'serious' : 'moderate',
    title: `${r.IncidentType || r.eventType || 'Crash'} - ${r.Location || r.location || 'Texas'}`,
    description: `TxDOT Active Incident: ${r.Description || r.description || 'crash'} at ${r.Location || r.location}. Lanes: ${r.LanesAffected || 'unknown'}.`,
    state: 'TX',
    city: r.City || r.city || null,
    highway: r.Roadway || r.roadway || r.highway || null,
    lat: parseFloat(r.Latitude || r.latitude) || null,
    lng: parseFloat(r.Longitude || r.longitude) || null,
    occurred_at: r.StartTime || r.startTime || r.IncidentStartTime || new Date().toISOString(),
    confidence: 88, raw: r
  }));
}

function parseGa511(records) {
  const arr = records?.events || records?.Events || (Array.isArray(records) ? records : []);
  return arr.filter(r =>
    /crash|accident|collision/i.test(r.eventType || r.event_type || r.type || '')
  ).map(r => ({
    source: 'state_ga511',
    source_reference: `GA511-${r.id || r.eventId || Date.now()}`,
    incident_type: 'car_accident',
    severity: /injury|fatal|serious/i.test(r.severity || r.eventDescription || '') ? 'serious' : 'moderate',
    title: `${r.eventType || 'Crash'} - ${r.location || 'Georgia'}`,
    description: `Georgia 511: ${r.eventDescription || r.description || 'crash'} at ${r.location || 'unknown'}.`,
    state: 'GA',
    city: r.city || null,
    highway: r.roadway || r.highway || null,
    lat: parseFloat(r.latitude || r.lat) || null,
    lng: parseFloat(r.longitude || r.lng) || null,
    occurred_at: r.startTime || r.start_time || new Date().toISOString(),
    confidence: 85, raw: r
  }));
}

function parseFl511(records) {
  const arr = records?.events || records?.crashes || (Array.isArray(records) ? records : []);
  return arr.map(r => ({
    source: 'state_fl511',
    source_reference: `FL511-${r.id || r.eventId || Date.now()}`,
    incident_type: 'car_accident',
    severity: /major|fatal/i.test(r.severity || r.title || '') ? 'serious' : 'moderate',
    title: `${r.title || 'Crash'} - ${r.location || 'Florida'}`,
    description: `Florida 511: ${r.description || r.title || 'crash'}. ${r.location || ''}`,
    state: 'FL',
    city: r.city || null,
    highway: r.roadway || null,
    lat: parseFloat(r.latitude) || null,
    lng: parseFloat(r.longitude) || null,
    occurred_at: r.startTime || new Date().toISOString(),
    confidence: 85, raw: r
  }));
}

const PARSERS = { txdot: parseTxDOT, ga511: parseGa511, fl511: parseFl511 };

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const results = { inserted: 0, corroborated: 0, skipped: 0, errors: [], states: {} };
  try {
    let stateDs = await db('data_sources').where('name', 'like', '%State Crash%').first();
    if (!stateDs) {
      const dsId = uuidv4();
      await db('data_sources').insert({
        id: dsId, name: 'State Crash Reports', type: 'crash_report',
        api_endpoint: 'multiple state DOT portals', is_active: true,
        last_polled_at: new Date(), created_at: new Date(), updated_at: new Date()
      });
      stateDs = { id: dsId };
    }

    const newIncidents = [];
    const newReports = [];
    const corroborations = [];

    for (const st of STATES) {
      try {
        const resp = await fetch(st.feedUrl, {
          headers: { 'Accept': 'application/json', 'User-Agent': 'AIP/1.0' },
          signal: AbortSignal.timeout(15000)
        });
        if (!resp.ok) {
          results.states[st.code] = `HTTP ${resp.status}`;
          await reportError(db, 'state-crash', st.code, `HTTP ${resp.status}`, { url: st.feedUrl });
          continue;
        }
        const ct = resp.headers.get('content-type') || '';
        if (!ct.includes('json')) { results.states[st.code] = 'non-JSON feed'; continue; }
        const data = await resp.json();
        const records = PARSERS[st.parser](data);
        results.states[st.code] = records.length;

        for (const record of records) {
          if (dedupCache.has(record.source_reference)) { results.skipped++; continue; }
          const existing = await db('source_reports').where('source_reference', record.source_reference).first();
          if (existing) { dedupCache.set(record.source_reference, 1); results.skipped++; continue; }
          dedupCache.set(record.source_reference, 1);

          let matched = null;
          if (record.lat && record.lng) {
            try {
              const r = await db.raw(`
                SELECT id FROM incidents
                WHERE occurred_at > NOW() - INTERVAL '6 hours'
                  AND geom IS NOT NULL
                  AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, 1500)
                LIMIT 1
              `, [record.lng, record.lat]);
              matched = r.rows?.[0];
            } catch (_) {}
          }
          if (matched) {
            corroborations.push({ id: matched.id, record, dsId: stateDs.id });
            results.corroborated++;
            continue;
          }

          const incidentId = uuidv4();
          const now = new Date();
          newIncidents.push({
            id: incidentId,
            incident_number: `STATE-${now.getFullYear().toString().slice(-2)}${String(now.getMonth()+1).padStart(2,'0')}${String(now.getDate()).padStart(2,'0')}-${Math.floor(Math.random()*9999).toString().padStart(4,'0')}`,
            incident_type: record.incident_type, severity: record.severity,
            status: 'new',
            priority: record.severity === 'fatal' ? 1 : record.severity === 'serious' ? 2 : 3,
            confidence_score: record.confidence,
            address: record.title, city: record.city || null, state: record.state,
            highway: record.highway || null,
            latitude: record.lat, longitude: record.lng,
            occurred_at: record.occurred_at ? new Date(record.occurred_at) : now,
            reported_at: now, discovered_at: now,
            description: record.description,
            source_count: 1, first_source_id: stateDs.id,
            tags: ['state_dot', record.source],
            created_at: now, updated_at: now
          });
          newReports.push({
            id: uuidv4(), incident_id: incidentId, data_source_id: stateDs.id,
            source_type: record.source, source_reference: record.source_reference,
            raw_data: JSON.stringify(record.raw),
            parsed_data: JSON.stringify({ title: record.title, description: record.description }),
            contributed_fields: ['description', 'incident_type', 'severity', 'location', 'highway'],
            confidence: record.confidence, is_verified: true,
            fetched_at: now, processed_at: now, created_at: now
          });
          results.inserted++;
        }
      } catch (e) {
        results.errors.push(`${st.code}: ${e.message}`);
        results.states[st.code] = `Error: ${e.message}`;
        await reportError(db, 'state-crash', st.code, e.message);
      }
    }

    if (newIncidents.length) await batchInsert(db, 'incidents', newIncidents);
    if (newReports.length) await batchInsert(db, 'source_reports', newReports);

    for (const corr of corroborations) {
      try {
        await db('incidents').where('id', corr.id).update({
          source_count: db.raw('COALESCE(source_count, 1) + 1'),
          confidence_score: db.raw('LEAST(99, COALESCE(confidence_score, 50) + 12)'),
          updated_at: new Date()
        });
        await db('source_reports').insert({
          id: uuidv4(), incident_id: corr.id, data_source_id: corr.dsId,
          source_type: corr.record.source, source_reference: corr.record.source_reference,
          raw_data: JSON.stringify(corr.record.raw),
          parsed_data: JSON.stringify({ title: corr.record.title }),
          contributed_fields: ['corroboration', 'state_dot'],
          confidence: corr.record.confidence, is_verified: true,
          fetched_at: new Date(), processed_at: new Date(), created_at: new Date()
        });
      } catch (e) { results.errors.push(`corroborate: ${e.message}`); }
    }

    await db('data_sources').where('id', stateDs.id).update({
      last_polled_at: new Date(), last_success_at: new Date(), updated_at: new Date()
    });

    res.json({
      success: true,
      message: `State Crash: ${results.inserted} new, ${results.corroborated} corroborated`,
      ...results,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'state-crash', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
