/**
 * INGESTION RUNNER
 * Orchestrates all data source polling and processing
 */

const db = require('../config/database');
const { logger } = require('../utils/logger');

// Import all source adapters
const PoliceScanner = require('./sources/policeScanner');
const PoliceReports = require('./sources/policeReports');
const CADDispatch = require('./sources/cadDispatch');
const EMSHospital = require('./sources/emsHospital');
const NewsMonitor = require('./sources/newsMonitor');
const PublicRecords = require('./sources/publicRecords');
const RadioFrequency = require('./sources/radioFrequency');
const SocialMonitor = require('./sources/socialMonitor');
const DOTCrashData = require('./sources/dotCrashData');
const InsuranceVerify = require('./sources/insuranceVerify');

const sourceAdapters = {
  police_scanner: PoliceScanner,
  police_report: PoliceReports,
  cad_dispatch: CADDispatch,
  hospital_ems: EMSHospital,
  news: NewsMonitor,
  public_records: PublicRecords,
  radio: RadioFrequency,
  social_media: SocialMonitor,
  dot_data: DOTCrashData,
  insurance_verify: InsuranceVerify
};

/**
 * Main ingestion cycle - called every 30 seconds by cron
 */
async function runIngestionCycle(io) {
  const activeSources = await db('data_sources')
    .where('is_active', true)
    .where(function () {
      this.whereNull('last_polled_at')
        .orWhereRaw("last_polled_at + (polling_interval_seconds || ' seconds')::interval <= NOW()");
    });

  if (activeSources.length === 0) return;

  logger.info(`Ingestion cycle: ${activeSources.length} sources to poll`);

  const results = await Promise.allSettled(
    activeSources.map(source => pollSource(source, io))
  );

  let totalNew = 0;
  results.forEach((result, idx) => {
    if (result.status === 'fulfilled') {
      totalNew += result.value || 0;
    } else {
      logger.error(`Source ${activeSources[idx].name} failed:`, result.reason);
    }
  });

  if (totalNew > 0) {
    logger.info(`Ingestion cycle complete: ${totalNew} new records processed`);
  }
}

/**
 * Poll a single data source
 */
async function pollSource(source, io) {
  const Adapter = sourceAdapters[source.type];
  if (!Adapter) {
    logger.warn(`No adapter for source type: ${source.type}`);
    return 0;
  }

  try {
    const adapter = new Adapter(source);
    const rawRecords = await adapter.fetch();

    await db('data_sources').where({ id: source.id }).update({
      last_polled_at: new Date(),
      last_success_at: new Date(),
      error_count: 0
    });

    if (!rawRecords || rawRecords.length === 0) return 0;

    let newCount = 0;
    for (const raw of rawRecords) {
      try {
        const normalized = adapter.normalize(raw);
        const result = await processRecord(normalized, source, raw, io);
        if (result === 'new') newCount++;
      } catch (err) {
        logger.error(`Error processing record from ${source.name}:`, err);
      }
    }

    return newCount;
  } catch (err) {
    await db('data_sources').where({ id: source.id }).update({
      last_polled_at: new Date(),
      error_count: db.raw('error_count + 1')
    });
    throw err;
  }
}

/**
 * Process a normalized record - match to existing or create new incident
 */
async function processRecord(normalized, source, raw, io) {
  // Try to match to existing incident
  const match = await findMatchingIncident(normalized);

  if (match) {
    // Enrich existing incident with new data
    await enrichIncident(match.id, normalized, source, raw);
    return 'enriched';
  }

  // Create new incident
  const incident = await createIncident(normalized, source, raw);

  // Emit real-time event
  if (io && incident) {
    io.to('all-incidents').emit('incident:new', incident);
    if (incident.metro_area_id) {
      io.to(`metro:${incident.metro_area_id}`).emit('incident:new', incident);
    }
  }

  return 'new';
}

/**
 * Find matching incident by location + time proximity, or report number
 */
async function findMatchingIncident(normalized) {
  // Match by police report number first (strongest match)
  if (normalized.police_report_number) {
    const match = await db('incidents')
      .where('police_report_number', normalized.police_report_number)
      .first();
    if (match) return match;
  }

  // Match by location + time (within 500m and 30 minutes)
  if (normalized.latitude && normalized.longitude && normalized.occurred_at) {
    const match = await db('incidents')
      .whereRaw(`
        ST_DWithin(
          geom::geography,
          ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography,
          500
        )
      `, [normalized.longitude, normalized.latitude])
      .whereRaw(`
        ABS(EXTRACT(EPOCH FROM (occurred_at - ?::timestamptz))) < 1800
      `, [normalized.occurred_at])
      .orderByRaw(`
        ST_Distance(
          geom::geography,
          ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography
        )
      `, [normalized.longitude, normalized.latitude])
      .first();
    if (match) return match;
  }

  // Match by address similarity + time
  if (normalized.address && normalized.occurred_at) {
    const match = await db('incidents')
      .whereRaw("similarity(address, ?) > 0.6", [normalized.address])
      .whereRaw(`
        ABS(EXTRACT(EPOCH FROM (occurred_at - ?::timestamptz))) < 3600
      `, [normalized.occurred_at])
      .orderByRaw("similarity(address, ?) DESC", [normalized.address])
      .first();
    if (match) return match;
  }

  return null;
}

/**
 * Create a new incident from normalized data
 */
async function createIncident(normalized, source, raw) {
  // Determine metro area
  let metroAreaId = source.metro_area_id;
  if (!metroAreaId && normalized.latitude && normalized.longitude) {
    const metro = await db('metro_areas')
      .whereRaw(`
        ST_DWithin(
          geom::geography,
          ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography,
          ? * 1609.34
        )
      `, [normalized.longitude, normalized.latitude, 50])
      .orderByRaw(`
        ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint(?, ?), 4326)::geography)
      `, [normalized.longitude, normalized.latitude])
      .first();
    if (metro) metroAreaId = metro.id;
  }

  // Calculate initial priority
  const priority = calculatePriority(normalized);

  const [incident] = await db('incidents').insert({
    incident_number: normalized.incident_number,
    incident_type: normalized.incident_type || 'car_accident',
    severity: normalized.severity || 'unknown',
    status: 'new',
    priority,
    confidence_score: normalized.confidence || 30,
    address: normalized.address,
    street: normalized.street,
    city: normalized.city,
    county: normalized.county,
    state: normalized.state,
    zip: normalized.zip,
    latitude: normalized.latitude,
    longitude: normalized.longitude,
    intersection: normalized.intersection,
    highway: normalized.highway,
    occurred_at: normalized.occurred_at,
    reported_at: normalized.reported_at,
    description: normalized.description,
    raw_description: normalized.description,
    responding_agencies: normalized.responding_agencies,
    dispatch_codes: normalized.dispatch_codes,
    ems_dispatched: normalized.ems_dispatched || false,
    helicopter_dispatched: normalized.helicopter_dispatched || false,
    extrication_needed: normalized.extrication_needed || false,
    vehicles_involved: normalized.vehicles_involved,
    persons_involved: normalized.persons_involved,
    injuries_count: normalized.injuries_count,
    fatalities_count: normalized.fatalities_count,
    police_report_number: normalized.police_report_number,
    police_department: normalized.police_department,
    metro_area_id: metroAreaId,
    source_count: 1,
    first_source_id: source.id,
    tags: normalized.tags
  }).returning('*');

  // Store raw source report
  await db('source_reports').insert({
    incident_id: incident.id,
    data_source_id: source.id,
    source_type: source.type,
    source_reference: normalized.source_reference,
    raw_data: JSON.stringify(raw),
    parsed_data: JSON.stringify(normalized),
    confidence: normalized.confidence || 30
  });

  // Create person records if available
  if (normalized.persons && normalized.persons.length > 0) {
    for (const person of normalized.persons) {
      await db('persons').insert({
        incident_id: incident.id,
        role: person.role || 'driver',
        is_injured: person.is_injured || false,
        first_name: person.first_name,
        last_name: person.last_name,
        full_name: person.full_name || `${person.first_name || ''} ${person.last_name || ''}`.trim(),
        age: person.age,
        gender: person.gender,
        phone: person.phone,
        email: person.email,
        address: person.address,
        city: person.city,
        state: person.state,
        zip: person.zip,
        injury_severity: person.injury_severity,
        injury_description: person.injury_description,
        transported_to: person.transported_to,
        transported_by: person.transported_by,
        insurance_company: person.insurance_company,
        insurance_policy_number: person.insurance_policy_number,
        insurance_type: person.insurance_type,
        policy_limits: person.policy_limits,
        has_attorney: person.has_attorney || false,
        employer: person.employer,
        confidence_score: person.confidence || 30
      });
    }
  }

  // Create vehicle records if available
  if (normalized.vehicles && normalized.vehicles.length > 0) {
    for (const vehicle of normalized.vehicles) {
      await db('vehicles').insert({
        incident_id: incident.id,
        year: vehicle.year,
        make: vehicle.make,
        model: vehicle.model,
        color: vehicle.color,
        body_type: vehicle.body_type,
        license_plate: vehicle.license_plate,
        license_state: vehicle.license_state,
        vin: vehicle.vin,
        damage_severity: vehicle.damage_severity,
        towed: vehicle.towed,
        tow_company: vehicle.tow_company,
        insurance_company: vehicle.insurance_company,
        insurance_policy: vehicle.insurance_policy,
        is_commercial: vehicle.is_commercial || false,
        dot_number: vehicle.dot_number,
        carrier_name: vehicle.carrier_name
      });
    }
  }

  return incident;
}

/**
 * Enrich existing incident with data from a new source
 */
async function enrichIncident(incidentId, normalized, source, raw) {
  const incident = await db('incidents').where({ id: incidentId }).first();

  // Build update with only new/better data
  const updates = {};
  const enrichFields = [
    'police_report_number', 'police_department', 'severity', 'injuries_count',
    'fatalities_count', 'vehicles_involved', 'persons_involved', 'address',
    'city', 'state', 'zip', 'latitude', 'longitude'
  ];

  enrichFields.forEach(field => {
    if (normalized[field] && !incident[field]) {
      updates[field] = normalized[field];
    }
  });

  // Append to description
  if (normalized.description && normalized.description !== incident.description) {
    updates.description = `${incident.description || ''}\n\n[${source.name}]: ${normalized.description}`.trim();
  }

  // Increase confidence and source count
  updates.source_count = (incident.source_count || 1) + 1;
  updates.confidence_score = Math.min(100, (incident.confidence_score || 30) + 15);

  // Upgrade severity if new source says worse
  const severityRank = { fatal: 5, critical: 4, serious: 3, moderate: 2, minor: 1, unknown: 0 };
  if (severityRank[normalized.severity] > severityRank[incident.severity]) {
    updates.severity = normalized.severity;
  }

  await db('incidents').where({ id: incidentId }).update(updates);

  // Store source report
  await db('source_reports').insert({
    incident_id: incidentId,
    data_source_id: source.id,
    source_type: source.type,
    source_reference: normalized.source_reference,
    raw_data: JSON.stringify(raw),
    parsed_data: JSON.stringify(normalized),
    contributed_fields: Object.keys(updates),
    confidence: normalized.confidence || 50
  });

  // Add new persons if not already present
  if (normalized.persons) {
    for (const person of normalized.persons) {
      const exists = await db('persons')
        .where('incident_id', incidentId)
        .where(function () {
          if (person.full_name) this.whereILike('full_name', person.full_name);
          else if (person.phone) this.where('phone', person.phone);
          else this.whereRaw('1=0');
        }).first();

      if (!exists) {
        await db('persons').insert({
          incident_id: incidentId,
          role: person.role || 'driver',
          is_injured: person.is_injured || false,
          full_name: person.full_name,
          first_name: person.first_name,
          last_name: person.last_name,
          phone: person.phone,
          insurance_company: person.insurance_company,
          policy_limits: person.policy_limits,
          injury_severity: person.injury_severity,
          transported_to: person.transported_to,
          confidence_score: person.confidence || 40
        });
      } else {
        // Enrich existing person with missing fields
        const personUpdates = {};
        ['phone', 'email', 'insurance_company', 'policy_limits', 'insurance_type',
          'injury_severity', 'transported_to', 'address', 'city', 'state'].forEach(f => {
            if (person[f] && !exists[f]) personUpdates[f] = person[f];
          });
        if (Object.keys(personUpdates).length > 0) {
          await db('persons').where({ id: exists.id }).update(personUpdates);
        }
      }
    }
  }
}

/**
 * Calculate priority score (1=highest, 10=lowest)
 */
function calculatePriority(normalized) {
  let priority = 5;

  // Severity-based
  if (normalized.fatalities_count > 0 || normalized.severity === 'fatal') priority = 1;
  else if (normalized.severity === 'critical' || normalized.helicopter_dispatched) priority = 2;
  else if (normalized.severity === 'serious' || normalized.extrication_needed) priority = 3;
  else if (normalized.injuries_count >= 3) priority = 3;
  else if (normalized.injuries_count >= 1) priority = 4;

  // Commercial vehicle boost
  if (normalized.incident_type === 'truck_accident') priority = Math.max(1, priority - 1);

  // Multi-vehicle boost
  if (normalized.vehicles_involved >= 3) priority = Math.max(1, priority - 1);

  return priority;
}

module.exports = { runIngestionCycle, processRecord, findMatchingIncident };
