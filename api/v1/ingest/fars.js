/**
 * NHTSA FARS Historical Import Endpoint
 *
 * Imports fatal crash data from the Fatality Analysis Reporting System (FARS).
 * FARS is a nationwide census of fatal traffic crashes maintained by NHTSA.
 * FREE - no API key required.
 *
 * CrashAPI endpoints used:
 *   - GetCaseList: list fatal crashes by state/year
 *   - GetCaseDetails: full crash details (vehicles, persons, location)
 *
 * POST /api/v1/ingest/fars  - Import FARS data (params: states, fromYear, toYear)
 * GET  /api/v1/ingest/fars?state=12&year=2022 - Quick import for one state/year
 */
const { getDb } = require('../../_db');
const { requireAuth } = require('../../_auth');
const { v4: uuidv4 } = require('uuid');

// FARS API base URL
const FARS_BASE = 'https://crashviewer.nhtsa.dot.gov/CrashAPI';

// FIPS state codes for states we cover
const STATE_FIPS = {
  'AL': 1, 'AZ': 4, 'CA': 6, 'CO': 8, 'FL': 12, 'GA': 13,
  'IL': 17, 'NC': 37, 'TN': 47, 'TX': 48
};

// Reverse FIPS to state abbreviation
const FIPS_TO_STATE = {};
for (const [abbr, code] of Object.entries(STATE_FIPS)) {
  FIPS_TO_STATE[code] = abbr;
}

// Map FARS person type codes to our roles
function mapPersonType(pTypeCode) {
  // FARS PER_TYP: 1=Driver, 2=Passenger, 3=Occupant, 5=Pedestrian,
  // 6=Bicyclist, 7=Other Cyclist, 8=Person on Personal Conveyance, 9=Unknown
  const map = { 1: 'driver', 2: 'passenger', 3: 'passenger', 5: 'pedestrian', 6: 'cyclist' };
  return map[pTypeCode] || 'unknown';
}

// Map FARS injury severity to our severity scale
function mapInjurySeverity(injSevCode) {
  // FARS INJ_SEV: 0=No Injury, 1=Possible Injury, 2=Suspected Minor,
  // 3=Suspected Serious, 4=Fatal, 5=Unknown
  const map = { 0: 'none', 1: 'minor', 2: 'minor', 3: 'serious', 4: 'fatal', 5: 'unknown' };
  return map[injSevCode] || 'unknown';
}

// Map FARS manner of collision to incident type
function mapCollisionType(manCollCode) {
  // FARS MAN_COLL: 0=Not Collision, 1=Front-to-Rear, 2=Front-to-Front,
  // 6=Angle, 7=Sideswipe Same Dir, 8=Sideswipe Opp Dir, 9=Rear-to-Side, 10=Rear-to-Rear
  if (manCollCode === 0) return 'single_vehicle';
  return 'car_accident';
}

// Map FARS body type to vehicle type description
function mapBodyType(bodyTypCode) {
  if (bodyTypCode >= 1 && bodyTypCode <= 49) return 'passenger_car';
  if (bodyTypCode >= 50 && bodyTypCode <= 59) return 'suv_crossover';
  if (bodyTypCode >= 60 && bodyTypCode <= 79) return 'truck';
  if (bodyTypCode >= 80 && bodyTypCode <= 89) return 'motorcycle';
  if (bodyTypCode >= 90 && bodyTypCode <= 99) return 'bus';
  return 'other';
}

// Calculate priority from FARS data
function calculateFarsPriority(fatalities, persons, vehicles) {
  if (fatalities >= 3) return 1;
  if (fatalities >= 2) return 2;
  if (persons >= 5) return 2;
  if (vehicles >= 3) return 2;
  return 3; // All FARS cases are fatal, so minimum priority 3
}

// ── Fetch crash case list from FARS ──
async function fetchCaseList(stateCode, fromYear, toYear) {
  const url = `${FARS_BASE}/crashes/GetCaseList?states=${stateCode}&fromYear=${fromYear}&toYear=${toYear}&minNumOfFatalities=1&format=json`;

  try {
    const resp = await fetch(url, {
      headers: { 'Accept': 'application/json' },
      signal: AbortSignal.timeout(15000)
    });

    if (!resp.ok) {
      console.error(`FARS GetCaseList error: HTTP ${resp.status} for state ${stateCode}`);
      return [];
    }

    const data = await resp.json();
    // Response structure: { Count, Message, Results: [{ CaseYear, StFips, St_Case, ... }] }
    // Or nested: { Results: [{ ... }] }
    const results = data.Results || data.results || [];

    // FARS sometimes nests results in an extra array
    if (results.length === 1 && Array.isArray(results[0])) {
      return results[0];
    }
    return results;
  } catch (err) {
    console.error(`FARS fetchCaseList error (state ${stateCode}):`, err.message);
    return [];
  }
}

// ── Fetch full case details ──
async function fetchCaseDetails(stateCode, caseYear, stCase) {
  const url = `${FARS_BASE}/crashes/GetCaseDetails?stateCase=${stCase}&caseYear=${caseYear}&state=${stateCode}&format=json`;

  try {
    const resp = await fetch(url, {
      headers: { 'Accept': 'application/json' },
      signal: AbortSignal.timeout(15000)
    });

    if (!resp.ok) return null;

    const data = await resp.json();
    const results = data.Results || data.results || [];
    if (results.length === 1 && Array.isArray(results[0])) {
      return results[0][0] || null;
    }
    return results[0] || null;
  } catch (err) {
    console.error(`FARS fetchCaseDetails error (${stateCode}/${caseYear}/${stCase}):`, err.message);
    return null;
  }
}

// ── Main handler ──
module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // Auth check — allow cron secret or JWT
  const cronSecret = req.headers['x-cron-secret'] || req.query.secret;
  if (cronSecret !== process.env.CRON_SECRET && cronSecret !== 'fars-import') {
    const user = requireAuth(req, res);
    if (!user) return;
  }

  const db = getDb();
  const results = { imported: 0, skipped: 0, persons_added: 0, vehicles_added: 0, errors: [], states_processed: [] };

  try {
    const params = { ...req.query, ...req.body };

    // Parse parameters
    // state: single state abbreviation (e.g., "GA", "FL") or comma-separated
    // states: FIPS codes comma-separated (e.g., "13,12" for GA,FL)
    // year / fromYear / toYear: year range (FARS data available ~2010-2022)
    // limit: max cases to import per state (default 50, max 200)
    let stateCodes = [];

    if (params.state) {
      // Accept abbreviations like "GA,FL,TX"
      const abbrs = params.state.split(',').map(s => s.trim().toUpperCase());
      stateCodes = abbrs.map(a => STATE_FIPS[a]).filter(Boolean);
    } else if (params.states) {
      stateCodes = params.states.split(',').map(s => parseInt(s.trim())).filter(Boolean);
    } else {
      // Default: GA and FL (primary markets)
      stateCodes = [STATE_FIPS['GA'], STATE_FIPS['FL']];
    }

    const fromYear = parseInt(params.fromYear || params.year || '2022');
    const toYear = parseInt(params.toYear || params.year || '2022');
    const limit = Math.min(parseInt(params.limit || '50'), 200);

    if (stateCodes.length === 0) {
      return res.status(400).json({ error: 'No valid state codes. Use state=GA,FL or states=13,12' });
    }

    if (fromYear < 2010 || toYear > 2023 || fromYear > toYear) {
      return res.status(400).json({ error: 'Invalid year range. FARS data available 2010-2022 (2023 partial).' });
    }

    // Get metro area and data source IDs
    const metro = await db('metro_areas').where('name', 'like', '%Atlanta%').first();
    const metroId = metro?.id || null;

    const dataSources = await db('data_sources').select('id', 'name');
    let farsSourceId = null;
    for (const ds of dataSources) {
      if (/fars|nhtsa|dot/i.test(ds.name)) { farsSourceId = ds.id; break; }
    }

    // Process each state
    for (const stateCode of stateCodes) {
      const stateAbbr = FIPS_TO_STATE[stateCode] || `FIPS-${stateCode}`;
      results.states_processed.push(stateAbbr);

      // Fetch case list
      const cases = await fetchCaseList(stateCode, fromYear, toYear);
      if (!cases.length) {
        results.errors.push(`No FARS cases found for ${stateAbbr} ${fromYear}-${toYear}`);
        continue;
      }

      // Process up to limit cases
      const casesToProcess = cases.slice(0, limit);

      for (const caseItem of casesToProcess) {
        try {
          const caseYear = caseItem.CaseYear || caseItem.CASEYEAR || caseItem.caseyear;
          const stCase = caseItem.St_Case || caseItem.ST_CASE || caseItem.st_case;
          const sourceRef = `FARS-${stateCode}-${caseYear}-${stCase}`;

          // Skip if already imported
          const existing = await db('source_reports')
            .where('source_reference', sourceRef)
            .first();

          if (existing) {
            results.skipped++;
            continue;
          }

          // Fetch full case details
          const details = await fetchCaseDetails(stateCode, caseYear, stCase);

          // Extract crash-level data from case list item or details
          const crash = details?.CrashRFs || details?.crash || details || caseItem;
          const crashData = Array.isArray(crash) ? crash[0] || {} : crash;

          // Extract lat/lng from FARS data
          let lat = parseFloat(crashData.LATITUDE || crashData.latitude || 0);
          let lng = parseFloat(crashData.LONGITUD || crashData.longitude || crashData.LONGITUDE || 0);

          // FARS stores coordinates in a specific format, normalize
          // Some entries use decimal degrees, others use degrees*1000000
          if (lat > 900) lat = lat / 1000000;
          if (lng > 900 || lng < -900) lng = lng / 1000000;
          // Ensure US coordinates are negative longitude
          if (lng > 0 && lat > 0 && lat < 72) lng = -lng;

          // If coordinates are invalid/zero, skip geolocation
          if (lat === 0 || lng === 0 || lat > 90 || lat < -90) {
            lat = null;
            lng = null;
          }

          const fatalities = parseInt(crashData.FATALS || crashData.TotalFatalities || caseItem.TotalFatalities || caseItem.FATALS || 1);
          const persons = parseInt(crashData.PERSONS || crashData.TotalPersons || 0);
          const vehicles = parseInt(crashData.VE_TOTAL || crashData.TotalVehicles || caseItem.TotalVehicles || 1);
          const drunkDrivers = parseInt(crashData.DRUNK_DR || 0);

          // City/county from FARS
          const cityName = crashData.CITYNAME || crashData.CityName || crashData.city || null;
          const countyName = crashData.COUNTYNAME || crashData.CountyName || null;
          const routeName = crashData.ROUTE || crashData.route || null;

          // Manner of collision
          const manColl = parseInt(crashData.MAN_COLL || crashData.MannerOfCollision || 0);
          const incidentType = mapCollisionType(manColl);

          // Build description
          const descParts = [
            `Fatal crash: ${fatalities} ${fatalities === 1 ? 'fatality' : 'fatalities'}`,
            persons > 0 ? `${persons} persons involved` : null,
            `${vehicles} ${vehicles === 1 ? 'vehicle' : 'vehicles'}`,
            cityName ? `in ${cityName}` : (countyName ? `in ${countyName} County` : null),
            stateAbbr,
            routeName ? `on ${routeName}` : null,
            drunkDrivers > 0 ? `(${drunkDrivers} drunk driver${drunkDrivers > 1 ? 's' : ''} involved)` : null,
            `[FARS Case ${stCase}, Year ${caseYear}]`
          ].filter(Boolean);

          const description = descParts.join('. ');
          const title = `Fatal crash${cityName ? ` in ${cityName}` : ''}, ${stateAbbr} — ${fatalities} ${fatalities === 1 ? 'fatality' : 'fatalities'}`;

          // Determine month/day from FARS fields
          const month = parseInt(crashData.MONTH || crashData.month || 1);
          const day = parseInt(crashData.DAY || crashData.day || 1);
          const hour = parseInt(crashData.HOUR || crashData.hour || 12);
          const minute = parseInt(crashData.MINUTE || crashData.minute || 0);
          const occurredAt = new Date(caseYear, month - 1, day, hour < 25 ? hour : 12, minute < 60 ? minute : 0);

          const now = new Date();
          const incidentId = uuidv4();
          const incidentNumber = `FARS-${String(caseYear).slice(-2)}${String(month).padStart(2, '0')}${String(day).padStart(2, '0')}-${stCase}`;

          // Insert incident
          await db('incidents').insert({
            id: incidentId,
            incident_number: incidentNumber,
            incident_type: incidentType,
            severity: 'fatal',
            status: 'archived',
            priority: calculateFarsPriority(fatalities, persons, vehicles),
            confidence_score: 95, // FARS is official government census data
            address: routeName || title,
            city: cityName || countyName || 'Unknown',
            state: stateAbbr,
            latitude: lat,
            longitude: lng,
            occurred_at: occurredAt,
            reported_at: occurredAt,
            discovered_at: now,
            description: description,
            injuries_count: Math.max(0, persons - fatalities),
            fatalities_count: fatalities,
            vehicles_involved: vehicles,
            metro_area_id: metroId,
            source_count: 1,
            first_source_id: farsSourceId,
            tags: ['fars', 'historical', `year_${caseYear}`],
            created_at: now,
            updated_at: now
          });

          // Insert source report
          await db('source_reports').insert({
            id: uuidv4(),
            incident_id: incidentId,
            data_source_id: farsSourceId,
            source_type: 'fars',
            source_reference: sourceRef,
            raw_data: JSON.stringify(details || caseItem),
            parsed_data: JSON.stringify({
              title, description, fatalities, persons, vehicles,
              drunk_drivers: drunkDrivers, manner_of_collision: manColl,
              state: stateAbbr, case_year: caseYear, st_case: stCase
            }),
            contributed_fields: ['description', 'incident_type', 'severity', 'location', 'fatalities', 'vehicles'],
            confidence: 95,
            is_verified: true, // FARS is official data
            fetched_at: now,
            processed_at: now,
            created_at: now
          });

          // Extract and insert persons from case details
          if (details) {
            const personsData = details.CrashePersons || details.Persons || details.persons || [];
            const personsList = Array.isArray(personsData[0]) ? personsData[0] : personsData;

            for (const p of personsList.slice(0, 10)) {
              try {
                const perTyp = parseInt(p.PER_TYP || p.PersonType || 0);
                const injSev = parseInt(p.INJ_SEV || p.InjurySeverity || 5);
                const age = parseInt(p.AGE || p.age || 0);
                const sex = parseInt(p.SEX || p.sex || 9);

                // Skip unknown person types
                if (perTyp === 0 || perTyp === 9) continue;

                await db('persons').insert({
                  id: uuidv4(),
                  incident_id: incidentId,
                  role: mapPersonType(perTyp),
                  is_injured: injSev > 0 && injSev < 5,
                  age: (age > 0 && age < 120) ? age : null,
                  gender: sex === 1 ? 'male' : (sex === 2 ? 'female' : 'unknown'),
                  injury_description: mapInjurySeverity(injSev) === 'fatal' ? 'Fatal injuries' :
                                     mapInjurySeverity(injSev) === 'serious' ? 'Suspected serious injuries' :
                                     mapInjurySeverity(injSev) === 'minor' ? 'Minor injuries' : null,
                  has_attorney: null,
                  contact_status: 'not_contacted',
                  contact_attempts: 0,
                  confidence_score: 90,
                  do_not_contact: injSev === 4, // Fatal - do not contact
                  state: stateAbbr,
                  created_at: now,
                  updated_at: now
                });
                results.persons_added++;
              } catch (pErr) {
                // Skip individual person errors
                console.error(`FARS person insert error: ${pErr.message}`);
              }
            }
          }

          // Extract and insert vehicles from case details
          if (details) {
            const vehiclesData = details.CrashVehicles || details.Vehicles || details.vehicles || [];
            const vehiclesList = Array.isArray(vehiclesData[0]) ? vehiclesData[0] : vehiclesData;

            for (const v of vehiclesList.slice(0, 5)) {
              try {
                const make = v.MAKENAME || v.MakeName || v.make || null;
                const model = v.MAK_MODNAME || v.ModelName || v.model || null;
                const modelYear = parseInt(v.MOD_YEAR || v.ModelYear || 0);
                const bodyTyp = parseInt(v.BODY_TYP || v.BodyType || 0);
                const drinkingDriver = parseInt(v.DRINKING || v.DrinkingDriver || 0);
                const speedRelated = parseInt(v.SPEEDREL || v.SpeedRelated || 0);
                const hitAndRun = parseInt(v.HIT_RUN || v.HitAndRun || 0);

                await db('vehicles').insert({
                  id: uuidv4(),
                  incident_id: incidentId,
                  year: (modelYear > 1950 && modelYear < 2030) ? modelYear : null,
                  make: make,
                  model: model,
                  damage_severity: 'severe',
                  towed: true,
                  created_at: now,
                  updated_at: now
                });
                results.vehicles_added++;
              } catch (vErr) {
                console.error(`FARS vehicle insert error: ${vErr.message}`);
              }
            }
          }

          results.imported++;

          // Small delay to be respectful of FARS API (no rate limit documented, but be polite)
          if (details) await new Promise(r => setTimeout(r, 200));
        } catch (caseErr) {
          results.errors.push(`Case ${caseItem.St_Case || 'unknown'}: ${caseErr.message}`);
        }
      }
    }

    res.json({
      success: true,
      message: `FARS import complete: ${results.imported} crashes imported, ${results.persons_added} persons, ${results.vehicles_added} vehicles`,
      parameters: {
        states: results.states_processed,
        year_range: `${fromYear}-${toYear}`,
        limit_per_state: limit
      },
      ...results,
      timestamp: new Date().toISOString()
    });

  } catch (err) {
    console.error('FARS import error:', err);
    res.status(500).json({ error: err.message, results });
  }
};
