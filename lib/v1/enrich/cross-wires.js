/**
 * CROSS-WIRES - Beyond-competitor enrichment helpers.
 * Free, fast, run on every new incident.
 */
const { trackApiCall } = require('../system/cost');
const { timeOfDayBucket, ensureColumns: ensureCaseValueColumns } = require('../system/_case_value');

const OPENWEATHER_KEY = process.env.OPENWEATHER_API_KEY || process.env.OPENWEATHER_KEY;

async function weatherSnapshot(db, lat, lng, occurredAt) {
  if (!OPENWEATHER_KEY || !lat || !lng) return null;
  const ts = occurredAt ? Math.floor(new Date(occurredAt).getTime() / 1000) : null;
  let url;
  if (ts && ts < Math.floor(Date.now() / 1000) - 3600) {
    url = `https://api.openweathermap.org/data/3.0/onecall/timemachine?lat=${lat}&lon=${lng}&dt=${ts}&appid=${OPENWEATHER_KEY}&units=imperial`;
  } else {
    url = `https://api.openweathermap.org/data/2.5/weather?lat=${lat}&lon=${lng}&appid=${OPENWEATHER_KEY}&units=imperial`;
  }
  try {
    const r = await fetch(url, { signal: AbortSignal.timeout(8000) });
    await trackApiCall(db, 'enrich-cross-wires', 'openweather', 0, 0, r.ok).catch(()=>{});
    if (!r.ok) return null;
    const d = await r.json();
    const cur = d.current || d;
    const w = (cur.weather && cur.weather[0]) || {};
    return {
      conditions: w.main || null,
      description: w.description || null,
      temp_f: cur.temp ?? cur.main?.temp ?? null,
      humidity: cur.humidity ?? cur.main?.humidity ?? null,
      wind_mph: cur.wind_speed ?? cur.wind?.speed ?? null,
      visibility_m: cur.visibility ?? null,
      hazardous: /rain|snow|ice|fog|storm|thunder/i.test(w.main || w.description || ''),
      raw: { dt: cur.dt },
    };
  } catch (_) { return null; }
}

async function priorIncidentsAtLocation(db, lat, lng, withinM = 100, withinYears = 5) {
  if (!lat || !lng) return { count: 0 };
  try {
    const r = await db.raw(`
      SELECT COUNT(*)::int as cnt FROM incidents
      WHERE geom IS NOT NULL
        AND occurred_at > NOW() - INTERVAL '${withinYears} years'
        AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, $3)
    `, [lng, lat, withinM]);
    return { count: r.rows?.[0]?.cnt || 0 };
  } catch (_) { return { count: 0 }; }
}

async function vehicleRecallSummary(db, vin) {
  if (!vin || vin.length !== 17) return null;
  try {
    const r = await fetch(`https://api.nhtsa.gov/recalls/recallsByVin?vin=${vin}`, {
      signal: AbortSignal.timeout(6000),
    });
    await trackApiCall(db, 'enrich-cross-wires', 'nhtsa_vin', 0, 0, r.ok).catch(()=>{});
    if (!r.ok) return null;
    const d = await r.json();
    const items = d.results || [];
    return {
      count: items.length,
      summaries: items.slice(0, 5).map(x => x.Component || x.Summary || '').filter(Boolean),
    };
  } catch (_) { return null; }
}

async function applyAllToIncident(db, incident) {
  await ensureCaseValueColumns(db);
  const updates = { updated_at: new Date() };
  const out = {};

  const bucket = timeOfDayBucket(incident.occurred_at);
  if (bucket) { updates.time_of_day_bucket = bucket; out.time_of_day_bucket = bucket; }

  if (incident.latitude && incident.longitude) {
    const w = await weatherSnapshot(db, incident.latitude, incident.longitude, incident.occurred_at);
    if (w) {
      updates.weather_at_incident = JSON.stringify(w);
      if (w.conditions) updates.weather_conditions = String(w.conditions).slice(0, 100);
      out.weather = w;
    }
    const pr = await priorIncidentsAtLocation(db, incident.latitude, incident.longitude, 100, 5);
    updates.prior_incidents_at_location = pr.count;
    out.prior_incidents = pr.count;
  }

  try {
    const vehicles = await db('vehicles').where('incident_id', incident.id).select('vin').limit(5);
    let totalRecalls = 0;
    for (const v of vehicles) {
      if (!v.vin) continue;
      const s = await vehicleRecallSummary(db, v.vin);
      if (s) totalRecalls += s.count;
    }
    if (totalRecalls > 0) {
      updates.vehicle_recalls_count = totalRecalls;
      out.vehicle_recalls_count = totalRecalls;
    }
  } catch (_) { /* vehicles table may not exist */ }

  if (incident.police_department) {
    updates.first_responder_agency = String(incident.police_department).slice(0, 200);
    out.first_responder_agency = updates.first_responder_agency;
  }

  if (Object.keys(updates).length > 1) {
    await db('incidents').where('id', incident.id).update(updates).catch(() => {});
  }
  return out;
}

/**
 * Phase 21 Wire #6: Property-records owner ↔ victim last-name match.
 * If property owner has the same last name AS THE VICTIM and same address,
 * mark likely_family_residence=true on the person.
 */
async function checkLikelyFamilyResidence(db, person, incident) {
  if (!person?.last_name || !person?.address) return null;
  try {
    const pr = require('./property-records');
    if (!pr.lookupOwner) return null;
    const owner = await pr.lookupOwner({ address: person.address, city: person.city || incident?.city, state: person.state || incident?.state });
    const ownerName = owner?.matches?.[0]?.owner_name || owner?.owner_name;
    if (!ownerName) return null;
    const last = String(person.last_name).toLowerCase();
    if (ownerName.toLowerCase().includes(last)) {
      await db('persons').where('id', person.id).update({
        likely_family_residence: true, updated_at: new Date()
      }).catch(() => {});
      return { likely_family_residence: true, owner: ownerName };
    }
  } catch (_) {}
  return null;
}

/**
 * Phase 21 Wire #7: Voter rolls DOB validation.
 * If voter year_of_birth implies an age within ±2 of person.age → +5 to identity_confidence.
 * If mismatch by >5 years → flag identity_conflict.
 */
async function validateAgeAgainstVoter(db, person) {
  if (!person?.first_name || !person?.last_name || !person?.state || !person?.age) return null;
  try {
    const vr = require('./voter-rolls');
    if (!vr.lookupVoter) return null;
    const matches = await vr.lookupVoter(db, person.first_name, person.last_name, person.state);
    if (!matches?.length) return null;
    const yob = matches[0].year_of_birth || (matches[0].dob ? new Date(matches[0].dob).getFullYear() : null);
    if (!yob) return null;
    const computedAge = new Date().getFullYear() - yob;
    const diff = Math.abs(computedAge - person.age);
    const cur = person.identity_confidence || person.confidence_score || 50;
    if (diff <= 2) {
      await db('persons').where('id', person.id).update({
        identity_confidence: Math.min(99, cur + 5), updated_at: new Date()
      }).catch(()=>{});
      return { agreement: true, diff };
    } else if (diff > 5) {
      await db('persons').where('id', person.id).update({
        identity_conflict: true, updated_at: new Date()
      }).catch(()=>{});
      return { conflict: true, diff };
    }
  } catch (_) {}
  return null;
}

/**
 * Phase 21 Wire #8: Cross-source NAME validation (super-smart).
 * Count distinct sources where the same full_name appears (via enrichment_logs source).
 * 3+ sources agree → identity_confidence += 20. Persists to persons.identity_confidence.
 */
async function crossSourceNameValidation(db, person) {
  if (!person?.full_name || !person?.id) return null;
  try {
    const cols = await db.raw(`SELECT column_name FROM information_schema.columns WHERE table_name='enrichment_logs'`);
    const colSet = new Set((cols.rows || []).map(r => r.column_name));
    const srcExpr = colSet.has('source') && colSet.has('source_url')
      ? `COALESCE(source, source_url, 'enrichment')`
      : colSet.has('source_url') ? `COALESCE(source_url, 'enrichment')`
      : colSet.has('source') ? `COALESCE(source, 'enrichment')`
      : `'enrichment'`;
    const r = await db.raw(
      `SELECT DISTINCT ${srcExpr} as src FROM enrichment_logs
       WHERE person_id = ? AND field_name = 'full_name'
         AND LOWER(new_value) = LOWER(?)`,
      [person.id, person.full_name]
    );
    const sources = (r.rows || []).map(x => x.src).filter(Boolean);
    // Also count source_reports referencing this name on the incident
    if (person.incident_id) {
      const sr = await db.raw(`
        SELECT DISTINCT source_type FROM source_reports
        WHERE incident_id = ?
          AND parsed_data::text ILIKE ?
      `, [person.incident_id, `%${person.full_name}%`]).then(x => x.rows || []).catch(() => []);
      for (const row of sr) sources.push(row.source_type);
    }
    const distinct = new Set(sources.map(s => String(s).toLowerCase()));
    const cur = person.identity_confidence || person.confidence_score || 50;
    let next = cur;
    if (distinct.size >= 3) next = Math.min(99, cur + 20);
    else if (distinct.size === 2) next = Math.min(99, cur + 10);
    if (next !== cur) {
      await db('persons').where('id', person.id).update({
        identity_confidence: next, updated_at: new Date()
      }).catch(()=>{});
    }
    return { distinct_sources: distinct.size, identity_confidence: next, sources: [...distinct] };
  } catch (_) {
    return null;
  }
}

module.exports = {
  weatherSnapshot, priorIncidentsAtLocation, vehicleRecallSummary,
  applyAllToIncident, timeOfDayBucket,
  // Phase 21 wires:
  checkLikelyFamilyResidence,
  validateAgeAgainstVoter,
  crossSourceNameValidation,
};
