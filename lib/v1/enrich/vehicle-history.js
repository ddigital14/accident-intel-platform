/**
 * VEHICLE HISTORY engine — VIN → NHTSA full decode + recalls + complaints + safety
 *
 * For incidents with a known VIN, fetches in parallel:
 *   - vPIC DecodeVinValues       (year/make/model/body — via lib/v1/enrich/vehicle.js)
 *   - NHTSA recalls              (active recalls for year/make/model)
 *   - NHTSA complaints           (consumer complaint count — defect signal)
 *   - NHTSA Safety Ratings (NCAP)(crash test star ratings)
 *
 * Combines into vehicle_safety_score 0-100 — defective vehicles strengthen the
 * product-liability angle on top of the standard MVA case.
 *
 * Cascade: when called with person_id (vehicle attributed to victim), emits
 * cascade so cross-exam treats NHTSA data at weight 95 (govt/manufacturer).
 *
 * Endpoints:
 *   GET /api/v1/enrich/vehicle-history?vin=<vin>
 *   GET /api/v1/enrich/vehicle-history?action=process&limit=20  (cron)
 *   GET /api/v1/enrich/vehicle-history?action=health
 *
 * Cost: $0 (NHTSA all free). Cross-exam weight: 95.
 */
const { v4: uuidv4 } = require('uuid');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { logChange } = require('../system/changelog');
const { enqueueCascade } = require('../system/_cascade');
const { decodeVin } = require('./vehicle');

const NHTSA_API = 'https://api.nhtsa.gov';

let _ensured = false;
async function ensureColumns(db) {
  if (_ensured) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS vehicles (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        incident_id UUID,
        vin VARCHAR(17),
        year INTEGER, make VARCHAR(80), model VARCHAR(120), body_type VARCHAR(80),
        is_commercial BOOLEAN DEFAULT FALSE,
        recall_count INTEGER DEFAULT 0,
        complaint_count INTEGER DEFAULT 0,
        ncap_overall_rating NUMERIC(3,1),
        vehicle_safety_score INTEGER,
        nhtsa_data JSONB DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_vehicles_incident ON vehicles(incident_id);
      CREATE INDEX IF NOT EXISTS idx_vehicles_vin ON vehicles(vin);
    `);
    _ensured = true;
  } catch (e) { /* non-fatal */ }
}

async function nhtsaFetch(db, url) {
  try {
    const resp = await fetch(url, { signal: AbortSignal.timeout(8000) });
    await trackApiCall(db, 'enrich-vehicle-history', 'nhtsa_vin', 0, 0, resp.ok);
    if (!resp.ok) return null;
    return await resp.json();
  } catch (e) {
    await trackApiCall(db, 'enrich-vehicle-history', 'nhtsa_vin', 0, 0, false);
    return null;
  }
}

async function fetchRecalls(db, year, make, model) {
  const data = await nhtsaFetch(db, `${NHTSA_API}/recalls/recallsByVehicle?make=${encodeURIComponent(make)}&model=${encodeURIComponent(model)}&modelYear=${year}`);
  return data?.results || [];
}

async function fetchComplaints(db, year, make, model) {
  const data = await nhtsaFetch(db, `${NHTSA_API}/complaints/complaintsByVehicle?make=${encodeURIComponent(make)}&model=${encodeURIComponent(model)}&modelYear=${year}`);
  return data?.results || [];
}

async function fetchSafetyRatings(db, year, make, model) {
  const idx = await nhtsaFetch(db, `${NHTSA_API}/SafetyRatings/modelyear/${year}/make/${encodeURIComponent(make)}/model/${encodeURIComponent(model)}?format=json`);
  const first = idx?.Results?.[0];
  if (!first?.VehicleId) return null;
  const det = await nhtsaFetch(db, `${NHTSA_API}/SafetyRatings/VehicleId/${first.VehicleId}?format=json`);
  return det?.Results?.[0] || null;
}

/**
 * Heuristic safety score: start 80, -4 per recall (cap -40), -1 per 5
 * complaints (cap -20), +(NCAP*4 - 12) so 5-star = +8, 1-star = -8.
 */
function computeSafetyScore({ recall_count = 0, complaint_count = 0, ncap_overall_rating }) {
  let s = 80;
  s -= Math.min(40, recall_count * 4);
  s -= Math.min(20, Math.floor(complaint_count / 5));
  if (ncap_overall_rating) s += Math.round(ncap_overall_rating * 4 - 12);
  return Math.max(0, Math.min(100, s));
}

async function lookupVin(db, vin, opts = {}) {
  await ensureColumns(db);
  if (!vin || vin.length !== 17) return { ok: false, error: 'invalid_vin' };

  const decoded = await decodeVin(vin);
  if (!decoded?.year || !decoded?.make || !decoded?.model) {
    return { ok: false, error: 'vin_decode_failed', decoded };
  }

  const [recalls, complaints, ratings] = await Promise.all([
    fetchRecalls(db, decoded.year, decoded.make, decoded.model),
    fetchComplaints(db, decoded.year, decoded.make, decoded.model),
    fetchSafetyRatings(db, decoded.year, decoded.make, decoded.model)
  ]);

  const ncap = ratings?.OverallRating ? parseFloat(ratings.OverallRating) : null;
  const score = computeSafetyScore({
    recall_count: recalls.length,
    complaint_count: complaints.length,
    ncap_overall_rating: ncap
  });

  const row = {
    incident_id: opts.incident_id || null,
    vin, year: decoded.year, make: decoded.make, model: decoded.model,
    body_type: decoded.body_type || null,
    is_commercial: !!decoded.is_commercial,
    recall_count: recalls.length,
    complaint_count: complaints.length,
    ncap_overall_rating: ncap,
    vehicle_safety_score: score,
    nhtsa_data: JSON.stringify({
      decoded,
      recalls: recalls.slice(0, 10),
      complaints_sample: complaints.slice(0, 5),
      safety_ratings: ratings || null
    }),
    updated_at: new Date()
  };

  let stored = null;
  try {
    const existing = await db('vehicles').where('vin', vin).first();
    if (existing) {
      await db('vehicles').where('id', existing.id).update(row);
      stored = { ...existing, ...row };
    } else {
      stored = { id: uuidv4(), created_at: new Date(), ...row };
      await db('vehicles').insert(stored);
    }
  } catch (e) {
    await reportError(db, 'enrich-vehicle-history', vin, e.message);
  }

  // Cascade only when vehicle is attributed to a specific person
  if (opts.person_id) {
    await enqueueCascade(db, {
      person_id: opts.person_id,
      incident_id: opts.incident_id,
      trigger_source: 'nhtsa_vin',
      trigger_field: 'vin',
      trigger_value: vin,
      priority: 4
    }).catch(() => {});
  }

  // Phase 21 Wire #1: VIN defects → severity boost +5 on incident lead_score
  // (vehicle defect strengthens product-liability angle)
  if (opts.incident_id && recalls.length > 0) {
    try {
      const inc = await db('incidents').where('id', opts.incident_id).first();
      if (inc) {
        const boost = Math.min(15, recalls.length >= 3 ? 10 : 5);
        const newScore = Math.min(100, (inc.lead_score || 0) + boost);
        if (newScore > (inc.lead_score || 0)) {
          await db('incidents').where('id', opts.incident_id).update({
            lead_score: newScore,
            vehicle_recalls_count: recalls.length,
            updated_at: new Date()
          }).catch(() => {});
          // Trigger incident-level cascade — defect changes case value
          await enqueueCascade(db, {
            incident_id: opts.incident_id,
            trigger_source: 'vehicle_recall_boost',
            trigger_field: 'lead_score',
            trigger_value: `${inc.lead_score}->${newScore}`,
            priority: 6
          }).catch(() => {});
        }
      }
    } catch (_) { /* non-fatal */ }
  }

  return {
    ok: true,
    vin, year: decoded.year, make: decoded.make, model: decoded.model,
    is_commercial: !!decoded.is_commercial,
    recall_count: recalls.length,
    complaint_count: complaints.length,
    ncap_overall_rating: ncap,
    vehicle_safety_score: score,
    sample_recalls: recalls.slice(0, 3),
    vehicle_id: stored?.id
  };
}

async function processBatch(db, limit = 20) {
  await ensureColumns(db);
  const startTime = Date.now();
  const stats = { evaluated: 0, enriched: 0, errors: [] };

  const rows = await db.raw(`
    SELECT sr.incident_id, sr.parsed_data
    FROM source_reports sr
    LEFT JOIN vehicles v ON v.incident_id = sr.incident_id
    WHERE v.id IS NULL
      AND sr.created_at > NOW() - INTERVAL '14 days'
      AND sr.parsed_data::text ~* '"vin"\s*:\s*"[A-HJ-NPR-Z0-9]{17}"'
    ORDER BY sr.created_at DESC
    LIMIT ?
  `, [limit]).then(r => r.rows || []).catch(() => []);

  for (const r of rows) {
    if (Date.now() - startTime > 45000) break;
    stats.evaluated++;
    try {
      const parsed = typeof r.parsed_data === 'string' ? JSON.parse(r.parsed_data) : r.parsed_data;
      for (const v of (parsed?.vehicles || [])) {
        if (!v.vin || v.vin.length !== 17) continue;
        const result = await lookupVin(db, v.vin, { incident_id: r.incident_id });
        if (result.ok) stats.enriched++;
      }
    } catch (e) {
      stats.errors.push(`${r.incident_id}: ${e.message}`);
      await reportError(db, 'enrich-vehicle-history', r.incident_id, e.message);
    }
  }
  return { ok: true, ...stats };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const db = getDb();
  const action = req.query.action || (req.query.vin ? 'lookup' : 'health');
  try {
    if (action === 'health') {
      return res.status(200).json({ ok: true, engine: 'vehicle-history', cost: 0, weight: 95, endpoints: ['recalls', 'complaints', 'safety', 'vin_decode'] });
    }
    if (action === 'lookup' || req.query.vin) {
      const vin = req.query.vin;
      if (!vin) return res.status(400).json({ error: 'vin required' });
      const result = await lookupVin(db, vin, { incident_id: req.query.incident_id || null, person_id: req.query.person_id || null });
      return res.status(200).json(result);
    }
    if (action === 'process') {
      const limit = parseInt(req.query.limit) || 20;
      const result = await processBatch(db, limit);
      if (result.enriched > 0) {
        try { await logChange(db, { kind: 'pipeline', title: `vehicle-history: +${result.enriched} VINs`, summary: `evaluated=${result.evaluated}`, ref: 'vehicle-history' }); } catch (_) {}
      }
      return res.status(200).json({
        success: true,
        message: `vehicle-history: enriched ${result.enriched}/${result.evaluated} VINs`,
        ...result,
        timestamp: new Date().toISOString()
      });
    }
    return res.status(400).json({ error: 'unknown action', valid: ['health', 'lookup', 'process'] });
  } catch (e) {
    await reportError(db, 'enrich-vehicle-history', null, e.message);
    return res.status(500).json({ error: e.message });
  }
};

module.exports.lookupVin = lookupVin;
module.exports.processBatch = processBatch;
module.exports.computeSafetyScore = computeSafetyScore;
