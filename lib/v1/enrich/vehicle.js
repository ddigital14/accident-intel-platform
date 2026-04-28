/**
 * Vehicle Identification Helpers
 *
 * Extracts license plates, VINs, and vehicle descriptions from accident text
 * and runs free lookups (NHTSA VIN decoder) to identify the vehicle.
 *
 * Owner identification from plate is GENERALLY private (state DMV records).
 * We use it for:
 *   - Vehicle make/model/year confirmation via VIN decoder
 *   - Future hookup to paid plate-to-owner services (Bumper, NICB, CarFax)
 *
 * NHTSA Vehicle API (FREE, no key): https://vpic.nhtsa.dot.gov/api/
 */

const { getModelForTask } = require('../system/model-registry');
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

const NHTSA_BASE = 'https://vpic.nhtsa.dot.gov/api/vehicles';

// ───────── NHTSA VIN decode (FREE) ─────────
async function decodeVin(vin) {
  if (!vin || vin.length !== 17) return null;
  try {
    const r = await fetch(`${NHTSA_BASE}/DecodeVinValues/${vin}?format=json`, {
      signal: AbortSignal.timeout(8000)
    });
    if (!r.ok) return null;
    const data = await r.json();
    const result = data.Results?.[0];
    if (!result) return null;
    return {
      vin,
      make: result.Make || null,
      model: result.Model || null,
      year: result.ModelYear ? parseInt(result.ModelYear) : null,
      body_type: result.BodyClass || null,
      fuel_type: result.FuelTypePrimary || null,
      manufacturer: result.Manufacturer || null,
      plant_country: result.PlantCountry || null,
      vehicle_type: result.VehicleType || null,
      gross_weight: result.GVWR || null,
      // Commercial vehicle indicators for truck-accident classification
      is_commercial: /commercial|truck|bus|MEDIUM|HEAVY/i.test(result.BodyClass || result.VehicleType || '')
    };
  } catch (_) { return null; }
}

// ───────── Plate-to-state validation (no owner — private) ─────────
async function validatePlate(plate, state) {
  // No free public DMV API — paid services like CarFax/NICB/Bumper have these
  // For now, just normalize + flag for manual lookup
  return {
    plate: String(plate || '').toUpperCase().replace(/[^A-Z0-9]/g, ''),
    state: (state || '').toUpperCase().substring(0, 2),
    owner_lookup_provider: 'manual', // 'bumper'|'nicb'|'carfax' if integrated
    note: 'Plate-to-owner requires paid DMV service. Add BUMPER_API_KEY env var to enable.'
  };
}

// ───────── Extract vehicle details from accident text ─────────
async function extractVehiclesFromText(text) {
  if (!OPENAI_API_KEY || !text) return null;
  const truncated = String(text).substring(0, 3000);
  const prompt = `Extract vehicle details from this accident description. Return JSON only:

"""
${truncated}
"""

{
  "vehicles": [
    {
      "year": number|null,
      "make": "string|null",
      "model": "string|null",
      "color": "string|null",
      "body_type": "sedan|suv|truck|motorcycle|commercial|semi|bus|other|null",
      "license_plate": "raw text if mentioned|null",
      "license_state": "two-letter|null",
      "vin": "17-char VIN if mentioned|null",
      "is_commercial": true|false,
      "carrier_name": "company name if commercial truck|null",
      "dot_number": "USDOT number if commercial|null",
      "damage_severity": "totaled|severe|moderate|minor|none|null"
    }
  ]
}
Return empty vehicles[] if no vehicle details mentioned.`;

  try {
    const r = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: [
          { role: 'system', content: 'Extract vehicle details from accident reports. Return JSON only. License plates only when explicitly mentioned.' },
          { role: 'user', content: prompt }
        ],
        temperature: 0,
        response_format: { type: 'json_object' }
      }),
      signal: AbortSignal.timeout(15000)
    });
    if (!r.ok) return null;
    const data = await r.json();
    return JSON.parse(data.choices?.[0]?.message?.content || '{}');
  } catch (_) { return null; }
}

// ───────── Enrich a vehicle row with NHTSA + future plate lookup ─────────
async function enrichVehicleRow(vehicle) {
  const updates = {};
  if (vehicle.vin && vehicle.vin.length === 17) {
    const decoded = await decodeVin(vehicle.vin);
    if (decoded) {
      if (!vehicle.make && decoded.make) updates.make = decoded.make;
      if (!vehicle.model && decoded.model) updates.model = decoded.model;
      if (!vehicle.year && decoded.year) updates.year = decoded.year;
      if (!vehicle.body_type && decoded.body_type) updates.body_type = decoded.body_type;
      if (decoded.is_commercial && !vehicle.is_commercial) updates.is_commercial = true;
    }
  }
  return Object.keys(updates).length ? updates : null;
}

module.exports = {
  decodeVin,
  validatePlate,
  extractVehiclesFromText,
  enrichVehicleRow,
};
