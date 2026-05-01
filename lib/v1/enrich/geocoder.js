/**
 * Phase 67: Free Nominatim-based forward geocoder for `persons` rows.
 *
 * Turns a person's address (or the parent incident's lat/lon if missing) into
 * precise lat/lon coordinates stored DIRECTLY on the persons row. Unlocks the
 * census-income, neighborhood-density, and other geo-anchored engines that are
 * currently being skipped because persons have no lat/lon.
 *
 * Endpoints:
 *   GET /api/v1/enrich/geocoder?secret=ingest-now&action=health
 *   GET /api/v1/enrich/geocoder?secret=ingest-now&action=geocode&person_id=<uuid>
 *   GET /api/v1/enrich/geocoder?secret=ingest-now&action=batch&limit=N
 *
 * Free Nominatim policy:
 *   - 1 req/sec hard rate limit (OSM TOS)
 *   - User-Agent header REQUIRED
 *   - Endpoint: https://nominatim.openstreetmap.org/search?q=<addr>&format=json&limit=1&addressdetails=1
 *
 * Self-applying schema migration: adds `lat` (double precision) and `lon`
 * (double precision) columns to persons table on first call.
 *
 * Phase 67 schema notes — enrichment_logs minimal: only person_id, field_name,
 * old_value, new_value, created_at. ALL metadata folded into new_value JSON.
 */

const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');

let trackApiCall = async () => {};
try { trackApiCall = require('../system/cost-tracker').trackApiCall || trackApiCall; } catch (_) {}

const SECRET = 'ingest-now';
const NOMINATIM_URL = 'https://nominatim.openstreetmap.org/search';
const UA = 'AccidentCommandCenter/1.0 (mason@accidentcommandcenter.com)';
const PER_CALL_BUDGET_MS = 25000;
const RATE_LIMIT_MS = 1100; // 1 req/sec per Nominatim TOS

// ───────────────────────── self-applying migration ─────────────────────────
let _migrated = false;
async function applyMigration(db) {
  if (_migrated) return;
  try {
    await db.raw(`
      ALTER TABLE persons ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION;
      ALTER TABLE persons ADD COLUMN IF NOT EXISTS lon DOUBLE PRECISION;
      CREATE INDEX IF NOT EXISTS idx_persons_lat_lon ON persons (lat, lon)
        WHERE lat IS NOT NULL AND lon IS NOT NULL;
    `);
    _migrated = true;
  } catch (e) {
    // silent: log once for diagnostics, don't block
    console.error('[geocoder] migration apply failed:', e.message);
  }
}

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ───────────────────────── address composition ─────────────────────────
// Try in order:
//   1. enrichment_logs[field_name='usps_canonical'].new_value JSON (if exists)
//   2. raw address + city + state + zip concat
//   3. (caller falls back to incident lat/lon)
async function composeAddressString(db, person) {
  // 1. Look for USPS-canonicalized address
  try {
    const usps = await db('enrichment_logs')
      .where({ person_id: person.id })
      .whereIn('field_name', ['usps_canonical', 'usps_validated', 'address_canonical'])
      .orderBy('created_at', 'desc')
      .first();
    if (usps && usps.new_value) {
      let v = usps.new_value;
      if (typeof v === 'string') {
        try { v = JSON.parse(v); } catch (_) { /* maybe a plain string */ }
      }
      if (typeof v === 'object' && v) {
        const street = v.street || v.streetAddress || v.address1;
        const city = v.city || person.city;
        const state = v.state || person.state;
        const zip = v.zip || v.zip5 || v.ZIPCode || person.zip;
        if (street && (city || state)) {
          return [street, city, state, zip].filter(Boolean).join(', ');
        }
      } else if (typeof v === 'string' && v.length > 6) {
        return v;
      }
    }
  } catch (_) {}

  // 2. Raw concat
  const parts = [person.address, person.city, person.state, person.zip].filter(Boolean);
  if (parts.length >= 2) return parts.join(', ');
  if (person.address) return person.address;
  return null;
}

// ───────────────────────── Nominatim call ─────────────────────────
async function nominatimSearch(addrString, db) {
  const url = `${NOMINATIM_URL}?q=${encodeURIComponent(addrString)}&format=json&limit=1&addressdetails=1`;
  let ok = false, body = null;
  try {
    const r = await fetch(url, {
      headers: { 'User-Agent': UA, 'Accept': 'application/json' },
      timeout: 12000
    });
    if (r.ok) { body = await r.json(); ok = true; }
  } catch (e) {
    body = null;
  }
  await trackApiCall(db, 'enrich-geocoder', 'nominatim_search', 0, 0, ok).catch(() => {});
  if (!Array.isArray(body) || body.length === 0) return null;
  const top = body[0];
  const lat = parseFloat(top.lat);
  const lon = parseFloat(top.lon);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  return {
    lat, lon,
    display_name: top.display_name || null,
    importance: typeof top.importance === 'number' ? top.importance : null,
    source: 'nominatim'
  };
}

// ───────────────────────── per-person geocode ─────────────────────────
async function geocodeOne(db, personId, opts = {}) {
  await applyMigration(db);
  const startedAt = Date.now();

  const person = await db('persons').where({ id: personId }).first();
  if (!person) return { ok: false, error: 'person_not_found' };

  // Skip if already geocoded unless force
  if (!opts.force && person.lat != null && person.lon != null) {
    return { ok: true, skipped: 'already_geocoded', lat: person.lat, lon: person.lon };
  }

  const addrString = await composeAddressString(db, person);

  let result = null;
  let usedSource = null;

  if (addrString) {
    if (Date.now() - startedAt > PER_CALL_BUDGET_MS) {
      return { ok: false, error: 'budget_exceeded_pre_request' };
    }
    result = await nominatimSearch(addrString, db);
    if (result) usedSource = 'address';
  }

  // Fallback: parent incident lat/lon
  if (!result && person.incident_id) {
    try {
      const inc = await db('incidents').where({ id: person.incident_id }).first();
      if (inc && inc.latitude != null && inc.longitude != null) {
        const lat = parseFloat(inc.latitude);
        const lon = parseFloat(inc.longitude);
        if (Number.isFinite(lat) && Number.isFinite(lon)) {
          result = {
            lat, lon,
            display_name: 'incident_location_fallback',
            importance: null,
            source: 'incident_fallback'
          };
          usedSource = 'incident';
        }
      }
    } catch (_) {}
  }

  if (!result) {
    return { ok: false, error: 'no_geocode_match', addr_tried: addrString || null };
  }

  // Update persons.lat / persons.lon
  try {
    await db('persons').where({ id: personId }).update({
      lat: result.lat,
      lon: result.lon,
      updated_at: new Date()
    });
  } catch (e) {
    await reportError(db, 'enrich-geocoder', null, 'persons_update_failed: ' + e.message, { severity: 'error' }).catch(() => {});
    return { ok: false, error: 'update_failed:' + e.message };
  }

  // Cache in enrichment_logs (minimal schema)
  try {
    await db('enrichment_logs').insert({
      person_id: personId,
      field_name: 'geocode',
      old_value: null,
      new_value: JSON.stringify({
        lat: result.lat,
        lon: result.lon,
        display_name: result.display_name,
        importance: result.importance,
        source: result.source,
        used_source: usedSource,
        addr_string: addrString,
        ts: new Date().toISOString()
      }).slice(0, 4000),
      created_at: new Date()
    });
  } catch (_) {}

  return {
    ok: true,
    person_id: personId,
    lat: result.lat,
    lon: result.lon,
    display_name: result.display_name,
    importance: result.importance,
    source: result.source,
    used_source: usedSource,
    duration_ms: Date.now() - startedAt
  };
}

// ───────────────────────── batch mode ─────────────────────────
async function batchGeocode(db, { limit = 25 } = {}) {
  await applyMigration(db);
  const lim = Math.max(1, Math.min(parseInt(limit) || 25, 200));

  // Persons with address but null lat/lon
  let rows = [];
  try {
    rows = await db('persons')
      .whereNotNull('address').where('address', '!=', '')
      .whereNull('lat')
      .orderBy('updated_at', 'desc')
      .limit(lim)
      .select('id');
  } catch (_) { rows = []; }

  const out = { ok: true, candidates: rows.length, geocoded: 0, failed: 0, results: [] };
  for (let i = 0; i < rows.length; i++) {
    const id = rows[i].id;
    if (i > 0) await sleep(RATE_LIMIT_MS); // 1 req/s TOS
    try {
      const r = await geocodeOne(db, id);
      if (r.ok && !r.skipped) out.geocoded++;
      else if (!r.ok) out.failed++;
      out.results.push({ person_id: id, ...r });
    } catch (e) {
      out.failed++;
      out.results.push({ person_id: id, ok: false, error: e.message });
    }
  }
  return out;
}

// ───────────────────────── HTTP handler ─────────────────────────
async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const action = (req.query?.action || 'health').toLowerCase();
  let db;
  try { db = getDb(); } catch (e) {
    return res.status(500).json({ success: false, error: 'db_unavailable:' + e.message });
  }

  if (action === 'health') {
    return res.status(200).json({
      success: true,
      service: 'enrich/geocoder',
      provider: 'nominatim',
      rate_limit_ms: RATE_LIMIT_MS,
      ts: new Date().toISOString()
    });
  }

  if (action === 'geocode') {
    const personId = req.query?.person_id;
    if (!personId) return res.status(400).json({ error: 'person_id required' });
    const force = req.query?.force === 'true';
    try {
      const r = await geocodeOne(db, personId, { force });
      return res.status(200).json({ success: !!r.ok, ...r });
    } catch (e) {
      await reportError(db, 'enrich-geocoder', null, e.message, { severity: 'error' }).catch(() => {});
      return res.status(500).json({ success: false, error: e.message });
    }
  }

  if (action === 'batch') {
    const limit = parseInt(req.query?.limit) || 25;
    try {
      const r = await batchGeocode(db, { limit });
      return res.status(200).json({ success: true, ...r });
    } catch (e) {
      await reportError(db, 'enrich-geocoder', null, e.message, { severity: 'error' }).catch(() => {});
      return res.status(500).json({ success: false, error: e.message });
    }
  }

  return res.status(400).json({ error: 'unknown action: ' + action });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.geocodeOne = geocodeOne;
module.exports.batchGeocode = batchGeocode;
module.exports.applyMigration = applyMigration;
