/**
 * Free reverse-geocode via OpenStreetMap Nominatim.
 * Wired BEFORE TomTom geocode in the smart router so we don't burn paid spend.
 * Endpoints: ?lat=&lon=  ?action=batch  ?action=health
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

const UA = 'AIP-AccidentIntel/1.0 (donovan@donovandigitalsolutions.com)';

async function reverseGeo(lat, lon, db) {
  const url = `https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat=${lat}&lon=${lon}&zoom=18&addressdetails=1`;
  let body = null, ok = false;
  try {
    const r = await fetch(url, { headers: { 'User-Agent': UA }, timeout: 8000 });
    if (r.ok) { body = await r.json(); ok = true; }
  } catch (_) {}
  await trackApiCall(db, 'enrich-nominatim', 'reverse', 0, 0, ok).catch(() => {});
  if (!body || !body.address) return null;
  const a = body.address;
  return {
    full_address: body.display_name,
    street: [a.house_number, a.road].filter(Boolean).join(' '),
    city: a.city || a.town || a.village || a.hamlet,
    state: a.state_code || a.state,
    postal_code: a.postcode,
    country: a.country_code,
    raw: body
  };
}

async function batch(db, limit = 20) {
  let rows = [];
  try {
    rows = await db('incidents')
      .whereNotNull('latitude').whereNotNull('longitude')
      .where(function () { this.whereNull('city').orWhere('city', '').orWhereNull('street').orWhere('street', ''); })
      .orderBy('created_at', 'desc').limit(limit);
  } catch (_) {}
  let updated = 0;
  for (const inc of rows) {
    const g = await reverseGeo(inc.latitude, inc.longitude, db);
    if (!g) continue;
    const patch = {};
    if (!inc.city && g.city) patch.city = g.city;
    if (!inc.state && g.state) patch.state = g.state;
    if (!inc.street && g.street) patch.street = g.street;
    if (!inc.postal_code && g.postal_code) patch.postal_code = g.postal_code;
    if (Object.keys(patch).length) {
      try { await db('incidents').where({ id: inc.id }).update(patch); updated++; } catch (_) {}
    }
    await new Promise(r => setTimeout(r, 1100)); // Nominatim usage policy: 1 req/s
  }
  return { rows: rows.length, updated };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  const { lat, lon, action } = req.query || {};
  try {
    if (action === 'health') return res.json({ ok: true, engine: 'nominatim-geocode', cost: 0, weight: 70 });
    if (action === 'batch') {
      const limit = parseInt(req.query.limit) || 20;
      const out = await batch(db, limit);
      return res.json({ success: true, ...out });
    }
    if (lat && lon) {
      const g = await reverseGeo(parseFloat(lat), parseFloat(lon), db);
      return res.json({ success: !!g, geocode: g });
    }
    return res.status(400).json({ error: 'need lat+lon or action=batch|health' });
  } catch (err) { await reportError(db, 'nominatim-geocode', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.reverseGeo = reverseGeo;
