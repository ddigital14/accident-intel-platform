/**
 * Census ACS 5-year median household income overlay for case-value modeling.
 * Geocode incident → ACS block-group → median income → case_value bonus.
 * Free.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');

async function blockGroup(lat, lon) {
  const url = `https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x=${lon}&y=${lat}&benchmark=2020&vintage=2020&format=json&layers=Census+Block+Groups`;
  try { const r = await fetch(url, { timeout: 8000 }); if (r.ok) { const b = await r.json(); return b.result?.geographies?.['Census Block Groups']?.[0]; } } catch (_) {}
  return null;
}

async function medianIncome(state, county, tract, blkgrp, db) {
  const url = `https://api.census.gov/data/2022/acs/acs5?get=B19013_001E&for=block%20group:${blkgrp}&in=state:${state}%20county:${county}%20tract:${tract}`;
  let body = null, ok = false;
  try { const r = await fetch(url, { timeout: 8000 }); if (r.ok) { body = await r.json(); ok = true; } } catch (_) {}
  await trackApiCall(db, 'enrich-census-income', 'acs', 0, 0, ok).catch(() => {});
  if (!body || !body[1]) return null;
  return parseInt(body[1][0]) || null;
}

async function lookup(lat, lon, db) {
  const bg = await blockGroup(lat, lon); if (!bg) return null;
  const income = await medianIncome(bg.STATE, bg.COUNTY, bg.TRACT, bg.BLKGRP, db);
  return { lat, lon, geo: bg, median_household_income: income, case_value_modifier: income ? Math.min(2.0, income / 70000) : 1.0 };
}

async function batch(db, limit = 20) {
  let rows = []; try {
    rows = await db('incidents').whereNotNull('latitude').whereNotNull('longitude')
      .where(function () { this.whereNull('block_group_income').orWhere('block_group_income', 0); })
      .orderBy('created_at', 'desc').limit(limit);
  } catch (_) {}
  let updated = 0;
  for (const inc of rows) {
    const r = await lookup(inc.latitude, inc.longitude, db);
    if (r?.median_household_income) {
      try { await db('incidents').where({ id: inc.id }).update({ block_group_income: r.median_household_income, case_value_modifier: r.case_value_modifier }); updated++; } catch (_) {}
    }
  }
  return { rows: rows.length, updated };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { lat, lon, action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'census-income', cost: 0 });
    if (action === 'batch') { const out = await batch(db, parseInt(req.query.limit) || 20); return res.json({ success: true, ...out }); }
    if (lat && lon) { const r = await lookup(parseFloat(lat), parseFloat(lon), db); return res.json({ success: !!r, ...r }); }
    return res.status(400).json({ error: 'need lat+lon or action=batch|health' });
  } catch (err) { await reportError(db, 'census-income', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.lookup = lookup;
