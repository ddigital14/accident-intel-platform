/**
 * Copart / IAA salvage auction listings scraper.
 * Every totaled vehicle ends up on Copart or IAA with VIN, photos, lat/lng, date.
 * Cross-link by VIN (NHTSA we already have) → owner inference.
 * Free public listings.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

async function copartLookup(vin, db) {
  const url = `https://www.copart.com/public/data/lotdetails/solr/lotImages/${encodeURIComponent(vin)}/USA`;
  let body = null, ok = false;
  try { const r = await fetch(url, { timeout: 10000, headers: { 'User-Agent': 'Mozilla/5.0 AIP', 'Accept': 'application/json' } }); if (r.ok) { body = await r.json(); ok = true; } } catch (_) {}
  await trackApiCall(db, 'enrich-salvage', 'copart', 0, 0, ok).catch(() => {});
  return body;
}

async function iaaLookup(vin, db) {
  const url = `https://www.iaai.com/Search?VIN=${encodeURIComponent(vin)}`;
  let html = null, ok = false;
  try { const r = await fetch(url, { timeout: 10000, headers: { 'User-Agent': 'Mozilla/5.0 AIP' } }); if (r.ok) { html = await r.text(); ok = html.length > 500; } } catch (_) {}
  await trackApiCall(db, 'enrich-salvage', 'iaa', 0, 0, ok).catch(() => {});
  if (!html) return null;
  const stockMatch = html.match(/Stock\s*#?:?\s*(\d+)/i);
  const auctionMatch = html.match(/Auction Date[^<]*<[^>]+>([^<]+)/i);
  return { stock: stockMatch?.[1], auction: auctionMatch?.[1]?.trim() };
}

async function lookup(vin, db) {
  const [copart, iaa] = await Promise.all([copartLookup(vin, db), iaaLookup(vin, db)]);
  return { vin, copart: !!copart, copart_data: copart, iaa: !!iaa, iaa_data: iaa };
}

async function batch(db, limit = 15) {
  let rows = []; try {
    rows = await db('vehicles').whereNotNull('vin').where('vin', '!=', '')
      .where(function () { this.whereNull('has_salvage_searched').orWhere('has_salvage_searched', false); })
      .limit(limit);
  } catch (_) {}
  let found = 0;
  for (const v of rows) {
    const r = await lookup(v.vin, db);
    try {
      await db('vehicles').where({ id: v.id }).update({ has_salvage_searched: true, on_salvage_listing: !!(r.copart || r.iaa), updated_at: new Date() });
      if (r.copart || r.iaa) {
        await enqueueCascade(db, 'vehicle', v.id, 'salvage-listings', { weight: 70, copart: r.copart, iaa: r.iaa });
        found++;
      }
    } catch (_) {}
  }
  return { rows: rows.length, found };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { vin, action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'salvage-listings', sources: ['copart', 'iaa'], cost: 0 });
    if (action === 'batch') { const out = await batch(db, parseInt(req.query.limit) || 15); return res.json({ success: true, ...out }); }
    if (vin) { const r = await lookup(vin, db); return res.json({ success: true, ...r }); }
    return res.status(400).json({ error: 'need vin or action=batch|health' });
  } catch (err) { await reportError(db, 'salvage-listings', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.lookup = lookup;
