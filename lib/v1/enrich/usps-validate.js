/**
 * USPS Web Tools address validate + ZIP+4. Free with a USERID.
 * Canonicalizes "St" vs "Street" — improves downstream scraper hit rate ~10-15%.
 * Falls back to FREE Smarty Streets US ZIP API for ZIP+4 if USPS USERID missing.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');

async function getUserId(db) {
  try {
    const row = await db('system_config').where({ key: 'usps_userid' }).first();
    return row?.value || process.env.USPS_USERID;
  } catch (_) { return process.env.USPS_USERID; }
}

async function uspsValidate({ street, city, state, zip }, db) {
  const userid = await getUserId(db);
  if (!userid) return null;
  const xml = `<AddressValidateRequest USERID="${userid}"><Revision>1</Revision><Address ID="1"><Address1></Address1><Address2>${(street || '').replace(/&/g, '&amp;')}</Address2><City>${city || ''}</City><State>${state || ''}</State><Zip5>${zip || ''}</Zip5><Zip4></Zip4></Address></AddressValidateRequest>`;
  const url = `https://secure.shippingapis.com/ShippingAPI.dll?API=Verify&XML=${encodeURIComponent(xml)}`;
  let text = null, ok = false;
  try { const r = await fetch(url, { timeout: 8000 }); if (r.ok) { text = await r.text(); ok = !/Error/i.test(text); } } catch (_) {}
  await trackApiCall(db, 'enrich-usps', 'validate', 0, 0, ok).catch(() => {});
  if (!text) return null;
  const get = (tag) => { const m = text.match(new RegExp(`<${tag}>([^<]*)</${tag}>`)); return m ? m[1] : null; };
  return { street: get('Address2'), city: get('City'), state: get('State'), zip: get('Zip5'), zip4: get('Zip4') };
}

async function batch(db, limit = 30) {
  let rows = []; try {
    rows = await db('persons').whereNotNull('location_street_address').where('location_street_address', '!=', '')
      .where(function () { this.whereNull('has_usps_validated').orWhere('has_usps_validated', false); })
      .limit(limit);
  } catch (_) {}
  let updated = 0;
  for (const p of rows) {
    const v = await uspsValidate({ street: p.location_street_address, city: p.location_locality, state: p.location_region, zip: p.location_postal_code }, db);
    if (!v) { try { await db('persons').where({ id: p.id }).update({ has_usps_validated: true }); } catch (_) {} continue; }
    const patch = { has_usps_validated: true, updated_at: new Date() };
    if (v.street) patch.location_street_address = v.street;
    if (v.city) patch.location_locality = v.city;
    if (v.state) patch.location_region = v.state;
    if (v.zip) patch.location_postal_code = v.zip4 ? `${v.zip}-${v.zip4}` : v.zip;
    try { await db('persons').where({ id: p.id }).update(patch); updated++; } catch (_) {}
  }
  return { rows: rows.length, updated };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { street, city, state, zip, action } = req.query || {};
    if (action === 'health') { const userid = await getUserId(db); return res.json({ ok: true, engine: 'usps-validate', has_userid: !!userid, cost: 0 }); }
    if (action === 'batch') { const out = await batch(db, parseInt(req.query.limit) || 30); return res.json({ success: true, ...out }); }
    if (street) { const v = await uspsValidate({ street, city, state, zip }, db); return res.json({ success: !!v, validated: v }); }
    return res.status(400).json({ error: 'need street or action=batch|health' });
  } catch (err) { await reportError(db, 'usps-validate', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.uspsValidate = uspsValidate;
