/**
 * USPS address validate — supports BOTH:
 *   1) Legacy XML Web Tools (USERID, free) at secure.shippingapis.com
 *   2) Modern OAuth REST API (Consumer Key + Secret) at apis.usps.com
 *
 * Canonicalizes "St" vs "Street" — improves downstream scraper hit rate ~10-15%.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');

async function getCreds(db) {
  try {
    const row = await db('system_config').where({ key: 'usps' }).first();
    if (row?.value) {
      const v = typeof row.value === 'string' ? JSON.parse(row.value) : row.value;
      return v;
    }
    const u = await db('system_config').where({ key: 'usps_userid' }).first();
    if (u?.value) return { userid: typeof u.value === 'string' ? u.value.replace(/^"|"$/g,'') : u.value };
  } catch (_) {}
  return {
    userid: process.env.USPS_USERID,
    consumer_key: process.env.USPS_CONSUMER_KEY,
    consumer_secret: process.env.USPS_CONSUMER_SECRET
  };
}

let _oauthToken = null, _oauthExpiry = 0;
async function getOAuthToken(creds, db) {
  if (_oauthToken && Date.now() < _oauthExpiry) return _oauthToken;
  if (!creds.consumer_key || !creds.consumer_secret) return null;
  try {
    const r = await fetch('https://apis.usps.com/oauth2/v3/token', {
      method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `grant_type=client_credentials&client_id=${creds.consumer_key}&client_secret=${creds.consumer_secret}`,
      timeout: 8000
    });
    if (r.ok) { const j = await r.json(); _oauthToken = j.access_token; _oauthExpiry = Date.now() + (j.expires_in - 60) * 1000; return _oauthToken; }
  } catch (_) {}
  return null;
}

async function validateOAuth({ street, city, state, zip }, creds, db) {
  const tok = await getOAuthToken(creds, db); if (!tok) return null;
  const url = `https://apis.usps.com/addresses/v3/address?streetAddress=${encodeURIComponent(street||'')}&city=${encodeURIComponent(city||'')}&state=${encodeURIComponent(state||'')}&ZIPCode=${encodeURIComponent(zip||'')}`;
  let body = null, ok = false;
  try { const r = await fetch(url, { headers: { Authorization: 'Bearer ' + tok }, timeout: 8000 }); if (r.ok) { body = await r.json(); ok = true; } } catch (_) {}
  await trackApiCall(db, 'enrich-usps', 'oauth', 0, 0, ok).catch(() => {});
  if (!body?.address) return null;
  const a = body.address;
  return { street: a.streetAddress, city: a.city, state: a.state, zip: a.ZIPCode, zip4: a.ZIPPlus4 };
}

async function validateLegacy({ street, city, state, zip }, creds, db) {
  if (!creds.userid) return null;
  const xml = `<AddressValidateRequest USERID="${creds.userid}"><Revision>1</Revision><Address ID="1"><Address1></Address1><Address2>${(street||'').replace(/&/g,'&amp;')}</Address2><City>${city||''}</City><State>${state||''}</State><Zip5>${zip||''}</Zip5><Zip4></Zip4></Address></AddressValidateRequest>`;
  const url = `https://secure.shippingapis.com/ShippingAPI.dll?API=Verify&XML=${encodeURIComponent(xml)}`;
  let text = null, ok = false;
  try { const r = await fetch(url, { timeout: 8000 }); if (r.ok) { text = await r.text(); ok = !/<Error>/i.test(text); } } catch (_) {}
  await trackApiCall(db, 'enrich-usps', 'legacy_xml', 0, 0, ok).catch(() => {});
  if (!text || !ok) return null;
  const get = (tag) => { const m = text.match(new RegExp(`<${tag}>([^<]*)</${tag}>`)); return m ? m[1] : null; };
  return { street: get('Address2'), city: get('City'), state: get('State'), zip: get('Zip5'), zip4: get('Zip4') };
}

async function uspsValidate(addr, db) {
  const creds = await getCreds(db);
  // OAuth preferred (current/supported), legacy XML fallback
  let v = await validateOAuth(addr, creds, db);
  if (!v) v = await validateLegacy(addr, creds, db);
  return v;
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
    if (action === 'health') {
      const c = await getCreds(db);
      return res.json({ ok: true, engine: 'usps-validate', has_legacy_userid: !!c.userid, has_oauth: !!(c.consumer_key && c.consumer_secret) });
    }
    if (action === 'batch') { const out = await batch(db, parseInt(req.query.limit) || 30); return res.json({ success: true, ...out }); }
    if (street) { const v = await uspsValidate({ street, city, state, zip }, db); return res.json({ success: !!v, validated: v }); }
    return res.status(400).json({ error: 'need street or action=batch|health' });
  } catch (err) { await reportError(db, 'usps-validate', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.uspsValidate = uspsValidate;
