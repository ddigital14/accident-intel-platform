/**
 * qpublic.schneidercorp.com fallback scraper for free GA/multi-state property records.
 * Used when paid env URLs aren't configured.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');

const APP_BY_STATE = {
  'GA-FULTON': 'FultonCountyGA', 'GA-DEKALB': 'DeKalbCountyGA', 'GA-COBB': 'CobbCountyGA',
  'GA-GWINNETT': 'GwinnettCountyGA', 'GA-CHATHAM': 'ChathamCountyGA',
  'TN-DAVIDSON': 'DavidsonCountyTN', 'NC-MECKLENBURG': 'MecklenburgCountyNC'
};

async function lookupOwner({ address, city, state, county }, db) {
  const key = `${state}-${(county || '').toUpperCase()}`;
  const app = APP_BY_STATE[key]; if (!app) return null;
  const url = `https://qpublic.schneidercorp.com/Application.aspx?App=${app}&Layer=Parcels&PageType=Search&KeyValue=${encodeURIComponent(address || '')}`;
  let html = null, ok = false;
  try { const r = await fetch(url, { timeout: 10000, headers: { 'User-Agent': 'Mozilla/5.0 AIP' } }); if (r.ok) { html = await r.text(); ok = true; } } catch (_) {}
  await trackApiCall(db, 'enrich-qpublic', app, 0, 0, ok).catch(() => {});
  if (!html) return null;
  const ownerMatch = html.match(/Owner\s*Name[^<]*<[^>]+>([^<]+)/i);
  const parcelMatch = html.match(/Parcel\s*ID[^<]*<[^>]+>([^<]+)/i);
  return { source: 'qpublic', owner_name: ownerMatch?.[1]?.trim(), parcel_id: parcelMatch?.[1]?.trim(), url };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { address, city, state, county, action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'qpublic-property', counties: Object.keys(APP_BY_STATE), cost: 0 });
    if (address) { const r = await lookupOwner({ address, city, state, county }, db); return res.json({ success: !!r, ...r }); }
    return res.status(400).json({ error: 'need address or action=health' });
  } catch (err) { await reportError(db, 'qpublic-property', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.lookupOwner = lookupOwner;
