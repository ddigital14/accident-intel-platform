/**
 * County Property Records Lookup — Phase 22 #3 expanded
 *
 * Premises liability cases (slip/fall, dog bite, fire) need property OWNER.
 * County tax-assessor records are public. Many publish ArcGIS feature
 * services (JSON) or simple HTML search forms.
 *
 * Currently supported:
 *   TX:Harris    (Houston)        — ArcGIS JSON
 *   IL:Cook      (Chicago)        — ArcGIS JSON
 *   GA:Fulton    (Atlanta)        — Fulton ArcGIS JSON
 *   FL:MiamiDade (Miami)          — public assessor search
 *   TX:Travis    (Austin)         — TravisCAD search
 *   AZ:Maricopa  (Phoenix)        — mcassessor.maricopa.gov public API
 *
 * GET /api/v1/enrich/property-records?address=&city=&state=
 *
 * Each county handler returns:
 *   { owner_name, parcel_id, assessed_value, year_built, mailing_address, classification }
 *
 * Tolerant of dead/changed URLs — graceful no-op so bulk runs don't blow up.
 */
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');

const FETCH_OPTS = {
  headers: { 'Accept': 'application/json', 'User-Agent': 'AIP/1.0 (research)' },
  signal: AbortSignal.timeout(25000)
};

// ── Per-county lookup endpoints ─────────────────────────────────────────
const COUNTY_ENDPOINTS = {
  // Harris County (Houston) — HCAD ArcGIS feature service
  'TX:Harris': {
    url: (addr) => `https://gis-web.hcad.org/server/rest/services/public/parcel/MapServer/0/query?where=SITE_ADDR%20LIKE%20'${encodeURIComponent('%' + addr.toUpperCase() + '%')}'&outFields=*&f=json&resultRecordCount=5`,
    parse: (a) => ({
      owner_name: a.OWNER || a.OWNER1,
      mailing_address: a.MAILING_ADDRESS,
      parcel_id: a.HCAD_NUM || a.PARCEL_ID,
      assessed_value: a.MARKET_VALUE || a.TOTAL_APPRAISED_VALUE,
      year_built: a.YEAR_BUILT,
      classification: a.SCHOOL_DIST ? 'residential' : 'unknown'
    }),
    type: 'arcgis'
  },

  // Cook County, IL (Chicago)
  'IL:Cook': {
    url: (addr) => `https://gis.cookcountyil.gov/arcgis/rest/services/Parcels/MapServer/0/query?where=ADDRESS%20LIKE%20'${encodeURIComponent('%' + addr.toUpperCase() + '%')}'&outFields=*&f=json&resultRecordCount=5`,
    parse: (a) => ({
      owner_name: a.OWNER1,
      mailing_address: a.MAILING_ADDRESS,
      parcel_id: a.PIN,
      assessed_value: a.MARKET_VALUE,
      year_built: a.YEAR_BUILT,
      classification: a.CLASS || 'unknown'
    }),
    type: 'arcgis'
  },

  // Fulton County, GA (Atlanta) — Fulton public Maps server
  // Default endpoint times out from Vercel egress IPs — gated until Mason
  // supplies a known-good URL via env FULTON_PARCEL_URL
  'GA:Fulton': {
    requires_env: 'FULTON_PARCEL_URL',
    url: (addr) => (process.env.FULTON_PARCEL_URL || `https://maps.fultoncountyga.gov/arcgis/rest/services/Tax/TaxParcel/MapServer/0/query`) +
                   `?where=SITUS_ADDR%20LIKE%20'${encodeURIComponent('%' + addr.toUpperCase() + '%')}'&outFields=*&f=json&resultRecordCount=5`,
    parse: (a) => ({
      owner_name: a.OWNER || a.OWNER_NAME,
      mailing_address: [a.MAIL_ADDR1, a.MAIL_CITY, a.MAIL_ST, a.MAIL_ZIP].filter(Boolean).join(', '),
      parcel_id: a.PARCEL_ID || a.PARCELID,
      assessed_value: a.APPR_VALUE || a.MARKET_VAL,
      year_built: a.YR_BLT,
      classification: a.CLASS || 'unknown'
    }),
    type: 'arcgis'
  },

  // Miami-Dade County, FL — Property Appraiser public ArcGIS feature service
  'FL:MiamiDade': {
    requires_env: 'MIAMIDADE_PARCEL_URL',
    url: (addr) => (process.env.MIAMIDADE_PARCEL_URL || `https://gisweb.miamidade.gov/arcgis/rest/services/Property/MD_Property_v1/MapServer/0/query`) +
                   `?where=TRUE_SITE_ADDR%20LIKE%20'${encodeURIComponent('%' + addr.toUpperCase() + '%')}'&outFields=*&f=json&resultRecordCount=5`,
    parse: (a) => ({
      owner_name: a.OWNER1 || a.OWNER || a.Owner1,
      mailing_address: [a.MAILING_ADDRESS, a.MAILING_CITY, a.MAILING_STATE, a.MAILING_ZIP].filter(Boolean).join(', '),
      parcel_id: a.FOLIO || a.FolioNumber,
      assessed_value: a.ASMNT_TOTAL || a.TotalAssessedValue,
      year_built: a.YEAR_BUILT || a.YearBuilt,
      classification: a.DOR_CODE || a.PrimaryUse || 'unknown'
    }),
    type: 'arcgis'
  },

  // Travis County, TX (Austin)
  'TX:Travis': {
    url: (addr) => `https://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services/Travis_Parcels/FeatureServer/0/query?where=SITUS_ADDRESS%20LIKE%20'${encodeURIComponent('%' + addr.toUpperCase() + '%')}'&outFields=*&f=json&resultRecordCount=5`,
    parse: (a) => ({
      owner_name: a.OWNER_NAME || a.OWNER,
      mailing_address: [a.MAIL_ADDR, a.MAIL_CITY, a.MAIL_STATE, a.MAIL_ZIP].filter(Boolean).join(', '),
      parcel_id: a.PROP_ID || a.PARCEL_ID,
      assessed_value: a.MARKET_VALUE || a.APPRAISED_VALUE,
      year_built: a.YEAR_BUILT,
      classification: a.LAND_USE || 'unknown'
    }),
    type: 'arcgis'
  },

  // Maricopa County, AZ (Phoenix) — official API needs MARICOPA_API_TOKEN env
  'AZ:Maricopa': {
    requires_token: 'MARICOPA_API_TOKEN',
    url: (addr) => `https://api.mcassessor.maricopa.gov/parcel/search/property?q=${encodeURIComponent(addr)}`,
    parse: (a) => ({
      owner_name: a.Ownership?.Owner || a.owner_name,
      mailing_address: a.MailingAddress?.Address1 || a.mailing_address,
      parcel_id: a.APN || a.parcel,
      assessed_value: a.AssessedValue || a.full_cash_value,
      year_built: a.YearBuilt || a.year_built,
      classification: a.PropertyType || a.land_use_description || 'unknown'
    }),
    type: 'maricopa',
    extract: (data) => {
      if (Array.isArray(data?.RealPropertyResults)) return data.RealPropertyResults;
      if (Array.isArray(data?.results)) return data.results;
      if (Array.isArray(data)) return data;
      return [];
    }
  }
};

// ── City → County mapping ───────────────────────────────────────────────
function getCountyKey(state, city) {
  if (!state || !city) return null;
  const norm = String(city).trim().toUpperCase();
  const map = {
    // Texas
    'TX:HOUSTON':    'TX:Harris',
    'TX:PASADENA':   'TX:Harris',
    'TX:SPRING':     'TX:Harris',
    'TX:KATY':       'TX:Harris',
    'TX:HUMBLE':     'TX:Harris',
    'TX:AUSTIN':     'TX:Travis',
    'TX:PFLUGERVILLE':'TX:Travis',
    'TX:MANOR':      'TX:Travis',
    // Illinois
    'IL:CHICAGO':    'IL:Cook',
    'IL:CICERO':     'IL:Cook',
    'IL:EVANSTON':   'IL:Cook',
    'IL:SKOKIE':     'IL:Cook',
    // Georgia
    'GA:ATLANTA':    'GA:Fulton',
    'GA:SANDY SPRINGS':'GA:Fulton',
    'GA:ROSWELL':    'GA:Fulton',
    'GA:JOHNS CREEK':'GA:Fulton',
    'GA:ALPHARETTA': 'GA:Fulton',
    'GA:MILTON':     'GA:Fulton',
    // Florida
    'FL:MIAMI':      'FL:MiamiDade',
    'FL:HIALEAH':    'FL:MiamiDade',
    'FL:MIAMI BEACH':'FL:MiamiDade',
    'FL:HOMESTEAD':  'FL:MiamiDade',
    'FL:CORAL GABLES':'FL:MiamiDade',
    'FL:NORTH MIAMI':'FL:MiamiDade',
    // Arizona
    'AZ:PHOENIX':    'AZ:Maricopa',
    'AZ:MESA':       'AZ:Maricopa',
    'AZ:CHANDLER':   'AZ:Maricopa',
    'AZ:GLENDALE':   'AZ:Maricopa',
    'AZ:SCOTTSDALE': 'AZ:Maricopa',
    'AZ:TEMPE':      'AZ:Maricopa',
    'AZ:GILBERT':    'AZ:Maricopa',
    'AZ:SURPRISE':   'AZ:Maricopa',
  };
  return map[`${state}:${norm}`] || null;
}

async function lookupOwner({ address, city, state }, db) {
  if (!address) return { error: 'address_required' };
  const key = getCountyKey(state, city);
  if (!key) return { error: 'no_endpoint_for_county', state, city };
  const ep = COUNTY_ENDPOINTS[key];
  if (!ep) return { error: 'no_endpoint_configured', county: key };

  // Phase 36: resolve token from system_config (DB) first, fall back to env
  let resolvedToken = null;
  if (ep.requires_token) {
    resolvedToken = process.env[ep.requires_token];
    if (!resolvedToken) {
      try {
        const { getDb } = require('../../_db');
        const _db = db || getDb();
        const tokKey = ep.requires_token.toLowerCase();
        const row = await _db('system_config').where({ key: tokKey }).first();
        if (row?.value) resolvedToken = typeof row.value === 'string' ? row.value.replace(/^"|"$/g,'') : row.value;
      } catch (_) {}
    }
  }
  // Honor requires_token (e.g. Maricopa) + requires_env (Fulton, MiamiDade) —
  // graceful no-op if env unset rather than blowing time budget on a 12s timeout.
  if (ep.requires_token && !resolvedToken) {
    return { error: `requires_${ep.requires_token}`, county: key, deferred: true };
  }
  if (ep.requires_env && !process.env[ep.requires_env]) {
    return { error: `requires_${ep.requires_env}`, county: key, deferred: true };
  }
  try {
    const opts = { ...FETCH_OPTS };
    if (ep.requires_token && resolvedToken) {
      opts.headers = { ...opts.headers, 'AUTHORIZATION': `Bearer ${resolvedToken}` };
    }
    const r = await fetch(ep.url(address), opts);
    if (db) await trackApiCall(db, 'property-records', `county_${key.replace(':','_')}`, 0, 0, r.ok);
    if (!r.ok) return { error: `HTTP ${r.status}`, county: key };
    let data;
    try { data = await r.json(); } catch (_) { return { error: 'non_json_response', county: key }; }

    // ArcGIS shape: { features: [{ attributes: {...} }] }
    if (ep.type === 'arcgis') {
      const feats = data.features || [];
      if (!feats.length) return { matches: [], county: key };
      return { matches: feats.map(f => ep.parse(f.attributes || {})), county: key };
    }
    // generic JSON array shape via custom extract
    const arr = ep.extract ? ep.extract(data) : (Array.isArray(data) ? data : []);
    if (!arr.length) return { matches: [], county: key };
    return { matches: arr.slice(0, 5).map(a => ep.parse(a)), county: key };
  } catch (e) {
    if (db) await trackApiCall(db, 'property-records', `county_${key.replace(':','_')}`, 0, 0, false).catch(()=>{});
    return { error: e.message, county: key };
  }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const { address, city, state } = req.query;
  if (!address) {
    return res.status(400).json({
      error: 'address required',
      supported_counties: Object.keys(COUNTY_ENDPOINTS),
      city_county_map_size: 'see source'
    });
  }
  try {
    const { getDb } = require('../../_db');
    const db = getDb();
    const result = await lookupOwner({ address, city, state }, db);
    res.json({ success: !result?.error, ...result, timestamp: new Date().toISOString() });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

module.exports.lookupOwner = lookupOwner;
module.exports.getCountyKey = getCountyKey;
module.exports.COUNTY_ENDPOINTS = COUNTY_ENDPOINTS;
