/**
 * County Property Records Lookup (scaffold)
 *
 * For premises liability cases (slip/fall, dog bite at residence), we need
 * to know who OWNS the property where the incident happened.
 *
 * County tax assessor records are public:
 *   - Search by address → owner name + mailing address
 *   - Search by owner name → all properties owned
 *
 * Many counties have web search forms that can be scraped via cheerio.
 * Some counties have public ArcGIS feature services that return JSON.
 *
 * Major counties with reasonable scrape paths:
 *   Harris County (Houston): hcad.org
 *   Cook County (Chicago): cookcountyassessor.com
 *   Los Angeles County: portal.assessor.lacounty.gov
 *   Maricopa County (Phoenix): mcassessor.maricopa.gov
 *   Fulton County (Atlanta): qpublic.net/ga/fulton
 *   Miami-Dade: miamidade.gov/Apps/PA/PAOnlineTools
 *
 * GET /api/v1/enrich/property-records?address=...&city=...&state=...
 *
 * Currently scaffolded for:
 *   - Free public APIs where available (ArcGIS feature services)
 *   - GPT-4o vision for OCR'd public scan PDFs (future)
 *
 * For paid alternatives, see: ATTOM Data, CoreLogic, PropertyShark.
 */
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const { reportError } = require('../system/_errors');

// Per-county lookup endpoints (working ones)
const COUNTY_ENDPOINTS = {
  // Harris County (Houston) — ArcGIS feature service
  'TX:Harris': {
    url: (addr) => `https://gis-web.hcad.org/server/rest/services/public/parcel/MapServer/0/query?where=SITE_ADDR%20LIKE%20'${encodeURIComponent('%' + addr.toUpperCase() + '%')}'&outFields=*&f=json&resultRecordCount=5`,
    parseField: (a) => ({ owner_name: a.OWNER, mailing_address: a.MAILING_ADDRESS, parcel_id: a.HCAD_NUM })
  },
  // Cook County, IL (Chicago)
  'IL:Cook': {
    url: (addr) => `https://gis.cookcountyil.gov/arcgis/rest/services/Parcels/MapServer/0/query?where=ADDRESS%20LIKE%20'${encodeURIComponent('%' + addr.toUpperCase() + '%')}'&outFields=*&f=json&resultRecordCount=5`,
    parseField: (a) => ({ owner_name: a.OWNER1, mailing_address: a.MAILING_ADDRESS, parcel_id: a.PIN })
  }
};

function getCountyKey(state, city) {
  // Naive city → county mapping; real-world should use a proper table
  const map = {
    'TX:Houston': 'TX:Harris',
    'TX:Pasadena': 'TX:Harris',
    'TX:Spring': 'TX:Harris',
    'IL:Chicago': 'IL:Cook',
    'IL:Cicero': 'IL:Cook',
  };
  return map[`${state}:${city}`] || null;
}

async function lookupOwner({ address, city, state }) {
  if (!address) return null;
  const key = getCountyKey(state, city);
  if (!key) return { error: 'no_endpoint_for_county', state, city };
  const endpoint = COUNTY_ENDPOINTS[key];
  try {
    const r = await fetch(endpoint.url(address), {
      headers: { 'Accept': 'application/json' },
      signal: AbortSignal.timeout(10000)
    });
    if (!r.ok) return { error: `HTTP ${r.status}` };
    const data = await r.json();
    const features = data.features || [];
    if (features.length === 0) return { matches: [] };
    return { matches: features.map(f => endpoint.parseField(f.attributes)) };
  } catch (e) {
    return { error: e.message };
  }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const { address, city, state } = req.query;
  if (!address) return res.status(400).json({ error: 'address required' });
  try {
    const result = await lookupOwner({ address, city, state });
    res.json({ success: !result?.error, ...result, timestamp: new Date().toISOString() });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

module.exports.lookupOwner = lookupOwner;
