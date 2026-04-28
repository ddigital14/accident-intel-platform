/**
 * Phase 37 Wave B - Property -> Family
 *
 * For accident victims, search Maricopa (and other) county property records
 * for parcels where the victim's LAST NAME appears in owner. Owners with the
 * same last name in same/nearby city are likely family/relatives. Attach as
 * `relative_candidate` evidence with weight 35 + emit cascade.
 *
 * GET /api/v1/enrich/property-to-family?secret=ingest-now&action=batch&limit=N
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');
const { enqueueCascade } = require('../system/_cascade');

const FETCH_OPTS = (extraHeaders = {}) => ({
  headers: { 'Accept': 'application/json', 'User-Agent': 'AIP/1.0 (research)', ...extraHeaders },
  signal: AbortSignal.timeout(12000)
});

const CITY_TO_STATE = {
  'phoenix': 'AZ', 'mesa': 'AZ', 'tempe': 'AZ', 'scottsdale': 'AZ', 'chandler': 'AZ',
  'glendale': 'AZ', 'peoria': 'AZ', 'gilbert': 'AZ', 'surprise': 'AZ',
  'houston': 'TX', 'austin': 'TX',
  'chicago': 'IL', 'atlanta': 'GA', 'miami': 'FL'
};

function splitName(full) {
  const parts = String(full || '').trim().split(/\s+/).filter(Boolean);
  return { first: parts[0] || '', last: parts[parts.length - 1] || '', middle: parts.length > 2 ? parts.slice(1, -1).join(' ') : '' };
}

async function maricopaSearchByOwner(lastName, token) {
  const url = `https://mcassessor.maricopa.gov/parcel/search?q=${encodeURIComponent(lastName)}`;
  try {
    const resp = await fetch(url, FETCH_OPTS({ 'AUTHORIZATION': token, 'User-Agent': '' }));
    if (!resp.ok) return { ok: false, status: resp.status };
    const data = await resp.json().catch(() => null);
    if (!data) return { ok: false, status: 0 };
    const arr = Array.isArray(data && data.Results) ? data.Results
              : Array.isArray(data && data.results) ? data.results
              : Array.isArray(data) ? data
              : [];
    return { ok: true, parcels: arr };
  } catch (e) { return { ok: false, err: e.message }; }
}

function ownerLooksLikeRelative(ownerName, victimLast, victimFirst) {
  if (!ownerName || !victimLast) return false;
  const ownerLow = String(ownerName).toLowerCase();
  const lastLow = String(victimLast).toLowerCase();
  const firstLow = String(victimFirst || '').toLowerCase();
  if (!ownerLow.includes(lastLow)) return false;
  if (firstLow && firstLow.length >= 3 && ownerLow.includes(firstLow)) return false;
  return true;
}

async function enrichOne(db, victim, results) {
  const { first, last } = splitName(victim.full_name);
  if (!last || last.length < 3) return false;
  let token = process.env.MARICOPA_API_TOKEN;
  if (!token) {
    try {
      const row = await db('system_config').where({ key: 'maricopa_api_token' }).first();
      if (row && row.value) token = typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
    } catch (_) {}
  }
  const city = (victim.city || victim.incident_city || '').toLowerCase();
  const guessedState = CITY_TO_STATE[city] || victim.state || victim.incident_state;
  let parcels = [];
  if (guessedState === 'AZ' && token) {
    const r = await maricopaSearchByOwner(last, token);
    await trackApiCall(db, 'property-to-family', 'maricopa_owner_search', 0, 0, r.ok).catch(() => {});
    if (r.ok) parcels = r.parcels;
  }
  results.searched++;
  if (!parcels.length) return false;
  const relatives = [];
  for (const parcel of parcels.slice(0, 20)) {
    const owner = parcel.Ownership || parcel.ownership || parcel.OwnerName || parcel.owner || parcel.OWNER;
    if (!ownerLooksLikeRelative(owner, last, first)) continue;
    const addr = parcel.PropertyAddress || parcel.SitusAddress || parcel.PhysicalAddress
              || parcel.situs_address || parcel.address || null;
    relatives.push({
      owner_name: String(owner).trim(),
      property_address: addr,
      parcel_id: parcel.APN || parcel.apn || parcel.parcel_id || parcel.ParcelNumber || null,
      assessed_value: parcel.AssessedValue || parcel.assessed_value || null
    });
    if (relatives.length >= 5) break;
  }
  if (!relatives.length) return false;
  results.matches++;
  let merged = { relative_candidates: relatives };
  try {
    const existing = await db('persons').where('id', victim.id).first('enrichment_data');
    if (existing && existing.enrichment_data) {
      const prev = typeof existing.enrichment_data === 'string' ? JSON.parse(existing.enrichment_data) : existing.enrichment_data;
      merged = { ...(prev || {}), relative_candidates: relatives };
    }
  } catch (_) {}
  await db('persons').where('id', victim.id).update({ enrichment_data: JSON.stringify(merged), updated_at: new Date() });
  for (const rel of relatives) {
    await db('enrichment_logs').insert({
      person_id: victim.id,
      field_name: 'relative_candidate',
      old_value: null,
      new_value: JSON.stringify(rel),
      source_url: 'https://mcassessor.maricopa.gov/parcel/search',
      source: 'property-to-family',
      confidence: 35,
      verified: false,
      created_at: new Date()
    }).catch(() => {});
  }
  results.evidence_rows += relatives.length;
  await enqueueCascade(db, { person_id: victim.id, trigger_source: 'property-to-family', trigger_field: 'relative_candidates', trigger_value: String(relatives.length), weight: 35 }).catch(() => {});
  return true;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  const db = getDb();
  const limit = Math.min(Number(req.query.limit) || 10, 25);
  const results = { candidates: 0, searched: 0, matches: 0, evidence_rows: 0, errors: [] };
  const start = Date.now();
  try {
    const candidates = await db('persons as p')
      .leftJoin('incidents as i', 'p.incident_id', 'i.id')
      .whereNotNull('p.full_name')
      .where(function () { this.whereIn('p.qualification_state', ['pending', 'pending_named', 'qualified']); })
      .where('p.created_at', '>', new Date(Date.now() - 60 * 86400000))
      .select('p.id', 'p.full_name', 'p.city', 'p.state', 'i.city as incident_city', 'i.state as incident_state')
      .orderBy('p.created_at', 'desc')
      .limit(limit);
    results.candidates = candidates.length;
    for (const v of candidates) {
      if (Date.now() - start > 50000) break;
      try { await enrichOne(db, v, results); }
      catch (e) { results.errors.push(`${v.full_name}: ${e.message}`); await reportError(db, 'property-to-family', v.id, e.message).catch(() => {}); }
    }
    res.json({ success: true, message: `property-to-family: ${results.searched} searched, ${results.matches} victims with relatives, ${results.evidence_rows} evidence rows`, ...results, timestamp: new Date().toISOString() });
  } catch (err) {
    await reportError(db, 'property-to-family', null, err.message).catch(() => {});
    res.status(500).json({ error: err.message, results });
  }
};
