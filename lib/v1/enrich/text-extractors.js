/**
 * Phase 68: Combined text extractor engine.
 *
 * Mines raw_description for fields that engines need but we usually don't have:
 *   - address: street + city + state + zip patterns
 *   - phone: NANP regex with formatting variants
 *   - vehicle_plate: state-format plates
 *   - vehicle_vin: 17-char VIN
 *   - insurance carrier name
 *
 * Each extraction writes directly to persons row + logs to enrichment_logs.
 * Triggered automatically on new incident insertion via cascade.
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');

const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

// ─── Regex patterns ────────────────────────────────────────────────────────
const PHONE_RE = /(?:\+?1[-.\s]?)?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})\b/g;
const VIN_RE = /\b([A-HJ-NPR-Z0-9]{17})\b/g;
const PLATE_RE = /\bplate(?:\s*number)?[:\s]*([A-Z0-9][A-Z0-9\- ]{3,8}[A-Z0-9])\b/gi;
const ADDRESS_RE = /\b(\d{1,6}\s+(?:[NSEW]\.?\s+)?[A-Z][a-zA-Z]{2,30}(?:\s+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Way|Place|Pl|Court|Ct|Highway|Hwy|Pkwy|Parkway))[A-Za-z\.\s]{0,40})/gi;
const ADDRESS_FULL_RE = /\b(\d{1,6}\s+[NSEW]?\s*[A-Z][a-zA-Z]{2,30}(?:\s+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Way|Place|Pl|Court|Ct|Highway|Hwy|Pkwy|Parkway))(?:\s+(?:Apt|Apartment|Suite|Ste|Unit)\s*[\w-]+)?,?\s+[A-Z][a-zA-Z\s]+,?\s+([A-Z]{2})\s*(\d{5}(?:-\d{4})?)?)/gi;
const INSURANCE_RE = /\b(GEICO|State\s*Farm|Allstate|Progressive|Liberty\s*Mutual|Farmers|Nationwide|USAA|American\s*Family|Travelers|Auto-Owners|MetLife|Esurance|Mercury|Hartford|Plymouth\s*Rock|Erie|Amica|Country\s*Financial|Safeco|GMAC|Direct\s*General|Kemper|National\s*General|Bristol\s*West|Infinity)\b/gi;

// State 2-letter codes for filtering
const STATES = new Set(['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC']);

function extractPhones(text) {
  const out = [];
  if (!text) return out;
  PHONE_RE.lastIndex = 0;
  let m;
  while ((m = PHONE_RE.exec(text)) !== null) {
    const phone = `+1${m[1]}${m[2]}${m[3]}`;
    if (!out.includes(phone)) out.push(phone);
  }
  return out;
}

function extractVINs(text) {
  if (!text) return [];
  VIN_RE.lastIndex = 0;
  const out = [];
  let m;
  while ((m = VIN_RE.exec(text)) !== null) {
    if (!out.includes(m[1])) out.push(m[1]);
  }
  return out;
}

function extractPlates(text) {
  if (!text) return [];
  PLATE_RE.lastIndex = 0;
  const out = [];
  let m;
  while ((m = PLATE_RE.exec(text)) !== null) {
    const plate = m[1].replace(/\s+/g, '').toUpperCase();
    if (!out.includes(plate) && plate.length >= 4 && plate.length <= 8) out.push(plate);
  }
  return out;
}

function extractAddresses(text) {
  if (!text) return [];
  ADDRESS_FULL_RE.lastIndex = 0;
  const out = [];
  let m;
  while ((m = ADDRESS_FULL_RE.exec(text)) !== null) {
    const stateCode = (m[2] || '').toUpperCase();
    if (!STATES.has(stateCode)) continue;
    out.push({
      full: m[1].replace(/\s+/g, ' ').trim(),
      state: stateCode,
      zip: m[3] || null
    });
  }
  if (!out.length) {
    // Fallback: street-only patterns
    ADDRESS_RE.lastIndex = 0;
    while ((m = ADDRESS_RE.exec(text)) !== null) {
      out.push({ full: m[1].replace(/\s+/g, ' ').trim(), state: null, zip: null });
    }
  }
  return out;
}

function extractInsurance(text) {
  if (!text) return [];
  INSURANCE_RE.lastIndex = 0;
  const out = [];
  let m;
  while ((m = INSURANCE_RE.exec(text)) !== null) {
    const c = m[1].replace(/\s+/g, ' ');
    if (!out.includes(c)) out.push(c);
  }
  return out;
}

async function extractOnePerson(db, personId) {
  const person = await db('persons').where('id', personId).first();
  if (!person) return { ok: false, error: 'person_not_found' };
  const inc = person.incident_id ? await db('incidents').where('id', person.incident_id).first() : null;
  const text = (inc?.raw_description || '') + '\n' + (inc?.description || '');
  if (!text.trim()) return { ok: true, skipped: 'no_text' };

  const phones = extractPhones(text);
  const vins = extractVINs(text);
  const plates = extractPlates(text);
  const addresses = extractAddresses(text);
  const insurance = extractInsurance(text);

  const updates = {};
  // Only fill when person is missing the field
  if (!person.phone && phones.length) updates.phone = phones[0];
  // vehicle_vin/plate are optional — only set if present in person record (column exists)
  if ('vehicle_vin' in person && !person.vehicle_vin && vins.length) updates.vehicle_vin = vins[0];
  if ('vehicle_plate' in person && !person.vehicle_plate && plates.length) updates.vehicle_plate = plates[0];
  if (!person.address && addresses.length) {
    updates.address = addresses[0].full;
    if (addresses[0].state && !person.state) updates.state = addresses[0].state;
    if (addresses[0].zip && !person.zip) updates.zip = addresses[0].zip;
  }
  if (!person.insurance_company && insurance.length) updates.insurance_company = insurance[0];

  if (Object.keys(updates).length) {
    updates.updated_at = new Date();
    await db('persons').where('id', personId).update(updates);
    // Log each new field
    for (const [field, value] of Object.entries(updates)) {
      if (field === 'updated_at') continue;
      try {
        await db('enrichment_logs').insert({
          person_id: personId,
          field_name: field,
          old_value: person[field] || null,
          new_value: JSON.stringify({ value, source: 'text-extractors', extracted_from: 'raw_description' }).slice(0, 4000),
          created_at: new Date()
        });
      } catch (_) {}
    }
  }

  return {
    ok: true, person_id: personId,
    extracted: { phones, vins, plates, addresses: addresses.length, insurance },
    fields_filled: Object.keys(updates).filter(k => k !== 'updated_at')
  };
}

async function batchExtract(db, limit = 50) {
  // Use only guaranteed columns. vehicle_vin/plate may not exist on persons in this schema.
  const persons = await db.raw(`
    SELECT p.id FROM persons p
    JOIN incidents i ON i.id = p.incident_id
    WHERE (i.raw_description IS NOT NULL OR i.description IS NOT NULL)
      AND (p.phone IS NULL OR p.address IS NULL OR p.insurance_company IS NULL)
    ORDER BY p.created_at DESC
    LIMIT ${parseInt(limit) || 50}
  `).then(r => r.rows || []);

  const results = [];
  for (const p of persons) {
    try { results.push(await extractOnePerson(db, p.id)); }
    catch (e) { results.push({ id: p.id, error: e.message }); }
  }
  const filled = results.reduce((s, r) => s + (r.fields_filled?.length || 0), 0);
  return { ok: true, scanned: persons.length, fields_filled_total: filled, results: results.slice(0, 20) };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();

  if (action === 'health') return res.json({ success: true, service: 'text-extractors',
    extractors: ['address','phone','vehicle_vin','vehicle_plate','insurance_company'] });

  if (action === 'extract') {
    const pid = req.query?.person_id;
    if (!pid) return res.status(400).json({ error: 'person_id required' });
    return res.json(await extractOnePerson(db, pid));
  }
  if (action === 'batch') {
    const limit = Math.min(200, parseInt(req.query?.limit) || 50);
    return res.json(await batchExtract(db, limit));
  }
  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.extractOnePerson = extractOnePerson;
module.exports.batchExtract = batchExtract;
module.exports.extractPhones = extractPhones;
module.exports.extractAddresses = extractAddresses;
