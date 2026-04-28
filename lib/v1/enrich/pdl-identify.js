/**
 * PDL Identify Endpoint — Phase 37 (better than enrich for partial data)
 *
 * /v5/person/identify returns up to 10 matches RANKED by confidence using
 * the same partial inputs that Enrich would 404 on. Perfect for accident
 * victims where we have name + city + state but no phone/email.
 *
 * GET /api/v1/enrich/pdl-identify?secret=ingest-now&action=batch&limit=10
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');
const { trackApiCall } = require('../system/cost');
const { logChange } = require('../system/changelog');

const PDL_KEY = process.env.PDL_API_KEY;
const PDL_IDENTIFY_URL = 'https://api.peopledatalabs.com/v5/person/identify';
const TIMEOUT_MS = 10000;

async function callIdentify(person) {
  if (!PDL_KEY) return { ok: false, error: 'no_pdl_key' };
  const params = new URLSearchParams();
  const first = (person.first_name || '').trim();
  const last  = (person.last_name  || '').trim();
  const fullName = (person.full_name || `${first} ${last}`).trim();
  if (!fullName) return { ok: false, error: 'no_name' };
  if (first) params.append('first_name', first);
  if (last)  params.append('last_name',  last);
  if (!first && !last && fullName) params.append('name', fullName);
  if (person.city)    params.append('locality', person.city);
  if (person.state)   params.append('region', person.state);
  if (person.phone)   params.append('phone', person.phone);
  if (person.email)   params.append('email', person.email);
  params.append('min_likelihood', '2'); // identify ranks; we filter ourselves

  try {
    const r = await fetch(`${PDL_IDENTIFY_URL}?${params}`, {
      headers: { 'X-API-Key': PDL_KEY, 'Accept': 'application/json' },
      signal: AbortSignal.timeout(TIMEOUT_MS)
    });
    if (!r.ok) return { ok: false, status: r.status };
    const d = await r.json();
    if (d.status !== 200 || !Array.isArray(d.matches) || !d.matches.length) {
      return { ok: false, status: d.status || 'no_matches' };
    }
    // top match is highest likelihood
    const top = d.matches[0];
    return { ok: true, data: top.data, likelihood: top.match_score, total: d.matches.length };
  } catch (e) { return { ok: false, error: e.message }; }
}

function pdlToFields(pdl) {
  const out = {};
  if (!pdl) return out;
  if (pdl.mobile_phone) out.phone = pdl.mobile_phone;
  else if (pdl.phone_numbers?.length) out.phone = pdl.phone_numbers[0];
  if (pdl.personal_emails?.length) out.email = pdl.personal_emails[0];
  else if (pdl.work_email) out.email = pdl.work_email;
  if (pdl.job_company_name) out.employer = pdl.job_company_name;
  if (pdl.job_title) out.occupation = pdl.job_title;
  if (pdl.linkedin_url) out.linkedin_url = pdl.linkedin_url;
  if (pdl.location_street_address) {
    out.address = pdl.location_street_address;
    if (pdl.location_locality) out.city = out.city || pdl.location_locality;
    if (pdl.location_region) out.state = out.state || pdl.location_region;
    if (pdl.location_postal_code) out.zip = pdl.location_postal_code;
  }
  if (pdl.birth_year) out.age = new Date().getFullYear() - pdl.birth_year;
  return out;
}

async function runBatch(db, limit = 10) {
  // Pull pending_named persons missing both phone+email, in US states only
  const rows = await db.raw(`
    SELECT p.id, p.first_name, p.last_name, p.full_name, p.phone, p.email,
           i.city, i.state
    FROM persons p
    JOIN incidents i ON i.id = p.incident_id
    WHERE i.qualification_state IN ('pending','pending_named')
      AND (p.phone IS NULL OR p.phone = '')
      AND (p.email IS NULL OR p.email = '')
      AND p.full_name IS NOT NULL
      AND i.state IN ('AZ','CA','TX','FL','GA','IL','OH','PA','NY','NC','MI','VA','WA','CO','OR','MA','TN','IN','MO','MD','WI','MN','SC','AL','KY','LA','OK','CT','UT','NV','AR','KS','MS','NM','NE','WV','ID','HI','NH','ME','RI','MT','DE','SD','AK','ND','VT','WY','DC')
    ORDER BY p.created_at DESC
    LIMIT ${parseInt(limit) || 10}
  `).then(r => r.rows || r);

  let enriched = 0, fields_filled = 0, errors = 0;
  for (const p of rows) {
    const res = await callIdentify(p);
    if (db) await trackApiCall(db, 'enrich-pdl-identify', 'pdl', 0, 0, !!res.ok).catch(() => {});
    if (!res.ok) { errors++; continue; }
    const fields = pdlToFields(res.data);
    const upd = {};
    for (const k of Object.keys(fields)) {
      if (fields[k] && (k === 'phone' || k === 'email' || k === 'employer' || k === 'occupation' || k === 'linkedin_url' || k === 'address' || k === 'zip' || k === 'age')) {
        upd[k] = fields[k];
      }
    }
    if (Object.keys(upd).length) {
      try {
        upd.updated_at = new Date();
        await db('persons').where({ id: p.id }).update(upd);
        await enqueueCascade(db, 'person', p.id, 'pdl-identify', { weight: 70, fields: Object.keys(upd) });
        await logChange(db, 'persons', p.id, 'pdl-identify', upd).catch(() => {});
        enriched++;
        fields_filled += Object.keys(upd).length - 1; // minus updated_at
      } catch (_) { errors++; }
    }
  }
  return { candidates: rows.length, enriched, fields_filled, errors };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'pdl-identify', has_key: !!PDL_KEY });
    const out = await runBatch(db, parseInt(req.query.limit) || 10);
    return res.json({ success: true, ...out });
  } catch (err) {
    await reportError(db, 'pdl-identify', null, err.message);
    return res.status(500).json({ error: err.message });
  }
};
module.exports.run = runBatch;
