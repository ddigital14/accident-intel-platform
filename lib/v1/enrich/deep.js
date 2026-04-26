/**
 * Deep Enrichment Chain
 *
 * Multi-step intelligent identification — given partial info on a person,
 * use chain of API calls to maximize what we learn about them.
 *
 * Strategy:
 *   STEP 1: Trestle Reverse Phone (if phone) → name, age, addresses, emails, ALT_PHONES
 *   STEP 2: For each alt_phone, run Trestle CNAM (cheap) → confirm secondary contact
 *   STEP 3: PDL enrich (name + city/state) → employer, LinkedIn, work email
 *   STEP 4: Hunter (if employer found) → verified work email
 *   STEP 5: Tracerfy skip-trace (if still missing) → relatives + alt addresses
 *   STEP 6: SearchBug (phone or address) → reverse lookup
 *   STEP 7: NumVerify (every phone) → carrier + line type for SMS strategy
 *
 * Returns full identity profile + confidence score.
 */
const trestle = require('./trestle');
const { reportError } = require('../system/_errors');

const PDL_KEY = process.env.PDL_API_KEY;
const HUNTER_KEY = process.env.HUNTER_API_KEY;
const NUMVERIFY_KEY = process.env.NUMVERIFY_API_KEY;
const TRACERFY_KEY = process.env.TRACERFY_API_KEY;
const SEARCHBUG_KEY = process.env.SEARCHBUG_API_KEY;
const SEARCHBUG_CO = process.env.SEARCHBUG_CO_CODE;

async function pdlEnrich(person) {
  if (!PDL_KEY || (!person.first_name && !person.last_name && !person.email && !person.phone)) return null;
  try {
    const params = new URLSearchParams();
    if (person.first_name) params.append('first_name', person.first_name);
    if (person.last_name) params.append('last_name', person.last_name);
    if (person.email) params.append('email', person.email);
    if (person.phone) params.append('phone', person.phone);
    if (person.state) params.append('region', person.state);
    if (person.city) params.append('locality', person.city);
    params.append('min_likelihood', '3');
    const r = await fetch(`https://api.peopledatalabs.com/v5/person/enrich?${params}`, {
      headers: { 'X-API-Key': PDL_KEY }, signal: AbortSignal.timeout(8000)
    });
    if (!r.ok) return null;
    const d = await r.json();
    if (d.status !== 200 || !d.data) return null;
    return d.data;
  } catch (_) { return null; }
}

async function hunterFindEmail(firstName, lastName, domain) {
  if (!HUNTER_KEY || !firstName || !lastName || !domain) return null;
  try {
    const r = await fetch(`https://api.hunter.io/v2/email-finder?domain=${domain}&first_name=${encodeURIComponent(firstName)}&last_name=${encodeURIComponent(lastName)}&api_key=${HUNTER_KEY}`, {
      signal: AbortSignal.timeout(8000)
    });
    if (!r.ok) return null;
    const d = await r.json();
    if (d.data?.email) return { email: d.data.email, score: d.data.score, verified: d.data.verification?.status === 'valid' };
    return null;
  } catch (_) { return null; }
}

async function tracerfySearch(person) {
  if (!TRACERFY_KEY || !person.first_name || !person.last_name) return null;
  try {
    const r = await fetch('https://api.tracerfy.com/v1/person/search', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${TRACERFY_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        first_name: person.first_name, last_name: person.last_name,
        state: person.state, city: person.city
      }),
      signal: AbortSignal.timeout(10000)
    });
    if (!r.ok) return null;
    const d = await r.json();
    return d.results?.[0] || null;
  } catch (_) { return null; }
}

async function numVerify(phone) {
  if (!NUMVERIFY_KEY || !phone) return null;
  try {
    const cleaned = String(phone).replace(/\D/g, '');
    const r = await fetch(`http://apilayer.net/api/validate?access_key=${NUMVERIFY_KEY}&number=${cleaned}&country_code=US&format=1`, {
      signal: AbortSignal.timeout(6000)
    });
    if (!r.ok) return null;
    return await r.json();
  } catch (_) { return null; }
}

// ───────── MAIN: chain through all enrichers ─────────
/**
 * deepEnrichPerson(person, db)
 * Returns {
 *   ok: boolean,
 *   confidence: 0-100 (overall),
 *   sources_used: [...],
 *   merged_fields: { phone, email, address, employer, linkedin_url, ... },
 *   alt_phones: [],
 *   relatives: [],
 *   evidence: { source: result } per chain step,
 *   cost_estimate_usd: number
 * }
 */
async function deepEnrichPerson(person, db) {
  const out = {
    ok: false,
    confidence: 0,
    sources_used: [],
    merged_fields: {},
    alt_phones: [],
    relatives: [],
    evidence: {},
    cost_estimate_usd: 0
  };

  // STEP 1: Trestle Reverse Phone (best ROI when we have phone)
  if (person.phone) {
    const t = await trestle.reversePhone(person.phone, db);
    if (t && !t.error) {
      out.evidence.trestle_reverse_phone = t;
      out.sources_used.push('trestle_reverse_phone');
      out.cost_estimate_usd += 0.07;
      const owner = (t.owners || [])[0];
      if (owner) {
        out.confidence = Math.max(out.confidence, 80);
        if (!person.full_name && owner.name) {
          out.merged_fields.full_name = owner.name;
          const parts = owner.name.trim().split(/\s+/);
          out.merged_fields.first_name = parts[0];
          out.merged_fields.last_name = parts[parts.length - 1];
        }
        // Address
        let addrs = owner.current_addresses;
        if (Array.isArray(addrs)) {} else if (addrs && Object.keys(addrs).length) addrs = [addrs];
        else addrs = [];
        if (addrs[0] && !person.address) {
          out.merged_fields.address = [addrs[0].street_line_1, addrs[0].city, addrs[0].state_code, addrs[0].postal_code].filter(Boolean).join(', ');
          out.merged_fields.city = addrs[0].city;
          out.merged_fields.state = addrs[0].state_code;
          out.merged_fields.zip = addrs[0].postal_code;
        }
        if (owner.emails?.length && !person.email) {
          out.merged_fields.email = typeof owner.emails[0] === 'string' ? owner.emails[0] : owner.emails[0].address;
        }
        if (owner.alternate_phones?.length) {
          out.alt_phones = owner.alternate_phones.map(p => typeof p === 'string' ? p : p.phone_number).filter(Boolean);
        }
        if (owner.age_range && !person.age) {
          out.merged_fields.age = owner.age_range.start || owner.age_range;
        }
      }
      // Phone metadata
      if (t.is_valid !== undefined) out.merged_fields.phone_verified = !!t.is_valid;
      if (t.carrier) out.merged_fields.phone_carrier = t.carrier;
      if (t.line_type) out.merged_fields.phone_line_type = t.line_type;
    }
  }

  // STEP 2: Confirm alt_phones via cheap CNAM (max 2 to control cost)
  for (const altPhone of out.alt_phones.slice(0, 2)) {
    const c = await trestle.smartCnam(altPhone, db);
    if (c && !c.error && c.belongs_to?.name) {
      out.cost_estimate_usd += 0.05;
      // If this confirms our person matches the alt phone, log it
      const personName = out.merged_fields.full_name || person.full_name;
      if (personName && c.belongs_to.name.toLowerCase().includes((person.last_name || personName.split(' ').slice(-1)[0] || '').toLowerCase())) {
        out.evidence[`cnam_${altPhone}`] = c;
      }
    }
  }

  // STEP 3: PDL — name → employer + LinkedIn
  const pdlPerson = { ...person, ...out.merged_fields };
  const pdl = await pdlEnrich(pdlPerson);
  if (pdl) {
    out.evidence.pdl = { full_name: pdl.full_name, job_company_name: pdl.job_company_name, job_title: pdl.job_title };
    out.sources_used.push('pdl');
    out.cost_estimate_usd += 0.02;
    out.confidence = Math.max(out.confidence, 75);
    if (!out.merged_fields.employer && pdl.job_company_name) out.merged_fields.employer = pdl.job_company_name;
    if (!out.merged_fields.occupation && pdl.job_title) out.merged_fields.occupation = pdl.job_title;
    if (!out.merged_fields.linkedin_url && pdl.linkedin_url) out.merged_fields.linkedin_url = pdl.linkedin_url;
    if (!out.merged_fields.email && pdl.work_email) out.merged_fields.email = pdl.work_email;
    if (!out.merged_fields.phone && pdl.mobile_phone) out.merged_fields.phone = pdl.mobile_phone;
    // PDL has structured location data
    if (!out.merged_fields.address && pdl.location_street_address) {
      out.merged_fields.address = pdl.location_street_address;
    }
  }

  // STEP 4: Hunter — find work email if we have employer
  if (out.merged_fields.employer && !out.merged_fields.email) {
    // Naive employer→domain — would be better with a real lookup
    const employerSlug = out.merged_fields.employer.toLowerCase().replace(/[^a-z0-9]/g, '');
    const tryDomains = [`${employerSlug}.com`];
    for (const dom of tryDomains) {
      const h = await hunterFindEmail(
        out.merged_fields.first_name || person.first_name,
        out.merged_fields.last_name || person.last_name,
        dom
      );
      if (h) {
        out.evidence.hunter = h;
        out.sources_used.push('hunter');
        out.cost_estimate_usd += 0.04;
        if (!out.merged_fields.email) out.merged_fields.email = h.email;
        break;
      }
    }
  }

  // STEP 5: Tracerfy — relatives + alt addresses
  if (!out.merged_fields.address || !out.merged_fields.phone) {
    const tr = await tracerfySearch(pdlPerson);
    if (tr) {
      out.evidence.tracerfy = { phones: tr.phones?.length, emails: tr.emails?.length, relatives: tr.relatives?.length };
      out.sources_used.push('tracerfy');
      out.cost_estimate_usd += 0.10;
      out.confidence = Math.max(out.confidence, 70);
      if (tr.phones?.[0] && !out.merged_fields.phone) out.merged_fields.phone = tr.phones[0];
      if (tr.emails?.[0] && !out.merged_fields.email) out.merged_fields.email = tr.emails[0];
      if (tr.address && !out.merged_fields.address) out.merged_fields.address = tr.address;
      if (tr.relatives) out.relatives = tr.relatives;
    }
  }

  // STEP 7: NumVerify the primary phone (free)
  if (out.merged_fields.phone || person.phone) {
    const phone = out.merged_fields.phone || person.phone;
    const nv = await numVerify(phone);
    if (nv && nv.valid !== undefined) {
      out.evidence.numverify = { valid: nv.valid, carrier: nv.carrier, line_type: nv.line_type };
      out.sources_used.push('numverify');
      // NumVerify is free
      if (!out.merged_fields.phone_verified) out.merged_fields.phone_verified = !!nv.valid;
      if (!out.merged_fields.phone_carrier && nv.carrier) out.merged_fields.phone_carrier = nv.carrier;
      if (!out.merged_fields.phone_line_type && nv.line_type) out.merged_fields.phone_line_type = nv.line_type;
    }
  }

  out.ok = Object.keys(out.merged_fields).length > 0;
  return out;
}

module.exports = { deepEnrichPerson };
