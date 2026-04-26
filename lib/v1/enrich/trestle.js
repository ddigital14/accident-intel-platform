/**
 * Trestle Identity API Integration
 * (formerly Whitepages Pro — now Trestle, a Bellevue-based spinoff after
 * Mastercard acquired Ekata)
 *
 * 5 endpoints:
 *   - Reverse Phone        — phone → all owners with names, addresses, emails
 *   - Caller Identification — phone → TOP owner only (cheaper variant)
 *   - Smart CNAM           — phone → name only (cheapest)
 *   - Reverse Address      — address → all current residents
 *   - Phone Validation     — phone → carrier, line type, prepaid, activity
 *   - Real Contact         — phone+email+address → contactability score
 *
 * Auth: x-api-key header
 * Base: https://api.trestleiq.com
 *
 * Exports both raw API callers (for trigger.js to use in parallel) and
 * helper enrichPersonViaTrestle() that wraps the right endpoint based on
 * what data is missing.
 */
const { reportError } = require('../system/_errors');

const TRESTLE_KEY = process.env.TRESTLE_API_KEY;
const BASE = 'https://api.trestleiq.com';

function isConfigured() { return !!TRESTLE_KEY; }

async function callTrestle(path, params = {}) {
  if (!TRESTLE_KEY) return { error: 'TRESTLE_API_KEY not set' };
  const qs = new URLSearchParams(params).toString();
  const url = `${BASE}${path}${qs ? '?' + qs : ''}`;
  try {
    const r = await fetch(url, {
      method: 'GET',
      headers: { 'x-api-key': TRESTLE_KEY, 'Accept': 'application/json' },
      signal: AbortSignal.timeout(10000)
    });
    const body = await r.json().catch(() => ({}));
    if (!r.ok) return { error: body.error?.message || `HTTP ${r.status}`, status: r.status, body };
    return body;
  } catch (e) {
    return { error: e.message };
  }
}

// ───────── Reverse Phone (most powerful — returns ALL phone owners) ─────────
async function reversePhone(phone) {
  if (!phone) return null;
  const cleaned = String(phone).replace(/\D/g, '');
  const e164 = cleaned.length === 10 ? `+1${cleaned}` : cleaned.length === 11 ? `+${cleaned}` : `+${cleaned}`;
  const r = await callTrestle('/3.1/phone', { phone: e164 });
  if (r.error) return null;
  return r;
}

// ───────── Caller ID (top owner only — cheaper) ─────────
async function callerIdentification(phone) {
  if (!phone) return null;
  const cleaned = String(phone).replace(/\D/g, '');
  const e164 = cleaned.length === 10 ? `+1${cleaned}` : cleaned.length === 11 ? `+${cleaned}` : `+${cleaned}`;
  return (await callTrestle('/3.1/caller_id', { phone: e164 }));
}

// ───────── Smart CNAM (name only — cheapest) ─────────
async function smartCnam(phone) {
  if (!phone) return null;
  const cleaned = String(phone).replace(/\D/g, '');
  const e164 = cleaned.length === 10 ? `+1${cleaned}` : cleaned.length === 11 ? `+${cleaned}` : `+${cleaned}`;
  return (await callTrestle('/3.1/cnam', { phone: e164 }));
}

// ───────── Phone Validation (carrier + line type + activity) ─────────
async function phoneValidation(phone) {
  if (!phone) return null;
  const cleaned = String(phone).replace(/\D/g, '');
  const e164 = cleaned.length === 10 ? `+1${cleaned}` : cleaned.length === 11 ? `+${cleaned}` : `+${cleaned}`;
  return (await callTrestle('/3.1/phone_intel', { phone: e164 }));
}

// ───────── Reverse Address (address → residents + their phones/emails) ─────────
async function reverseAddress({ street, city, state, postal_code, country = 'US' }) {
  const params = {};
  if (street) params['street_line_1'] = street;
  if (city) params['city'] = city;
  if (state) params['state_code'] = state;
  if (postal_code) params['postal_code'] = postal_code;
  if (country) params['country_code'] = country;
  return (await callTrestle('/3.1/location', params));
}

// ───────── Real Contact (verify+grade phone+email+address) ─────────
async function realContact({ phone, email, name_first, name_last, address_street, address_city, address_state, address_postal_code }) {
  const params = {};
  if (phone) params['phone'] = phone.replace(/\D/g, '').length === 10 ? `+1${phone.replace(/\D/g, '')}` : phone;
  if (email) params['email'] = email;
  if (name_first) params['name.first_name'] = name_first;
  if (name_last) params['name.last_name'] = name_last;
  if (address_street) params['address.street_line_1'] = address_street;
  if (address_city) params['address.city'] = address_city;
  if (address_state) params['address.state_code'] = address_state;
  if (address_postal_code) params['address.postal_code'] = address_postal_code;
  return (await callTrestle('/3.1/real_contact', params));
}

// ───────── Helper: pick best owner from Reverse Phone result ─────────
function bestOwnerFromReversePhone(result) {
  if (!result || result.error) return null;
  const owners = result.belongs_to || result.owners || [];
  if (!owners.length) return null;
  // Highest match confidence first
  const sorted = [...owners].sort((a, b) => (b.match_confidence || 0) - (a.match_confidence || 0));
  return sorted[0];
}

// ───────── Wrapper for AIP enrich/trigger.js ─────────
/**
 * Given an AIP person record, hit the cheapest Trestle endpoint that fills
 * the most missing fields.
 *
 * Returns:
 *   { source: 'trestle', confidence, fields: {...} }
 *   or null if no useful data found
 */
async function enrichPersonViaTrestle(person) {
  if (!isConfigured()) return null;

  const out = { source: 'trestle', confidence: 0, fields: {} };

  // CASE 1: We have a phone but no name/address → reverse phone (most info)
  if (person.phone && (!person.full_name || !person.address)) {
    const r = await reversePhone(person.phone);
    if (r && !r.error) {
      const owner = bestOwnerFromReversePhone(r);
      if (owner) {
        out.confidence = Math.round((owner.match_confidence || 0.5) * 100) || 70;
        out.fields = {
          full_name: !person.full_name ? (owner.name || `${owner.firstname || ''} ${owner.lastname || ''}`.trim()) : null,
          first_name: !person.first_name ? (owner.firstname || null) : null,
          last_name: !person.last_name ? (owner.lastname || null) : null,
          age: !person.age ? (owner.age_range?.start || null) : null,
          address: !person.address && owner.current_addresses?.[0] ?
            [owner.current_addresses[0].street_line_1, owner.current_addresses[0].city, owner.current_addresses[0].state_code, owner.current_addresses[0].postal_code]
              .filter(Boolean).join(', ') : null,
          city: !person.city ? owner.current_addresses?.[0]?.city : null,
          state: !person.state ? owner.current_addresses?.[0]?.state_code : null,
          zip: !person.zip ? owner.current_addresses?.[0]?.postal_code : null,
          email: !person.email ? owner.emails?.[0]?.address : null,
        };
      }
      // Phone metadata even without owner match
      if (r.is_valid !== undefined) out.fields.phone_verified = !!r.is_valid;
      if (r.carrier) out.fields.phone_carrier = r.carrier;
      if (r.line_type) out.fields.phone_line_type = r.line_type;
    }
  }
  // CASE 2: We have a name + state but no phone → can't directly use Trestle for this (it's phone/address-keyed).
  //         BUT if we have an address → use Reverse Address to get phone+email
  else if (person.address && !person.phone && person.state) {
    const r = await reverseAddress({
      street: person.address, city: person.city, state: person.state, postal_code: person.zip
    });
    if (r && !r.error) {
      const residents = r.current_residents || r.residents || [];
      // Match by name if we have one
      const target = person.full_name ?
        residents.find(p => (p.name || '').toLowerCase().includes((person.last_name || '').toLowerCase())) :
        residents[0];
      if (target) {
        out.confidence = 70;
        out.fields = {
          full_name: !person.full_name ? target.name : null,
          phone: !person.phone ? target.phones?.[0]?.phone_number : null,
          email: !person.email ? target.emails?.[0]?.address : null,
          age: !person.age ? target.age_range?.start : null,
        };
      }
    }
  }
  // CASE 3: We have a phone + name + email/address — verify with Real Contact (cheap quality grade)
  else if (person.phone || person.email) {
    const r = await realContact({
      phone: person.phone, email: person.email,
      name_first: person.first_name, name_last: person.last_name,
      address_street: person.address, address_city: person.city,
      address_state: person.state, address_postal_code: person.zip
    });
    if (r && !r.error) {
      out.confidence = 60;
      out.fields = {
        phone_verified: r.phone?.is_valid,
        phone_carrier: r.phone?.carrier,
        phone_line_type: r.phone?.line_type,
        email_verified: r.email?.is_valid,
        contact_grade: r.contact_grade  // A/B/C/D/F
      };
    }
  }

  // Strip null-valued fields
  for (const k of Object.keys(out.fields)) if (out.fields[k] === null || out.fields[k] === undefined) delete out.fields[k];

  return Object.keys(out.fields).length ? out : null;
}

module.exports = {
  isConfigured,
  reversePhone,
  callerIdentification,
  smartCnam,
  phoneValidation,
  reverseAddress,
  realContact,
  enrichPersonViaTrestle,
};
