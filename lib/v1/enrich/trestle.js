/**
 * Trestle Identity API Integration (v3.2 schema)
 *
 * Account: Mason Donovan, Self-Serve plan, $200 credit
 * Auth: x-api-key header
 * Base: https://api.trestleiq.com
 *
 * Currently enabled (no request needed):
 *   - Reverse Phone API   /3.2/phone         $0.07/query — returns owners[] with name, addresses, emails, alt phones
 *   - Smart CNAM          /3.1/cnam          $0.05/query — phone → top owner name only
 *
 * Available on request (future expansion):
 *   - Reverse Address     /3.2/location      address → residents
 *   - Caller ID           /3.2/caller_id     phone → top owner only
 *   - Real Contact        /3.0/real_contact  verify+grade
 *   - Phone Validation    /3.0/phone_intel   carrier/line type
 *
 * NOTE: /3.2/phone (Reverse Phone) RETURNS phone validation data automatically
 * (carrier, line_type, is_valid, is_prepaid). So we don't need a separate
 * Phone Validation API call when we use Reverse Phone.
 */
let _dbCacheKey = null;
const { trackApiCall } = require('../system/cost');

async function getApiKey(db) {
  // Prefer env var (Vercel-managed), fall back to system_config table
  if (process.env.TRESTLE_API_KEY) return process.env.TRESTLE_API_KEY;
  if (_dbCacheKey) return _dbCacheKey;
  if (!db) return null;
  try {
    const row = await db('system_config').where('key', 'trestle').first();
    if (row?.value) {
      const cfg = typeof row.value === 'string' ? JSON.parse(row.value) : row.value;
      _dbCacheKey = cfg.api_key || null;
      return _dbCacheKey;
    }
  } catch (_) {}
  return null;
}

const BASE = 'https://api.trestleiq.com';

async function isConfigured(db) { return !!(await getApiKey(db)); }

async function callTrestle(path, params = {}, db = null) {
  const key = await getApiKey(db);
  if (!key) return { error: 'TRESTLE_API_KEY not set (env or system_config)' };
  const qs = new URLSearchParams(params).toString();
  const url = `${BASE}${path}${qs ? '?' + qs : ''}`;
  // Cost per endpoint
  const costMap = {
    '/3.2/phone': 0.07, '/3.1/cnam': 0.05, '/3.2/location': 0.07,
    '/3.2/caller_id': 0.07, '/3.0/real_contact': 0.03, '/3.0/phone_intel': 0.015
  };
  const cost = costMap[path] || 0.05;
  try {
    const r = await fetch(url, {
      method: 'GET',
      headers: { 'x-api-key': key, 'Accept': 'application/json' },
      signal: AbortSignal.timeout(10000)
    });
    const body = await r.json().catch(() => ({}));
    const success = r.ok;
    if (db) await trackApiCall(db, 'trestle', `trestle${path.replace(/\//g, '_')}`, 0, 0, success).catch(() => {});
    if (!success) return { error: body.message || `HTTP ${r.status}`, status: r.status, body };
    return body;
  } catch (e) {
    if (db) await trackApiCall(db, 'trestle', 'trestle_error', 0, 0, false).catch(() => {});
    return { error: e.message };
  }
}

function toE164(phone) {
  if (!phone) return null;
  const d = String(phone).replace(/\D/g, '');
  if (d.length === 10) return `+1${d}`;
  if (d.length === 11 && d[0] === '1') return `+${d}`;
  return `+${d}`;
}

// ───────── Reverse Phone (v3.2) — most powerful ─────────
async function reversePhone(phone, db) {
  const e164 = toE164(phone);
  if (!e164) return null;
  return callTrestle('/3.2/phone', { phone: e164 }, db);
}

// ───────── Smart CNAM (v3.1) — name only, cheaper ─────────
async function smartCnam(phone, db) {
  const e164 = toE164(phone);
  if (!e164) return null;
  return callTrestle('/3.1/cnam', { phone: e164 }, db);
}

// ───────── Reverse Address (v3.2) — needs access ─────────
async function reverseAddress({ street, city, state, postal_code, country = 'US' }, db) {
  const params = {};
  if (street) params['street_line_1'] = street;
  if (city) params['city'] = city;
  if (state) params['state_code'] = state;
  if (postal_code) params['postal_code'] = postal_code;
  if (country) params['country_code'] = country;
  return callTrestle('/3.2/location', params, db);
}

// ───────── Caller Identification (v3.2) — needs access ─────────
async function callerIdentification(phone, db) {
  const e164 = toE164(phone);
  if (!e164) return null;
  return callTrestle('/3.2/caller_id', { phone: e164 }, db);
}

// ───────── Real Contact (v3.0) — needs access ─────────
async function realContact({ phone, email, name_first, name_last, address_street, address_city, address_state, address_postal_code }, db) {
  const params = {};
  if (phone) params['phone'] = toE164(phone);
  if (email) params['email'] = email;
  if (name_first) params['name.first_name'] = name_first;
  if (name_last) params['name.last_name'] = name_last;
  if (address_street) params['address.street_line_1'] = address_street;
  if (address_city) params['address.city'] = address_city;
  if (address_state) params['address.state_code'] = address_state;
  if (address_postal_code) params['address.postal_code'] = address_postal_code;
  return callTrestle('/3.0/real_contact', params, db);
}

// ───────── Helper: pick best owner from Reverse Phone v3.2 result ─────────
function bestOwnerFromReversePhone(result) {
  if (!result || result.error) return null;
  const owners = result.owners || [];
  if (!owners.length) return null;
  // v3.2 doesn't have explicit confidence — first owner is highest ranked
  return owners[0];
}

function flattenAddress(addr) {
  if (!addr) return null;
  if (typeof addr === 'string') return addr;
  return [
    addr.street_line_1, addr.street_line_2,
    addr.city, addr.state_code, addr.postal_code
  ].filter(Boolean).join(', ');
}

// ───────── Main wrapper used by enrich/trigger.js ─────────
/**
 * Returns:
 *   { source: 'trestle', confidence, fields: {...}, cost_usd, endpoint }
 *   or null if no useful data
 */
async function enrichPersonViaTrestle(person, db = null) {
  if (!(await isConfigured(db))) return null;

  // CASE A: We have phone + missing core fields → Reverse Phone (best data, $0.07)
  if (person.phone && (!person.full_name || !person.address || !person.email)) {
    const r = await reversePhone(person.phone, db);
    if (!r || r.error) return null;
    const owner = bestOwnerFromReversePhone(r);
    const out = {
      source: 'trestle_reverse_phone',
      confidence: 0,
      cost_usd: 0.07,
      endpoint: '/3.2/phone',
      fields: {}
    };

    // Phone metadata (always available even if no owner)
    if (r.is_valid !== undefined) out.fields.phone_verified = !!r.is_valid;
    if (r.carrier) out.fields.phone_carrier = r.carrier;
    if (r.line_type) out.fields.phone_line_type = r.line_type;
    if (r.is_prepaid !== null) out.fields.phone_is_prepaid = r.is_prepaid;

    if (owner) {
      out.confidence = 80;
      const name = owner.name;
      if (!person.full_name && name) {
        out.fields.full_name = name;
        const parts = name.trim().split(/\s+/);
        out.fields.first_name = parts[0] || null;
        out.fields.last_name = parts[parts.length - 1] || null;
      }
      // current_addresses can be {} or [] in v3.2 — handle both
      let addrs = owner.current_addresses;
      if (Array.isArray(addrs)) addrs = addrs;
      else if (addrs && typeof addrs === 'object' && Object.keys(addrs).length) addrs = [addrs];
      else addrs = [];
      const primaryAddr = addrs[0];
      if (primaryAddr && !person.address) {
        out.fields.address = flattenAddress(primaryAddr);
        out.fields.city = primaryAddr.city || null;
        out.fields.state = primaryAddr.state_code || null;
        out.fields.zip = primaryAddr.postal_code || null;
      }
      // emails array
      if (owner.emails?.length && !person.email) {
        out.fields.email = typeof owner.emails[0] === 'string' ? owner.emails[0] : owner.emails[0].address;
      }
      // age_range
      if (owner.age_range && !person.age) {
        out.fields.age = owner.age_range.start || owner.age_range || null;
      }
      // alternate phones — useful as backup contact
      if (owner.alternate_phones?.length) {
        out.fields.phone_secondary = typeof owner.alternate_phones[0] === 'string' ? owner.alternate_phones[0] : owner.alternate_phones[0].phone_number;
      }
    } else {
      out.confidence = 30; // phone validates but no owner
    }
    // Strip null/undefined
    for (const k of Object.keys(out.fields)) if (out.fields[k] == null) delete out.fields[k];
    return Object.keys(out.fields).length ? out : null;
  }

  // CASE B: We have phone but already have name/address — try cheap CNAM to confirm name
  if (person.phone && person.full_name && !person.phone_verified) {
    const r = await smartCnam(person.phone, db);
    if (!r || r.error) return null;
    const out = {
      source: 'trestle_cnam',
      confidence: 60,
      cost_usd: 0.05,
      endpoint: '/3.1/cnam',
      fields: {}
    };
    if (r.is_valid !== undefined) out.fields.phone_verified = !!r.is_valid;
    if (r.belongs_to?.name && !person.full_name) out.fields.full_name = r.belongs_to.name;
    return Object.keys(out.fields).length ? out : null;
  }

  // CASE C: We have an address but no phone — Reverse Address (needs access; will 403 until enabled)
  if (person.address && !person.phone && person.state) {
    const r = await reverseAddress({
      street: person.address, city: person.city, state: person.state, postal_code: person.zip
    }, db);
    if (!r || r.error) return null;
    const out = {
      source: 'trestle_reverse_address',
      confidence: 0,
      cost_usd: 0.07,
      endpoint: '/3.2/location',
      fields: {}
    };
    const residents = r.current_residents || r.residents || [];
    const target = person.last_name ?
      residents.find(p => (p.name || '').toLowerCase().includes(person.last_name.toLowerCase())) :
      residents[0];
    if (target) {
      out.confidence = 70;
      const phone = typeof target.phones?.[0] === 'string' ? target.phones[0] : target.phones?.[0]?.phone_number;
      if (phone && !person.phone) out.fields.phone = phone;
      if (target.emails?.[0] && !person.email) {
        out.fields.email = typeof target.emails[0] === 'string' ? target.emails[0] : target.emails[0].address;
      }
      if (!person.full_name && target.name) out.fields.full_name = target.name;
      if (!person.age && target.age_range) out.fields.age = target.age_range.start || target.age_range;
    }
    return Object.keys(out.fields).length ? out : null;
  }

  return null;
}

module.exports = {
  isConfigured, getApiKey,
  reversePhone, smartCnam, reverseAddress, callerIdentification, realContact,
  enrichPersonViaTrestle,
};
