/**
 * CANONICAL SCHEMA + NORMALIZERS
 *
 * Single source of truth for:
 *   - Incident vocabulary (severity, incident_type, status, qualification_state)
 *   - Person vocabulary (role, injury_severity, contact_status)
 *   - Source type strings (must match SOURCE_WEIGHTS in correlate.js)
 *
 * EVERY ingest pipeline MUST call normalizeIncident() and normalizePerson()
 * before writing to the DB. This guarantees consistent field shapes,
 * vocabulary alignment, and all required defaults.
 */

// ── Vocabularies ──────────────────────────────────────────────
const SEVERITY = ['fatal', 'critical', 'serious', 'moderate', 'minor', 'unknown'];
const INCIDENT_TYPES = ['car_accident', 'motorcycle_accident', 'truck_accident', 'pedestrian', 'bicycle', 'work_accident', 'slip_fall', 'other'];
const STATUS = ['new', 'verified', 'enriched', 'assigned', 'contacted', 'in_progress', 'closed', 'invalid', 'merged'];
const QUALIFICATION = ['pending', 'pending_named', 'qualified', 'has_attorney_skip'];
const PERSON_ROLES = ['driver', 'passenger', 'pedestrian', 'cyclist', 'worker', 'witness', 'other'];
const INJURY = ['fatal', 'incapacitating', 'non_incapacitating', 'possible', 'none', 'unknown'];
const CONTACT_STATUS = ['not_contacted', 'attempted', 'contacted', 'interested', 'not_interested', 'retained', 'has_attorney'];
const SOURCE_TYPES = [
  // Dispatch / police
  'tomtom', 'waze', 'scanner', 'nhtsa',
  'opendata_seattle', 'opendata_sf', 'opendata_dallas', 'opendata_chicago',
  'opendata_cincinnati', 'opendata_houston', 'opendata_atlanta',
  'state_txdot', 'state_ga511', 'state_fl511',
  // News / social
  'newsapi', 'rss', 'reddit', 'police_social', 'obituary', 'trauma_hems',
  // Public records
  'court_records', 'people_search', 'pdl', 'hunter_io', 'tracerfy',
  'searchbug', 'numverify', 'openweather'
];

// ── Severity normalizer ───────────────────────────────────────
function normalizeSeverity(v) {
  if (!v) return 'unknown';
  const s = String(v).toLowerCase().trim();
  if (SEVERITY.includes(s)) return s;
  // Aliases
  const map = {
    'killed': 'fatal', 'dead': 'fatal', 'death': 'fatal',
    'crit': 'critical', 'severe': 'critical',
    'major': 'serious', 'serious_injury': 'serious',
    'mod': 'moderate', 'mid': 'moderate',
    'minor_injury': 'minor', 'fender': 'minor', 'fender_bender': 'minor',
    'no_injury': 'minor'
  };
  return map[s] || 'unknown';
}

// ── Incident type normalizer ──────────────────────────────────
function normalizeIncidentType(v) {
  if (!v) return 'car_accident';
  const s = String(v).toLowerCase().trim();
  if (INCIDENT_TYPES.includes(s)) return s;
  // Aliases
  if (/motorcycle|bike\s*crash|m\/c/i.test(s)) return 'motorcycle_accident';
  if (/truck|semi|commercial|18.?wheeler|tractor/i.test(s)) return 'truck_accident';
  if (/pedestrian|ped\s|walker/i.test(s)) return 'pedestrian';
  if (/bicycl|cyclist|pedalcycl/i.test(s)) return 'bicycle';
  if (/slip|fall|premises/i.test(s)) return 'slip_fall';
  if (/work|workplace|industrial|osha/i.test(s)) return 'work_accident';
  if (/car|auto|mva|collision|crash|accident/i.test(s)) return 'car_accident';
  return 'other';
}

// ── Source type normalizer ────────────────────────────────────
function normalizeSourceType(v) {
  if (!v) return 'other';
  const s = String(v).toLowerCase().trim().replace(/[\s-]+/g, '_');
  if (SOURCE_TYPES.includes(s)) return s;
  // Common aliases
  const map = {
    'opendata': 'opendata_seattle',
    'open_data': 'opendata_seattle',
    'twitter': 'police_social',
    'nitter': 'police_social',
    'pdl_v5': 'pdl',
    'people_data_labs': 'pdl',
    'hunter': 'hunter_io',
  };
  return map[s] || s;  // pass through unrecognized for forward-compat
}

// ── Status / role / injury normalizers ─────────────────────────
function normalizeStatus(v, allowed = STATUS) {
  if (!v) return allowed[0];
  const s = String(v).toLowerCase().trim();
  return allowed.includes(s) ? s : allowed[0];
}

function normalizePersonRole(v) { return normalizeStatus(v, PERSON_ROLES); }
function normalizeInjurySeverity(v) {
  if (!v) return null;
  const s = String(v).toLowerCase().trim();
  return INJURY.includes(s) ? s : null;
}

// ── Phone normalizer (E.164) ──────────────────────────────────
function normalizePhone(v) {
  if (!v) return null;
  const digits = String(v).replace(/\D/g, '');
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits.startsWith('1')) return `+${digits}`;
  if (digits.length >= 10 && digits.length <= 15) return `+${digits}`;
  return null;
}

// ── Email normalizer ───────────────────────────────────────────
function normalizeEmail(v) {
  if (!v) return null;
  const s = String(v).trim().toLowerCase();
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s) ? s : null;
}

// ── Name normalizer ────────────────────────────────────────────
function normalizeName(v) {
  if (!v) return null;
  return String(v).trim()
    .replace(/\s+/g, ' ')
    .split(' ')
    .map(w => w.length > 0 ? w[0].toUpperCase() + w.slice(1).toLowerCase() : '')
    .join(' ');
}

// ── Coordinate normalizer ─────────────────────────────────────
function normalizeCoord(v, kind) {
  const n = parseFloat(v);
  if (isNaN(n)) return null;
  if (kind === 'lat' && (n < -90 || n > 90)) return null;
  if (kind === 'lng' && (n < -180 || n > 180)) return null;
  // Reject obviously invalid (0,0)
  if (n === 0) return null;
  return n;
}

// ── INCIDENT NORMALIZER ────────────────────────────────────────
function normalizeIncident(input, defaults = {}) {
  const now = new Date();
  const lat = normalizeCoord(input.latitude ?? input.lat, 'lat');
  const lng = normalizeCoord(input.longitude ?? input.lng, 'lng');

  const out = {
    id: input.id || null,
    incident_number: input.incident_number || null,
    incident_type: normalizeIncidentType(input.incident_type),
    severity: normalizeSeverity(input.severity),
    status: normalizeStatus(input.status || 'new'),
    priority: input.priority ?? severityToPriority(normalizeSeverity(input.severity)),
    confidence_score: clampNumber(input.confidence_score, 0, 100, 50),
    address: truncate(input.address, 500),
    city: truncate(input.city, 100),
    state: input.state ? String(input.state).toUpperCase().slice(0, 2) : null,
    zip: truncate(input.zip, 10),
    latitude: lat, longitude: lng,
    highway: truncate(input.highway, 100),
    occurred_at: toDate(input.occurred_at) || now,
    reported_at: toDate(input.reported_at) || now,
    discovered_at: toDate(input.discovered_at) || now,
    description: truncate(input.description, 5000),
    injuries_count: nullableInt(input.injuries_count),
    fatalities_count: nullableInt(input.fatalities_count),
    vehicles_involved: nullableInt(input.vehicles_involved),
    weather_conditions: truncate(input.weather_conditions, 100),
    lighting_conditions: truncate(input.lighting_conditions, 50),
    road_conditions: truncate(input.road_conditions, 100),
    police_department: truncate(input.police_department, 200),
    police_report_number: truncate(input.police_report_number, 100),
    metro_area_id: input.metro_area_id || null,
    source_count: input.source_count ?? 1,
    first_source_id: input.first_source_id || null,
    tags: Array.isArray(input.tags) ? input.tags.filter(Boolean).map(t => String(t).toLowerCase()) : [],
    qualification_state: QUALIFICATION.includes(input.qualification_state) ? input.qualification_state : 'pending',
    lead_score: clampNumber(input.lead_score, 0, 100, 0),
    has_contact_info: !!input.has_contact_info,
    ems_dispatched: !!input.ems_dispatched,
    helicopter_dispatched: !!input.helicopter_dispatched,
    created_at: toDate(input.created_at) || now,
    updated_at: now,
    ...defaults
  };
  return out;
}

// ── PERSON NORMALIZER ──────────────────────────────────────────
function normalizePerson(input, defaults = {}) {
  const now = new Date();
  const fullName = input.full_name || ((input.first_name || '') + ' ' + (input.last_name || '')).trim();
  const norm = normalizeName(fullName) || null;

  const out = {
    id: input.id || null,
    incident_id: input.incident_id || null,
    role: normalizePersonRole(input.role || 'driver'),
    is_injured: !!input.is_injured,
    first_name: normalizeName(input.first_name) || (norm ? norm.split(' ')[0] : null),
    last_name: normalizeName(input.last_name) || (norm ? norm.split(' ').slice(-1)[0] : null),
    full_name: norm,
    age: nullableInt(input.age),
    phone: normalizePhone(input.phone),
    email: normalizeEmail(input.email),
    address: truncate(input.address, 500),
    city: truncate(input.city, 100),
    state: input.state ? String(input.state).toUpperCase().slice(0, 2) : null,
    zip: truncate(input.zip, 10),
    injury_severity: normalizeInjurySeverity(input.injury_severity),
    transported_to: truncate(input.transported_to, 200),
    insurance_company: truncate(input.insurance_company, 200),
    has_attorney: input.has_attorney === true || input.has_attorney === false ? input.has_attorney : null,
    attorney_name: truncate(input.attorney_name, 200),
    attorney_firm: truncate(input.attorney_firm, 200),
    contact_status: CONTACT_STATUS.includes(input.contact_status) ? input.contact_status : 'not_contacted',
    employer: truncate(input.employer, 200),
    occupation: truncate(input.occupation, 200),
    confidence_score: clampNumber(input.confidence_score, 0, 100, 50),
    enrichment_score: clampNumber(input.enrichment_score, 0, 100, 0),
    metadata: typeof input.metadata === 'object' ? JSON.stringify(input.metadata) : input.metadata,
    created_at: toDate(input.created_at) || now,
    updated_at: now,
    ...defaults
  };
  return out;
}

// ── SOURCE REPORT NORMALIZER ──────────────────────────────────
function normalizeSourceReport(input) {
  const now = new Date();
  return {
    id: input.id || null,
    incident_id: input.incident_id,
    data_source_id: input.data_source_id || null,
    source_type: normalizeSourceType(input.source_type),
    source_reference: truncate(input.source_reference, 500),
    raw_data: typeof input.raw_data === 'object' ? JSON.stringify(input.raw_data) : input.raw_data,
    parsed_data: typeof input.parsed_data === 'object' ? JSON.stringify(input.parsed_data) : input.parsed_data,
    contributed_fields: Array.isArray(input.contributed_fields) ? input.contributed_fields : [],
    confidence: clampNumber(input.confidence, 0, 100, 50),
    is_verified: !!input.is_verified,
    fetched_at: toDate(input.fetched_at) || now,
    processed_at: toDate(input.processed_at) || now,
    created_at: toDate(input.created_at) || now
  };
}

// ── Helpers ────────────────────────────────────────────────────
function severityToPriority(sev) {
  return { fatal: 1, critical: 1, serious: 2, moderate: 3, minor: 4, unknown: 5 }[sev] || 5;
}
function clampNumber(v, min, max, def) {
  const n = parseFloat(v);
  if (isNaN(n)) return def;
  return Math.min(max, Math.max(min, n));
}
function nullableInt(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = parseInt(v);
  return isNaN(n) ? null : n;
}
function truncate(v, n) {
  if (!v) return null;
  return String(v).substring(0, n);
}
function toDate(v) {
  if (!v) return null;
  if (v instanceof Date) return v;
  const d = new Date(v);
  return isNaN(d.getTime()) ? null : d;
}

module.exports = {
  // Vocabularies
  SEVERITY, INCIDENT_TYPES, STATUS, QUALIFICATION, PERSON_ROLES, INJURY, CONTACT_STATUS, SOURCE_TYPES,
  // Single-field normalizers
  normalizeSeverity, normalizeIncidentType, normalizeSourceType, normalizeStatus,
  normalizePersonRole, normalizeInjurySeverity,
  normalizePhone, normalizeEmail, normalizeName, normalizeCoord,
  // Object normalizers
  normalizeIncident, normalizePerson, normalizeSourceReport,
  // Helpers
  severityToPriority
};
