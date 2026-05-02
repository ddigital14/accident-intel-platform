/**
 * Phase 78: Property Registry v2 — entity + surfaces[] + defaults + audit/public flags.
 * Salesforce/HubSpot-grade: every field declares which UI surfaces render it,
 * which entity it belongs to, defaults, audit/public flags.
 */
const { getDb } = require('../../_db');
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

const PROPERTIES = {
  full_name: {
    entity: 'Person', label: 'Full name', type: 'string', validation: /^.{2,255}$/, default: null,
    isReportableToRep: true, isAuditable: true, isPublic: false,
    surfaces: ['mobile-card', 'desktop-detail', 'daily-email', 'master-list-row', 'rep-handoff'],
    producers: [
      { engine: 'pdl-identify', weight: 9 }, { engine: 'apollo-match', weight: 9 },
      { engine: 'trestle-phone', weight: 8 }, { engine: 'voter-rolls', weight: 8 },
      { engine: 'people-search-multi', weight: 6 }, { engine: 'opencnam', weight: 5 },
      { engine: 'osint-miner', weight: 4 }
    ],
    consumers: ['victim-resolver', 'family-graph', 'attorney-cross-link', 'hypothesis-generator', 'evidence-cross-checker']
  },
  date_of_birth: {
    entity: 'Person', label: 'Date of birth', type: 'date', default: null,
    isReportableToRep: true, isAuditable: true, isPublic: false,
    surfaces: ['desktop-detail', 'rep-handoff'],
    producers: [{ engine: 'pdl-identify', weight: 9 }, { engine: 'voter-rolls', weight: 9 }],
    consumers: ['evidence-cross-checker', 'person-merge-finder']
  },
  age: {
    entity: 'Person', label: 'Age', type: 'integer', default: null,
    isReportableToRep: true, isAuditable: false, isPublic: false,
    surfaces: ['mobile-card', 'desktop-detail', 'daily-email', 'master-list-row'],
    producers: [{ engine: 'pdl-identify', weight: 7 }, { engine: 'whitepages-scrape', weight: 6 }],
    consumers: ['person-merge-finder']
  },
  phone: {
    entity: 'Person', label: 'Phone (E.164)', type: 'phone', validation: /^\+1\d{10}$/, default: null,
    isReportableToRep: true, isAuditable: true, isPublic: false,
    surfaces: ['mobile-card', 'desktop-detail', 'daily-email', 'master-list-row', 'rep-handoff'],
    producers: [
      { engine: 'apollo-unlock', weight: 9 }, { engine: 'trestle-phone', weight: 9 },
      { engine: 'pdl-identify', weight: 8 }, { engine: 'people-search-multi', weight: 5 },
      { engine: 'osint-miner', weight: 4 }, { engine: 'text-extractors', weight: 4 }
    ],
    consumers: ['opencnam', 'numverify', 'twilio-lookup', 'fcc-carrier', 'trestle-phone', 'pdl-by-phone', 'apollo-unlock', 'adversarial-cross-check', 'evidence-cross-checker']
  },
  email: {
    entity: 'Person', label: 'Email', type: 'email', validation: /^[^@\s]+@[^@\s]+\.[^@\s]+$/, default: null,
    isReportableToRep: true, isAuditable: true, isPublic: false,
    surfaces: ['mobile-card', 'desktop-detail', 'daily-email', 'master-list-row', 'rep-handoff'],
    producers: [{ engine: 'apollo-unlock', weight: 9 }, { engine: 'pdl-identify', weight: 9 }, { engine: 'hunter-domain', weight: 7 }, { engine: 'trestle-phone', weight: 6 }],
    consumers: ['hunter-verify', 'pdl-by-email', 'apollo-unlock', 'adversarial-cross-check']
  },
  address: {
    entity: 'Person', label: 'Address', type: 'address', default: null,
    isReportableToRep: true, isAuditable: true, isPublic: false,
    surfaces: ['desktop-detail', 'daily-email', 'rep-handoff'],
    producers: [
      { engine: 'trestle-phone', weight: 9 }, { engine: 'pdl-identify', weight: 8 },
      { engine: 'voter-rolls', weight: 8 }, { engine: 'maricopa-property', weight: 9 },
      { engine: 'fulton-property', weight: 9 }, { engine: 'address-sonnet-extractor', weight: 7 },
      { engine: 'text-extractors', weight: 5 }
    ],
    consumers: ['usps-validate', 'geocoder', 'co-residence', 'census-income', 'adversarial-cross-check']
  },
  city: {
    entity: 'Person', label: 'City', type: 'string', default: null,
    isReportableToRep: true, isAuditable: false, isPublic: true,
    surfaces: ['mobile-card', 'desktop-detail', 'daily-email', 'master-list-row'],
    producers: [
      { engine: 'pd-press', weight: 8 }, { engine: 'news-rss', weight: 7 },
      { engine: 'nyc-open-data', weight: 9 }, { engine: 'state-crash', weight: 9 },
      { engine: 'pdl-identify', weight: 7 }, { engine: 'voter-rolls', weight: 8 }
    ],
    consumers: ['geocoder', 'pattern-miner']
  },
  state: {
    entity: 'Person', label: 'State (2-letter)', type: 'string', validation: /^[A-Z]{2}$/, default: null,
    isReportableToRep: true, isAuditable: false, isPublic: true,
    surfaces: ['mobile-card', 'desktop-detail', 'daily-email', 'master-list-row'],
    producers: [
      { engine: 'pd-press', weight: 8 }, { engine: 'news-rss', weight: 7 },
      { engine: 'nyc-open-data', weight: 9 }, { engine: 'state-crash', weight: 9 },
      { engine: 'pdl-identify', weight: 7 }, { engine: 'voter-rolls', weight: 8 }
    ],
    consumers: ['voter-rolls', 'state-courts', 'pattern-miner']
  },
  zip: {
    entity: 'Person', label: 'ZIP', type: 'string', default: null,
    isReportableToRep: true, isAuditable: false, isPublic: true,
    surfaces: ['desktop-detail'],
    producers: [{ engine: 'usps-validate', weight: 10 }], consumers: ['geocoder']
  },
  lat: {
    entity: 'Person', label: 'Latitude', type: 'float', default: null,
    isReportableToRep: false, isAuditable: false, isPublic: false,
    surfaces: ['map-view', 'desktop-detail'],
    producers: [{ engine: 'geocoder', weight: 10 }],
    consumers: ['census-income', 'adversarial-cross-check']
  },
  lon: {
    entity: 'Person', label: 'Longitude', type: 'float', default: null,
    isReportableToRep: false, isAuditable: false, isPublic: false,
    surfaces: ['map-view', 'desktop-detail'],
    producers: [{ engine: 'geocoder', weight: 10 }],
    consumers: ['census-income', 'adversarial-cross-check']
  },
  employer: {
    entity: 'Person', label: 'Employer', type: 'string', default: null,
    isReportableToRep: true, isAuditable: false, isPublic: false,
    surfaces: ['desktop-detail', 'rep-handoff'],
    producers: [{ engine: 'apollo-match', weight: 9 }, { engine: 'pdl-identify', weight: 8 }, { engine: 'hunter-verify', weight: 7 }],
    consumers: ['apollo-unlock', 'hunter-domain', 'pattern-miner']
  },
  attorney_firm: {
    entity: 'Person', label: 'Attorney firm', type: 'string', default: null,
    isReportableToRep: true, isAuditable: true, isPublic: false,
    surfaces: ['desktop-detail', 'daily-email', 'rep-handoff'],
    producers: [{ engine: 'state-courts', weight: 9 }, { engine: 'courtlistener', weight: 9 }],
    consumers: ['attorney-cross-link', 'pattern-miner']
  },
  has_attorney: {
    entity: 'Person', label: 'Has attorney', type: 'boolean', default: false,
    isReportableToRep: true, isAuditable: true, isPublic: false,
    surfaces: ['mobile-card', 'desktop-detail', 'master-list-row'],
    producers: [{ engine: 'state-courts', weight: 8 }, { engine: 'courtlistener', weight: 8 }],
    consumers: ['lead-quality-scorer']
  },
  vehicle_vin: {
    entity: 'Person', label: 'VIN', type: 'string', validation: /^[A-HJ-NPR-Z0-9]{17}$/, default: null,
    isReportableToRep: true, isAuditable: false, isPublic: false,
    surfaces: ['desktop-detail'],
    producers: [{ engine: 'text-extractors', weight: 7 }],
    consumers: ['nhtsa-vin', 'fars', 'vehicle-owner']
  },
  vehicle_plate: {
    entity: 'Person', label: 'License plate', type: 'string', default: null,
    isReportableToRep: true, isAuditable: false, isPublic: false,
    surfaces: ['desktop-detail'],
    producers: [{ engine: 'text-extractors', weight: 6 }],
    consumers: ['vehicle-owner', 'fars']
  },
  victim_verified: {
    entity: 'Person', label: 'Victim verified', type: 'boolean', default: false,
    isReportableToRep: true, isAuditable: true, isPublic: false,
    surfaces: ['mobile-card', 'desktop-detail', 'daily-email', 'master-list-row'],
    producers: [{ engine: 'victim-verifier', weight: 10 }],
    consumers: ['lead-quality-scorer', 'qualify']
  },
  lead_tier: {
    entity: 'Person', label: 'Lead tier', type: 'enum',
    enum_values: ['normal', 'review', 'demoted'], default: 'normal',
    isReportableToRep: true, isAuditable: true, isPublic: false,
    surfaces: ['mobile-card', 'desktop-detail', 'daily-email', 'master-list-row'],
    producers: [{ engine: 'relationship-detector', weight: 10 }],
    consumers: ['master-lead-list', 'daily-intel-email', 'auto-assign']
  },
  notes: {
    entity: 'Person', label: 'Discrepancy / rep notes', type: 'string', default: null,
    isReportableToRep: true, isAuditable: true, isPublic: false,
    surfaces: ['mobile-card', 'desktop-detail', 'daily-email'],
    producers: [{ engine: 'relationship-detector', weight: 10 }], consumers: []
  },
  severity: {
    entity: 'Incident', label: 'Severity', type: 'enum',
    enum_values: ['fatal', 'critical', 'serious', 'moderate', 'minor', 'unknown'], default: 'unknown',
    isReportableToRep: true, isAuditable: false, isPublic: true,
    surfaces: ['mobile-card', 'desktop-detail', 'daily-email', 'master-list-row', 'map-view'],
    producers: [{ engine: 'news-rss', weight: 7 }, { engine: 'pd-press', weight: 8 }, { engine: 'state-crash', weight: 9 }],
    consumers: ['qualify', 'pattern-miner', 'lead-quality-scorer']
  },
  fatalities_count: {
    entity: 'Incident', label: 'Fatalities count', type: 'integer', default: 0,
    isReportableToRep: true, isAuditable: true, isPublic: true,
    surfaces: ['desktop-detail', 'master-list-row'],
    producers: [{ engine: 'news-rss', weight: 8 }, { engine: 'pd-press', weight: 9 }],
    consumers: ['lead-quality-scorer']
  },
  // ── INCIDENT · GEO + LOCATION (mirrored from DB) ──
  incident_address: {
    entity: 'Incident', label: 'Incident address', type: 'address', default: null,
    isReportableToRep: true, isAuditable: false, isPublic: true,
    surfaces: ['desktop-detail', 'master-list-row', 'map-view'],
    producers: [{ engine: 'news-rss', weight: 6 }, { engine: 'pd-press', weight: 8 }, { engine: 'state-crash', weight: 9 }],
    consumers: ['geocoder', 'pattern-miner']
  },
  incident_city: {
    entity: 'Incident', label: 'Incident city', type: 'string', default: null,
    isReportableToRep: true, isAuditable: false, isPublic: true,
    surfaces: ['mobile-card', 'desktop-detail', 'daily-email', 'master-list-row', 'map-view'],
    producers: [
      { engine: 'nyc-open-data', weight: 9 }, { engine: 'state-crash', weight: 9 },
      { engine: 'news-rss', weight: 7 }, { engine: 'pd-press', weight: 8 },
      { engine: 'spanish-news', weight: 7 }, { engine: 'ntsb-aviation', weight: 7 }
    ],
    consumers: ['pattern-miner']
  },
  incident_state: {
    entity: 'Incident', label: 'Incident state', type: 'string', default: null,
    isReportableToRep: true, isAuditable: false, isPublic: true,
    surfaces: ['mobile-card', 'desktop-detail', 'daily-email', 'master-list-row', 'map-view'],
    producers: [
      { engine: 'nyc-open-data', weight: 9 }, { engine: 'state-crash', weight: 9 },
      { engine: 'news-rss', weight: 7 }, { engine: 'pd-press', weight: 8 },
      { engine: 'spanish-news', weight: 7 }, { engine: 'ntsb-aviation', weight: 7 }
    ],
    consumers: ['pattern-miner', 'voter-rolls']
  },
  latitude: {
    entity: 'Incident', label: 'Incident latitude', type: 'float', default: null,
    isReportableToRep: false, isAuditable: false, isPublic: false,
    surfaces: ['map-view'],
    producers: [{ engine: 'geocoder', weight: 9 }, { engine: 'state-crash', weight: 9 }],
    consumers: ['adversarial-cross-check']
  },
  longitude: {
    entity: 'Incident', label: 'Incident longitude', type: 'float', default: null,
    isReportableToRep: false, isAuditable: false, isPublic: false,
    surfaces: ['map-view'],
    producers: [{ engine: 'geocoder', weight: 9 }, { engine: 'state-crash', weight: 9 }],
    consumers: ['adversarial-cross-check']
  },

  qualification_state: {
    entity: 'Incident', label: 'Qualification state', type: 'enum',
    enum_values: ['pending', 'pending_named', 'pending_review', 'qualified'], default: 'pending',
    isReportableToRep: true, isAuditable: true, isPublic: false,
    surfaces: ['mobile-card', 'desktop-detail', 'daily-email', 'master-list-row'],
    producers: [{ engine: 'qualify', weight: 10 }],
    consumers: ['master-lead-list', 'daily-intel-email', 'auto-assign']
  },
  lead_score: {
    entity: 'Incident', label: 'Lead score (0-100)', type: 'integer', default: 0,
    isReportableToRep: true, isAuditable: false, isPublic: false,
    surfaces: ['mobile-card', 'desktop-detail', 'daily-email', 'master-list-row'],
    producers: [{ engine: 'qualify', weight: 9 }, { engine: 'pattern-miner', weight: 7 }],
    consumers: ['auto-assign', 'master-lead-list', 'lead-quality-scorer']
  },
  master_quality_score: {
    entity: 'Incident', label: 'Master quality score (0-100)', type: 'integer', default: 0,
    isReportableToRep: true, isAuditable: false, isPublic: false,
    surfaces: ['mobile-card', 'desktop-detail', 'daily-email', 'master-list-row'],
    producers: [{ engine: 'lead-quality-scorer', weight: 10 }],
    consumers: ['master-lead-list', 'auto-assign']
  },
  cross_engine_conflict: {
    entity: 'Incident', label: 'Cross-engine conflict flag', type: 'boolean', default: false,
    isReportableToRep: true, isAuditable: true, isPublic: false,
    surfaces: ['desktop-detail', 'daily-email'],
    producers: [{ engine: 'evidence-cross-checker', weight: 10 }, { engine: 'adversarial-cross-check', weight: 10 }],
    consumers: ['lead-quality-scorer']
  }
};

function describe(field) {
  const p = PROPERTIES[field];
  if (!p) return null;
  return { id: field, ...p, validation: p.validation ? p.validation.source : null };
}
function listAll() {
  return Object.entries(PROPERTIES).map(([id, p]) => ({ id, ...p, validation: p.validation ? p.validation.source : null }));
}
function listByEntity(entity) {
  return Object.entries(PROPERTIES).filter(([_, p]) => p.entity === entity).map(([id, p]) => ({ id, ...p, validation: p.validation ? p.validation.source : null }));
}
function listForSurface(surface) {
  return Object.entries(PROPERTIES).filter(([_, p]) => (p.surfaces || []).includes(surface)).map(([id, p]) => ({ id, ...p, validation: p.validation ? p.validation.source : null }));
}
function listEntities() {
  const e = new Set();
  for (const p of Object.values(PROPERTIES)) if (p.entity) e.add(p.entity);
  return [...e];
}
function listSurfaces() {
  const s = new Set();
  for (const p of Object.values(PROPERTIES)) for (const x of (p.surfaces || [])) s.add(x);
  return [...s];
}
function bestProducerFor(field) {
  const p = PROPERTIES[field];
  if (!p?.producers?.length) return null;
  return [...p.producers].sort((a, b) => b.weight - a.weight)[0];
}
function consumersOf(field) { return PROPERTIES[field]?.consumers || []; }
function dependencyGraph() {
  const graph = { nodes: {}, edges: [] };
  for (const [id, p] of Object.entries(PROPERTIES)) {
    graph.nodes[id] = { id, label: p.label, type: p.type, entity: p.entity };
    for (const prod of p.producers || []) graph.edges.push({ from: prod.engine, to: id, kind: 'produces', weight: prod.weight });
    for (const cons of p.consumers || []) graph.edges.push({ from: id, to: cons, kind: 'consumes-by' });
  }
  return graph;
}
function validateValue(field, value) {
  const p = PROPERTIES[field];
  if (!p) return { ok: false, error: 'unknown_field' };
  if (value == null || value === '') return { ok: true, empty: true, default: p.default };
  if (p.validation && !p.validation.test(String(value))) return { ok: false, error: 'failed_validation', regex: p.validation.source };
  if (p.type === 'integer' && !Number.isInteger(Number(value))) return { ok: false, error: 'not_integer' };
  if (p.type === 'float' && isNaN(Number(value))) return { ok: false, error: 'not_float' };
  if (p.type === 'enum' && !p.enum_values?.includes(value)) return { ok: false, error: 'not_in_enum', allowed: p.enum_values };
  return { ok: true };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  const action = (req.query?.action || 'list').toLowerCase();

  if (action === 'health') return res.json({ success: true, service: 'property-registry-v2', total_properties: Object.keys(PROPERTIES).length, entities: listEntities(), surfaces: listSurfaces() });
  if (action === 'list') return res.json({ success: true, total: Object.keys(PROPERTIES).length, properties: listAll() });
  if (action === 'describe') {
    const field = req.query?.field;
    if (!field) return res.status(400).json({ error: 'field required' });
    const d = describe(field);
    if (!d) return res.status(404).json({ error: 'unknown field' });
    return res.json(d);
  }
  if (action === 'for_entity') {
    const entity = req.query?.entity;
    if (!entity) return res.status(400).json({ error: 'entity required' });
    return res.json({ entity, properties: listByEntity(entity) });
  }
  if (action === 'for_surface') {
    const surface = req.query?.surface;
    if (!surface) return res.status(400).json({ error: 'surface required' });
    return res.json({ surface, properties: listForSurface(surface) });
  }
  if (action === 'entities') return res.json({ entities: listEntities() });
  if (action === 'surfaces') return res.json({ surfaces: listSurfaces() });
  if (action === 'best_producer') return res.json({ field: req.query?.field, best: bestProducerFor(req.query?.field) });
  if (action === 'consumers') return res.json({ field: req.query?.field, consumers: consumersOf(req.query?.field) });
  if (action === 'graph') return res.json(dependencyGraph());
  if (action === 'validate') return res.json({ field: req.query?.field, value: req.query?.value, ...validateValue(req.query?.field, req.query?.value) });

  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.PROPERTIES = PROPERTIES;
module.exports.describe = describe;
module.exports.listAll = listAll;
module.exports.listByEntity = listByEntity;
module.exports.listForSurface = listForSurface;
module.exports.listEntities = listEntities;
module.exports.listSurfaces = listSurfaces;
module.exports.bestProducerFor = bestProducerFor;
module.exports.consumersOf = consumersOf;
module.exports.validateValue = validateValue;
