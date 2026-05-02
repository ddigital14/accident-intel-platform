/**
 * Phase 77: Property Registry — single source of truth for every field
 * the platform tracks across persons, incidents, and enrichment.
 *
 * Inspired by Salesforce/HubSpot CRM property architecture:
 *   - Every field has: id, label, data_type, producers, consumers,
 *     validation_regex, confidence_weight per producer
 *   - Engines REGISTER their producers/consumers — the registry is the
 *     source of truth for "what fields exist + which engine knows them best"
 *   - UI components query the registry to know how to render a value
 *     and where it came from
 *
 * This unblocks: smart provenance, automatic UI rendering, schema-level
 * type checking, propagation graph visualization, dead-engine detection.
 */
const { getDb } = require('../../_db');
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

const PROPERTIES = {
  // ── IDENTITY ──
  full_name: {
    label: 'Full name',
    type: 'string',
    validation: /^.{2,255}$/,
    producers: [
      { engine: 'pdl-identify', weight: 9 },
      { engine: 'apollo-match', weight: 9 },
      { engine: 'trestle-phone', weight: 8 },
      { engine: 'voter-rolls', weight: 8 },
      { engine: 'people-search-multi', weight: 6 },
      { engine: 'opencnam', weight: 5 },
      { engine: 'osint-miner', weight: 4 }
    ],
    consumers: ['victim-resolver', 'family-graph', 'attorney-cross-link', 'hypothesis-generator', 'evidence-cross-checker']
  },
  date_of_birth: {
    label: 'Date of birth',
    type: 'date',
    producers: [
      { engine: 'pdl-identify', weight: 9 },
      { engine: 'voter-rolls', weight: 9 }
    ],
    consumers: ['evidence-cross-checker', 'person-merge-finder']
  },
  age: {
    label: 'Age',
    type: 'integer',
    producers: [
      { engine: 'pdl-identify', weight: 7 },
      { engine: 'whitepages-scrape', weight: 6 }
    ],
    consumers: ['person-merge-finder']
  },

  // ── CONTACT ──
  phone: {
    label: 'Phone (E.164)',
    type: 'phone',
    validation: /^\+1\d{10}$/,
    producers: [
      { engine: 'apollo-unlock', weight: 9 },
      { engine: 'trestle-phone', weight: 9 },
      { engine: 'pdl-identify', weight: 8 },
      { engine: 'people-search-multi', weight: 5 },
      { engine: 'osint-miner', weight: 4 },
      { engine: 'text-extractors', weight: 4 }
    ],
    consumers: [
      'opencnam', 'numverify', 'twilio-lookup', 'fcc-carrier',
      'trestle-phone', 'pdl-by-phone', 'apollo-unlock',
      'adversarial-cross-check', 'evidence-cross-checker'
    ]
  },
  email: {
    label: 'Email',
    type: 'email',
    validation: /^[^@\s]+@[^@\s]+\.[^@\s]+$/,
    producers: [
      { engine: 'apollo-unlock', weight: 9 },
      { engine: 'pdl-identify', weight: 9 },
      { engine: 'hunter-domain', weight: 7 },
      { engine: 'trestle-phone', weight: 6 }
    ],
    consumers: ['hunter-verify', 'pdl-by-email', 'apollo-unlock', 'adversarial-cross-check']
  },
  address: {
    label: 'Address',
    type: 'address',
    producers: [
      { engine: 'trestle-phone', weight: 9 },
      { engine: 'pdl-identify', weight: 8 },
      { engine: 'voter-rolls', weight: 8 },
      { engine: 'maricopa-property', weight: 9 },
      { engine: 'fulton-property', weight: 9 },
      { engine: 'text-extractors', weight: 5 }
    ],
    consumers: ['usps-validate', 'geocoder', 'co-residence', 'census-income', 'adversarial-cross-check']
  },
  city: { label: 'City', type: 'string', producers: [], consumers: ['geocoder', 'pattern-miner'] },
  state: { label: 'State (2-letter)', type: 'string', validation: /^[A-Z]{2}$/, producers: [], consumers: ['voter-rolls', 'state-courts', 'pattern-miner'] },
  zip: { label: 'ZIP', type: 'string', producers: [{ engine: 'usps-validate', weight: 10 }], consumers: ['geocoder'] },
  lat: { label: 'Latitude', type: 'float', producers: [{ engine: 'geocoder', weight: 10 }], consumers: ['census-income', 'adversarial-cross-check'] },
  lon: { label: 'Longitude', type: 'float', producers: [{ engine: 'geocoder', weight: 10 }], consumers: ['census-income', 'adversarial-cross-check'] },

  // ── EMPLOYMENT ──
  employer: {
    label: 'Employer',
    type: 'string',
    producers: [
      { engine: 'apollo-match', weight: 9 },
      { engine: 'pdl-identify', weight: 8 },
      { engine: 'hunter-verify', weight: 7 }
    ],
    consumers: ['apollo-unlock', 'hunter-domain', 'pattern-miner']
  },

  // ── LEGAL ──
  attorney_firm: {
    label: 'Attorney firm',
    type: 'string',
    producers: [
      { engine: 'state-courts', weight: 9 },
      { engine: 'courtlistener', weight: 9 }
    ],
    consumers: ['attorney-cross-link', 'pattern-miner']
  },
  has_attorney: {
    label: 'Has attorney',
    type: 'boolean',
    producers: [{ engine: 'state-courts', weight: 8 }, { engine: 'courtlistener', weight: 8 }],
    consumers: ['lead-quality-scorer']
  },

  // ── VEHICLE ──
  vehicle_vin: {
    label: 'VIN',
    type: 'string',
    validation: /^[A-HJ-NPR-Z0-9]{17}$/,
    producers: [{ engine: 'text-extractors', weight: 7 }],
    consumers: ['nhtsa-vin', 'fars', 'vehicle-owner']
  },
  vehicle_plate: {
    label: 'License plate',
    type: 'string',
    producers: [{ engine: 'text-extractors', weight: 6 }],
    consumers: ['vehicle-owner', 'fars']
  },

  // ── ENRICHMENT METADATA ──
  victim_verified: {
    label: 'Victim verified (Stage A+B)',
    type: 'boolean',
    producers: [{ engine: 'victim-verifier', weight: 10 }],
    consumers: ['lead-quality-scorer', 'qualify']
  },
  lead_score: {
    label: 'Lead score (0-100)',
    type: 'integer',
    producers: [{ engine: 'qualify', weight: 9 }, { engine: 'pattern-miner', weight: 7 }],
    consumers: ['auto-assign', 'master-lead-list']
  },
  master_quality_score: {
    label: 'Master quality score (0-100)',
    type: 'integer',
    producers: [{ engine: 'lead-quality-scorer', weight: 10 }],
    consumers: ['master-lead-list', 'auto-assign']
  },
  lead_tier: {
    label: 'Lead tier',
    type: 'enum',
    enum_values: ['normal', 'review', 'demoted'],
    producers: [{ engine: 'relationship-detector', weight: 10 }],
    consumers: ['master-lead-list', 'daily-intel-email']
  },
  cross_engine_conflict: {
    label: 'Cross-engine conflict flag',
    type: 'boolean',
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

function bestProducerFor(field) {
  const p = PROPERTIES[field];
  if (!p?.producers?.length) return null;
  return [...p.producers].sort((a, b) => b.weight - a.weight)[0];
}

function consumersOf(field) {
  return PROPERTIES[field]?.consumers || [];
}

function dependencyGraph() {
  // Returns adjacency list: produces what, consumes what
  const graph = { nodes: {}, edges: [] };
  for (const [id, p] of Object.entries(PROPERTIES)) {
    graph.nodes[id] = { id, label: p.label, type: p.type };
    for (const prod of p.producers || []) {
      graph.edges.push({ from: prod.engine, to: id, kind: 'produces', weight: prod.weight });
    }
    for (const cons of p.consumers || []) {
      graph.edges.push({ from: id, to: cons, kind: 'consumes-by' });
    }
  }
  return graph;
}

function validateValue(field, value) {
  const p = PROPERTIES[field];
  if (!p) return { ok: false, error: 'unknown_field' };
  if (value == null || value === '') return { ok: true, empty: true };
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

  if (action === 'health') return res.json({ success: true, service: 'property-registry', total_properties: Object.keys(PROPERTIES).length });
  if (action === 'list') return res.json({ success: true, total: Object.keys(PROPERTIES).length, properties: listAll() });
  if (action === 'describe') {
    const field = req.query?.field;
    if (!field) return res.status(400).json({ error: 'field required' });
    const d = describe(field);
    if (!d) return res.status(404).json({ error: 'unknown field' });
    return res.json(d);
  }
  if (action === 'best_producer') {
    const field = req.query?.field;
    return res.json({ field, best: bestProducerFor(field) });
  }
  if (action === 'consumers') {
    const field = req.query?.field;
    return res.json({ field, consumers: consumersOf(field) });
  }
  if (action === 'graph') return res.json(dependencyGraph());
  if (action === 'validate') {
    const field = req.query?.field;
    const value = req.query?.value;
    return res.json({ field, value, ...validateValue(field, value) });
  }
  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.PROPERTIES = PROPERTIES;
module.exports.describe = describe;
module.exports.bestProducerFor = bestProducerFor;
module.exports.consumersOf = consumersOf;
module.exports.validateValue = validateValue;
