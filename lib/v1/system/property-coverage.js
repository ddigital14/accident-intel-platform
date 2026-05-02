/**
 * Phase 79: Property Coverage Matrix.
 *
 * From CaseFlow analysis: "DB column with no surface = dead data; surface field
 * with no DB column = ghost UI". This endpoint queries Postgres + property
 * registry and surfaces three classes of platform-wide bugs:
 *
 *   1. ORPHAN COLUMNS — DB columns that exist but aren't in the registry
 *      (we're storing data nothing renders → potential dead data or PII risk)
 *
 *   2. GHOST FIELDS — registry entries with no DB column to back them
 *      (UI tries to render but nothing exists → silent display bugs)
 *
 *   3. ENGINE-LESS FIELDS — registry entries with no producer engines
 *      (we declare we'll show this field but no engine writes it → forever empty)
 *
 *   4. SURFACE-LESS FIELDS — registry entries with empty surfaces[]
 *      (we declare the field but never render it → silent waste)
 */
const { getDb } = require('../../_db');
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function getDbColumns(db, table) {
  const rows = await db.raw(`
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = ?
    ORDER BY ordinal_position
  `, [table]);
  return rows.rows || rows;
}

async function buildCoverage(db) {
  const reg = require('./property-registry');
  const props = reg.PROPERTIES;
  const allRegFields = Object.keys(props);
  const personRegFields = allRegFields.filter(k => props[k].entity === 'Person');
  const incidentRegFields = allRegFields.filter(k => props[k].entity === 'Incident');

  const personCols = await getDbColumns(db, 'persons').catch(() => []);
  const incidentCols = await getDbColumns(db, 'incidents').catch(() => []);
  const personColNames = personCols.map(c => c.column_name);
  const incidentColNames = incidentCols.map(c => c.column_name);

  // 1. ORPHANS — DB columns not in registry
  const ignoreCols = new Set(['id', 'created_at', 'updated_at', 'incident_id', 'victim_id',
    'first_name', 'last_name', 'role', 'is_injured', 'gender', 'phone_secondary',
    'injury_severity', 'injury_description', 'transported_to', 'transported_by',
    'treatment_status', 'insurance_company', 'insurance_policy_number',
    'insurance_type', 'policy_limits', 'policy_limits_bodily_injury',
    'policy_limits_property', 'insurance_claim_number', 'insurance_agent',
    'insurance_agent_phone', 'vehicle_id', 'attorney_name', 'attorney_phone',
    'incident_number', 'incident_type', 'status', 'priority', 'confidence_score',
    'street', 'county', 'highway', 'mile_marker', 'intersection',
    'damage_severity', 'occurred_at', 'discovered_at', 'source_count',
    'description', 'raw_description', 'damage_description',
    'victim_verifier_reason', 'victim_verifier_stage',
    'relationship_to_victim', 'metro_area_id', 'assigned_to', 'assigned_at',
    'cost_estimate_usd', 'enrichment_data', 'last_enriched_at',
    'identity_confidence', 'confidence',
    // Phase 78+ additions
    'lead_tier', 'notes',
    // Phase 65 geo
    'lat', 'lon'
  ]);

  const personOrphans = personColNames.filter(c => !ignoreCols.has(c) && !personRegFields.includes(c));
  const incidentOrphans = incidentColNames.filter(c => !ignoreCols.has(c) && !incidentRegFields.includes(c));

  // 2. GHOSTS — registry entries with no DB column
  // Phase 80b: incident_ prefixed registry fields map to unprefixed DB columns
  function aliasFor(f) {
    if (f.startsWith('incident_')) return f.replace(/^incident_/, '');
    return f;
  }
  const personGhosts = personRegFields.filter(f => !personColNames.includes(f) && !personColNames.includes(aliasFor(f)));
  const incidentGhosts = incidentRegFields.filter(f => !incidentColNames.includes(f) && !incidentColNames.includes(aliasFor(f)));

  // 3. ENGINE-LESS — registry entries with empty producers[]
  const engineLess = allRegFields.filter(f => !props[f].producers || props[f].producers.length === 0);

  // 4. SURFACE-LESS — registry entries with empty surfaces[]
  const surfaceLess = allRegFields.filter(f => !props[f].surfaces || props[f].surfaces.length === 0);

  // 5. SURFACE COVERAGE — for each surface, count fields and entity breakdown
  const surfaces = reg.listSurfaces();
  const surfaceCoverage = {};
  for (const s of surfaces) {
    const fields = reg.listForSurface(s);
    surfaceCoverage[s] = {
      total: fields.length,
      person: fields.filter(f => f.entity === 'Person').length,
      incident: fields.filter(f => f.entity === 'Incident').length
    };
  }

  // 6. PRODUCER FANOUT — engines with how many fields each can write
  const producerCounts = {};
  for (const [field, p] of Object.entries(props)) {
    for (const prod of (p.producers || [])) {
      producerCounts[prod.engine] = producerCounts[prod.engine] || { engine: prod.engine, fields: [], total_weight: 0 };
      producerCounts[prod.engine].fields.push(field);
      producerCounts[prod.engine].total_weight += prod.weight;
    }
  }
  const topProducers = Object.values(producerCounts)
    .sort((a, b) => b.fields.length - a.fields.length)
    .slice(0, 15);

  return {
    timestamp: new Date().toISOString(),
    summary: {
      registry_fields: allRegFields.length,
      person_fields: personRegFields.length,
      incident_fields: incidentRegFields.length,
      db_person_cols: personColNames.length,
      db_incident_cols: incidentColNames.length,
      orphan_count: personOrphans.length + incidentOrphans.length,
      ghost_count: personGhosts.length + incidentGhosts.length,
      engine_less_count: engineLess.length,
      surface_less_count: surfaceLess.length
    },
    orphans: {
      Person: personOrphans,
      Incident: incidentOrphans,
      note: 'DB columns that exist but are NOT in property registry. Audit for PII/dead data.'
    },
    ghosts: {
      Person: personGhosts,
      Incident: incidentGhosts,
      note: 'Registry fields with NO matching DB column. Likely silent display bugs.'
    },
    engine_less: {
      fields: engineLess,
      note: 'Registry fields with NO producer engines. Will be forever empty unless an engine is added.'
    },
    surface_less: {
      fields: surfaceLess,
      note: 'Registry fields rendered NOWHERE. Either add to a surface or drop from registry.'
    },
    surface_coverage: surfaceCoverage,
    top_producers: topProducers
  };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'matrix').toLowerCase();
  if (action === 'health') return res.json({ success: true, service: 'property-coverage' });
  if (action === 'matrix') return res.json(await buildCoverage(db));
  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.buildCoverage = buildCoverage;
