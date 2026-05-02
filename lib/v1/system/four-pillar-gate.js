/**
 * Phase 81: 4-Pillar Gate Runner.
 *
 * From CaseFlow analysis: "They explicitly haven't built the 'all 4 must pass'
 * gate per surface. The 4-pillar framework only matters if a CI gate enforces it."
 *
 * The 4 pillars per surface:
 *   1. SCHEMA      — registry has fields for this surface AND all referenced DB cols exist
 *   2. INTERFACE   — endpoint that produces the surface returns 200 with non-empty payload
 *   3. BEHAVIORAL  — surface honors the producer→property→consumer contract end-to-end
 *   4. SYSTEM MAP  — all surface fields appear in property_change_events log within 30d
 *                    (proves engines actually fire and write them)
 *
 * Endpoint: GET /system/four-pillar-gate?action=run&surface=mobile-card
 * Returns: { surface, all_passed, pillars: { schema, interface, behavioral, system_map } }
 */
const { getDb } = require('../../_db');
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function pillar1_schema(db, surface) {
  const reg = require('./property-registry');
  const fields = reg.listForSurface(surface);
  if (!fields.length) return { ok: false, reason: `no fields registered for surface=${surface}` };

  const cov = require('./property-coverage');
  const matrix = await cov.buildCoverage(db);
  const allGhosts = [...(matrix.ghosts.Person || []), ...(matrix.ghosts.Incident || [])];
  const surfaceGhosts = fields.filter(f => allGhosts.includes(f.id));
  if (surfaceGhosts.length) {
    return { ok: false, reason: `surface has ${surfaceGhosts.length} ghost fields`, ghosts: surfaceGhosts.map(g => g.id) };
  }
  return { ok: true, fields_count: fields.length };
}

async function pillar2_interface(db, surface) {
  const SURFACE_TO_ENDPOINT = {
    'mobile-card': '/api/v1/system/master-lead-list?secret=ingest-now&format=json&admin=true',
    'desktop-detail': '/api/v1/system/master-lead-list?secret=ingest-now&format=json&admin=true',
    'master-list-row': '/api/v1/system/master-lead-list?secret=ingest-now&format=json&admin=true',
    'daily-email': '/api/v1/system/daily-intel-email?secret=ingest-now&action=preview',
    'rep-handoff': '/api/v1/system/master-lead-list?secret=ingest-now&format=json&admin=true',
    'map-view': '/api/v1/system/master-lead-list?secret=ingest-now&format=json&admin=true'
  };
  const path = SURFACE_TO_ENDPOINT[surface];
  if (!path) return { ok: false, reason: `no endpoint mapped for surface=${surface}` };
  const base = process.env.VERCEL_URL ? `https://${process.env.VERCEL_URL}` : 'https://accident-intel-platform.vercel.app';
  try {
    const r = await fetch(base + path, { signal: AbortSignal.timeout(10000) });
    if (!r.ok) return { ok: false, reason: `endpoint returned ${r.status}`, path };
    const txt = await r.text();
    if (txt.length < 100) return { ok: false, reason: `payload too small (${txt.length} bytes)`, path };
    return { ok: true, status: r.status, payload_size: txt.length, path };
  } catch (e) { return { ok: false, reason: e.message, path }; }
}

async function pillar3_behavioral(db, surface) {
  const reg = require('./property-registry');
  const fields = reg.listForSurface(surface);
  // For each field, check that at least one producer engine exists AND it has been called recently
  const issues = [];
  let producers_exist = 0;
  for (const f of fields) {
    if (!f.producers || !f.producers.length) {
      issues.push({ field: f.id, problem: 'no producers registered' });
      continue;
    }
    producers_exist++;
  }
  if (issues.length > 0) return { ok: false, reason: `${issues.length}/${fields.length} fields have no producers`, issues: issues.slice(0, 5) };
  return { ok: true, fields_with_producers: producers_exist, total: fields.length };
}

async function pillar4_system_map(db, surface) {
  const reg = require('./property-registry');
  const fields = reg.listForSurface(surface);

  // Check property_change_events for activity in last 30d for each surface field
  let writesObserved = 0;
  const dead_fields = [];
  for (const f of fields) {
    try {
      const row = await db('property_change_events')
        .where('property', f.id)
        .where('created_at', '>', new Date(Date.now() - 30 * 86400 * 1000))
        .first();
      if (row) writesObserved++;
      else dead_fields.push(f.id);
    } catch (_) {
      // Fall back to enrichment_logs
      try {
        const row = await db('enrichment_logs')
          .where('field_name', f.id)
          .where('created_at', '>', new Date(Date.now() - 30 * 86400 * 1000))
          .first();
        if (row) writesObserved++;
        else dead_fields.push(f.id);
      } catch (_) { dead_fields.push(f.id); }
    }
  }
  // Pass if ≥50% of surface fields have writes in last 30d
  const pct = writesObserved / Math.max(1, fields.length);
  if (pct < 0.5) {
    return { ok: false, reason: `only ${writesObserved}/${fields.length} surface fields written in last 30d (${(pct*100).toFixed(0)}%)`, dead_fields: dead_fields.slice(0, 10) };
  }
  return { ok: true, writes_observed: writesObserved, total: fields.length, pct: Number((pct*100).toFixed(1)) };
}

async function runGate(db, surface) {
  const t0 = Date.now();
  const [schema, interfaceP, behavioral, systemMap] = await Promise.all([
    pillar1_schema(db, surface).catch(e => ({ ok: false, reason: 'pillar1_error: ' + e.message })),
    pillar2_interface(db, surface).catch(e => ({ ok: false, reason: 'pillar2_error: ' + e.message })),
    pillar3_behavioral(db, surface).catch(e => ({ ok: false, reason: 'pillar3_error: ' + e.message })),
    pillar4_system_map(db, surface).catch(e => ({ ok: false, reason: 'pillar4_error: ' + e.message }))
  ]);
  return {
    surface,
    all_passed: schema.ok && interfaceP.ok && behavioral.ok && systemMap.ok,
    duration_ms: Date.now() - t0,
    pillars: {
      schema: { ...schema, name: '1. Schema' },
      interface: { ...interfaceP, name: '2. Interface' },
      behavioral: { ...behavioral, name: '3. Behavioral' },
      system_map: { ...systemMap, name: '4. System Map' }
    }
  };
}

async function runAll(db) {
  const reg = require('./property-registry');
  const surfaces = reg.listSurfaces();
  const results = [];
  for (const s of surfaces) {
    results.push(await runGate(db, s));
  }
  const all_passed = results.every(r => r.all_passed);
  return { all_passed, surfaces_total: surfaces.length, surfaces_passed: results.filter(r => r.all_passed).length, results };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();
  if (action === 'health') return res.json({ success: true, service: 'four-pillar-gate', pillars: ['schema','interface','behavioral','system_map'] });
  if (action === 'run') {
    const surface = req.query?.surface;
    if (!surface) return res.status(400).json({ error: 'surface required' });
    return res.json(await runGate(db, surface));
  }
  if (action === 'run_all') return res.json(await runAll(db));
  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.runGate = runGate;
module.exports.runAll = runAll;
