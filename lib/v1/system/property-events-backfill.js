/**
 * Phase 82: Backfill property_change_events from existing enrichment_logs.
 * The events table is brand-new (Phase 79). Most engine writes in the past
 * went to enrichment_logs. This scans the last N days and synthesizes events.
 */
const { getDb } = require('../../_db');
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function backfill(db, days = 30) {
  const reg = require('./property-registry');
  const allFields = Object.keys(reg.PROPERTIES);
  const validFields = new Set(allFields);
  const cutoff = new Date(Date.now() - days * 86400 * 1000);

  // Pull enrichment_logs whose field_name matches a registry property
  let scanned = 0, synthesized = 0;
  const rows = await db('enrichment_logs')
    .where('created_at', '>', cutoff)
    .whereIn('field_name', allFields)
    .select('person_id', 'field_name', 'old_value', 'new_value', 'created_at')
    .limit(5000);

  scanned = rows.length;

  for (const r of rows) {
    if (!validFields.has(r.field_name) || !r.person_id) continue;
    let extractedValue = r.new_value;
    let sourceEngine = 'enrichment_logs_backfill';
    try {
      const parsed = typeof r.new_value === 'string' ? JSON.parse(r.new_value) : r.new_value;
      if (parsed?.source) sourceEngine = parsed.source;
      if (parsed?.value) extractedValue = parsed.value;
    } catch (_) {}

    try {
      await db('property_change_events').insert({
        entity: 'Person',
        record_id: r.person_id,
        property: r.field_name,
        old_value: r.old_value == null ? null : String(r.old_value).slice(0, 4000),
        new_value: extractedValue == null ? null : String(extractedValue).slice(0, 4000),
        source_engine: sourceEngine,
        created_at: r.created_at
      });
      synthesized++;
    } catch (_) {}
  }

  return { ok: true, scanned, synthesized, days };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();
  if (action === 'health') return res.json({ success: true, service: 'property-events-backfill' });
  if (action === 'seed') {
    return res.json(await seedFromCurrentRows(db));
  }
  if (action === 'run') {
    const days = parseInt(req.query?.days) || 30;
    return res.json(await backfill(db, days));
  }
  return res.status(400).json({ error: 'unknown action' });
}


async function seedFromCurrentRows(db) {
  // Seed property_change_events from current row state on persons + incidents.
  // Uses INSERT … SELECT … WHERE NOT EXISTS to avoid duplicates without needing
  // a unique constraint.
  const personFields = [
    { col: 'phone', type: 'text' },
    { col: 'email', type: 'text' },
    { col: 'address', type: 'text' },
    { col: 'full_name', type: 'text' },
    { col: 'city', type: 'text' },
    { col: 'state', type: 'text' },
    { col: 'zip', type: 'text' },
    { col: 'victim_verified', type: 'bool' },
    { col: 'lead_tier', type: 'text' },
    { col: 'has_attorney', type: 'bool' },
    { col: 'lat', type: 'float' },
    { col: 'lon', type: 'float' }
  ];
  const incidentFields = [
    { col: 'severity', registry: 'severity', type: 'text' },
    { col: 'city', registry: 'incident_city', type: 'text' },
    { col: 'state', registry: 'incident_state', type: 'text' },
    { col: 'address', registry: 'incident_address', type: 'text' },
    { col: 'latitude', registry: 'latitude', type: 'float' },
    { col: 'longitude', registry: 'longitude', type: 'float' },
    { col: 'lead_score', registry: 'lead_score', type: 'int' },
    { col: 'qualification_state', registry: 'qualification_state', type: 'text' },
    { col: 'fatalities_count', registry: 'fatalities_count', type: 'int' },
    { col: 'master_quality_score', registry: 'master_quality_score', type: 'int' }
  ];

  let seeded = 0;
  for (const f of personFields) {
    const cast = f.type === 'bool' ? 'p.' + f.col + '::text' : (f.type === 'float' ? 'p.' + f.col + '::text' : 'p.' + f.col);
    try {
      const r = await db.raw(\`
        INSERT INTO property_change_events (entity, record_id, property, new_value, source_engine, created_at)
        SELECT 'Person', p.id, ?, \${cast}, 'seed_existing', p.created_at
        FROM persons p
        WHERE p.\${f.col} IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM property_change_events e
            WHERE e.entity = 'Person' AND e.record_id = p.id AND e.property = ?
          )
      \`, [f.col, f.col]);
      seeded += r.rowCount || 0;
    } catch (e) { console.error('[seed] persons.' + f.col + ': ' + e.message); }
  }
  for (const f of incidentFields) {
    const cast = f.type === 'int' ? 'i.' + f.col + '::text' : (f.type === 'float' ? 'i.' + f.col + '::text' : 'i.' + f.col);
    try {
      const r = await db.raw(\`
        INSERT INTO property_change_events (entity, record_id, property, new_value, source_engine, created_at)
        SELECT 'Incident', i.id, ?, \${cast}, 'seed_existing', COALESCE(i.discovered_at, NOW())
        FROM incidents i
        WHERE i.\${f.col} IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM property_change_events e
            WHERE e.entity = 'Incident' AND e.record_id = i.id AND e.property = ?
          )
      \`, [f.registry, f.registry]);
      seeded += r.rowCount || 0;
    } catch (e) { console.error('[seed] incidents.' + f.col + ': ' + e.message); }
  }
  return { ok: true, seeded };
}

module.exports = handler;
module.exports.handler = handler;
module.exports.backfill = backfill;
module.exports.seedFromCurrentRows = seedFromCurrentRows;
