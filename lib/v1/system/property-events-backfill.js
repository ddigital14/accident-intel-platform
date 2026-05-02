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

async function seedFromCurrentRows(db) {
  // Seed property_change_events from current row state on persons + incidents.
  // Uses INSERT … SELECT … WHERE NOT EXISTS to avoid duplicates without needing
  // a unique constraint.
  const personFields = [
    { col: 'phone', registry: 'phone', cast: '' },
    { col: 'email', registry: 'email', cast: '' },
    { col: 'address', registry: 'address', cast: '' },
    { col: 'full_name', registry: 'full_name', cast: '' },
    { col: 'city', registry: 'city', cast: '' },
    { col: 'state', registry: 'state', cast: '' },
    { col: 'zip', registry: 'zip', cast: '' },
    { col: 'victim_verified', registry: 'victim_verified', cast: '::text' },
    { col: 'lead_tier', registry: 'lead_tier', cast: '' },
    { col: 'has_attorney', registry: 'has_attorney', cast: '::text' },
    { col: 'lat', registry: 'lat', cast: '::text' },
    { col: 'lon', registry: 'lon', cast: '::text' }
  ];
  const incidentFields = [
    { col: 'severity', registry: 'severity', cast: '' },
    { col: 'city', registry: 'incident_city', cast: '' },
    { col: 'state', registry: 'incident_state', cast: '' },
    { col: 'address', registry: 'incident_address', cast: '' },
    { col: 'latitude', registry: 'latitude', cast: '::text' },
    { col: 'longitude', registry: 'longitude', cast: '::text' },
    { col: 'lead_score', registry: 'lead_score', cast: '::text' },
    { col: 'qualification_state', registry: 'qualification_state', cast: '' },
    { col: 'fatalities_count', registry: 'fatalities_count', cast: '::text' },
    { col: 'master_quality_score', registry: 'master_quality_score', cast: '::text' }
  ];

  let seeded = 0;
  for (const f of personFields) {
    try {
      const sql = `
        INSERT INTO property_change_events (entity, record_id, property, new_value, source_engine, created_at)
        SELECT 'Person', p.id, '${f.registry}', p.${f.col}${f.cast}, 'seed_existing', p.created_at
        FROM persons p
        WHERE p.${f.col} IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM property_change_events e
            WHERE e.entity = 'Person' AND e.record_id = p.id AND e.property = '${f.registry}'
          )
      `;
      const r = await db.raw(sql);
      seeded += r.rowCount || 0;
    } catch (e) { console.error('[seed] persons.' + f.col + ': ' + e.message); }
  }
  for (const f of incidentFields) {
    try {
      const sql = `
        INSERT INTO property_change_events (entity, record_id, property, new_value, source_engine, created_at)
        SELECT 'Incident', i.id, '${f.registry}', i.${f.col}${f.cast}, 'seed_existing', COALESCE(i.discovered_at, NOW())
        FROM incidents i
        WHERE i.${f.col} IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM property_change_events e
            WHERE e.entity = 'Incident' AND e.record_id = i.id AND e.property = '${f.registry}'
          )
      `;
      const r = await db.raw(sql);
      seeded += r.rowCount || 0;
    } catch (e) { console.error('[seed] incidents.' + f.col + ': ' + e.message); }
  }
  return { ok: true, seeded };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();
  if (action === 'health') return res.json({ success: true, service: 'property-events-backfill' });
  if (action === 'seed') return res.json(await seedFromCurrentRows(db));
  if (action === 'run') {
    const days = parseInt(req.query?.days) || 30;
    return res.json(await backfill(db, days));
  }
  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.backfill = backfill;
module.exports.seedFromCurrentRows = seedFromCurrentRows;
