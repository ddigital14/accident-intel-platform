/**
 * Phase 79: Property Change Event Bus.
 *
 * From CaseFlow analysis: formal `property_change_events` table beats ad-hoc
 * JSON in enrichment_logs.new_value for state changes. Every property write
 * gets a typed audit row that downstream subscribers can replay/query.
 *
 * Schema:
 *   id BIGSERIAL PK
 *   entity VARCHAR(20)   ('Person'|'Incident'|...)
 *   record_id UUID
 *   property VARCHAR(64) (registry field name)
 *   old_value TEXT, new_value TEXT
 *   source_engine VARCHAR(64)
 *   confidence INT
 *   created_at TIMESTAMP
 */
const { getDb } = require('../../_db');
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

let _migrated = false;
async function ensureSchema(db) {
  if (_migrated) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS property_change_events (
        id BIGSERIAL PRIMARY KEY,
        entity VARCHAR(20) NOT NULL,
        record_id UUID NOT NULL,
        property VARCHAR(64) NOT NULL,
        old_value TEXT,
        new_value TEXT,
        source_engine VARCHAR(64),
        confidence INT,
        created_at TIMESTAMP DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_pce_record ON property_change_events(entity, record_id);
      CREATE INDEX IF NOT EXISTS idx_pce_property ON property_change_events(property);
      CREATE INDEX IF NOT EXISTS idx_pce_created ON property_change_events(created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_pce_engine ON property_change_events(source_engine);
    `);
    _migrated = true;
  } catch (_) {}
}

async function record(db, { entity, record_id, property, old_value, new_value, source_engine, confidence }) {
  await ensureSchema(db);
  // Validate property against registry
  const reg = require('./property-registry');
  const def = reg.describe(property);
  if (def && new_value != null) {
    const v = reg.validateValue(property, new_value);
    if (!v.ok && !v.empty) {
      // Don't write, but log the rejection
      console.warn(`[property-events] rejected ${entity}.${property}=${new_value}: ${v.error}`);
      return { ok: false, error: v.error };
    }
  }
  try {
    await db('property_change_events').insert({
      entity, record_id, property,
      old_value: old_value == null ? null : String(old_value).slice(0, 4000),
      new_value: new_value == null ? null : String(new_value).slice(0, 4000),
      source_engine: source_engine || 'unknown',
      confidence: confidence || null,
      created_at: new Date()
    });
    return { ok: true };
  } catch (e) { return { ok: false, error: e.message }; }
}

async function recent(db, { limit = 100, entity, record_id, property, source_engine } = {}) {
  await ensureSchema(db);
  let q = db('property_change_events').orderBy('created_at', 'desc').limit(Math.min(500, parseInt(limit) || 100));
  if (entity) q = q.where('entity', entity);
  if (record_id) q = q.where('record_id', record_id);
  if (property) q = q.where('property', property);
  if (source_engine) q = q.where('source_engine', source_engine);
  return q;
}

async function timeline(db, recordId) {
  await ensureSchema(db);
  return db('property_change_events').where('record_id', recordId).orderBy('created_at', 'asc');
}

async function stats(db) {
  await ensureSchema(db);
  const row = await db.raw(`
    SELECT
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') as last_24h,
      COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '7 days') as last_7d,
      COUNT(DISTINCT property) as distinct_properties,
      COUNT(DISTINCT source_engine) as distinct_engines,
      COUNT(DISTINCT record_id) as distinct_records
    FROM property_change_events
  `).then(r => (r.rows || r)[0] || {});
  const byProperty = await db.raw(`
    SELECT property, COUNT(*) AS changes, MAX(created_at) AS last_changed
    FROM property_change_events GROUP BY property ORDER BY changes DESC LIMIT 15
  `).then(r => r.rows || r);
  const byEngine = await db.raw(`
    SELECT source_engine, COUNT(*) AS changes, MAX(created_at) AS last_active
    FROM property_change_events GROUP BY source_engine ORDER BY changes DESC LIMIT 15
  `).then(r => r.rows || r);
  return { ...row, top_properties: byProperty, top_engines: byEngine };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();

  if (action === 'health') return res.json({ success: true, service: 'property-events' });
  if (action === 'recent') return res.json({ events: await recent(db, req.query || {}) });
  if (action === 'timeline') {
    if (!req.query?.record_id) return res.status(400).json({ error: 'record_id required' });
    return res.json({ record_id: req.query.record_id, events: await timeline(db, req.query.record_id) });
  }
  if (action === 'stats') return res.json(await stats(db));
  if (action === 'record') {
    let body = req.body;
    if (!body || typeof body !== 'object') {
      body = await new Promise(r => {
        let d=''; req.on('data', c=>d+=c);
        req.on('end', () => { try { r(JSON.parse(d || '{}')); } catch { r({}); } });
      });
    }
    return res.json(await record(db, body));
  }
  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.record = record;
module.exports.recent = recent;
module.exports.timeline = timeline;
module.exports.stats = stats;
