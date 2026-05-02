/**
 * Phase 80: Sync JS PROPERTIES into Postgres property_registry_db table.
 * Lets non-Node consumers (Python tools, future SSR frontend) query the registry.
 */
const { getDb } = require('../../_db');
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function syncToDb(db) {
  const reg = require('./property-registry');
  const props = reg.PROPERTIES;
  let upserted = 0;
  for (const [id, p] of Object.entries(props)) {
    try {
      await db.raw(`
        INSERT INTO property_registry_db (id, entity, label, type, validation, default_value,
          is_reportable_to_rep, is_auditable, is_public, surfaces, producers, consumers, enum_values, synced_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::text[], ?::jsonb, ?::text[], ?::text[], NOW())
        ON CONFLICT (id) DO UPDATE SET
          entity = EXCLUDED.entity, label = EXCLUDED.label, type = EXCLUDED.type,
          validation = EXCLUDED.validation, default_value = EXCLUDED.default_value,
          is_reportable_to_rep = EXCLUDED.is_reportable_to_rep,
          is_auditable = EXCLUDED.is_auditable, is_public = EXCLUDED.is_public,
          surfaces = EXCLUDED.surfaces, producers = EXCLUDED.producers,
          consumers = EXCLUDED.consumers, enum_values = EXCLUDED.enum_values,
          synced_at = NOW()
      `, [
        id, p.entity || 'Unknown', p.label || id, p.type || 'string',
        p.validation ? p.validation.source : null,
        p.default == null ? null : String(p.default),
        p.isReportableToRep !== false, !!p.isAuditable, !!p.isPublic,
        '{' + (p.surfaces || []).join(',') + '}',
        JSON.stringify(p.producers || []),
        '{' + (p.consumers || []).join(',') + '}',
        '{' + (p.enum_values || []).join(',') + '}'
      ]);
      upserted++;
    } catch (_) {}
  }
  return { ok: true, upserted, total_in_js: Object.keys(props).length };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();
  if (action === 'health') return res.json({ success: true, service: 'property-registry-sync' });
  if (action === 'sync') return res.json(await syncToDb(db));
  if (action === 'list_db') {
    const rows = await db('property_registry_db').select('*').orderBy('entity').orderBy('id').catch(() => []);
    return res.json({ count: rows.length, properties: rows });
  }
  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.syncToDb = syncToDb;
