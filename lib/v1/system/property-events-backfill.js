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
  if (action === 'run') {
    const days = parseInt(req.query?.days) || 30;
    return res.json(await backfill(db, days));
  }
  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.backfill = backfill;
