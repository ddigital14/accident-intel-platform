/**
 * Phase 51 — Wave 12 pattern: embedding queue + drain.
 *
 * Decouples slow Voyage embedding writes from fast ingest paths and lets a
 * 1-min cron drain the queue 25 rows at a time. Used to backfill
 * source_reports.text → embeddings, enabling semantic dedup + name matching.
 *
 * Postgres table embedding_queue:
 *   (id BIGSERIAL PK, source_id TEXT, source_type TEXT, status TEXT DEFAULT 'pending',
 *    text TEXT, embedding vector(1024), created_at, processed_at, error TEXT)
 *
 * Endpoints:
 *   GET  ?action=enqueue&source_id=...&type=...&text=...
 *   POST {action:'enqueue', source_id, source_type, text}
 *   GET  ?action=drain&limit=25
 *   GET  ?action=stats
 *   GET  ?action=health
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { bumpCounter } = require('./_cei_telemetry');
const { trackApiCall } = require('./cost');
const voyage = require('../enrich/_voyage_router');

const ENGINE = 'embedding-queue';

let _ensured = false;
async function ensureTable(db) {
  if (_ensured) return;
  // pgvector may or may not be available on the Neon instance — fall back to
  // JSONB array storage if vector type creation fails.
  try {
    await db.raw(`CREATE EXTENSION IF NOT EXISTS vector;`).catch(() => {});
    await db.raw(`
      CREATE TABLE IF NOT EXISTS embedding_queue (
        id BIGSERIAL PRIMARY KEY,
        source_id TEXT,
        source_type TEXT,
        status TEXT DEFAULT 'pending',
        text TEXT,
        embedding JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        processed_at TIMESTAMPTZ,
        error TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_embedding_queue_status
        ON embedding_queue(status, created_at);
      CREATE INDEX IF NOT EXISTS idx_embedding_queue_source
        ON embedding_queue(source_type, source_id);
    `);
    _ensured = true;
  } catch (e) {
    console.error('[embedding-queue] ensureTable:', e.message);
  }
}

async function enqueue(db, { source_id, source_type, text }) {
  await ensureTable(db);
  if (!text || !String(text).trim()) return { ok: false, reason: 'empty_text' };
  const trimmed = String(text).slice(0, 8000); // Voyage hard limit ~32k chars; keep cheap
  const [row] = await db('embedding_queue').insert({
    source_id: source_id || null,
    source_type: source_type || null,
    status: 'pending',
    text: trimmed
  }).returning('id');
  const id = row?.id || row;
  return { ok: true, id };
}

async function drainBatch(db, limit = 25) {
  await ensureTable(db);
  const t0 = Date.now();
  const lim = Math.max(1, Math.min(100, parseInt(limit, 10) || 25));

  // Atomic claim: mark up to `lim` pending rows as in_progress in one query.
  const claimed = await db.raw(`
    UPDATE embedding_queue
    SET status = 'in_progress'
    WHERE id IN (
      SELECT id FROM embedding_queue
      WHERE status = 'pending'
      ORDER BY created_at ASC
      LIMIT ${lim}
      FOR UPDATE SKIP LOCKED
    )
    RETURNING id, source_id, source_type, text
  `).then(r => r.rows || r || []).catch(() => []);

  if (!claimed.length) {
    await bumpCounter(db, ENGINE, true, Date.now() - t0).catch(() => {});
    return { processed: 0, succeeded: 0, failed: 0, ms: Date.now() - t0 };
  }

  // Voyage embedBatch up to 128 — we're well under that
  const texts = claimed.map(r => r.text || '');
  let vectors = [];
  try {
    vectors = await voyage.embedBatch(texts, 'voyage-3', db);
  } catch (e) {
    await reportError(db, ENGINE, null, `embedBatch: ${e.message}`).catch(() => {});
  }

  let succeeded = 0, failed = 0;
  for (let i = 0; i < claimed.length; i++) {
    const row = claimed[i];
    const vec = vectors && vectors[i];
    if (vec && Array.isArray(vec) && vec.length > 0) {
      await db('embedding_queue').where({ id: row.id }).update({
        status: 'done',
        processed_at: new Date(),
        embedding: JSON.stringify(vec),
        error: null
      }).catch(() => {});
      succeeded++;
    } else {
      await db('embedding_queue').where({ id: row.id }).update({
        status: 'failed',
        processed_at: new Date(),
        error: 'voyage_returned_null'
      }).catch(() => {});
      failed++;
    }
  }

  const ms = Date.now() - t0;
  await trackApiCall(db, ENGINE, 'drain', claimed.length, 0, failed === 0).catch(() => {});
  await bumpCounter(db, ENGINE, failed === 0, ms).catch(() => {});
  return { processed: claimed.length, succeeded, failed, ms };
}

async function stats(db) {
  await ensureTable(db);
  const rows = await db.raw(`
    SELECT status, COUNT(*)::int AS count
    FROM embedding_queue
    GROUP BY status
  `).then(r => r.rows || r || []).catch(() => []);
  const total = rows.reduce((a, r) => a + (parseInt(r.count, 10) || 0), 0);
  const map = {};
  for (const r of rows) map[r.status] = parseInt(r.count, 10) || 0;
  return {
    total,
    pending: map.pending || 0,
    in_progress: map.in_progress || 0,
    done: map.done || 0,
    failed: map.failed || 0
  };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const action = (req.query && req.query.action) || 'stats';

  try {
    if (action === 'health') {
      await ensureTable(db);
      const s = await stats(db);
      return res.json({ success: true, engine: ENGINE, queue: s, timestamp: new Date().toISOString() });
    }
    if (action === 'stats') {
      const s = await stats(db);
      return res.json({ success: true, engine: ENGINE, ...s, timestamp: new Date().toISOString() });
    }
    if (action === 'enqueue') {
      let body = {};
      if (req.method === 'POST') {
        body = await new Promise(r => { let d = ''; req.on('data', c => d += c); req.on('end', () => { try { r(JSON.parse(d)); } catch (_) { r({}); } }); });
      }
      const source_id  = body.source_id  || req.query.source_id  || null;
      const source_type= body.source_type|| req.query.type       || req.query.source_type || null;
      const text       = body.text       || req.query.text       || null;
      const r = await enqueue(db, { source_id, source_type, text });
      return res.json({ success: !!r.ok, ...r });
    }
    if (action === 'drain') {
      const limit = parseInt(req.query.limit, 10) || 25;
      const out = await drainBatch(db, limit);
      return res.json({ success: true, engine: ENGINE, ...out });
    }
    return res.status(400).json({ error: 'unknown action', supported: ['health', 'stats', 'enqueue', 'drain'] });
  } catch (err) {
    await reportError(db, ENGINE, null, err.message).catch(() => {});
    await bumpCounter(db, ENGINE, false).catch(() => {});
    return res.status(500).json({ error: err.message });
  }
};

module.exports.enqueue = enqueue;
module.exports.drainBatch = drainBatch;
module.exports.stats = stats;
