/**
 * VoyageAI Semantic Router (Phase 44A)
 *
 * Single utility for all semantic ops across the platform.
 * Wraps VoyageAI embeddings + reranking with Postgres caching.
 *
 *   embed(text, model='voyage-3')          -> 1024-dim vector
 *   embedBatch(texts[], model='voyage-3')  -> vectors[] (batched up to 128)
 *   rerank(query, docs[], top_k=5,
 *           model='rerank-2.5')            -> [{ index, doc, relevance_score }]
 *   cosineSim(a, b)                        -> 0..1
 *
 * Cache: vector_cache(text_hash CHAR(64) PK, model, vector JSONB, created_at).
 * Auto-created on first use. Free fallback: returns nulls when key not set.
 *
 * Endpoints:
 *   POST https://api.voyageai.com/v1/embeddings
 *   POST https://api.voyageai.com/v1/rerank
 *   Auth: Authorization: Bearer <key>
 *
 * Health probe:
 *   GET /api/v1/enrich/_voyage_router?secret=ingest-now&action=health
 *   GET /api/v1/enrich/_voyage_router?secret=ingest-now&action=embed&text=hello
 */
const crypto = require('crypto');
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');

const EMBED_URL = 'https://api.voyageai.com/v1/embeddings';
const RERANK_URL = 'https://api.voyageai.com/v1/rerank';
const HTTP_TIMEOUT_MS = 15000;
const BATCH_MAX = 128;
const SECRET = 'ingest-now';

let _tableEnsured = false;
async function ensureCacheTable(db) {
  if (_tableEnsured) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS vector_cache (
        text_hash CHAR(64) NOT NULL,
        model VARCHAR(40) NOT NULL,
        vector JSONB NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (text_hash, model)
      );
      CREATE INDEX IF NOT EXISTS idx_vector_cache_created ON vector_cache(created_at);
    `);
    _tableEnsured = true;
  } catch (e) {
    console.error('vector_cache table:', e.message);
  }
}

async function getKey(db) {
  if (process.env.VOYAGEAI_API_KEY) return process.env.VOYAGEAI_API_KEY;
  if (process.env.VOYAGE_API_KEY) return process.env.VOYAGE_API_KEY;
  if (!db) return null;
  try {
    const row = await db('system_config').where({ key: 'voyageai_api_key' }).first();
    if (row && row.value) return typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
  } catch (_) {}
  return null;
}

function hashText(text, model) {
  return crypto.createHash('sha256').update(`${model}::${String(text || '')}`).digest('hex');
}
function normText(t) { return String(t == null ? '' : t).slice(0, 8000); }

async function embed(text, model = 'voyage-3', _db) {
  const db = _db || getDb();
  const t = normText(text);
  if (!t) return null;
  await ensureCacheTable(db);
  const h = hashText(t, model);
  try {
    const hit = await db('vector_cache').where({ text_hash: h, model }).first();
    if (hit && hit.vector) {
      const v = typeof hit.vector === 'string' ? JSON.parse(hit.vector) : hit.vector;
      if (Array.isArray(v) && v.length > 0) return v;
    }
  } catch (_) {}
  const key = await getKey(db);
  if (!key) return null;
  try {
    const resp = await fetch(EMBED_URL, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ input: [t], model, input_type: 'document' }),
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS)
    });
    await trackApiCall(db, '_voyage_router', 'voyage_embed', t.length, 0, resp.ok).catch(() => {});
    if (!resp.ok) return null;
    const data = await resp.json().catch(() => null);
    const vec = data && data.data && data.data[0] && data.data[0].embedding;
    if (!Array.isArray(vec)) return null;
    try {
      await db('vector_cache').insert({ text_hash: h, model, vector: JSON.stringify(vec), created_at: new Date() }).onConflict(['text_hash', 'model']).ignore();
    } catch (_) {}
    return vec;
  } catch (e) {
    await reportError(db, '_voyage_router', null, `embed: ${e.message}`).catch(() => {});
    return null;
  }
}

async function embedBatch(texts, model = 'voyage-3', _db) {
  const db = _db || getDb();
  if (!Array.isArray(texts) || texts.length === 0) return [];
  await ensureCacheTable(db);
  const out = new Array(texts.length).fill(null);
  const toFetch = [];
  for (let i = 0; i < texts.length; i++) {
    const t = normText(texts[i]);
    if (!t) continue;
    const h = hashText(t, model);
    try {
      const hit = await db('vector_cache').where({ text_hash: h, model }).first();
      if (hit && hit.vector) {
        const v = typeof hit.vector === 'string' ? JSON.parse(hit.vector) : hit.vector;
        if (Array.isArray(v)) { out[i] = v; continue; }
      }
    } catch (_) {}
    toFetch.push({ i, text: t, hash: h });
  }
  if (toFetch.length === 0) return out;
  const key = await getKey(db);
  if (!key) return out;
  for (let start = 0; start < toFetch.length; start += BATCH_MAX) {
    const slice = toFetch.slice(start, start + BATCH_MAX);
    try {
      const resp = await fetch(EMBED_URL, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ input: slice.map(s => s.text), model, input_type: 'document' }),
        signal: AbortSignal.timeout(HTTP_TIMEOUT_MS)
      });
      const totalChars = slice.reduce((s, x) => s + x.text.length, 0);
      await trackApiCall(db, '_voyage_router', 'voyage_embed', totalChars, 0, resp.ok).catch(() => {});
      if (!resp.ok) continue;
      const data = await resp.json().catch(() => null);
      const arr = (data && data.data) || [];
      for (let j = 0; j < slice.length; j++) {
        const vec = arr[j] && arr[j].embedding;
        if (Array.isArray(vec)) {
          out[slice[j].i] = vec;
          try {
            await db('vector_cache').insert({ text_hash: slice[j].hash, model, vector: JSON.stringify(vec), created_at: new Date() }).onConflict(['text_hash', 'model']).ignore();
          } catch (_) {}
        }
      }
    } catch (e) {
      await reportError(db, '_voyage_router', null, `embedBatch: ${e.message}`).catch(() => {});
    }
  }
  return out;
}

async function rerank(query, documents, top_k = 5, model = 'rerank-2.5', _db) {
  const db = _db || getDb();
  if (!query || !Array.isArray(documents) || documents.length === 0) return [];
  const docs = documents.map(d => normText(typeof d === 'string' ? d : (d && d.text) || JSON.stringify(d || {})));
  const key = await getKey(db);
  if (!key) return null;
  try {
    const resp = await fetch(RERANK_URL, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${key}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: normText(query), documents: docs, model, top_k: Math.min(Math.max(1, top_k), docs.length), return_documents: true }),
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS)
    });
    await trackApiCall(db, '_voyage_router', 'voyage_rerank', docs.length, 0, resp.ok).catch(() => {});
    if (!resp.ok) {
      const t = await resp.text().catch(() => '');
      await reportError(db, '_voyage_router', null, `rerank HTTP ${resp.status}: ${t.substring(0, 200)}`).catch(() => {});
      return null;
    }
    const data = await resp.json().catch(() => null);
    const results = (data && (data.data || data.results)) || [];
    return results.map(r => ({ index: r.index, document: r.document || documents[r.index], relevance_score: r.relevance_score }));
  } catch (e) {
    await reportError(db, '_voyage_router', null, `rerank: ${e.message}`).catch(() => {});
    return null;
  }
}

function cosineSim(a, b) {
  if (!Array.isArray(a) || !Array.isArray(b) || a.length === 0 || a.length !== b.length) return 0;
  let dot = 0, na = 0, nb = 0;
  for (let i = 0; i < a.length; i++) { dot += a[i]*b[i]; na += a[i]*a[i]; nb += b[i]*b[i]; }
  if (na === 0 || nb === 0) return 0;
  const s = dot / (Math.sqrt(na) * Math.sqrt(nb));
  if (Number.isNaN(s)) return 0;
  return Math.max(0, Math.min(1, (s + 1) / 2));
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = (req.query && req.query.secret) || (req.headers && req.headers['x-cron-secret']);
  if (secret !== SECRET && secret !== process.env.CRON_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  const db = getDb();
  const action = String((req.query && req.query.action) || 'health').toLowerCase();

  if (action === 'health') {
    const key = await getKey(db);
    return res.json({ success: true, pipeline: '_voyage_router', key_set: !!key, embed_url: EMBED_URL, rerank_url: RERANK_URL, models: { default_embed: 'voyage-3', default_rerank: 'rerank-2.5' }, timestamp: new Date().toISOString() });
  }
  if (action === 'embed') {
    const text = String((req.query && req.query.text) || '').slice(0, 4000);
    if (!text) return res.status(400).json({ error: 'text required' });
    const v = await embed(text, (req.query && req.query.model) || 'voyage-3', db);
    return res.json({ success: true, action: 'embed', text_preview: text.substring(0, 80), dim: (v && v.length) || 0, first_8: v ? v.slice(0, 8) : null, cached: !!v, timestamp: new Date().toISOString() });
  }
  if (action === 'rerank') {
    const q = String((req.query && (req.query.q || req.query.query)) || '');
    const docs = String((req.query && req.query.docs) || '').split('|').map(s => s.trim()).filter(Boolean);
    if (!q || docs.length === 0) return res.status(400).json({ error: 'q & docs (pipe-separated) required' });
    const r = await rerank(q, docs, Number((req.query && req.query.top_k)) || 5, (req.query && req.query.model) || 'rerank-2.5', db);
    return res.json({ success: true, action: 'rerank', results: r, timestamp: new Date().toISOString() });
  }
  if (action === 'cosine') {
    const text_a = String((req.query && req.query.a) || '');
    const text_b = String((req.query && req.query.b) || '');
    if (!text_a || !text_b) return res.status(400).json({ error: 'a & b required' });
    const va = await embed(text_a, 'voyage-3', db);
    const vb = await embed(text_b, 'voyage-3', db);
    const sim = cosineSim(va, vb);
    return res.json({ success: true, action: 'cosine', similarity: sim, dim: va && va.length, timestamp: new Date().toISOString() });
  }
  return res.status(400).json({ error: 'unknown action', supported: ['health', 'embed', 'rerank', 'cosine'] });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.default = handler;
module.exports.embed = embed;
module.exports.embedBatch = embedBatch;
module.exports.rerank = rerank;
module.exports.cosineSim = cosineSim;
module.exports.ensureCacheTable = ensureCacheTable;
