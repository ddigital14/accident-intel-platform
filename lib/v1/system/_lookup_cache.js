/**
 * Local lookup-result cache — Phase 44B
 * Postgres lookup_cache(key, payload, expires_at)
 */
const crypto = require('crypto');

const DEFAULT_TTL = 86400;
let _ensured = false;

async function ensureTable(db) {
  if (_ensured) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS lookup_cache (
        key TEXT PRIMARY KEY,
        payload JSONB,
        provider VARCHAR(60),
        hit_count INTEGER DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        expires_at TIMESTAMPTZ NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_lookup_cache_expires ON lookup_cache(expires_at);
      CREATE INDEX IF NOT EXISTS idx_lookup_cache_provider ON lookup_cache(provider);
    `);
    _ensured = true;
  } catch (e) { console.error('lookup_cache ensureTable:', e.message); }
}

function normalizeIdentity(identity) {
  if (!identity) return '';
  if (typeof identity === 'string') return identity.trim().toLowerCase();
  const obj = {};
  Object.keys(identity).sort().forEach(k => {
    const v = identity[k];
    if (v == null || v === '') return;
    obj[k] = typeof v === 'string' ? v.trim().toLowerCase() : v;
  });
  return JSON.stringify(obj);
}

function key(provider, identity) {
  const norm = normalizeIdentity(identity);
  const h = crypto.createHash('sha1').update(provider + '|' + norm).digest('hex').slice(0, 24);
  return `${provider}:${h}`;
}

async function cacheGet(db, k) {
  if (!db || !k) return null;
  await ensureTable(db);
  try {
    const row = await db('lookup_cache').where({ key: k }).first();
    if (!row) return null;
    if (new Date(row.expires_at).getTime() < Date.now()) {
      await db('lookup_cache').where({ key: k }).del().catch(() => {});
      return null;
    }
    db('lookup_cache').where({ key: k }).increment('hit_count', 1).catch(() => {});
    const p = row.payload;
    return typeof p === 'string' ? JSON.parse(p) : p;
  } catch (_) { return null; }
}

async function cacheSet(db, k, payload, ttlSeconds = DEFAULT_TTL) {
  if (!db || !k) return false;
  await ensureTable(db);
  const provider = String(k).split(':')[0] || 'unknown';
  const expiresAt = new Date(Date.now() + Math.max(60, ttlSeconds) * 1000);
  try {
    await db.raw(`
      INSERT INTO lookup_cache (key, payload, provider, hit_count, created_at, expires_at)
      VALUES (?, ?::jsonb, ?, 0, NOW(), ?)
      ON CONFLICT (key) DO UPDATE SET
        payload = EXCLUDED.payload,
        provider = EXCLUDED.provider,
        expires_at = EXCLUDED.expires_at,
        created_at = NOW()
    `, [k, JSON.stringify(payload || {}), provider, expiresAt]);
    return true;
  } catch (e) { return false; }
}

async function cacheClean(db) {
  if (!db) return { deleted: 0 };
  await ensureTable(db);
  try {
    const r = await db.raw(`DELETE FROM lookup_cache WHERE expires_at < NOW() RETURNING key`);
    return { deleted: (r.rows || []).length };
  } catch (e) { return { deleted: 0, error: e.message }; }
}

async function withCache(db, k, ttlSeconds, fn) {
  const hit = await cacheGet(db, k);
  if (hit) return { ok: true, cached: true, ...hit };
  const fresh = await fn();
  if (fresh && (fresh.ok !== false)) {
    await cacheSet(db, k, fresh, ttlSeconds);
  }
  return { ok: !!fresh?.ok, cached: false, ...fresh };
}

async function stats(db) {
  await ensureTable(db);
  try {
    const r = await db.raw(`
      SELECT provider, COUNT(*) AS entries, COALESCE(SUM(hit_count), 0) AS hits,
             COUNT(*) FILTER (WHERE expires_at < NOW()) AS expired
        FROM lookup_cache GROUP BY provider ORDER BY entries DESC
    `);
    return r.rows || [];
  } catch (e) { return []; }
}

async function handler(req, res) {
  const { getDb } = require('../../_db');
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const SECRET = 'ingest-now';
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  if (s !== SECRET && s !== process.env.CRON_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  const db = getDb();
  const action = (req.query?.action || 'stats').toLowerCase();
  try {
    if (action === 'stats')   { const s = await stats(db); return res.json({ success: true, providers: s }); }
    if (action === 'clean')   { const r = await cacheClean(db); return res.json({ success: true, ...r }); }
    if (action === 'get')     {
      const k = req.query?.key;
      if (!k) return res.status(400).json({ error: 'key required' });
      const v = await cacheGet(db, k);
      return res.json({ success: !!v, value: v });
    }
    return res.status(400).json({ error: 'unknown action', valid: ['stats', 'clean', 'get'] });
  } catch (e) { return res.status(500).json({ error: e.message }); }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.key = key;
module.exports.cacheGet = cacheGet;
module.exports.cacheSet = cacheSet;
module.exports.cacheClean = cacheClean;
module.exports.withCache = withCache;
module.exports.stats = stats;
module.exports.ensureTable = ensureTable;
