/**
 * Vercel KV-backed real-time cursor (replaces realtime-feed long-poll once Vercel KV available).
 * Falls back to in-memory cache when KV not configured. Activates automatically when KV env vars set.
 */
const { getDb } = require('../../_db');

let kvClient = null;
async function getKv() {
  if (kvClient !== null) return kvClient;
  if (!process.env.KV_REST_API_URL || !process.env.KV_REST_API_TOKEN) { kvClient = false; return false; }
  try {
    const { kv } = await import('@vercel/kv');
    kvClient = kv;
    return kv;
  } catch (_) { kvClient = false; return false; }
}

const _memCursor = new Map();

async function setCursor(key, value, ttl = 300) {
  const kv = await getKv();
  if (kv) { await kv.set(key, value, { ex: ttl }); return; }
  _memCursor.set(key, { value, expires: Date.now() + ttl * 1000 });
}

async function getCursor(key) {
  const kv = await getKv();
  if (kv) return await kv.get(key);
  const e = _memCursor.get(key);
  if (!e) return null;
  if (e.expires < Date.now()) { _memCursor.delete(key); return null; }
  return e.value;
}

module.exports = async function handler(req, res) {
  try {
    if (req.query?.action === 'health') {
      const kv = await getKv();
      return res.json({ ok: true, engine: 'kv-cursor', kv_active: !!kv, fallback: !kv ? 'in_memory' : null });
    }
    return res.status(400).json({ error: 'health only' });
  } catch (err) { res.status(500).json({ error: err.message }); }
};
module.exports.setCursor = setCursor;
module.exports.getCursor = getCursor;
