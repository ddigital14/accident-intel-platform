/**
 * In-memory dedup cache for serverless functions
 *
 * Each Vercel function invocation can re-use this cache across multiple
 * iterations within a single execution. Lambda containers are reused for ~5min,
 * so frequently-hit functions also benefit cross-invocation.
 *
 * Purpose: Avoid redundant DB queries when checking source_reference dedup.
 */

class TTLCache {
  constructor(maxSize = 5000, ttlMs = 30 * 60 * 1000) {
    this.cache = new Map();
    this.maxSize = maxSize;
    this.ttlMs = ttlMs;
  }
  has(key) {
    const entry = this.cache.get(key);
    if (!entry) return false;
    if (Date.now() > entry.expires) { this.cache.delete(key); return false; }
    return true;
  }
  get(key) { if (!this.has(key)) return undefined; return this.cache.get(key).value; }
  set(key, value) {
    if (this.cache.size >= this.maxSize) {
      const firstKey = this.cache.keys().next().value;
      this.cache.delete(firstKey);
    }
    this.cache.set(key, { value, expires: Date.now() + this.ttlMs });
  }
  clear() { this.cache.clear(); }
  size() { return this.cache.size; }
}

const dedupCache = new TTLCache(10000, 30 * 60 * 1000);
const geoCache = new TTLCache(2000, 5 * 60 * 1000);
const enrichmentCache = new TTLCache(5000, 60 * 60 * 1000);

module.exports = { TTLCache, dedupCache, geoCache, enrichmentCache };
