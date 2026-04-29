/**
 * Smart Batcher (Phase 44A)
 *
 * Splits a list of work-items into time-bounded shards so a single
 * concurrency timeout doesn't kill the whole job. Used by heavy scrapers
 * (news-rss, pd-press, people-search-multi, homegrown-osint-miner).
 *
 *   batchInShards(items, processFn, opts) - runs processFn(item) in
 *     parallel up to opts.shardMs milliseconds, then yields control,
 *     then continues with the next shard.
 *
 *   opts:
 *     shardMs            (default 5000)  - soft wall-clock per shard
 *     concurrency        (default 4)     - parallel items inside a shard
 *     totalBudgetMs      (default 50000) - hard global ceiling
 *     yieldGapMs         (default 25)    - sleep between shards
 *
 * Return shape:
 *   { shards, items_total, items_processed, items_per_shard, results, errors }
 */
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function batchInShards(items, processFn, opts = {}) {
  const arr = Array.isArray(items) ? items : [];
  const shardMs = Number(opts.shardMs) || 5000;
  const concurrency = Math.max(1, Number(opts.concurrency) || 4);
  const totalBudgetMs = Number(opts.totalBudgetMs) || 50000;
  const yieldGapMs = Math.max(0, Number(opts.yieldGapMs) || 25);
  const startGlobal = Date.now();
  const out = { shards: 0, items_total: arr.length, items_processed: 0, items_per_shard: 0, results: new Array(arr.length).fill(null), errors: [] };
  if (arr.length === 0) return out;
  let cursor = 0;
  while (cursor < arr.length && (Date.now() - startGlobal) < totalBudgetMs) {
    out.shards++;
    const shardStart = Date.now();
    while (cursor < arr.length && (Date.now() - shardStart) < shardMs && (Date.now() - startGlobal) < totalBudgetMs) {
      const wave = [];
      for (let i = 0; i < concurrency && cursor < arr.length; i++) {
        const idx = cursor++;
        wave.push(
          Promise.resolve()
            .then(() => processFn(arr[idx], idx))
            .then(r => { out.results[idx] = r; out.items_processed++; })
            .catch(e => { out.errors.push({ index: idx, error: String(e && e.message || e).substring(0, 300) }); })
        );
      }
      await Promise.all(wave);
    }
    if (yieldGapMs > 0 && cursor < arr.length) await sleep(yieldGapMs);
  }
  out.items_per_shard = out.shards > 0 ? Math.round(out.items_processed / out.shards) : 0;
  out.elapsed_ms = Date.now() - startGlobal;
  return out;
}

async function runShardedJob(handler, parentReq, opts = {}) {
  if (typeof handler !== 'function') {
    if (handler && typeof handler.default === 'function') handler = handler.default;
    else if (handler && typeof handler.handler === 'function') handler = handler.handler;
  }
  if (typeof handler !== 'function') return { error: 'no callable handler' };
  const fakeReq = {
    method: 'GET',
    query: { ...((parentReq && parentReq.query) || {}) },
    headers: { ...((parentReq && parentReq.headers) || {}), 'x-internal-cron': '1' },
    body: null, url: ''
  };
  fakeReq.query._sharded = '1';
  const startT = Date.now();
  let body = null;
  const fakeRes = { _statusCode: 200, setHeader() { return this; }, status(c) { this._statusCode = c; return this; }, json(o) { body = o; return this; }, end() { return this; } };
  try { await handler(fakeReq, fakeRes); }
  catch (e) { return { error: e.message, latency_ms: Date.now() - startT }; }
  return {
    ...(body || {}),
    sharded: true,
    shards: (body && body.shards) || 1,
    items_per_shard: (body && body.items_per_shard) || (body && body.processed) || 0,
    latency_ms: Date.now() - startT
  };
}

module.exports = { batchInShards, runShardedJob, sleep };
