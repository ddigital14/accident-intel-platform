/**
 * Phase 50: CEI invocation telemetry — Wave 12 pattern from CaseFlow.
 *
 * Every successful engine call bumps engine_capabilities.invocation_count
 * so auto-promote logic, model-eval cron, and admin dashboards have actual
 * telemetry. As enrichments fire, victims verify, contacts merge — each
 * lands on its capability row. The engines are now learning.
 *
 * Usage:
 *   const { bumpCounter } = require('../system/_cei_telemetry');
 *   await bumpCounter(db, 'spanish-detector', true, latency_ms);
 */
let _ensured = false;

async function ensureTable(db) {
  if (_ensured) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS engine_capabilities (
        engine_name        TEXT PRIMARY KEY,
        invocation_count   BIGINT DEFAULT 0,
        success_count      BIGINT DEFAULT 0,
        failure_count      BIGINT DEFAULT 0,
        last_invoked_at    TIMESTAMPTZ,
        last_success_at    TIMESTAMPTZ,
        last_failure_at    TIMESTAMPTZ,
        success_rate       REAL DEFAULT 0,
        avg_latency_ms     INTEGER DEFAULT 0,
        total_latency_ms   BIGINT DEFAULT 0,
        meta               JSONB DEFAULT '{}'::jsonb,
        created_at         TIMESTAMPTZ DEFAULT NOW(),
        updated_at         TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_engine_capabilities_last
        ON engine_capabilities(last_invoked_at DESC);
    `);
    _ensured = true;
  } catch (e) {
    console.error('[cei-telemetry] ensureTable failed:', e.message);
  }
}

/**
 * Bump invocation counters atomically. Safe to fire-and-forget.
 * @param {object} db - knex instance
 * @param {string} engine - engine_name (e.g., 'spanish-detector', 'smart-cross-ref')
 * @param {boolean} success - whether the call succeeded
 * @param {number} latencyMs - call latency (optional)
 */
async function bumpCounter(db, engine, success = true, latencyMs = 0) {
  if (!engine) return;
  try {
    await ensureTable(db);
    const lat = Math.max(0, parseInt(latencyMs, 10) || 0);
    const succ = success ? 1 : 0;
    const fail = success ? 0 : 1;
    const successAt = success ? new Date() : null;
    const failureAt = success ? null : new Date();
    // Two-step UPSERT (avoids parameter-numbering surprises across pg drivers).
    const exists = await db('engine_capabilities').where('engine_name', engine).first();
    if (!exists) {
      try {
        await db('engine_capabilities').insert({
          engine_name: engine,
          invocation_count: 1,
          success_count: succ,
          failure_count: fail,
          last_invoked_at: new Date(),
          last_success_at: successAt,
          last_failure_at: failureAt,
          total_latency_ms: lat,
          avg_latency_ms: lat,
          success_rate: success ? 1.0 : 0.0,
          updated_at: new Date()
        });
      } catch (e) {
        // Race with another bump on same engine — fall through to update path
        if (!/duplicate|unique/i.test(e.message || '')) throw e;
      }
      return;
    }
    const newCount = (parseInt(exists.invocation_count, 10) || 0) + 1;
    const newSucc = (parseInt(exists.success_count, 10) || 0) + succ;
    const newFail = (parseInt(exists.failure_count, 10) || 0) + fail;
    const newTotalLat = (parseInt(exists.total_latency_ms, 10) || 0) + lat;
    await db('engine_capabilities').where('engine_name', engine).update({
      invocation_count: newCount,
      success_count: newSucc,
      failure_count: newFail,
      last_invoked_at: new Date(),
      last_success_at: success ? new Date() : exists.last_success_at,
      last_failure_at: success ? exists.last_failure_at : new Date(),
      total_latency_ms: newTotalLat,
      avg_latency_ms: Math.round(newTotalLat / Math.max(1, newCount)),
      success_rate: newSucc / Math.max(1, newCount),
      updated_at: new Date()
    });
  } catch (e) {
    try { console.warn('[cei-telemetry] bump failed for', engine, '-', e.message); } catch (_) {}
  }
}

async function getAllCounters(db, { limit = 200 } = {}) {
  await ensureTable(db);
  const rows = await db('engine_capabilities')
    .select('*')
    .orderBy('invocation_count', 'desc')
    .limit(limit);
  return rows;
}

async function getCounter(db, engine) {
  await ensureTable(db);
  return db('engine_capabilities').where('engine_name', engine).first();
}

module.exports = { bumpCounter, getAllCounters, getCounter, ensureTable };
