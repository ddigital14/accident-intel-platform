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
    await db.raw(`
      INSERT INTO engine_capabilities(engine_name, invocation_count, success_count, failure_count, last_invoked_at, last_success_at, last_failure_at, total_latency_ms, avg_latency_ms, success_rate)
      VALUES ($1, 1, $2, $3, NOW(), $4, $5, $6, $6, $7)
      ON CONFLICT (engine_name) DO UPDATE SET
        invocation_count = engine_capabilities.invocation_count + 1,
        success_count    = engine_capabilities.success_count + EXCLUDED.success_count,
        failure_count    = engine_capabilities.failure_count + EXCLUDED.failure_count,
        last_invoked_at  = NOW(),
        last_success_at  = COALESCE(EXCLUDED.last_success_at, engine_capabilities.last_success_at),
        last_failure_at  = COALESCE(EXCLUDED.last_failure_at, engine_capabilities.last_failure_at),
        total_latency_ms = engine_capabilities.total_latency_ms + $6,
        avg_latency_ms   = ((engine_capabilities.total_latency_ms + $6) / GREATEST(1, engine_capabilities.invocation_count + 1))::INT,
        success_rate     = (
          (engine_capabilities.success_count + EXCLUDED.success_count)::REAL
          / GREATEST(1, engine_capabilities.invocation_count + 1)
        ),
        updated_at = NOW()
    `, [
      engine,
      success ? 1 : 0,
      success ? 0 : 1,
      success ? new Date() : null,
      success ? null : new Date(),
      lat,
      success ? 1.0 : 0.0
    ]);
  } catch (e) {
    // never block caller
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
