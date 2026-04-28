/**
 * Shared Neon PostgreSQL connection for serverless functions.
 * Phase 33: pool tuning + connection caching across warm Lambda invocations.
 *
 * - Module-level singleton (db) survives warm-start invocations on Vercel.
 * - max=8 keeps fanout from saturating Neon's per-database conn ceiling
 *   (Neon free: 100 total; with 12-job parallel cron and 8/job we'd hit cap).
 * - acquireConnectionTimeout prevents stuck queries from cascading.
 * - statement_timeout caps any query at 25s (prevents 30s Vercel function timeouts).
 */
const knex = require('knex');

let db;

function getDb() {
  if (!db) {
    db = knex({
      client: 'pg',
      connection: {
        connectionString: process.env.DATABASE_URL,
        ssl: { rejectUnauthorized: false },
        statement_timeout: 25000,    // hard cap per-query
        idle_in_transaction_session_timeout: 10000,
        application_name: 'aip-vercel'
      },
      pool: {
        min: 1,
        max: 8,                       // safe under fanout
        idleTimeoutMillis: 30000,
        acquireTimeoutMillis: 12000,
        propagateCreateError: false   // don't wedge on startup blip
      },
      searchPath: ['public'],
      acquireConnectionTimeout: 30000
    });
  }
  return db;
}

// Graceful close — call from process.on('SIGTERM') in long-running envs (no-op on Vercel)
async function closeDb() { if (db) { try { await db.destroy(); } catch (_) {} db = null; } }

module.exports = { getDb, closeDb };
