/**
 * Centralized error reporting + retrieval for all pipelines
 */
let _tableEnsured = false;

async function ensureTable(db) {
  if (_tableEnsured) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS system_errors (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        pipeline VARCHAR(80) NOT NULL,
        source VARCHAR(120),
        message TEXT NOT NULL,
        context JSONB DEFAULT '{}'::jsonb,
        severity VARCHAR(20) DEFAULT 'error',
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_system_errors_created
        ON system_errors(created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_system_errors_pipeline
        ON system_errors(pipeline);
    `);
    _tableEnsured = true;
  } catch (e) {
    console.error('Failed to ensure system_errors table:', e.message);
  }
}

async function reportError(db, pipeline, source, message, context = {}) {
  try {
    await ensureTable(db);
    await db('system_errors').insert({
      pipeline,
      source: source || null,
      message: String(message).substring(0, 2000),
      context: JSON.stringify(context).substring(0, 8000),
      severity: context.severity || 'error',
      created_at: new Date()
    });
  } catch (e) {
    console.error('reportError failed:', e.message, '| original:', message);
  }
}

async function listErrors(db, { limit = 100, pipeline = null, since = null } = {}) {
  await ensureTable(db);
  let q = db('system_errors').select('*').orderBy('created_at', 'desc').limit(limit);
  if (pipeline) q = q.where('pipeline', pipeline);
  if (since) q = q.where('created_at', '>', new Date(since));
  return q;
}

async function clearOldErrors(db, olderThanDays = 7) {
  await ensureTable(db);
  return db('system_errors')
    .where('created_at', '<', new Date(Date.now() - olderThanDays * 86400000))
    .del();
}

module.exports = { reportError, listErrors, clearOldErrors, ensureTable };
