/**
 * Phase 51 — Wave 12 pattern: schema-drift auto-detect.
 *
 * Compares live information_schema columns against AIP's canonical column
 * map for the most-touched tables (incidents, persons, source_reports,
 * system_errors, system_api_calls, etc.). Reports missing columns and
 * type mismatches.
 *
 * Endpoints:
 *   GET ?action=check    — read-only drift report (default)
 *   GET ?action=fix      — also adds missing columns via ALTER TABLE ... ADD COLUMN IF NOT EXISTS
 *   GET ?action=health   — quick OK/FAIL summary
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { bumpCounter } = require('./_cei_telemetry');
const { trackApiCall } = require('./cost');

const ENGINE = 'schema-drift-check';

// Canonical column map. Pulled from the schema rules + Phase-by-phase
// migrations. Keep types Postgres-friendly. When a new column is added in
// migrate/columns.js, mirror it here so drift detection covers it.
const EXPECTED = {
  incidents: {
    id: 'uuid',
    severity: 'text',
    incident_type: 'text',
    status: 'text',
    qualification_state: 'text',
    lead_score: 'integer',
    accident_date: 'timestamptz',
    accident_time: 'text',
    metro: 'text',
    state: 'text',
    city: 'text',
    location: 'text',
    lat: 'numeric',
    lng: 'numeric',
    case_value: 'numeric',
    created_at: 'timestamptz',
    updated_at: 'timestamptz'
  },
  persons: {
    id: 'uuid',
    incident_id: 'uuid',
    name: 'text',
    role: 'text',
    injury_severity: 'text',
    contact_status: 'text',
    phone: 'text',
    email: 'text',
    address: 'text',
    city: 'text',
    state: 'text',
    identity_confidence: 'numeric',
    qualification_state: 'text',
    created_at: 'timestamptz',
    updated_at: 'timestamptz'
  },
  source_reports: {
    id: 'uuid',
    incident_id: 'uuid',
    source_type: 'text',
    source_url: 'text',
    text: 'text',
    raw: 'jsonb',
    created_at: 'timestamptz'
  },
  system_errors: {
    pipeline: 'text',
    message: 'text',
    created_at: 'timestamptz'
  },
  system_api_calls: {
    pipeline: 'text',
    service: 'text',
    success: 'boolean',
    cost_usd: 'numeric',
    created_at: 'timestamptz'
  },
  engine_capabilities: {
    engine_name: 'text',
    invocation_count: 'bigint',
    success_count: 'bigint',
    failure_count: 'bigint',
    success_rate: 'real',
    avg_latency_ms: 'integer',
    last_invoked_at: 'timestamptz'
  },
  embedding_queue: {
    id: 'bigint',
    source_id: 'text',
    source_type: 'text',
    status: 'text',
    text: 'text',
    embedding: 'jsonb',
    created_at: 'timestamptz',
    processed_at: 'timestamptz',
    error: 'text'
  }
};

function normalizeType(t) {
  if (!t) return '';
  const s = String(t).toLowerCase();
  if (s === 'character varying' || s === 'varchar' || s === 'character') return 'text';
  if (s === 'timestamp with time zone') return 'timestamptz';
  if (s === 'timestamp without time zone') return 'timestamp';
  if (s === 'double precision') return 'numeric';
  return s;
}

async function liveColumns(db, table) {
  try {
    const rows = await db.raw(`
      SELECT column_name, data_type, is_nullable
      FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = ?
    `, [table]).then(r => r.rows || r || []).catch(() => []);
    const map = {};
    for (const r of rows) map[r.column_name] = normalizeType(r.data_type);
    return map;
  } catch (_) { return {}; }
}

async function diffTable(db, table, expected) {
  const live = await liveColumns(db, table);
  const tableExists = Object.keys(live).length > 0;
  const missing = [];
  const mismatches = [];

  if (!tableExists) {
    return { table, exists: false, missing: Object.keys(expected), mismatches: [] };
  }

  for (const [col, expType] of Object.entries(expected)) {
    if (!(col in live)) {
      missing.push({ column: col, expected_type: expType });
    } else if (normalizeType(expType) !== live[col]) {
      mismatches.push({ column: col, expected_type: normalizeType(expType), actual_type: live[col] });
    }
  }
  return { table, exists: true, missing, mismatches };
}

async function checkAll(db) {
  const results = [];
  for (const [table, expected] of Object.entries(EXPECTED)) {
    results.push(await diffTable(db, table, expected));
  }
  return results;
}

async function fixAll(db) {
  // Add missing columns via ALTER TABLE ... ADD COLUMN IF NOT EXISTS.
  // Skip non-existent tables (would need a CREATE TABLE — punt to migrate/columns).
  const reports = await checkAll(db);
  const fixed = [];
  for (const rep of reports) {
    if (!rep.exists) continue;
    for (const m of rep.missing) {
      const sql = `ALTER TABLE ${rep.table} ADD COLUMN IF NOT EXISTS ${m.column} ${m.expected_type}`;
      try {
        await db.raw(sql);
        fixed.push({ table: rep.table, column: m.column, type: m.expected_type, sql });
      } catch (e) {
        fixed.push({ table: rep.table, column: m.column, type: m.expected_type, sql, error: e.message });
      }
    }
  }
  return fixed;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const action = (req.query && req.query.action) || 'check';
  const t0 = Date.now();

  try {
    if (action === 'health') {
      const reports = await checkAll(db);
      const drift = reports.some(r => !r.exists || r.missing.length > 0 || r.mismatches.length > 0);
      return res.json({
        success: true,
        engine: ENGINE,
        drift_detected: drift,
        tables_checked: reports.length,
        timestamp: new Date().toISOString()
      });
    }
    if (action === 'check') {
      const reports = await checkAll(db);
      const totalMissing = reports.reduce((a, r) => a + (r.missing?.length || 0), 0);
      const totalMismatches = reports.reduce((a, r) => a + (r.mismatches?.length || 0), 0);
      const missingTables = reports.filter(r => !r.exists).map(r => r.table);
      const ms = Date.now() - t0;
      await trackApiCall(db, ENGINE, 'check', 0, 0, true).catch(() => {});
      await bumpCounter(db, ENGINE, true, ms).catch(() => {});
      return res.json({
        success: true,
        engine: ENGINE,
        tables_checked: reports.length,
        missing_tables: missingTables,
        total_missing_columns: totalMissing,
        total_type_mismatches: totalMismatches,
        drift_detected: totalMissing > 0 || totalMismatches > 0 || missingTables.length > 0,
        reports,
        ms,
        timestamp: new Date().toISOString()
      });
    }
    if (action === 'fix') {
      const fixed = await fixAll(db);
      const reports = await checkAll(db);
      const ms = Date.now() - t0;
      await trackApiCall(db, ENGINE, 'fix', 0, 0, true).catch(() => {});
      await bumpCounter(db, ENGINE, true, ms).catch(() => {});
      return res.json({
        success: true,
        engine: ENGINE,
        columns_added: fixed.length,
        fixed,
        post_fix_reports: reports,
        ms,
        timestamp: new Date().toISOString()
      });
    }
    return res.status(400).json({ error: 'unknown action', supported: ['health', 'check', 'fix'] });
  } catch (err) {
    await reportError(db, ENGINE, null, err.message).catch(() => {});
    await bumpCounter(db, ENGINE, false).catch(() => {});
    return res.status(500).json({ error: err.message });
  }
};

module.exports.checkAll = checkAll;
module.exports.fixAll = fixAll;
module.exports.EXPECTED = EXPECTED;
