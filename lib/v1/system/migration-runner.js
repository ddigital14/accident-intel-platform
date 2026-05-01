/**
 * Phase 67: Migration Runner — explicit, idempotent migrations + DROP/CREATE for triggers.
 *
 * Use cases:
 *   GET ?action=health
 *   POST ?action=run body:{name, sql, drop_first?}
 *   GET ?action=apply_all — re-applies every known migration in /database/migrations/
 *   GET ?action=list — lists what's been applied (from migration_history table)
 */
const fs = require('fs');
const path = require('path');
const { getDb } = require('../../_db');

const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function ensureHistoryTable(db) {
  await db.raw(`
    CREATE TABLE IF NOT EXISTS migration_history (
      name VARCHAR(200) PRIMARY KEY,
      applied_at TIMESTAMP DEFAULT NOW(),
      duration_ms INT,
      status VARCHAR(20) DEFAULT 'applied'
    )
  `).catch(()=>{});
}

async function applyMigration(db, name, sql, dropFirst = false) {
  await ensureHistoryTable(db);
  const t0 = Date.now();
  try {
    if (dropFirst) {
      // Heuristic: extract trigger/function names and drop them first
      const trgMatch = sql.match(/CREATE TRIGGER\s+(\w+)\s+ON\s+(\w+)/i);
      const fnMatch = sql.match(/CREATE OR REPLACE FUNCTION\s+(\w+)/i);
      if (trgMatch) await db.raw(`DROP TRIGGER IF EXISTS ${trgMatch[1]} ON ${trgMatch[2]} CASCADE`).catch(()=>{});
      if (fnMatch) await db.raw(`DROP FUNCTION IF EXISTS ${fnMatch[1]}() CASCADE`).catch(()=>{});
    }
    await db.raw(sql);
    const dur = Date.now() - t0;
    await db.raw(`
      INSERT INTO migration_history (name, applied_at, duration_ms, status)
      VALUES (?, NOW(), ?, 'applied')
      ON CONFLICT (name) DO UPDATE SET applied_at = NOW(), duration_ms = EXCLUDED.duration_ms
    `, [name, dur]).catch(()=>{});
    return { ok: true, name, duration_ms: dur };
  } catch (e) {
    return { ok: false, name, error: e.message };
  }
}

async function applyAll(db) {
  const dir = path.join(__dirname, '../../../database/migrations');
  let files;
  try {
    files = fs.readdirSync(dir).filter(f => f.endsWith('.sql')).sort();
  } catch (e) {
    return { ok: false, error: 'migrations_dir_unreadable: ' + e.message };
  }
  const results = [];
  for (const f of files) {
    const sql = fs.readFileSync(path.join(dir, f), 'utf-8');
    const dropFirst = /CREATE TRIGGER|CREATE OR REPLACE FUNCTION/i.test(sql);
    results.push(await applyMigration(db, f, sql, dropFirst));
  }
  return { ok: true, applied: results.filter(r => r.ok).length, failed: results.filter(r => !r.ok).length, results };
}

async function listApplied(db) {
  await ensureHistoryTable(db);
  return db('migration_history').orderBy('applied_at', 'desc').limit(100);
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();

  if (action === 'health') return res.json({ success: true, service: 'migration-runner' });
  if (action === 'list') return res.json({ ok: true, applied: await listApplied(db) });
  if (action === 'apply_all') return res.json(await applyAll(db));

  if (action === 'run') {
    let body = req.body;
    if (!body || typeof body !== 'object') {
      body = await new Promise(r => {
        let d=''; req.on('data', c=>d+=c);
        req.on('end', () => { try { r(JSON.parse(d || '{}')); } catch { r({}); } });
        req.on('error', () => r({}));
      });
    }
    if (!body.name || !body.sql) return res.status(400).json({ error: 'name+sql required' });
    return res.json(await applyMigration(db, body.name, body.sql, !!body.drop_first));
  }

  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.applyMigration = applyMigration;
module.exports.applyAll = applyAll;
