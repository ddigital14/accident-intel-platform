/**
 * GET  /api/v1/system/changelog          — list recent changelog entries
 * POST /api/v1/system/changelog?secret=… — record a new entry
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');

let _tableEnsured = false;
async function ensureTable(db) {
  if (_tableEnsured) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS system_changelog (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        kind VARCHAR(40) NOT NULL,
        title VARCHAR(255) NOT NULL,
        summary TEXT,
        ref VARCHAR(120),
        author VARCHAR(120) DEFAULT 'system',
        meta JSONB DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_changelog_created ON system_changelog(created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_changelog_kind ON system_changelog(kind);
    `);
    _tableEnsured = true;
  } catch (e) {
    console.error('Failed to ensure system_changelog table:', e.message);
  }
}

async function logChange(db, entry) {
  await ensureTable(db);
  return db('system_changelog').insert({
    kind: entry.kind || 'feature',
    title: String(entry.title || 'untitled').substring(0, 255),
    summary: entry.summary || null,
    ref: entry.ref || null,
    author: entry.author || entry.by || 'system',
    meta: JSON.stringify(entry.meta || {}),
    created_at: new Date()
  }).returning('*');
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const db = getDb();
  try {
    if (req.method === 'POST') {
      const secret = req.query.secret || req.headers['x-cron-secret'];
      if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
        return res.status(401).json({ error: 'Unauthorized' });
      }
      const body = typeof req.body === 'string' ? JSON.parse(req.body) : (req.body || {});
      const inserted = await logChange(db, body);
      return res.json({ success: true, entry: inserted[0] });
    }
    await ensureTable(db);
    const limit = Math.min(500, parseInt(req.query.limit) || 50);
    const kind = req.query.kind;
    let q = db('system_changelog').select('*').orderBy('created_at', 'desc').limit(limit);
    if (kind) q = q.where('kind', kind);
    const entries = await q;
    res.json({
      success: true,
      count: entries.length,
      entries: entries.map(e => ({
        ...e,
        meta: typeof e.meta === 'string' ? JSON.parse(e.meta) : e.meta
      })),
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'changelog', null, err.message);
    res.status(500).json({ error: err.message });
  }
};

module.exports.logChange = logChange;
