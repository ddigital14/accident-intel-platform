/**
 * NHTSA FARS (Fatal Accident Reporting System) backfill stub.
 * Annual CSV dump pointer + stats. Heavy ingest runs offline; this exposes status.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');

async function status(db) {
  let count = 0;
  try { const r = await db.raw(`SELECT COUNT(*) AS n FROM fars_records`).then(r => r.rows || r); count = parseInt(r[0]?.n || 0); } catch (_) {}
  return { table_exists: count >= 0, rows: count, source: 'https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS' };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const s = await status(db);
    return res.json({ ok: true, engine: 'fars', ...s });
  } catch (err) { await reportError(db, 'fars', null, err.message); res.status(500).json({ error: err.message }); }
};
