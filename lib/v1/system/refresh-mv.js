/**
 * Refreshes mv_dashboard_summary CONCURRENTLY (no read-blocking).
 * Cron: every 60s.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('./cost');
const { reportError } = require('./_errors');

async function refresh(db) {
  try {
    await db.raw('REFRESH MATERIALIZED VIEW CONCURRENTLY mv_dashboard_summary');
    return { ok: true };
  } catch (e) {
    // Fallback to non-concurrent if unique index missing
    try { await db.raw('REFRESH MATERIALIZED VIEW mv_dashboard_summary'); return { ok: true, fallback: 'non_concurrent' }; }
    catch (e2) { return { ok: false, error: e2.message }; }
  }
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'refresh-mv' });
    const out = await refresh(db);
    await trackApiCall(db, 'system-refresh-mv', 'mv', 0, 0, out.ok).catch(() => {});
    return res.json({ success: out.ok !== false, ...out });
  } catch (err) { await reportError(db, 'refresh-mv', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.refresh = refresh;
