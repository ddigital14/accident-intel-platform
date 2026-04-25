/**
 * GET /api/v1/system/errors — list recent pipeline errors
 *   ?limit=100&pipeline=opendata&since=2026-04-25T00:00:00Z
 *   ?action=clear&secret=...&days=7
 */
const { getDb } = require('../../_db');
const { listErrors, clearOldErrors, reportError } = require('./_errors');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const db = getDb();
  try {
    if (req.query.action === 'clear') {
      const secret = req.query.secret || req.headers['x-cron-secret'];
      if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
        return res.status(401).json({ error: 'Unauthorized' });
      }
      const days = parseInt(req.query.days) || 7;
      const removed = await clearOldErrors(db, days);
      return res.json({ success: true, removed, older_than_days: days });
    }
    const limit = Math.min(500, parseInt(req.query.limit) || 100);
    const pipeline = req.query.pipeline || null;
    const since = req.query.since || null;
    const errors = await listErrors(db, { limit, pipeline, since });
    const summary = {};
    for (const e of errors) summary[e.pipeline] = (summary[e.pipeline] || 0) + 1;
    res.json({
      success: true,
      count: errors.length,
      summary,
      errors: errors.map(e => ({
        id: e.id,
        pipeline: e.pipeline,
        source: e.source,
        message: e.message,
        severity: e.severity,
        context: typeof e.context === 'string' ? JSON.parse(e.context) : e.context,
        created_at: e.created_at
      })),
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'system_errors', 'list', err.message);
    res.status(500).json({ error: err.message });
  }
};
