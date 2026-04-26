/**
 * GET /api/v1/system/cost-debug?secret=ingest-now
 * Direct test: ensure table, insert one row, count rows, return diagnostic.
 */
const { getDb } = require('../../_db');
const cost = require('./cost');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const out = { steps: [] };
  try {
    out.steps.push({ step: 'trackApiCall_imported', value: typeof cost.trackApiCall });
    out.steps.push({ step: 'estimateCost_imported', value: typeof cost.estimateCost });

    // Manually call trackApiCall
    if (typeof cost.trackApiCall === 'function') {
      await cost.trackApiCall(db, 'cost-debug', 'test-service', 0, 0, true);
      out.steps.push({ step: 'trackApiCall_invoked', success: true });
    } else {
      out.steps.push({ step: 'trackApiCall_invoked', success: false, err: 'not a function' });
    }

    // Count rows in system_api_calls
    try {
      const countRow = await db('system_api_calls').count('* as c').first();
      out.row_count = parseInt(countRow.c);
    } catch (e) {
      out.row_count_error = e.message;
    }

    // Pull last 5 rows
    try {
      const rows = await db('system_api_calls').orderBy('created_at', 'desc').limit(5);
      out.recent_rows = rows;
    } catch (e) {
      out.recent_rows_error = e.message;
    }

    res.json({ success: true, ...out });
  } catch (err) {
    res.status(500).json({ error: err.message, ...out });
  }
};
