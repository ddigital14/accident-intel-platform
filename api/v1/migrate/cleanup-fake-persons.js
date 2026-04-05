/**
 * One-time cleanup: Remove fake persons with no real data
 * GET /api/v1/migrate/cleanup-fake-persons?secret=cleanup-now
 */
const { getDb } = require('../../_db');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'cleanup-now') {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  const db = getDb();
  const results = {};

  try {
    // Count before
    const before = await db.raw(`
      SELECT
        (SELECT COUNT(*) FROM persons) as total_persons,
        (SELECT COUNT(*) FROM persons WHERE phone IS NULL AND email IS NULL AND address IS NULL AND (enrichment_sources IS NULL OR enrichment_sources::text = '{}')) as fake_persons,
        (SELECT COUNT(*) FROM enrichment_logs) as total_enrichment_logs
    `);
    results.before = before.rows[0];

    // Step 1: Delete enrichment_logs for fake persons
    const delLogs = await db.raw(`
      DELETE FROM enrichment_logs WHERE person_id IN (
        SELECT id FROM persons
        WHERE phone IS NULL AND email IS NULL AND address IS NULL
        AND (enrichment_sources IS NULL OR enrichment_sources::text = '{}')
      )
    `);
    results.enrichment_logs_deleted = delLogs.rowCount;

    // Step 2: Delete fake persons
    const delPersons = await db.raw(`
      DELETE FROM persons
      WHERE phone IS NULL AND email IS NULL AND address IS NULL
      AND (enrichment_sources IS NULL OR enrichment_sources::text = '{}')
    `);
    results.persons_deleted = delPersons.rowCount;

    // Count after
    const after = await db.raw(`
      SELECT
        (SELECT COUNT(*) FROM persons) as total_persons,
        (SELECT COUNT(*) FROM enrichment_logs) as total_enrichment_logs
    `);
    results.after = after.rows[0];

    res.json({ success: true, ...results, timestamp: new Date().toISOString() });
  } catch (err) {
    console.error('Cleanup error:', err);
    res.status(500).json({ error: err.message, results });
  }
};
