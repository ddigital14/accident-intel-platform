/**
 * Public cascade endpoint
 *
 * GET /api/v1/system/cascade?secret=ingest-now
 *   Process queue (default 4 jobs)
 *
 * GET /api/v1/system/cascade?secret=ingest-now&action=enqueue&person_id=...
 *   Manually enqueue a person
 *
 * GET /api/v1/system/cascade?secret=ingest-now&action=run&person_id=...
 *   Synchronously run cascade now (for testing)
 *
 * GET /api/v1/system/cascade?action=stats
 *   Show queue stats (public, no secret needed)
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { enqueueCascade, processCascadeQueue, runCascadeForPerson, ensureQueueTable } = require('./_cascade');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const db = getDb();
  await ensureQueueTable(db);

  const action = req.query?.action || 'process';

  // Stats endpoint is public
  if (action === 'stats') {
    try {
      const counts = await db.raw(`
        SELECT status, COUNT(*) as c FROM cascade_queue
        WHERE enqueued_at > NOW() - INTERVAL '7 days'
        GROUP BY status
      `).then(r => r.rows || []);
      const recent = await db.raw(`
        SELECT person_id, trigger_source, confidence_before, confidence_after,
               fields_filled, sources_fired, completed_at
        FROM cascade_queue
        WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '24 hours'
        ORDER BY completed_at DESC LIMIT 20
      `).then(r => r.rows || []);
      return res.json({ success: true, counts, recent_runs: recent, timestamp: new Date().toISOString() });
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  }

  // Authenticated actions
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    if (action === 'enqueue' && req.query.person_id) {
      const result = await enqueueCascade(db, {
        person_id: req.query.person_id,
        trigger_source: req.query.source || 'manual',
        priority: parseInt(req.query.priority) || 5
      });
      return res.json({ success: true, ...result });
    }
    if (action === 'run' && req.query.person_id) {
      const log = await runCascadeForPerson(db, req.query.person_id);
      return res.json({ success: true, log });
    }
    // Default: process queue
    const result = await processCascadeQueue(db, { maxJobs: parseInt(req.query.max) || 4 });
    return res.json({ success: true, ...result, timestamp: new Date().toISOString() });
  } catch (err) {
    await reportError(db, 'cascade', null, err.message);
    res.status(500).json({ error: err.message });
  }
};
