/**
 * Re-ingestion scheduler with backoff. Every URL/feed gets a freshness rank.
 * Dead feeds retried less, hot feeds retried more. Saves cron budget.
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');

async function ensureTable(db) {
  await db.raw(`CREATE TABLE IF NOT EXISTS feed_freshness (
    feed_url TEXT PRIMARY KEY,
    success_count INT DEFAULT 0,
    failure_count INT DEFAULT 0,
    last_success_at TIMESTAMPTZ,
    last_failure_at TIMESTAMPTZ,
    backoff_minutes INT DEFAULT 30,
    next_due_at TIMESTAMPTZ DEFAULT NOW(),
    rank INT DEFAULT 50
  )`).catch(() => {});
}

async function recordResult(db, url, success, items = 0) {
  await ensureTable(db);
  if (success && items > 0) {
    await db.raw(`INSERT INTO feed_freshness (feed_url, success_count, last_success_at, backoff_minutes, next_due_at, rank)
      VALUES (?, 1, NOW(), 30, NOW() + INTERVAL '30 minutes', LEAST(99, 50 + ?))
      ON CONFLICT (feed_url) DO UPDATE SET success_count = feed_freshness.success_count + 1,
        last_success_at = NOW(),
        backoff_minutes = GREATEST(15, feed_freshness.backoff_minutes / 2),
        next_due_at = NOW() + (GREATEST(15, feed_freshness.backoff_minutes / 2) || ' minutes')::interval,
        rank = LEAST(99, feed_freshness.rank + 2)`, [url, items]);
  } else {
    await db.raw(`INSERT INTO feed_freshness (feed_url, failure_count, last_failure_at, backoff_minutes, next_due_at, rank)
      VALUES (?, 1, NOW(), 60, NOW() + INTERVAL '60 minutes', 30)
      ON CONFLICT (feed_url) DO UPDATE SET failure_count = feed_freshness.failure_count + 1,
        last_failure_at = NOW(),
        backoff_minutes = LEAST(2880, feed_freshness.backoff_minutes * 2),
        next_due_at = NOW() + (LEAST(2880, feed_freshness.backoff_minutes * 2) || ' minutes')::interval,
        rank = GREATEST(0, feed_freshness.rank - 5)`, [url]);
  }
}

async function dueNow(db, limit = 25) {
  await ensureTable(db);
  return await db('feed_freshness').where('next_due_at', '<=', new Date()).orderBy('rank', 'desc').limit(limit);
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { action, url, success, items } = req.query || {};
    if (action === 'health' || action === 'list') {
      await ensureTable(db);
      const top = await db('feed_freshness').orderBy('rank', 'desc').limit(20);
      return res.json({ ok: true, engine: 'feed-freshness', top });
    }
    if (action === 'record' && url) { await recordResult(db, url, success === 'true', parseInt(items || 0)); return res.json({ success: true }); }
    if (action === 'due') { const r = await dueNow(db, parseInt(req.query.limit) || 25); return res.json({ rows: r }); }
    return res.status(400).json({ error: 'unknown action' });
  } catch (err) { await reportError(db, 'feed-freshness', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.recordResult = recordResult;
module.exports.dueNow = dueNow;
