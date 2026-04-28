/**
 * Reddit user-history scraper via Pushshift / pullpush mirrors.
 * For name+city extracted from PI cases, finds Reddit posts with job/family/photos.
 * Free, weight 60.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

const ENDPOINTS = [
  'https://api.pullpush.io/reddit/search/comment/?q=',
  'https://api.pushshift.io/reddit/search/comment/?q='
];

async function searchUser(query, db) {
  for (const base of ENDPOINTS) {
    const url = `${base}${encodeURIComponent(query)}&size=10&sort=desc`;
    let body = null, ok = false;
    try { const r = await fetch(url, { timeout: 8000 }); if (r.ok) { body = await r.json(); ok = true; } } catch (_) {}
    await trackApiCall(db, 'enrich-reddit-history', new URL(base).hostname, 0, 0, ok).catch(() => {});
    if (body?.data?.length) {
      return body.data.slice(0, 5).map(c => ({ author: c.author, subreddit: c.subreddit, body: (c.body || '').slice(0, 280), url: `https://reddit.com${c.permalink || ''}`, created: c.created_utc }));
    }
  }
  return [];
}

async function find(name, city, db) {
  if (!name || name.split(' ').length < 2) return { hits: [], weight: 0 };
  const q = `"${name}"${city ? ` "${city}"` : ''}`;
  const hits = await searchUser(q, db);
  return { hits, weight: hits.length ? 60 : 0 };
}

async function batch(db, limit = 10) {
  let rows = []; try {
    rows = await db('persons').whereNotNull('full_name').where('full_name', '!=', '')
      .where(function () { this.whereNull('has_reddit_searched').orWhere('has_reddit_searched', false); })
      .where('identity_confidence', '<', 80).orderBy('updated_at', 'desc').limit(limit);
  } catch (_) {}
  let hit = 0;
  for (const p of rows) {
    const r = await find(p.full_name, p.location_locality, db);
    try {
      await db('persons').where({ id: p.id }).update({ has_reddit_searched: true, updated_at: new Date() });
      if (r.weight) {
        await db('enrichment_logs').insert({ person_id: p.id, source: 'reddit-history', data: JSON.stringify(r.hits), created_at: new Date() }).catch(() => {});
        await enqueueCascade(db, 'person', p.id, 'reddit-history', { weight: r.weight });
        hit++;
      }
    } catch (_) {}
  }
  return { rows: rows.length, hit };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { name, city, action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'reddit-history', sources: ENDPOINTS, cost: 0, weight: 60 });
    if (action === 'batch') { const out = await batch(db, parseInt(req.query.limit) || 10); return res.json({ success: true, ...out }); }
    if (name) { const r = await find(name, city, db); return res.json({ success: true, ...r }); }
    return res.status(400).json({ error: 'need name or action=batch|health' });
  } catch (err) { await reportError(db, 'reddit-history', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.find = find;
