/**
 * Archive.org Wayback + Common Crawl historical reverse-lookup.
 * Surfaces deleted obituaries, defunct PD pages, archived news where name+city appeared.
 * Free, weight ~65.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

async function waybackSearch(query, db) {
  const q = encodeURIComponent(`"${query}"`);
  const url = `https://archive.org/wayback/available?url=${q}`;
  const url2 = `https://web.archive.org/__wb/sparkline?output=json&url=${q}&collection=web`;
  let snapshots = [], ok = false;
  try {
    const r = await fetch(url, { timeout: 8000 });
    if (r.ok) { const b = await r.json(); if (b?.archived_snapshots?.closest) { snapshots.push(b.archived_snapshots.closest); ok = true; } }
  } catch (_) {}
  await trackApiCall(db, 'enrich-archive-search', 'wayback', 0, 0, ok).catch(() => {});
  return { source: 'wayback', snapshots };
}

async function commonCrawlIndex(domain, db) {
  const url = `https://index.commoncrawl.org/CC-MAIN-2024-51-index?url=${encodeURIComponent(domain)}&output=json&limit=5`;
  let lines = [], ok = false;
  try {
    const r = await fetch(url, { timeout: 8000 });
    if (r.ok) { const t = await r.text(); lines = t.trim().split('\n').slice(0, 5).map(l => { try { return JSON.parse(l); } catch (_) { return null; } }).filter(Boolean); ok = lines.length > 0; }
  } catch (_) {}
  await trackApiCall(db, 'enrich-archive-search', 'commoncrawl', 0, 0, ok).catch(() => {});
  return { source: 'commoncrawl', hits: lines };
}

async function search(name, city, db) {
  const q = `${name} ${city || ''}`.trim();
  const [w, cc] = await Promise.all([waybackSearch(q, db), commonCrawlIndex(`${name.split(' ').join('+')}.com`, db)]);
  const total = (w.snapshots?.length || 0) + (cc.hits?.length || 0);
  return { wayback: w, commoncrawl: cc, weight: total ? 65 : 0 };
}

async function batch(db, limit = 10) {
  let rows = []; try {
    rows = await db('persons').whereNotNull('full_name').where('full_name', '!=', '')
      .where(function () { this.whereNull('has_archive_searched').orWhere('has_archive_searched', false); })
      .where('identity_confidence', '<', 80)
      .orderBy('updated_at', 'desc').limit(limit);
  } catch (_) {}
  let hit = 0;
  for (const p of rows) {
    const r = await search(p.full_name, p.location_locality || p.city, db);
    try {
      await db('persons').where({ id: p.id }).update({ has_archive_searched: true, updated_at: new Date() });
      if (r.weight) {
        await db('enrichment_logs').insert({ person_id: p.id, source: 'archive-search', data: JSON.stringify(r), created_at: new Date() }).catch(() => {});
        await enqueueCascade(db, 'person', p.id, 'archive-search', { weight: r.weight });
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
    if (action === 'health') return res.json({ ok: true, engine: 'archive-search', sources: ['wayback', 'commoncrawl'], cost: 0, weight: 65 });
    if (action === 'batch') { const out = await batch(db, parseInt(req.query.limit) || 10); return res.json({ success: true, ...out }); }
    if (name) { const r = await search(name, city, db); return res.json({ success: true, ...r }); }
    return res.status(400).json({ error: 'need name or action=batch|health' });
  } catch (err) { await reportError(db, 'archive-search', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.search = search;
