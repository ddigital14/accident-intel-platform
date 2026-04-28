/**
 * GitHub + npm + StackOverflow public profile finder. Free, identity weight ~70.
 * Hits public APIs (no auth needed for basic profile metadata).
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

async function github(name, db) {
  const q = encodeURIComponent(`${name} in:fullname`);
  const url = `https://api.github.com/search/users?q=${q}&per_page=3`;
  let body = null, ok = false;
  try { const r = await fetch(url, { timeout: 8000, headers: { 'User-Agent': 'AIP-DevProfile' } }); if (r.ok) { body = await r.json(); ok = true; } } catch (_) {}
  await trackApiCall(db, 'enrich-dev-profiles', 'github', 0, 0, ok).catch(() => {});
  return (body?.items || []).map(u => ({ source: 'github', login: u.login, url: u.html_url, avatar: u.avatar_url }));
}

async function npmUser(name, db) {
  const url = `https://registry.npmjs.org/-/v1/search?text=author:${encodeURIComponent(name)}&size=3`;
  let body = null, ok = false;
  try { const r = await fetch(url, { timeout: 8000 }); if (r.ok) { body = await r.json(); ok = true; } } catch (_) {}
  await trackApiCall(db, 'enrich-dev-profiles', 'npm', 0, 0, ok).catch(() => {});
  return (body?.objects || []).map(o => ({ source: 'npm', package: o.package?.name, author: o.package?.author?.name, url: o.package?.links?.npm }));
}

async function stackOverflow(name, db) {
  const url = `https://api.stackexchange.com/2.3/users?inname=${encodeURIComponent(name)}&site=stackoverflow&pagesize=3&order=desc&sort=reputation`;
  let body = null, ok = false;
  try { const r = await fetch(url, { timeout: 8000 }); if (r.ok) { body = await r.json(); ok = true; } } catch (_) {}
  await trackApiCall(db, 'enrich-dev-profiles', 'stackoverflow', 0, 0, ok).catch(() => {});
  return (body?.items || []).map(u => ({ source: 'stackoverflow', display_name: u.display_name, url: u.link, avatar: u.profile_image, reputation: u.reputation }));
}

async function find(fullName, db) {
  if (!fullName || fullName.split(' ').length < 2) return { hits: [], weight: 0 };
  const [gh, np, so] = await Promise.all([github(fullName, db), npmUser(fullName, db), stackOverflow(fullName, db)]);
  const hits = [...gh, ...np, ...so];
  return { hits, weight: hits.length ? 70 : 0 };
}

async function batch(db, limit = 15) {
  let rows = []; try {
    rows = await db('persons').whereNotNull('full_name').where('full_name', '!=', '')
      .where(function () { this.whereNull('has_dev_profile_searched').orWhere('has_dev_profile_searched', false); })
      .orderBy('updated_at', 'desc').limit(limit);
  } catch (_) {}
  let found = 0;
  for (const p of rows) {
    const r = await find(p.full_name, db);
    try {
      await db('persons').where({ id: p.id }).update({ has_dev_profile_searched: true, updated_at: new Date() });
      if (r.hits.length) {
        const linkedinish = r.hits.filter(h => h.url).map(h => h.url).join(' | ');
        await db('enrichment_logs').insert({ person_id: p.id, source: 'dev-profiles', data: JSON.stringify(r.hits), created_at: new Date() }).catch(() => {});
        await enqueueCascade(db, 'person', p.id, 'dev-profiles', { weight: r.weight, hits: r.hits.length });
        found++;
      }
    } catch (_) {}
  }
  return { rows: rows.length, found };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { name, action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'dev-profiles', sources: ['github', 'npm', 'stackoverflow'], cost: 0, weight: 70 });
    if (action === 'batch') { const out = await batch(db, parseInt(req.query.limit) || 15); return res.json({ success: true, ...out }); }
    if (name) { const r = await find(name, db); return res.json({ success: true, ...r }); }
    return res.status(400).json({ error: 'need name or action=batch|health' });
  } catch (err) { await reportError(db, 'dev-profiles', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.find = find;
