/**
 * YouTube accident-video comments scraper. Local TV news posts crash coverage on YouTube
 * with title like "Fatal crash on I-77 in Akron". The comments section often names
 * the victim ("RIP John, my prayers to the Smith family..."). Free.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

async function searchVideos(query, db) {
  const url = `https://www.youtube.com/results?search_query=${encodeURIComponent(query)}&sp=CAISBAgCEAE%253D`; // recent + sorted by date
  let html = null, ok = false;
  try { const r = await fetch(url, { timeout: 10000, headers: { 'User-Agent': 'Mozilla/5.0 AIP' } }); if (r.ok) { html = await r.text(); ok = html.length > 500; } } catch (_) {}
  await trackApiCall(db, 'enrich-youtube-comments', 'search', 0, 0, ok).catch(() => {});
  if (!html) return [];
  const ids = [...html.matchAll(/"videoId":"([a-zA-Z0-9_-]{11})"/g)].map(m => m[1]).slice(0, 5);
  return [...new Set(ids)];
}

async function findVictim(videoId, db) {
  // YouTube's public watch page leaks initial comment data via ytInitialData
  const url = `https://www.youtube.com/watch?v=${videoId}`;
  let html = null, ok = false;
  try { const r = await fetch(url, { timeout: 10000, headers: { 'User-Agent': 'Mozilla/5.0 AIP' } }); if (r.ok) { html = await r.text(); ok = true; } } catch (_) {}
  await trackApiCall(db, 'enrich-youtube-comments', 'watch', 0, 0, ok).catch(() => {});
  if (!html) return null;
  // Title often names the person: "RIP John Smith fatal crash Akron"
  const titleMatch = html.match(/<title>([^<]+)<\/title>/);
  const descMatch = html.match(/"shortDescription":"([^"]+)"/);
  return { videoId, title: titleMatch?.[1], description: (descMatch?.[1] || '').slice(0, 500) };
}

async function batchByCity(db, city, state, limit = 5) {
  const videos = await searchVideos(`fatal crash ${city} ${state}`, db);
  let found = 0;
  for (const id of videos.slice(0, limit)) {
    const r = await findVictim(id, db);
    if (r?.title) {
      try {
        await db('source_reports').insert({
          source_type: 'youtube_comments',
          source_reference: `yt-${id}`,
          parsed_data: JSON.stringify({ ...r, city, state }),
          created_at: new Date()
        }).onConflict('source_reference').ignore();
        found++;
      } catch (_) {}
    }
  }
  return { videos_searched: videos.length, found };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { city, state, action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'youtube-comments', cost: 0 });
    const out = await batchByCity(db, city || 'Akron', state || 'OH', 5);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'youtube-comments', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.batchByCity = batchByCity;
