/**
 * Social Media Profile Search via Google Custom Search Engine
 *
 * Free 100 queries/day with API key + CSE ID.
 * Returns Facebook/LinkedIn/Twitter/Instagram profiles matching name+city.
 *
 * Setup:
 *   1. Get API key: https://developers.google.com/custom-search/v1/introduction
 *   2. Create CSE at programmablesearchengine.google.com — restrict to:
 *      facebook.com, linkedin.com, twitter.com, x.com, instagram.com
 *   3. Set GOOGLE_CSE_API_KEY + GOOGLE_CSE_ID env vars
 *
 * For accident victims: gives social profiles which often expose:
 *   - Workplace
 *   - Hometown / current city
 *   - Family members tagged in posts
 *   - Photos for visual confirmation
 */
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');

async function getCseConfig(db) {
  try {
    const row = await db('system_config').where('key', 'google_cse').first();
    if (row && row.value) {
      const v = typeof row.value === 'string' ? JSON.parse(row.value) : row.value;
      return { key: v.api_key || process.env.GOOGLE_CSE_API_KEY, cx: v.cse_id || process.env.GOOGLE_CSE_ID };
    }
  } catch (_) {}
  return { key: process.env.GOOGLE_CSE_API_KEY, cx: process.env.GOOGLE_CSE_ID };
}

async function searchSocial(name, city, state) {
  if (!cfg.key || !cfg.cx) return null;
  const q = `"${name}" ${city || ''} ${state || ''}`.trim();
  try {
    const r = await fetch(`https://www.googleapis.com/customsearch/v1?key=${cfg.key}&cx=${cfg.cx}&q=${encodeURIComponent(q)}&num=10`, {
      signal: AbortSignal.timeout(10000)
    });
    if (!r.ok) return null;
    const d = await r.json();
    const items = (d.items || []).map(i => ({
      title: i.title,
      link: i.link,
      snippet: i.snippet,
      platform: detectPlatform(i.link)
    }));
    return items.filter(i => i.platform);
  } catch (e) { return null; }
}

function detectPlatform(url) {
  if (/facebook\.com/.test(url)) return 'facebook';
  if (/linkedin\.com/.test(url)) return 'linkedin';
  if (/twitter\.com|x\.com/.test(url)) return 'twitter';
  if (/instagram\.com/.test(url)) return 'instagram';
  return null;
}

module.exports = async function handler(req, res) {
  const { getDb } = require('../../_db');
  const db = getDb();
  const cfg = await getCseConfig(db);
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const { name, city, state } = req.query;
  if (!name) return res.status(400).json({ error: 'name required' });
  if (!cfg.key || !cfg.cx) {
    return res.status(400).json({
      error: 'Google CSE not configured',
      hint: 'Set GOOGLE_CSE_API_KEY and GOOGLE_CSE_ID env vars. Free tier 100/day at console.developers.google.com.'
    });
  }
  try {
    const results = await searchSocial(name, city, state);
    res.json({ success: true, count: results?.length || 0, results });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
module.exports.searchSocial = searchSocial;
