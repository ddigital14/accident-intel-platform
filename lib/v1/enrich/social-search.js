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

async function searchSocial(name, city, state, cfg) {
  if (!cfg || !cfg.key || !cfg.cx) return null;
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
  const action = req.query.action;
  // Batch mode — pull persons-without-socials from DB and enrich
  if (action === 'batch') {
    if (!cfg.key || !cfg.cx) return res.status(200).json({ success: true, processed: 0, hint: 'CSE not configured' });
    const limit = Math.min(50, parseInt(req.query.limit) || 15);
    let candidates = [];
    try {
      candidates = await db('persons')
        .whereNotNull('full_name').where('full_name', '!=', '')
        .where(function() {
          this.whereNull('facebook_url').orWhere('facebook_url', '');
        })
        .orderBy('updated_at', 'desc')
        .limit(limit);
    } catch (_) { candidates = []; }
    let enriched = 0;
    for (const p of candidates) {
      try {
        const r = await searchSocial(p.full_name || `${p.first_name} ${p.last_name}`, p.city, p.state, cfg);
        if (r && r.length) {
          const updates = {};
          for (const item of r) {
            if (item.platform === 'facebook' && !p.facebook_url) updates.facebook_url = item.link;
            if (item.platform === 'linkedin' && !p.linkedin_url) updates.linkedin_url = item.link;
            if (item.platform === 'twitter' && !p.twitter_url) updates.twitter_url = item.link;
            if (item.platform === 'instagram' && !p.instagram_url) updates.instagram_url = item.link;
          }
          if (Object.keys(updates).length) {
            try { await db('persons').where('id', p.id).update({ ...updates, updated_at: new Date() }); } catch(_) {}
            enriched++;
          }
        }
      } catch (_) {}
    }
    return res.status(200).json({ success: true, candidates: candidates.length, enriched });
  }
  const { name, city, state } = req.query;
  if (!name) return res.status(400).json({ error: 'name required' });
  if (!cfg.key || !cfg.cx) {
    return res.status(400).json({
      error: 'Google CSE not configured',
      hint: 'Set GOOGLE_CSE_API_KEY and GOOGLE_CSE_ID env vars. Free tier 100/day at console.developers.google.com.'
    });
  }
  try {
    const results = await searchSocial(name, city, state, cfg);
    res.json({ success: true, count: results?.length || 0, results });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
module.exports.searchSocial = searchSocial;
