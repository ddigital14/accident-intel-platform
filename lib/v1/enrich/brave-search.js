/**
 * BRAVE SEARCH FALLBACK — Phase 43
 *
 * Brave Search API has a free tier (2,000 queries/month, no IP blocks, no
 * CAPTCHA). Used as automatic fallback when Google CSE returns 403/429
 * (which it does often once the daily quota burns).
 *
 * SETUP (one-time, free):
 *   1. Sign up at https://brave.com/search/api/
 *   2. Grab your subscription token (free tier is fine)
 *   3. POST to /api/v1/system/setup with {"brave_api_key": "..."}
 *      OR set env var BRAVE_API_KEY on Vercel.
 *
 * If no key is configured, every call gracefully no-ops (returns ok:false,
 * error:'no_brave_key'). The OSINT miner treats that as "skip Brave, keep
 * going" — never throws.
 *
 * Endpoints:
 *   GET /api/v1/enrich/brave-search?secret=ingest-now&action=health
 *   GET /api/v1/enrich/brave-search?secret=ingest-now&action=search&q=<query>
 *
 * Library export:
 *   const { searchBrave } = require('./brave-search');
 *   const r = await searchBrave('John Doe Akron obituary', { db, count: 5 });
 *   // r => { ok, results: [{title, url, description, age}], raw }
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');

const SECRET = 'ingest-now';
const HTTP_TIMEOUT_MS = 15000;
const BRAVE_ENDPOINT = 'https://api.search.brave.com/res/v1/web/search';

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function getBraveKey(db) {
  if (process.env.BRAVE_API_KEY) return process.env.BRAVE_API_KEY;
  try {
    const row = await db('system_config').where({ key: 'brave_api_key' }).first();
    if (row?.value) {
      const v = typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
      if (v && typeof v === 'string') return v;
      if (v && v.api_key) return v.api_key;
    }
  } catch (_) {}
  try {
    const row2 = await db('system_config').where({ key: 'brave' }).first();
    if (row2?.value) {
      const v = typeof row2.value === 'string' ? JSON.parse(row2.value) : row2.value;
      if (v?.api_key) return v.api_key;
    }
  } catch (_) {}
  return null;
}

/**
 * Library-callable. Pass in db (optional) for cost tracking.
 * Returns { ok, results: [...], raw, status }.
 */
async function searchBrave(query, options = {}) {
  const db = options.db || null;
  const count = Math.min(20, Math.max(1, parseInt(options.count || 5)));
  const country = options.country || 'us';
  const key = options.apiKey || (db ? await getBraveKey(db) : process.env.BRAVE_API_KEY);

  if (!key) return { ok: false, error: 'no_brave_key', skipped: true, results: [] };
  if (!query || !String(query).trim()) return { ok: false, error: 'empty_query', results: [] };

  const url = `${BRAVE_ENDPOINT}?q=${encodeURIComponent(query)}&count=${count}&country=${country}&safesearch=off`;
  try {
    const resp = await fetch(url, {
      headers: {
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip',
        'X-Subscription-Token': key
      },
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS)
    });
    const status = resp.status;
    if (db) await trackApiCall(db, 'brave-search', 'web/search', 0, 0, resp.ok).catch(() => {});
    if (!resp.ok) {
      const text = await resp.text().catch(() => '');
      return { ok: false, error: `http_${status}`, status, results: [], raw: text.slice(0, 300) };
    }
    const data = await resp.json().catch(() => ({}));
    const web = data?.web?.results || [];
    const results = web.map(r => ({
      title: r.title || '',
      url: r.url || '',
      description: r.description || r.snippet || '',
      age: r.age || null,
      meta_url: r.meta_url || null,
      deep_links: (r.deep_results?.buttons || []).map(b => b.url).filter(Boolean).slice(0, 4)
    }));
    return {
      ok: true,
      results,
      total: web.length,
      query,
      raw: { discussions: data.discussions?.results?.length || 0, news: data.news?.results?.length || 0 }
    };
  } catch (e) {
    if (db) await trackApiCall(db, 'brave-search', 'web/search', 0, 0, false).catch(() => {});
    return { ok: false, error: `exception:${e.name}:${e.message}`, results: [] };
  }
}

async function health(db) {
  const key = await getBraveKey(db);
  return {
    ok: true,
    has_key: !!key,
    setup_url: 'https://brave.com/search/api/',
    setup_hint: 'POST /api/v1/system/setup with {"brave_api_key": "..."} or set BRAVE_API_KEY env',
    free_tier_quota: '2000 queries/month'
  };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const db = getDb();
  const action = (req.query?.action || 'health').toLowerCase();

  try {
    if (action === 'health') {
      const h = await health(db);
      return res.json({ success: true, action: 'health', ...h, timestamp: new Date().toISOString() });
    }
    if (action === 'search') {
      const q = req.query?.q || req.query?.query;
      if (!q) return res.status(400).json({ error: 'q required' });
      const count = parseInt(req.query?.count || '5');
      const out = await searchBrave(q, { db, count });
      return res.json({ success: !!out.ok, ...out, timestamp: new Date().toISOString() });
    }
    return res.status(400).json({ error: 'unknown action', valid: ['health', 'search'] });
  } catch (e) {
    try { await reportError(db, 'brave-search', null, e.message); } catch (_) {}
    return res.status(500).json({ success: false, error: e.message });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.searchBrave = searchBrave;
module.exports.getBraveKey = getBraveKey;
module.exports.health = health;
