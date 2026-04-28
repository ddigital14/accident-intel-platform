/**
 * Citizen.com __NEXT_DATA__ shape probe + adaptive parser.
 * Reddit-style fallback: if pageProps.incidents is empty, walk the entire JSON
 * tree for any object with {title, location, type} that smells like an incident.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');

function deepFindIncidents(node, results = [], depth = 0) {
  if (depth > 8 || !node) return results;
  if (Array.isArray(node)) {
    for (const item of node) deepFindIncidents(item, results, depth + 1);
    return results;
  }
  if (typeof node === 'object') {
    if ((node.title || node.headline || node.subject) && (node.location || node.address || node.coordinates) && /accident|crash|injury|fire|shooting|fight|incident|emergency/i.test(`${node.title || ''} ${node.headline || ''} ${node.subject || ''} ${node.type || ''}`)) {
      results.push(node);
    }
    for (const k of Object.keys(node)) deepFindIncidents(node[k], results, depth + 1);
  }
  return results;
}

async function probe(city, db) {
  const url = `https://citizen.com/${city || 'akron-oh'}`;
  let html = null, ok = false;
  try { const r = await fetch(url, { timeout: 10000, headers: { 'User-Agent': 'Mozilla/5.0 AIP' } }); if (r.ok) { html = await r.text(); ok = true; } } catch (_) {}
  await trackApiCall(db, 'ingest-citizen-probe', city || 'akron-oh', 0, 0, ok).catch(() => {});
  if (!html) return { error: 'fetch_failed' };
  const m = html.match(/<script[^>]+id=["']__NEXT_DATA__["'][^>]*>([\s\S]*?)<\/script>/);
  if (!m) return { error: 'no_next_data' };
  let data = null; try { data = JSON.parse(m[1]); } catch (_) { return { error: 'parse_fail' }; }
  // Try canonical paths first
  const canonical = data?.props?.pageProps?.incidents || data?.props?.pageProps?.feed || [];
  // Fallback: deep search
  const fallback = deepFindIncidents(data);
  return { canonical_count: canonical.length, fallback_count: fallback.length, sample: (canonical[0] || fallback[0] || null), shape_signature: Object.keys(data?.props?.pageProps || {}).slice(0, 10) };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { city, action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'citizen-probe' });
    const out = await probe(city || 'akron-oh', db);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'citizen-probe', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.probe = probe;
module.exports.deepFindIncidents = deepFindIncidents;
