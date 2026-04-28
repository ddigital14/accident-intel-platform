/**
 * WHOIS + SOS state filings for newly-formed LLCs at incident addresses.
 * Estate-of LLC pattern: families form an LLC after fatal accident for legal/financial purposes.
 * Hits: GA Corporations Division, FL Sunbiz, OH Business Search.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

const SOS = {
  GA: name => `https://ecorp.sos.ga.gov/BusinessSearch/BusinessSearchResults?searchType=ENTITY&businessName=${encodeURIComponent(name)}`,
  FL: name => `https://search.sunbiz.org/Inquiry/CorporationSearch/SearchResults?inquiryType=EntityName&searchTerm=${encodeURIComponent(name)}`,
  OH: name => `https://businesssearch.ohiosos.gov/?=businessName${encodeURIComponent('=' + name)}`,
  TX: name => `https://mycpa.cpa.state.tx.us/coa/coaSearchBtn`
};

async function searchSos(name, state, db) {
  const fn = SOS[state]; if (!fn) return null;
  const url = fn(name);
  let html = null, ok = false;
  try { const r = await fetch(url, { timeout: 10000, headers: { 'User-Agent': 'Mozilla/5.0 AIP' } }); if (r.ok) { html = await r.text(); ok = html.length > 500; } } catch (_) {}
  await trackApiCall(db, 'ingest-whois-llc', `sos-${state}`, 0, 0, ok).catch(() => {});
  if (!html) return null;
  const hasMatch = new RegExp(name.replace(/\s+/g, '\\s+'), 'i').test(html);
  return { state, hasMatch, url };
}

async function batch(db, limit = 10) {
  // Find recent fatal incidents → search SOS for "Estate of {name}" or "{name} LLC"
  let rows = []; try {
    rows = await db('persons').whereNotNull('full_name').where('full_name', '!=', '')
      .where('deceased', true)
      .where(function () { this.whereNull('has_llc_searched').orWhere('has_llc_searched', false); })
      .whereIn('location_region', ['GA', 'FL', 'OH', 'TX'])
      .limit(limit);
  } catch (_) {}
  let found = 0;
  for (const p of rows) {
    const r = await searchSos(`Estate of ${p.full_name}`, p.location_region, db);
    try {
      await db('persons').where({ id: p.id }).update({ has_llc_searched: true, updated_at: new Date() });
      if (r?.hasMatch) {
        await enqueueCascade(db, 'person', p.id, 'whois-llc', { weight: 65, sos_state: r.state });
        found++;
      }
    } catch (_) {}
  }
  return { rows: rows.length, found };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { name, state, action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'whois-llc', states: Object.keys(SOS) });
    if (action === 'batch') { const out = await batch(db, parseInt(req.query.limit) || 10); return res.json({ success: true, ...out }); }
    if (name) { const r = await searchSos(name, state, db); return res.json({ success: !!r, ...r }); }
    return res.status(400).json({ error: 'need name or action=batch|health' });
  } catch (err) { await reportError(db, 'whois-llc', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.searchSos = searchSos;
