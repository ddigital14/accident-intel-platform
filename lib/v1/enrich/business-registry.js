/**
 * OpenCorporates + SEC EDGAR + state SoS business search.
 * Address ↔ LLC registration tie-out. If victim's home address matches a
 * registered LLC, registered agent name is a strong identity signal.
 * Free, weight 75.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

async function openCorporates(query, db) {
  const url = `https://api.opencorporates.com/v0.4/companies/search?q=${encodeURIComponent(query)}&per_page=5`;
  let body = null, ok = false;
  try { const r = await fetch(url, { timeout: 8000 }); if (r.ok) { body = await r.json(); ok = true; } } catch (_) {}
  await trackApiCall(db, 'enrich-business-registry', 'opencorporates', 0, 0, ok).catch(() => {});
  return (body?.results?.companies || []).map(c => ({ source: 'opencorporates', name: c.company?.name, registered_address: c.company?.registered_address_in_full, jurisdiction: c.company?.jurisdiction_code, agent: c.company?.agent_name }));
}

async function secEdgar(query, db) {
  const url = `https://efts.sec.gov/LATEST/search-index?q=${encodeURIComponent(query)}&forms=10-K&dateRange=custom`;
  let body = null, ok = false;
  try { const r = await fetch(url, { timeout: 8000, headers: { 'User-Agent': 'AIP-AccidentIntel donovan@donovandigitalsolutions.com' } }); if (r.ok) { body = await r.json(); ok = true; } } catch (_) {}
  await trackApiCall(db, 'enrich-business-registry', 'edgar', 0, 0, ok).catch(() => {});
  return (body?.hits?.hits || []).slice(0, 3).map(h => ({ source: 'edgar', cik: h._source?.ciks?.[0], company: h._source?.display_names?.[0] }));
}

async function search(query, db) {
  const [oc, ed] = await Promise.all([openCorporates(query, db), secEdgar(query, db)]);
  const total = oc.length + ed.length;
  return { opencorporates: oc, edgar: ed, weight: total ? 75 : 0 };
}

async function batch(db, limit = 8) {
  let rows = []; try {
    rows = await db('persons').whereNotNull('location_street_address').where('location_street_address', '!=', '')
      .where(function () { this.whereNull('has_business_searched').orWhere('has_business_searched', false); })
      .limit(limit);
  } catch (_) {}
  let hits = 0;
  for (const p of rows) {
    const q = `"${p.location_street_address}" ${p.location_locality || ''}`.trim();
    const r = await search(q, db);
    try {
      await db('persons').where({ id: p.id }).update({ has_business_searched: true, updated_at: new Date() });
      if (r.weight) {
        await db('enrichment_logs').insert({ person_id: p.id, source: 'business-registry', data: JSON.stringify(r), created_at: new Date() }).catch(() => {});
        await enqueueCascade(db, 'person', p.id, 'business-registry', { weight: r.weight });
        hits++;
      }
    } catch (_) {}
  }
  return { rows: rows.length, hits };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { q, action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'business-registry', sources: ['opencorporates', 'edgar'], cost: 0, weight: 75 });
    if (action === 'batch') { const out = await batch(db, parseInt(req.query.limit) || 8); return res.json({ success: true, ...out }); }
    if (q) { const r = await search(q, db); return res.json({ success: true, ...r }); }
    return res.status(400).json({ error: 'need q or action=batch|health' });
  } catch (err) { await reportError(db, 'business-registry', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.search = search;
