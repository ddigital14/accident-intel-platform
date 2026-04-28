/**
 * State court e-file public docket scraper. Catches "already represented" earlier than CourtListener.
 * Free RSS / public JSON for: Maryland Judiciary, Florida CCIS, Texas re:SearchTX (where exposed).
 * Weight 80 (court filing = legal representation signal).
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

const SOURCES = [
  { state: 'MD', name: 'mdjudiciary', url: name => `https://casesearch.courts.state.md.us/casesearch/inquirySearch.jis?lastName=${encodeURIComponent(name.split(' ').slice(-1)[0])}&firstName=${encodeURIComponent(name.split(' ')[0])}` },
  { state: 'FL', name: 'flclerks', url: name => `https://www.myfloridacounty.com/ori/search.do?lastName=${encodeURIComponent(name.split(' ').slice(-1)[0])}` },
  { state: 'TX', name: 'txresearch', url: name => `https://research.txcourts.gov/CourtRecordsSearch/Search/PartySearch?lastName=${encodeURIComponent(name.split(' ').slice(-1)[0])}` }
];

async function searchState(name, state, db) {
  const src = SOURCES.find(s => s.state === state); if (!src) return null;
  const url = src.url(name);
  let html = null, ok = false;
  try { const r = await fetch(url, { timeout: 10000, headers: { 'User-Agent': 'Mozilla/5.0 AIP' } }); if (r.ok) { html = await r.text(); ok = html.length > 500; } } catch (_) {}
  await trackApiCall(db, 'enrich-state-courts', src.name, 0, 0, ok).catch(() => {});
  if (!html) return null;
  const hasPlaintiff = /plaintiff|petitioner|claimant/i.test(html);
  const hasAttorney = /attorney|esq\.|counsel/i.test(html);
  const caseCount = (html.match(/case\s*(no|number|#)/gi) || []).length;
  return { source: src.name, state, hasPlaintiff, hasAttorney, caseCount, weight: caseCount ? 80 : 0, url };
}

async function search(name, state, db) {
  if (state) return await searchState(name, state, db);
  const out = [];
  for (const s of SOURCES) { const r = await searchState(name, s.state, db); if (r?.caseCount) out.push(r); }
  return out;
}

async function batch(db, limit = 10) {
  let rows = []; try {
    rows = await db('persons').whereNotNull('full_name').where('full_name', '!=', '')
      .where(function () { this.whereNull('has_attorney').orWhere('has_attorney', false); })
      .whereIn('location_region', ['MD', 'FL', 'TX'])
      .where(function () { this.whereNull('has_state_court_searched').orWhere('has_state_court_searched', false); })
      .limit(limit);
  } catch (_) {}
  let attorneyFound = 0;
  for (const p of rows) {
    const r = await searchState(p.full_name, p.location_region, db);
    try {
      await db('persons').where({ id: p.id }).update({ has_state_court_searched: true, updated_at: new Date() });
      if (r?.hasAttorney) {
        await db('persons').where({ id: p.id }).update({ has_attorney: true });
        await enqueueCascade(db, 'person', p.id, 'state-courts', { weight: 80 });
        attorneyFound++;
      }
    } catch (_) {}
  }
  return { rows: rows.length, attorneyFound };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { name, state, action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'state-courts', states: SOURCES.map(s => s.state), cost: 0, weight: 80 });
    if (action === 'batch') { const out = await batch(db, parseInt(req.query.limit) || 10); return res.json({ success: true, ...out }); }
    if (name) { const r = await search(name, state, db); return res.json({ success: true, results: r }); }
    return res.status(400).json({ error: 'need name or action=batch|health' });
  } catch (err) { await reportError(db, 'state-courts', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.search = search;
