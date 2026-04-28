/**
 * Florida county-court PI scrapers — Broward / Miami-Dade / Hillsborough / Orange.
 * Catches "already-represented" plaintiffs missed by myfloridacounty.com statewide.
 * Free, weight 80.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

const COUNTIES = [
  { name: 'broward',     url: name => `https://www.browardclerk.org/Web2/CaseSearch/Results?LastName=${encodeURIComponent(name.split(' ').slice(-1)[0])}&FirstName=${encodeURIComponent(name.split(' ')[0])}&CaseType=PI` },
  { name: 'miamidade',   url: name => `https://www2.miami-dadeclerk.com/cjis/casesearch.aspx?srch=${encodeURIComponent(name)}&type=civil` },
  { name: 'hillsborough',url: name => `https://www.hillsclerk.com/PublicWebSearchVerification.aspx?SearchType=Name&LastName=${encodeURIComponent(name.split(' ').slice(-1)[0])}` },
  { name: 'orange',      url: name => `https://myeclerk.myorangeclerk.com/Cases/Search?searchTerm=${encodeURIComponent(name)}` }
];

async function searchCounty(name, county, db) {
  const c = COUNTIES.find(x => x.name === county); if (!c) return null;
  const url = c.url(name);
  let html = null, ok = false;
  try { const r = await fetch(url, { timeout: 10000, headers: { 'User-Agent': 'Mozilla/5.0 AIP' } }); if (r.ok) { html = await r.text(); ok = html.length > 500; } } catch (_) {}
  await trackApiCall(db, 'enrich-fl-county-courts', c.name, 0, 0, ok).catch(() => {});
  if (!html) return null;
  const hasAttorney = /attorney|esq\.|counsel|representing/i.test(html);
  const caseCount = (html.match(/case\s*(no|number|#)/gi) || []).length;
  return { source: c.name, hasAttorney, caseCount, weight: caseCount ? 80 : 0, url };
}

async function batch(db, limit = 8) {
  let rows = []; try {
    rows = await db('persons').whereNotNull('full_name').where('full_name', '!=', '')
      .where('location_region', 'FL')
      .where(function () { this.whereNull('has_fl_county_searched').orWhere('has_fl_county_searched', false); })
      .where(function () { this.whereNull('has_attorney').orWhere('has_attorney', false); })
      .limit(limit);
  } catch (_) {}
  let attorneyFound = 0;
  for (const p of rows) {
    const city = (p.location_locality || '').toLowerCase();
    let target = 'broward';
    if (/miami|hialeah/i.test(city)) target = 'miamidade';
    else if (/tampa|brandon|temple terrace/i.test(city)) target = 'hillsborough';
    else if (/orlando|winter park|pine hills/i.test(city)) target = 'orange';
    const r = await searchCounty(p.full_name, target, db);
    try {
      await db('persons').where({ id: p.id }).update({ has_fl_county_searched: true, updated_at: new Date() });
      if (r?.hasAttorney) {
        await db('persons').where({ id: p.id }).update({ has_attorney: true });
        await enqueueCascade(db, 'person', p.id, 'fl-county-courts', { weight: 80 });
        attorneyFound++;
      }
    } catch (_) {}
  }
  return { rows: rows.length, attorneyFound };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { name, county, action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'fl-county-courts', counties: COUNTIES.map(c => c.name), cost: 0, weight: 80 });
    if (action === 'batch') { const out = await batch(db, parseInt(req.query.limit) || 8); return res.json({ success: true, ...out }); }
    if (name) { const r = await searchCounty(name, county || 'broward', db); return res.json({ success: true, ...r }); }
    return res.status(400).json({ error: 'need name or action=batch|health' });
  } catch (err) { await reportError(db, 'fl-county-courts', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.searchCounty = searchCounty;
