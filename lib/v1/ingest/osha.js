/**
 * OSHA Establishment Search + DOL workplace fatality reports.
 * Free. Catches workplace incidents before news. PI gold for premises/construction.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');

async function fetchFatalities(stateAbbr, db) {
  const url = `https://www.osha.gov/fatalities/api?state=${stateAbbr || ''}&fatalities=true&pageSize=25`;
  let body = null, ok = false;
  try { const r = await fetch(url, { timeout: 10000 }); if (r.ok) { body = await r.json(); ok = true; } } catch (_) {}
  await trackApiCall(db, 'ingest-osha', 'fatalities', 0, 0, ok).catch(() => {});
  return body?.fatalities || body || [];
}

async function run(db, state) {
  const items = await fetchFatalities(state, db);
  let inserted = 0;
  for (const it of items) {
    try {
      await db('incidents').insert({
        source: 'osha',
        source_id: `osha-${it.id || it.activityNum || Math.random().toString(36).slice(2)}`,
        description: it.preliminaryDescription || it.description || 'OSHA workplace fatality',
        accident_type: 'work_accident',
        severity: 'fatal',
        city: it.city, state: it.state || state,
        occurred_at: it.eventDate ? new Date(it.eventDate) : new Date(),
        created_at: new Date()
      }).onConflict('source_id').ignore();
      inserted++;
    } catch (_) {}
  }
  return { fetched: items.length, inserted };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { state, action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'osha', cost: 0 });
    const out = await run(db, state || 'TX');
    return res.json({ success: true, state: state || 'TX', ...out });
  } catch (err) { await reportError(db, 'osha', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
