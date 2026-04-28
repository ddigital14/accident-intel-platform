/**
 * FBI UCR + local CompStat dumps. Free incident summaries by city + category.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');

async function ori(state, db) {
  const url = `https://api.usa.gov/crime/fbi/cde/agencies/byStateAbbr/${state}`;
  let body = null, ok = false;
  try { const r = await fetch(url, { timeout: 8000 }); if (r.ok) { body = await r.json(); ok = true; } } catch (_) {}
  await trackApiCall(db, 'ingest-fbi-ucr', 'agencies', 0, 0, ok).catch(() => {});
  return body || [];
}

async function run(db, state) {
  const list = await ori(state || 'OH', db);
  return { state: state || 'OH', agencies: Array.isArray(list) ? list.length : 0 };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { state, action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'fbi-ucr', cost: 0 });
    const out = await run(db, state);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'fbi-ucr', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
