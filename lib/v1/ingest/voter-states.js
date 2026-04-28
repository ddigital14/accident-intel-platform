/**
 * Multi-state voter file ingest stub. Adds OH/NC/MI/NJ to the existing FL/GA/TX path.
 * This is a thin wrapper around the existing voter-rolls loader pattern.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');

const STATES = {
  OH: { url: 'https://www6.ohiosos.gov/ords/f?p=VOTERFTP:STWD', format: 'csv', notes: 'Ohio statewide CSV, free download, ~7M rows' },
  NC: { url: 'https://dl.ncsbe.gov/?prefix=data/', format: 'pipe-delimited', notes: 'NC statewide, free, ~7M rows' },
  MI: { url: 'https://mvic.sos.state.mi.us/', format: 'csv', notes: 'Michigan QVF, request via secretary of state' },
  NJ: { url: 'https://www.elections.nj.gov/', format: 'request', notes: 'NJ requires written request, $5-200' }
};

async function status(db) {
  const out = {};
  for (const [code, info] of Object.entries(STATES)) {
    try {
      const r = await db.raw(`SELECT COUNT(*) AS n FROM voter_records WHERE state = ?`, [code]).then(r => r.rows || r).catch(() => [{ n: 0 }]);
      out[code] = { ...info, rows_loaded: parseInt(r[0]?.n || 0) };
    } catch (_) { out[code] = { ...info, rows_loaded: 0 }; }
  }
  return out;
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { action } = req.query || {};
    if (action === 'health' || !action) {
      const s = await status(db);
      return res.json({ ok: true, engine: 'voter-states', states: s });
    }
    return res.status(400).json({ error: 'unknown action' });
  } catch (err) { await reportError(db, 'voter-states', null, err.message); res.status(500).json({ error: err.message }); }
};
