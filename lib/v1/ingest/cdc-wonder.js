/**
 * CDC Wonder mortality cross-match. Surfaces fatals our news/PD-press missed.
 * CDC Wonder API requires SOAP request; this stub exposes intake placeholder.
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    return res.json({ ok: true, engine: 'cdc-wonder', status: 'stub', note: 'CDC Wonder uses SOAP — full integration requires offline batch query', source: 'https://wonder.cdc.gov/wonder/help/main.html' });
  } catch (err) { await reportError(db, 'cdc-wonder', null, err.message); res.status(500).json({ error: err.message }); }
};
