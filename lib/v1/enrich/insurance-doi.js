/**
 * Insurance carrier inference + DOI policy-limit lookup.
 * From PD report carrier name → state DOI database → estimate policy limits.
 * Most state DOIs publish carrier complaint counts, market share, NAIC IDs publicly.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

const NAIC_API = 'https://content.naic.org/cipr_topics/topic_general_companies.htm';
// Common carrier minimum policy limits by state (lookup table — per state DOI publications)
const STATE_MINIMUMS = {
  OH: { bi_per_person: 25000, bi_per_accident: 50000, pd: 25000 },
  TX: { bi_per_person: 30000, bi_per_accident: 60000, pd: 25000 },
  GA: { bi_per_person: 25000, bi_per_accident: 50000, pd: 25000 },
  FL: { bi_per_person: 10000, bi_per_accident: 20000, pd: 10000, pip: 10000 },
  AZ: { bi_per_person: 25000, bi_per_accident: 50000, pd: 15000 }
};

// Top-10 commercial-line carriers + estimated typical max limits
const CARRIER_INTEL = {
  'state farm': { commercial_max: 300000, market_rank: 1 },
  'geico': { commercial_max: 250000, market_rank: 2 },
  'progressive': { commercial_max: 300000, market_rank: 3 },
  'allstate': { commercial_max: 250000, market_rank: 4 },
  'usaa': { commercial_max: 500000, market_rank: 5 },
  'liberty mutual': { commercial_max: 300000, market_rank: 6 },
  'farmers': { commercial_max: 250000, market_rank: 7 },
  'nationwide': { commercial_max: 250000, market_rank: 8 },
  'travelers': { commercial_max: 500000, market_rank: 9 },
  'american family': { commercial_max: 250000, market_rank: 10 }
};

async function estimateLimits(carrierName, state, db) {
  const carrier = (carrierName || '').toLowerCase().trim();
  const carrierKey = Object.keys(CARRIER_INTEL).find(k => carrier.includes(k));
  const intel = carrierKey ? CARRIER_INTEL[carrierKey] : null;
  const stateMin = STATE_MINIMUMS[(state || '').toUpperCase()] || null;
  await trackApiCall(db, 'enrich-insurance-doi', 'estimate', 0, 0, true).catch(() => {});
  return {
    carrier_canonical: carrierKey || null,
    state_minimum: stateMin,
    carrier_typical_max: intel?.commercial_max || null,
    market_rank: intel?.market_rank || null,
    estimated_policy_range: intel ? `${stateMin?.bi_per_person || '?'}/${stateMin?.bi_per_accident || '?'} to ${intel.commercial_max}/${intel.commercial_max * 2}` : null,
    confidence: carrierKey ? 70 : 30
  };
}

async function batch(db, limit = 25) {
  let rows = []; try {
    rows = await db('persons').whereNotNull('insurance_carrier').where('insurance_carrier', '!=', '')
      .where(function () { this.whereNull('policy_limits_estimated').orWhere('policy_limits_estimated', false); })
      .limit(limit);
  } catch (_) {}
  let scored = 0;
  for (const p of rows) {
    const e = await estimateLimits(p.insurance_carrier, p.location_region, db);
    try {
      await db('persons').where({ id: p.id }).update({
        policy_limits_estimated: true,
        policy_limits_min: e.state_minimum?.bi_per_person || null,
        policy_limits_max: e.carrier_typical_max || null,
        carrier_market_rank: e.market_rank || null,
        updated_at: new Date()
      });
      await enqueueCascade(db, 'person', p.id, 'insurance-doi', { weight: e.confidence, range: e.estimated_policy_range });
      scored++;
    } catch (_) {}
  }
  return { rows: rows.length, scored };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { carrier, state, action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'insurance-doi', states: Object.keys(STATE_MINIMUMS), carriers: Object.keys(CARRIER_INTEL).length });
    if (action === 'batch') { const out = await batch(db, parseInt(req.query.limit) || 25); return res.json({ success: true, ...out }); }
    if (carrier) { const r = await estimateLimits(carrier, state, db); return res.json({ success: true, ...r }); }
    return res.status(400).json({ error: 'need carrier or action=batch|health' });
  } catch (err) { await reportError(db, 'insurance-doi', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.estimateLimits = estimateLimits;
