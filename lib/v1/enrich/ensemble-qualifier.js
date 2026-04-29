/**
 * Confidence-weighted ensemble qualifier.
 * Sum of all source weights → qualification, instead of single hard threshold.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

const QUALIFY_THRESHOLD = 120;  // Phase 37 default
const VERIFIED_VICTIM_THRESHOLD = 60;  // Phase 45: dropped 80->60 to surface non-fatal injury cases earlier  // Phase 39: verified victims qualify earlier - they're already trusted

async function run(db, limit = 50) {
  // Phase 37: require linked incident with US city+state; reject obvious non-US (e.g. Spanish news bleed-through)
  const US_STATES = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC'];
  const sql = `
    SELECT p.id, p.full_name, COALESCE(SUM(weight_n), 0) AS evidence_sum
    FROM persons p
    LEFT JOIN incidents i ON p.incident_id = i.id
    LEFT JOIN LATERAL (
      SELECT (data::jsonb->>'weight')::int AS weight_n
      FROM enrichment_logs el
      WHERE el.person_id = p.id AND el.created_at > NOW() - INTERVAL '30 days'
    ) e ON true
    WHERE p.qualification_state IS DISTINCT FROM 'qualified'
      AND COALESCE(p.victim_verified, false) = true  -- Phase 38: only verified victims qualify
      AND (i.state IS NULL OR i.state IN ('${US_STATES.join("','")}'))
      AND (i.city IS NULL OR length(i.city) >= 2)
    GROUP BY p.id, p.full_name
    HAVING COALESCE(SUM(weight_n), 0) >= ${VERIFIED_VICTIM_THRESHOLD}
    LIMIT ${parseInt(limit) || 50}
  `;
  let rows = []; try { const r = await db.raw(sql); rows = r.rows || r; } catch (_) {}
  let promoted = 0;
  for (const row of rows) {
    try {
      await db('persons').where({ id: row.id }).update({
        qualification_state: 'qualified',
        identity_confidence: db.raw('GREATEST(COALESCE(identity_confidence, 0), 85)'),
        updated_at: new Date()
      });
      await enqueueCascade(db, 'person', row.id, 'ensemble-qualifier', { weight: 25, evidence_sum: row.evidence_sum });
      promoted++;
    } catch (_) {}
  }
  await trackApiCall(db, 'enrich-ensemble-qualifier', 'sql', 0, 0, true).catch(() => {});
  return { candidates: rows.length, promoted, threshold: VERIFIED_VICTIM_THRESHOLD };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'ensemble-qualifier', threshold: VERIFIED_VICTIM_THRESHOLD });
    const out = await run(db, parseInt(req.query.limit) || 50);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'ensemble-qualifier', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
