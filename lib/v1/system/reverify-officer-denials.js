/**
 * Phase 38 v2: persons denied via the over-aggressive 'official_title' rule
 * (Heather Avery flagged because article said "fallen Deputy Heather Avery")
 * are reset to NULL so the new victim-verb-dominant rule can re-evaluate them.
 */
const { getDb } = require('../../_db');

module.exports = async function handler(req, res) {
  if (req.query?.secret !== 'ingest-now') return res.status(401).json({ error: 'unauthorized' });
  const db = getDb();
  try {
    const r = await db('persons')
      .where('victim_verified', false)
      .where(function() {
        this.where('victim_verifier_reason', 'like', 'stage_a:official_title%')
            .orWhere('victim_verifier_reason', 'like', '%officer%')
            .orWhere('victim_verifier_reason', 'like', '%attribution_only%');
      })
      .update({ victim_verified: null, victim_verifier_reason: null, victim_verifier_stage: null });
    return res.json({ success: true, reset_count: r, timestamp: new Date().toISOString() });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
};
