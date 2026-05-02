/**
 * Phase 77 #5: Lead Quality Scorer (master_quality_score 0-100).
 *
 * Combines:
 *   - lead_score (severity × contact × recency)
 *   - pattern-miner signal delta
 *   - adversarial conflicts (subtract)
 *   - relationship-detector tier (review = -10, demoted = -30)
 *   - evidence cross-check matches/conflicts
 *   - victim_verified flag (+15)
 *   - has_attorney (-20: lawyer already engaged)
 *
 * Returns one number reps sort by — "this lead is qualified but risky" surfaces.
 */
const { getDb } = require('../../_db');
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

let _migrated = false;
async function ensureSchema(db) {
  if (_migrated) return;
  try {
    await db.raw('ALTER TABLE incidents ADD COLUMN IF NOT EXISTS master_quality_score INTEGER');
    await db.raw('CREATE INDEX IF NOT EXISTS idx_incidents_mqs ON incidents(master_quality_score DESC)');
    _migrated = true;
  } catch (_) {}
}

async function scoreOne(db, incidentId) {
  await ensureSchema(db);
  const inc = await db('incidents').where('id', incidentId).first();
  if (!inc) return { ok: false, error: 'incident_not_found' };

  let q = inc.lead_score || 0;
  const breakdown = { base_lead_score: q };

  // 1. Pattern-miner adjustment
  try {
    const pm = require('./pattern-miner');
    const r = await pm.applySignals(db, incidentId);
    if (r?.ok) {
      q += (r.adjusted_score_delta || 0);
      breakdown.pattern_delta = r.adjusted_score_delta || 0;
    }
  } catch (_) {}

  // 2. Pull persons + their adversarial logs
  const persons = await db('persons').where('incident_id', incidentId).select('*');
  let conflicts = 0, matches = 0, verified_count = 0, attorney_count = 0, demoted_count = 0, review_count = 0;
  for (const p of persons) {
    if (p.victim_verified) verified_count++;
    if (p.has_attorney) attorney_count++;
    if (p.lead_tier === 'demoted') demoted_count++;
    else if (p.lead_tier === 'review') review_count++;

    try {
      const cc = await db('enrichment_logs')
        .where('person_id', p.id)
        .where('field_name', 'evidence_cross_check_summary')
        .orderBy('created_at', 'desc').first();
      if (cc) {
        const d = typeof cc.new_value === 'string' ? JSON.parse(cc.new_value) : cc.new_value;
        conflicts += d?.conflicts || 0;
        matches += d?.matches || 0;
      }
    } catch (_) {}
  }

  breakdown.cross_check = { matches, conflicts };
  breakdown.persons = { total: persons.length, verified: verified_count, attorney: attorney_count, review: review_count, demoted: demoted_count };

  // 3. Apply adjustments
  q += matches * 3;          // each cross-source match: +3
  q -= conflicts * 8;        // each cross-engine conflict: -8
  q += verified_count * 8;   // each verified victim: +8
  q -= attorney_count * 15;  // already-lawyered up: -15 each
  q -= review_count * 10;    // discrepancy review: -10
  q -= demoted_count * 25;   // demoted: -25
  if (matches >= 2 && conflicts === 0) q += 5; // multi-source agreement bonus

  // 4. Clamp to [0, 100]
  q = Math.max(0, Math.min(100, Math.round(q)));
  breakdown.final = q;

  // 5. Persist
  try {
    await db('incidents').where('id', incidentId).update({ master_quality_score: q, updated_at: new Date() });
  } catch (_) {}

  return { ok: true, incident_id: incidentId, master_quality_score: q, breakdown };
}

async function scoreBatch(db, limit = 100) {
  await ensureSchema(db);
  const incidents = await db('incidents')
    .whereIn('qualification_state', ['qualified', 'pending_named', 'pending_review'])
    .orderBy('lead_score', 'desc')
    .limit(limit)
    .select('id');
  const results = [];
  for (const inc of incidents) {
    try {
      const r = await scoreOne(db, inc.id);
      results.push({ id: inc.id, score: r.master_quality_score });
    } catch (e) {
      results.push({ id: inc.id, error: e.message });
    }
  }
  return { ok: true, scored: results.length, top: results.sort((a, b) => (b.score || 0) - (a.score || 0)).slice(0, 20) };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();

  if (action === 'health') return res.json({ success: true, service: 'lead-quality-scorer' });
  if (action === 'score') {
    const id = req.query?.incident_id;
    if (!id) return res.status(400).json({ error: 'incident_id required' });
    return res.json(await scoreOne(db, id));
  }
  if (action === 'batch') {
    const limit = Math.min(200, parseInt(req.query?.limit) || 50);
    return res.json(await scoreBatch(db, limit));
  }
  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.scoreOne = scoreOne;
module.exports.scoreBatch = scoreBatch;
