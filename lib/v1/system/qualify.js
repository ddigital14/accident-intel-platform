/**
 * GET /api/v1/system/qualify?secret=ingest-now
 * Scans recent incidents, evaluates qualification rule, updates state + lead_score.
 * Auto-promotes Pending → Qualified when contact info appears.
 *
 * Cron: every 5 minutes
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('./_errors');
const { ensureColumns, evaluateIncident, computeLeadScore } = require('./_qualify');
const { logChange } = require('./changelog');
const { computeCaseValue, ensureColumns: ensureCaseValueColumns } = require('./_case_value');
const { applyAllToIncident, checkLikelyFamilyResidence, validateAgeAgainstVoter, crossSourceNameValidation } = require('../enrich/cross-wires');
const { enqueueIncidentCascade } = require('./_cascade');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const results = { evaluated: 0, promoted: 0, demoted: 0, score_updates: 0, errors: [], samples: [] };
  try {
    await ensureColumns(db);
    await ensureCaseValueColumns(db);
    // Phase 21 columns
    try {
      await db.raw(`
        ALTER TABLE persons ADD COLUMN IF NOT EXISTS identity_confidence INTEGER;
        ALTER TABLE persons ADD COLUMN IF NOT EXISTS identity_conflict BOOLEAN DEFAULT FALSE;
        ALTER TABLE persons ADD COLUMN IF NOT EXISTS likely_family_residence BOOLEAN DEFAULT FALSE;
        ALTER TABLE persons ADD COLUMN IF NOT EXISTS contact_quality VARCHAR(10);
        ALTER TABLE persons ADD COLUMN IF NOT EXISTS caller_name VARCHAR(120);
        ALTER TABLE persons ADD COLUMN IF NOT EXISTS has_relatives_searched BOOLEAN DEFAULT FALSE;
        ALTER TABLE incidents ADD COLUMN IF NOT EXISTS has_attorney_known BOOLEAN DEFAULT FALSE;
        ALTER TABLE incidents ADD COLUMN IF NOT EXISTS vehicle_recalls_count INTEGER DEFAULT 0;
        CREATE INDEX IF NOT EXISTS idx_persons_identity_confidence ON persons(identity_confidence DESC NULLS LAST);
        CREATE INDEX IF NOT EXISTS idx_persons_contact_quality ON persons(contact_quality);
      `);
    } catch (_) {}

    const lookbackHours = parseInt(req.query.hours) || 720; // 30 days — keep older qualified leads in re-eval loop // 7d default
    const since = new Date(Date.now() - lookbackHours * 3600000);

    // Pull incidents that need re-evaluation:
    //   1. Created/updated since last run (default last 30 min)
    //   2. OR currently pending (give them another shot at qualifying)
    //   3. OR force=all to re-score everything in window
    const force = req.query.force === 'all';
    let q = db('incidents')
      .where('discovered_at', '>=', since)
      .select('id', 'severity', 'source_count', 'occurred_at', 'discovered_at',
              'qualification_state', 'lead_score', 'has_contact_info');
    if (!force) {
      q = q.where(function() {
        this.where('updated_at', '>', new Date(Date.now() - 30 * 60 * 1000))
          .orWhere('qualification_state', 'pending')
          .orWhere('qualification_state', 'pending_named')
          .orWhere('qualification_state', 'qualified')  // keep qualified leads re-scored each tick
          .orWhereNull('qualification_state');
      });
    }
    const incidents = await q.limit(force ? 5000 : 500);

    // Bulk fetch all persons for these incidents
    const incIds = incidents.map(i => i.id);
    const allPersons = incIds.length
      ? await db('persons').whereIn('incident_id', incIds).select('*')
      : [];
    const personsByIncident = {};
    for (const p of allPersons) {
      (personsByIncident[p.incident_id] ||= []).push(p);
    }

    const newlyQualified = [];

    for (const inc of incidents) {
      try {
        const persons = personsByIncident[inc.id] || [];
        const { state, score, qualified_persons } = await evaluateIncident(db, inc, persons);

        const update = {};
        const wasQualified = inc.qualification_state === 'qualified';
        const isNowQualified = state === 'qualified';

        if (state !== inc.qualification_state) {
          update.qualification_state = state;
          if (isNowQualified && !wasQualified) {
            update.qualified_at = new Date();
            update.has_contact_info = true;
            results.promoted++;
            newlyQualified.push({ id: inc.id, score });
          } else if (!isNowQualified && wasQualified) {
            results.demoted++;
          }
        }

        if (Math.abs((inc.lead_score || 0) - score) >= 2) {
          update.lead_score = score;
          results.score_updates++;
        }

        // Predictive case value (band + factors) — beyond-competitor signal
        try {
          const cv = computeCaseValue(inc, persons, {});
          if (cv && cv.score !== inc.case_value_score) {
            update.case_value_score = cv.score;
            update.case_value_band = cv.band;
            update.case_value_low_usd = cv.band_low_usd;
            update.case_value_high_usd = cv.band_high_usd;
            update.case_value_factors = JSON.stringify(cv.factors);
          }
        } catch (_) {}

        if (Object.keys(update).length > 0) {
          update.updated_at = new Date();
          await db('incidents').where('id', inc.id).update(update);
          // Phase 21 Wire #9: incident-level cascade on key changes
          if (update.qualification_state || update.case_value_band || update.lead_score) {
            await enqueueIncidentCascade(db, inc.id, {
              trigger_source: 'qualify_change',
              trigger_field: Object.keys(update).filter(k => k !== 'updated_at').join(','),
              trigger_value: JSON.stringify({ score: update.lead_score, state: update.qualification_state }),
              priority: 6
            }).catch(()=>{});
          }
        }
        // Phase 21: run cross-source validations on each named person of this incident
        for (const pp of persons) {
          if (!pp.full_name) continue;
          await crossSourceNameValidation(db, pp).catch(()=>{});
          if (pp.address) await checkLikelyFamilyResidence(db, pp, inc).catch(()=>{});
          if (pp.age) await validateAgeAgainstVoter(db, pp).catch(()=>{});
        }
        results.evaluated++;
      } catch (e) {
        results.errors.push(`${inc.id}: ${e.message}`);
        await reportError(db, 'qualify', inc.id, e.message);
      }
    }

    // Apply cross-wires (weather, prior-incidents, recalls, time-of-day) to newly qualified incidents
    for (const nq of newlyQualified.slice(0, 25)) {
      try {
        const fullInc = await db('incidents').where('id', nq.id).first();
        if (fullInc) await applyAllToIncident(db, fullInc).catch(() => {});
      } catch (_) {}
    }

    // Log promotions to activity_log so reps see them
    for (const nq of newlyQualified.slice(0, 100)) {
      try {
        await db('activity_log').insert({
          id: uuidv4(),
          incident_id: nq.id,
          action: 'auto_qualified',
          details: JSON.stringify({ lead_score: nq.score }),
          created_at: new Date()
        });
      } catch (_) {}
    }

    // Sample some qualified results for the response
    if (newlyQualified.length) {
      const sampleIds = newlyQualified.slice(0, 5).map(x => x.id);
      const samples = await db('incidents')
        .whereIn('id', sampleIds)
        .select('id','city','state','severity','lead_score','qualified_at');
      results.samples = samples;
    }

    res.json({
      success: true,
      message: `Qualify: evaluated ${results.evaluated}, promoted ${results.promoted}, demoted ${results.demoted}, ${results.score_updates} score updates`,
      ...results,
      lookback_hours: lookbackHours,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'qualify', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
