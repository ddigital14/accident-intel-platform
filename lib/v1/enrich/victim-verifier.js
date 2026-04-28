/**
 * VICTIM VERIFIER — Two-stage classifier
 *
 * Phase 38 Wave A. Decides whether an extracted name on an incident is an
 * actual victim/driver/passenger/pedestrian, vs. a journalist, official,
 * witness, family member quoted, or bystander.
 *
 * Stage A: cheap regex hard rules (in lib/v1/enrich/_name_filter.js)
 * Stage B: Claude Sonnet 4.6 fallback for ambiguous cases
 *
 * HTTP entrypoint:
 *   GET  /api/v1/enrich/victim-verifier?secret=ingest-now&action=health
 *   POST /api/v1/enrich/victim-verifier?secret=ingest-now&action=verify
 *        body: { name, text, incident_type?, city?, state? }
 *   GET  /api/v1/enrich/victim-verifier?secret=ingest-now&action=batch&limit=20
 *
 * In-process API:
 *   const { verify } = require('./victim-verifier');
 *   const r = await verify(name, text, { incident_type, city, state });
 *   //   { is_victim, role, confidence, reason, stage }
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');
const { extractJson } = require('./_ai_router');
const { quickClassify, applyDenyList } = require('./_name_filter');
const { enqueueCascade } = require('../system/_cascade');

const SECRET = 'ingest-now';

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

/**
 * verify(name, text, meta)
 *   meta: { incident_type?, city?, state? }
 * Returns { is_victim, role, confidence, reason, stage }
 */
async function verify(name, text, meta = {}) {
  // Phase 38 fix: temporal recency — reject historical/anniversary cases
  // PI cases require recent injuries (statute of limitations 1-4 years typically)
  const lowerText = String(text || '').toLowerCase();
  const HISTORICAL_PATTERNS = [
    /\b(80|70|60|50|40|30|25|20|15|10)\s*years?\s*(ago|later|after|since)\b/,
    /\bworld war\s*(i{1,3}|1|2|two|three)\b/i,
    /\bvietnam war\b/i,
    /\bkorean war\b/i,
    /\bremains (returned|recovered|identified|found)\b/,
    /\bidentified\s+\d+\s+years?\s+(after|later)\b/,
    /\bhistorical(ly)?\s+(crash|accident|fatality)\b/,
    /\bmemorial(ized)?\b/,
    /\banniversary of\b/,
    /\b(1[89]\d{2}|20[0-1]\d)\b/  // pre-2020 year mentions in headline context
  ];
  // Only apply if a historical pattern hits AND no recent-event signal also present
  const hasHistorical = HISTORICAL_PATTERNS.slice(0, 9).some(rx => rx.test(lowerText));
  const hasRecent = /\b(yesterday|today|last (night|week|month)|this (morning|afternoon|evening|week)|breaking|just (in|happened)|hours? ago|minutes? ago)\b/i.test(lowerText);
  if (hasHistorical && !hasRecent) {
    return {
      is_victim: false,
      role: 'historical',
      confidence: 90,
      reason: 'stage_a:historical_or_anniversary_case',
      stage: 'A'
    };
  }

  // Stage A — hard rules first
  const stageA = quickClassify(name, text);
  if (stageA.decision === 'deny' && stageA.confidence >= 70) {
    return {
      is_victim: false,
      role: _roleFromReason(stageA.reason) || 'unknown',
      confidence: stageA.confidence,
      reason: 'stage_a:' + stageA.reason,
      stage: 'A'
    };
  }
  if (stageA.decision === 'accept' && stageA.confidence >= 80) {
    return {
      is_victim: true,
      role: 'victim',
      confidence: stageA.confidence,
      reason: 'stage_a:' + stageA.reason,
      stage: 'A'
    };
  }

  // Stage B — Claude fallback (only if Stage A confidence < 70)
  let db;
  try { db = getDb(); } catch (_) { db = null; }
  const truncated = String(text || '').slice(0, 3500);
  const userPrompt =
    'Article: ' + truncated + '\n\n' +
    'Extracted name: ' + name + '\n' +
    'Incident type: ' + (meta.incident_type || 'unknown') + '\n' +
    'Incident location: ' + (meta.city || 'unknown') + ', ' + (meta.state || 'unknown') + '\n\n' +
    'Return JSON: {"is_victim": true|false, "role": "victim"|"driver"|"passenger"|"pedestrian"|"author"|"officer"|"witness"|"family"|"unknown", "confidence": 0-100, "reason": "<one sentence>"}\n\n' +
    'Only return is_victim:true if you are confident this person was directly involved in the accident as a victim, driver, passenger, or pedestrian. Bylines, officials providing statements, witnesses, and family members quoted are NOT victims.';
  const systemPrompt = 'You are determining whether a person mentioned in a news article was an ACTUAL VICTIM of the accident, or someone else (journalist, official, witness, family member quoted, bystander). Return JSON only.';

  try {
    const parsed = await extractJson(db, {
      pipeline: 'victim-verifier',
      systemPrompt,
      userPrompt,
      provider: 'claude',
      tier: 'cheap',
      severityHint: meta.incident_type,
      timeoutMs: 18000,
      temperature: 0
    });
    if (!parsed || typeof parsed.is_victim !== 'boolean') {
      // Conservative fallback: if AI fails, treat unsure as DENY (safer for false-positives)
      return {
        is_victim: false,
        role: 'unknown',
        confidence: 50,
        reason: 'stage_b:ai_unavailable_default_deny',
        stage: 'B'
      };
    }
    return {
      is_victim: !!parsed.is_victim,
      role: String(parsed.role || 'unknown').toLowerCase(),
      confidence: Math.max(0, Math.min(100, parseInt(parsed.confidence) || 50)),
      reason: 'stage_b:' + String(parsed.reason || 'ai_classified').slice(0, 200),
      stage: 'B'
    };
  } catch (e) {
    if (db) await reportError(db, 'victim-verifier', null, 'verify_exception:' + e.message, { severity: 'warning' });
    return {
      is_victim: false,
      role: 'unknown',
      confidence: 50,
      reason: 'stage_b:exception:' + (e.message || ''),
      stage: 'B'
    };
  }
}

function _roleFromReason(reason) {
  if (!reason) return null;
  if (reason.startsWith('byline_match')) return 'author';
  if (reason.startsWith('outlet_tag')) return 'author';
  if (reason.startsWith('official_title')) return 'officer';
  if (reason.startsWith('attribution_only')) return 'witness';
  if (reason.startsWith('hard_ban_name')) return 'unknown';
  if (reason.startsWith('role_token')) return 'unknown';
  return null;
}

/**
 * Batch worker — pull recent persons whose verification status is null and
 * verify them against the latest source_report text we have.
 */
async function batchVerify(db, { limit = 20 } = {}) {
  await _ensureColumns(db);
  const rows = await db('persons')
    .leftJoin('incidents', 'incidents.id', 'persons.incident_id')
    .whereNull('persons.victim_verified')
    .whereNotNull('persons.full_name')
    .orderBy('persons.created_at', 'desc')
    .limit(limit)
    .select(
      'persons.id as pid',
      'persons.full_name as name',
      'persons.incident_id',
      'incidents.incident_type as inc_type',
      'incidents.city as city',
      'incidents.state as state'
    );

  const results = { checked: 0, accepted: 0, rejected: 0, samples: [] };
  for (const r of rows) {
    results.checked++;
    // Pull a representative source_report text for this incident
    const rep = await db('source_reports')
      .where('incident_id', r.incident_id)
      .orderBy('fetched_at', 'desc')
      .first('raw_data', 'parsed_data');
    let text = '';
    if (rep) {
      try {
        const raw = typeof rep.raw_data === 'string' ? JSON.parse(rep.raw_data) : rep.raw_data;
        text = (raw?.item?.title || '') + '\n' + (raw?.item?.description || '') + '\n' + (raw?.title || '') + '\n' + (raw?.description || '') + '\n' + (raw?.body || '') + '\n' + (raw?.text || '');
      } catch (_) { text = String(rep.raw_data || '').slice(0, 4000); }
    }
    const v = await verify(r.name, text, { incident_type: r.inc_type, city: r.city, state: r.state });
    await db('persons').where('id', r.pid).update({
      victim_verified: v.is_victim,
      victim_role: v.role,
      victim_verifier_reason: v.reason,
      victim_verifier_stage: v.stage,
      updated_at: new Date()
    });
    if (v.is_victim) {
      results.accepted++;
      // Cascade so downstream enrichment runs only on confirmed victims
      try { await enqueueCascade(db, { person_id: r.pid, incident_id: r.incident_id, trigger_source: 'victim-verifier' }); } catch (_) {}
    } else {
      results.rejected++;
    }
    if (results.samples.length < 8) {
      results.samples.push({ name: r.name, is_victim: v.is_victim, role: v.role, reason: v.reason, stage: v.stage });
    }
  }
  return results;
}

let _columnsEnsured = false;
async function _ensureColumns(db) {
  if (_columnsEnsured) return;
  try {
    await db.raw(
      'ALTER TABLE persons ADD COLUMN IF NOT EXISTS victim_verified BOOLEAN; ' +
      'ALTER TABLE persons ADD COLUMN IF NOT EXISTS victim_role VARCHAR(40); ' +
      'ALTER TABLE persons ADD COLUMN IF NOT EXISTS victim_verifier_reason TEXT; ' +
      'ALTER TABLE persons ADD COLUMN IF NOT EXISTS victim_verifier_stage VARCHAR(4); ' +
      'CREATE INDEX IF NOT EXISTS idx_persons_victim_verified ON persons(victim_verified);'
    );
    _columnsEnsured = true;
  } catch (e) {
    console.error('victim-verifier ensure columns failed:', e.message);
  }
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const action = (req.query?.action || '').toLowerCase();
  const db = getDb();

  if (action === 'health' || !action) {
    return res.status(200).json({
      success: true,
      service: 'victim-verifier',
      stages: ['A:regex_hard_rules', 'B:claude_sonnet_4_6'],
      ts: new Date().toISOString()
    });
  }

  if (action === 'verify') {
    const body = req.body || {};
    const name = body.name || req.query?.name;
    const text = body.text || req.query?.text || '';
    if (!name) return res.status(400).json({ error: 'name required' });
    try {
      const v = await verify(name, text, {
        incident_type: body.incident_type || req.query?.incident_type,
        city: body.city || req.query?.city,
        state: body.state || req.query?.state
      });
      await trackApiCall(db, 'victim-verifier', v.stage === 'B' ? 'claude-sonnet-4-6' : 'rules', 0, 0, true).catch(() => {});
      return res.status(200).json({ success: true, ...v });
    } catch (e) {
      await reportError(db, 'victim-verifier', null, e.message, { severity: 'error' });
      return res.status(500).json({ error: e.message });
    }
  }

  if (action === 'batch') {
    const limit = Math.max(1, Math.min(parseInt(req.query?.limit) || 20, 100));
    try {
      const r = await batchVerify(db, { limit });
      await trackApiCall(db, 'victim-verifier', 'batch', 0, 0, true).catch(() => {});
      return res.status(200).json({ success: true, ...r });
    } catch (e) {
      await reportError(db, 'victim-verifier', null, e.message, { severity: 'error' });
      return res.status(500).json({ error: e.message, success: false });
    }
  }

  return res.status(400).json({ error: 'unknown action: ' + action });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.verify = verify;
module.exports.batchVerify = batchVerify;
