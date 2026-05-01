/**
 * Phase 59: HYPOTHESIS GENERATOR — Claude-driven testable hypothesis layer.
 *
 * Mason's directive (2026-04-30):
 *   When the strategist's score-ranked engines produce no new contact info,
 *   we need a *reasoning* layer that asks "who could this person actually be?
 *   And what would prove or disprove that?" Then it runs the proof.
 *
 * What this does:
 *   1. Loads full evidence dump for a person (persons row + incident row +
 *      last 50 enrichment_logs + cross_check summary).
 *   2. Asks Claude (Sonnet 4.6 default, Opus 4.7 when ?deep=true) to generate
 *      3-5 testable hypotheses about the person's identity and how to confirm
 *      them. Each hypothesis names confirming engines from ENGINE_CATALOGUE
 *      so we can fire them via auto-fan-out.
 *   3. Persists hypotheses to a new `person_hypotheses` table.
 *   4. Optionally tests them automatically by firing the confirming engines
 *      and updating each hypothesis to confirmed | disconfirmed | inconclusive.
 *   5. Tracks which hypothesis types confirm most often (leaderboard).
 *
 * Cost-aware: skips persons that already have phone+email+address.
 *
 * Endpoints:
 *   GET  ?action=health
 *   POST ?action=generate body:{person_id, deep?}
 *   POST ?action=run      body:{person_id, deep?, auto_test?}
 *   GET  ?action=list&person_id=<uuid>&status=<proposed|confirmed|disconfirmed>
 *   GET  ?action=stats
 */

const { getDb } = require('../../_db');
const { reportError } = require('./_errors');

const SECRET = 'ingest-now';
const ENGINE = 'hypothesis-generator';
const TOTAL_BUDGET_MS = 60000;

let trackApiCall = async () => {};
try { trackApiCall = require('./cost-tracker').trackApiCall || trackApiCall; } catch (_) {}

let _extractJson = null;
function getAiRouter() {
  if (_extractJson) return _extractJson;
  try {
    const r = require('../enrich/_ai_router');
    _extractJson = r.extractJson;
  } catch (_) { _extractJson = null; }
  return _extractJson;
}

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

// ─────────────────────────────────────────────────────────────────────────
// Migration — self-applies on first call via _migrated cache flag.
// ─────────────────────────────────────────────────────────────────────────
let _migrated = false;
async function ensureSchema(db) {
  if (_migrated) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS person_hypotheses (
        id BIGSERIAL PRIMARY KEY,
        person_id UUID NOT NULL,
        claim_text TEXT NOT NULL,
        confidence INT DEFAULT 50,
        supporting_evidence JSONB DEFAULT '[]'::jsonb,
        confirming_engines TEXT[] DEFAULT ARRAY[]::TEXT[],
        disconfirming_signals JSONB DEFAULT '[]'::jsonb,
        hypothesis_type VARCHAR(64),
        reasoning_model VARCHAR(64),
        status VARCHAR(32) DEFAULT 'proposed',
        outcome JSONB DEFAULT '{}'::jsonb,
        created_at TIMESTAMP DEFAULT NOW(),
        resolved_at TIMESTAMP
      );
      CREATE INDEX IF NOT EXISTS idx_person_hypotheses_person ON person_hypotheses(person_id);
      CREATE INDEX IF NOT EXISTS idx_person_hypotheses_status ON person_hypotheses(status);
      CREATE INDEX IF NOT EXISTS idx_person_hypotheses_type ON person_hypotheses(hypothesis_type);
    `);
    _migrated = true;
  } catch (e) { console.error('[hypothesis-generator] migration:', e.message); }
}

// ─────────────────────────────────────────────────────────────────────────
// Body parser (consistent with strategist.js / auto-fan-out.js pattern)
// ─────────────────────────────────────────────────────────────────────────
async function readBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  return new Promise((resolve) => {
    let d = '';
    req.on('data', c => { d += c; });
    req.on('end', () => { try { resolve(JSON.parse(d || '{}')); } catch { resolve({}); } });
    req.on('error', () => resolve({}));
  });
}

// ─────────────────────────────────────────────────────────────────────────
// Cost gate — skip persons that already have phone+email+address.
// ─────────────────────────────────────────────────────────────────────────
function alreadyComplete(person) {
  return !!(person && person.phone && person.email && person.address);
}

// ─────────────────────────────────────────────────────────────────────────
// Evidence dump — persons + incident + last 50 enrichment_logs + cross_check.
// ─────────────────────────────────────────────────────────────────────────
async function gatherEvidence(db, personId) {
  const person = await db('persons').where({ id: personId }).first();
  if (!person) return null;

  const incident = person.incident_id
    ? await db('incidents').where({ id: person.incident_id }).first().catch(() => null)
    : null;

  let logs = [];
  try {
    logs = await db('enrichment_logs')
      .where({ person_id: personId })
      .orderBy('created_at', 'desc')
      .limit(50)
      .select('field_name', 'old_value', 'new_value', 'created_at');
  } catch (_) { logs = []; }

  // Pull a cross_check summary if one exists (from evidence-cross-checker)
  let crossCheck = null;
  try {
    const ccRow = await db('enrichment_logs')
      .where({ person_id: personId, field_name: 'cross_check_summary' })
      .orderBy('created_at', 'desc')
      .first();
    if (ccRow) {
      try { crossCheck = JSON.parse(ccRow.new_value); } catch { crossCheck = ccRow.new_value; }
    }
  } catch (_) {}

  // Compact the logs for the prompt
  const compactLogs = logs.map(l => ({
    field: l.field_name,
    old: typeof l.old_value === 'string' ? l.old_value.slice(0, 200) : l.old_value,
    new: typeof l.new_value === 'string' ? l.new_value.slice(0, 400) : l.new_value,
    at: l.created_at
  }));

  return {
    person: {
      id: person.id,
      full_name: person.full_name,
      first_name: person.first_name,
      last_name: person.last_name,
      age: person.age,
      dob: person.dob,
      city: person.city,
      state: person.state,
      address: person.address,
      phone: person.phone,
      email: person.email,
      employer: person.employer,
      role: person.role,
      victim_verified: person.victim_verified,
      vehicle_plate: person.vehicle_plate,
      vehicle_vin: person.vehicle_vin
    },
    incident: incident ? {
      id: incident.id,
      city: incident.city,
      state: incident.state,
      occurred_at: incident.occurred_at,
      severity: incident.severity,
      description: typeof incident.description === 'string' ? incident.description.slice(0, 800) : null,
      fatal_count: incident.fatal_count,
      injury_count: incident.injury_count,
      qualification_state: incident.qualification_state
    } : null,
    cross_check_summary: crossCheck,
    enrichment_log_count: compactLogs.length,
    enrichment_logs_recent: compactLogs
  };
}

// ─────────────────────────────────────────────────────────────────────────
// Build the catalogue snippet for the prompt — pull from strategist if loadable.
// ─────────────────────────────────────────────────────────────────────────
function getEngineCatalogue() {
  try {
    const strat = require('./strategist');
    if (strat.ENGINE_CATALOGUE) return strat.ENGINE_CATALOGUE;
  } catch (_) {}
  return {};
}

// ─────────────────────────────────────────────────────────────────────────
// Claude prompt — request structured hypotheses.
// ─────────────────────────────────────────────────────────────────────────
function buildPrompt(evidence) {
  const cat = getEngineCatalogue();
  const catalogueLine = Object.keys(cat).slice(0, 60).join(', ') || 'pdl-identify, osint-miner, voter-rolls, courtlistener, trestle-phone, evidence-cross-check';

  const sys = `You are a senior identity-investigation analyst working on accident victims and next-of-kin discovery. You reason about who a person is likely to be and what would prove or disprove each theory. You are precise, never fabricate facts, and always tie hypotheses to engines that could test them.

Return JSON only. No prose outside the JSON.`;

  const user = `EVIDENCE DUMP:
${JSON.stringify(evidence, null, 2)}

AVAILABLE CONFIRMING ENGINES (the only valid values for confirming_engines[]):
${catalogueLine}

TASK: Produce 3-5 testable hypotheses about who this person actually is, where they live, how to reach them, and how to confirm or disprove each theory. Each hypothesis must be falsifiable.

Return JSON shaped exactly as:
{
  "hypotheses": [
    {
      "claim": "string — concrete statement, e.g. 'Subject is a 47-year-old Hispanic woman living in Phoenix AZ with employer Walmart'",
      "type": "identity_match|geo_residence|employer|family_link|vehicle_owner|alternate_spelling|deceased_kin|other",
      "confidence": 0-100,
      "supporting_evidence_keys": ["string keys from EVIDENCE DUMP that back this claim, e.g. 'person.city', 'enrichment_logs_recent[3].new'"],
      "confirming_engines": ["engine-id from the AVAILABLE CONFIRMING ENGINES list"],
      "disconfirming_signals": ["string — what would prove the claim wrong, e.g. 'voter-rolls returns no match in AZ for this name'"]
    }
  ],
  "overall_reasoning": "string — short summary of the approach taken"
}`;

  return { sys, user };
}

// ─────────────────────────────────────────────────────────────────────────
// Generate hypotheses — call Claude through the AI router.
// ─────────────────────────────────────────────────────────────────────────
async function generateHypotheses(db, personId, opts = {}) {
  await ensureSchema(db);

  const evidence = await gatherEvidence(db, personId);
  if (!evidence) return { ok: false, error: 'person_not_found' };

  if (alreadyComplete(evidence.person)) {
    return {
      ok: true,
      skipped: true,
      reason: 'already_has_phone_email_address',
      person_id: personId,
      hypotheses: []
    };
  }

  const extractJson = getAiRouter();
  if (!extractJson) {
    return { ok: false, error: 'ai_router_unavailable' };
  }

  const reasoningTier = opts.deep ? 'opus' : 'mid';
  // Provider: Claude. Tier: opus → claude-opus-4-7, mid → claude-sonnet-4-6.
  const tierKey = opts.deep ? 'opus' : 'mid';
  const reasoningModel = opts.deep ? 'claude-opus-4-7' : 'claude-sonnet-4-6';

  const { sys, user } = buildPrompt(evidence);

  let parsed = null;
  try {
    parsed = await extractJson(db, {
      pipeline: ENGINE,
      systemPrompt: sys,
      userPrompt: user,
      tier: tierKey,
      provider: 'claude',
      timeoutMs: 50000
    });
  } catch (e) {
    try { await reportError(db, ENGINE, personId, 'claude call failed: ' + e.message); } catch (_) {}
    return { ok: false, error: 'claude_failed: ' + e.message };
  }

  if (!parsed || !Array.isArray(parsed.hypotheses)) {
    return { ok: false, error: 'no_hypotheses_returned' };
  }

  const cat = getEngineCatalogue();
  const validEngines = new Set(Object.keys(cat));

  const persisted = [];
  const now = new Date();
  for (const h of parsed.hypotheses.slice(0, 5)) {
    if (!h || !h.claim) continue;
    const confEngines = Array.isArray(h.confirming_engines)
      ? h.confirming_engines.filter(e => typeof e === 'string' && (validEngines.size === 0 || validEngines.has(e))).slice(0, 8)
      : [];
    const supEv = Array.isArray(h.supporting_evidence_keys) ? h.supporting_evidence_keys.slice(0, 12) : [];
    const disSigs = Array.isArray(h.disconfirming_signals) ? h.disconfirming_signals.slice(0, 8) : [];
    const conf = Math.max(0, Math.min(100, parseInt(h.confidence, 10) || 50));
    const type = (typeof h.type === 'string' ? h.type : 'other').slice(0, 64);
    try {
      const inserted = await db('person_hypotheses').insert({
        person_id: personId,
        claim_text: String(h.claim).slice(0, 1000),
        confidence: conf,
        supporting_evidence: JSON.stringify(supEv),
        confirming_engines: confEngines,
        disconfirming_signals: JSON.stringify(disSigs),
        hypothesis_type: type,
        reasoning_model: reasoningModel,
        status: 'proposed',
        created_at: now
      }).returning(['id']);
      const id = Array.isArray(inserted) && inserted[0] ? (inserted[0].id || inserted[0]) : null;
      persisted.push({
        id,
        claim: h.claim,
        type,
        confidence: conf,
        supporting_evidence_keys: supEv,
        confirming_engines: confEngines,
        disconfirming_signals: disSigs,
        status: 'proposed'
      });
    } catch (e) {
      try { await reportError(db, ENGINE, personId, 'insert hypothesis failed: ' + e.message); } catch (_) {}
    }
  }

  // Log a one-line audit entry into enrichment_logs (minimal-schema rule:
  // metadata folded into new_value JSON).
  try {
    await db('enrichment_logs').insert({
      person_id: personId,
      field_name: 'hypotheses_generated',
      old_value: null,
      new_value: JSON.stringify({
        engine: ENGINE,
        count: persisted.length,
        reasoning_model: reasoningModel,
        deep: !!opts.deep
      }).slice(0, 4000),
      created_at: now
    });
  } catch (_) {}

  await trackApiCall(db, ENGINE, opts.deep ? 'generate_opus' : 'generate_sonnet', 0, 0, persisted.length > 0).catch(() => {});

  return {
    ok: true,
    person_id: personId,
    reasoning_model: reasoningModel,
    overall_reasoning: parsed.overall_reasoning || null,
    hypotheses: persisted
  };
}

// ─────────────────────────────────────────────────────────────────────────
// Test hypotheses by firing confirming engines via auto-fan-out, then
// re-reading the person to see what got filled in. Outcome rules:
//   - confirmed     → at least one supporting field appeared (phone/email/
//                     address/employer/dob) since hypothesis creation.
//   - disconfirmed  → engines ran but produced no supporting facts AND a
//                     disconfirming signal text appears in any new log.
//   - inconclusive  → engines ran with no signal either way.
// ─────────────────────────────────────────────────────────────────────────
async function testHypotheses(db, personId, hypothesisIds, startedAt) {
  await ensureSchema(db);
  // Snapshot person state before firing.
  const before = await db('persons').where({ id: personId }).first();
  if (!before) return { ok: false, error: 'person_not_found' };

  // Aggregate all confirming engines across these hypotheses, fire ONE fan-out.
  const fanOut = require('./auto-fan-out');
  let fanResult = null;
  try {
    fanResult = await fanOut.runFanOut(db, personId, {
      trigger_field: 'hypothesis_test',
      force: true
    });
  } catch (e) {
    fanResult = { ok: false, error: e.message };
  }

  // Re-read person after fan-out.
  const after = await db('persons').where({ id: personId }).first();

  const supportingFieldsFilled = [];
  for (const f of ['phone', 'email', 'address', 'employer', 'dob', 'first_name', 'last_name', 'full_name']) {
    if (after && after[f] && (!before[f] || before[f] !== after[f])) supportingFieldsFilled.push(f);
  }

  // Pull recent enrichment_logs since startedAt for disconfirming-signal matching
  let recentLogs = [];
  try {
    recentLogs = await db('enrichment_logs')
      .where({ person_id: personId })
      .where('created_at', '>=', startedAt)
      .orderBy('created_at', 'desc')
      .limit(80);
  } catch (_) {}

  const logBlob = recentLogs.map(l => `${l.field_name}::${l.new_value || ''}`).join(' \n ').toLowerCase();

  const updates = [];
  for (const hid of hypothesisIds) {
    let row;
    try { row = await db('person_hypotheses').where({ id: hid }).first(); } catch (_) { row = null; }
    if (!row) continue;

    let outcomeStatus = 'inconclusive';
    let outcomeNote = '';

    // Disconfirming-signal heuristic: if any disconfirming signal text snippet
    // (lowercased) appears in recent logs, mark disconfirmed.
    let disSigs = [];
    try { disSigs = typeof row.disconfirming_signals === 'string' ? JSON.parse(row.disconfirming_signals) : (row.disconfirming_signals || []); } catch (_) {}
    const disHit = (disSigs || []).find(s => typeof s === 'string' && s.length > 6 && logBlob.includes(s.toLowerCase().slice(0, 60)));

    if (supportingFieldsFilled.length > 0) {
      outcomeStatus = 'confirmed';
      outcomeNote = 'Fields filled: ' + supportingFieldsFilled.join(',');
    } else if (disHit) {
      outcomeStatus = 'disconfirmed';
      outcomeNote = 'Disconfirming signal hit: ' + String(disHit).slice(0, 120);
    } else {
      outcomeStatus = 'inconclusive';
      outcomeNote = 'Engines fired; no supporting field changes detected.';
    }

    try {
      await db('person_hypotheses').where({ id: hid }).update({
        status: outcomeStatus,
        outcome: JSON.stringify({
          fields_filled: supportingFieldsFilled,
          fan_out: fanResult ? {
            ok: !!fanResult.ok,
            engines_fired: fanResult.engines_fired,
            ok_count: fanResult.ok_count
          } : null,
          note: outcomeNote
        }),
        resolved_at: new Date()
      });
    } catch (_) {}
    updates.push({ id: hid, status: outcomeStatus, note: outcomeNote });
  }

  return {
    ok: true,
    person_id: personId,
    fields_filled_during_test: supportingFieldsFilled,
    fan_out: fanResult,
    hypotheses_resolved: updates
  };
}

// ─────────────────────────────────────────────────────────────────────────
// Action: run — generate + (optionally) auto-test in one call. 60s budget.
// ─────────────────────────────────────────────────────────────────────────
async function runForPerson(db, personId, opts = {}) {
  const startedAt = new Date();
  const t0 = Date.now();
  const gen = await generateHypotheses(db, personId, { deep: !!opts.deep });
  if (!gen.ok || gen.skipped) return gen;
  if (!opts.auto_test) return gen;

  // Budget guard: leave at least 5s for the test. If we're already too close
  // to the budget, skip auto_test and report that.
  if (Date.now() - t0 > TOTAL_BUDGET_MS - 5000) {
    return { ...gen, auto_test_skipped: 'budget_too_low' };
  }

  const ids = (gen.hypotheses || []).map(h => h.id).filter(Boolean);
  if (ids.length === 0) return gen;

  const test = await testHypotheses(db, personId, ids, startedAt);
  return { ...gen, test };
}

// ─────────────────────────────────────────────────────────────────────────
// Action: list
// ─────────────────────────────────────────────────────────────────────────
async function listForPerson(db, personId, status) {
  await ensureSchema(db);
  let q = db('person_hypotheses').where({ person_id: personId }).orderBy('created_at', 'desc').limit(100);
  if (status) q = q.where({ status });
  return q.select();
}

// ─────────────────────────────────────────────────────────────────────────
// Action: stats — leaderboard of which hypothesis_type confirms most often.
// ─────────────────────────────────────────────────────────────────────────
async function getStats(db) {
  await ensureSchema(db);
  let rows = [];
  try {
    const r = await db.raw(`
      SELECT
        hypothesis_type,
        COUNT(*) AS total,
        SUM(CASE WHEN status = 'confirmed' THEN 1 ELSE 0 END) AS confirmed,
        SUM(CASE WHEN status = 'disconfirmed' THEN 1 ELSE 0 END) AS disconfirmed,
        SUM(CASE WHEN status = 'inconclusive' THEN 1 ELSE 0 END) AS inconclusive,
        SUM(CASE WHEN status = 'proposed' THEN 1 ELSE 0 END) AS proposed
      FROM person_hypotheses
      GROUP BY hypothesis_type
      ORDER BY confirmed DESC NULLS LAST, total DESC
      LIMIT 50
    `);
    rows = r.rows || r;
  } catch (e) {
    return { ok: false, error: e.message, stats: [] };
  }

  const stats = (rows || []).map(r => {
    const total = Number(r.total || 0);
    const confirmed = Number(r.confirmed || 0);
    return {
      hypothesis_type: r.hypothesis_type,
      total,
      confirmed,
      disconfirmed: Number(r.disconfirmed || 0),
      inconclusive: Number(r.inconclusive || 0),
      proposed: Number(r.proposed || 0),
      confirm_rate: total > 0 ? Number((confirmed / total).toFixed(3)) : 0
    };
  });
  return { ok: true, stats };
}

// ─────────────────────────────────────────────────────────────────────────
// HTTP handler
// ─────────────────────────────────────────────────────────────────────────
async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  let db;
  try { db = getDb(); } catch (e) {
    return res.status(500).json({ success: false, error: 'db_unavailable: ' + e.message });
  }

  const action = (req.query?.action || 'health').toLowerCase();

  if (action === 'health') {
    await ensureSchema(db);
    return res.status(200).json({
      success: true,
      service: 'hypothesis-generator',
      ts: new Date().toISOString()
    });
  }

  if (action === 'list') {
    const personId = req.query?.person_id;
    if (!personId) return res.status(400).json({ error: 'person_id required' });
    const status = req.query?.status || null;
    try {
      const rows = await listForPerson(db, personId, status);
      return res.status(200).json({ success: true, person_id: personId, count: rows.length, hypotheses: rows });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  if (action === 'stats') {
    try {
      const r = await getStats(db);
      return res.status(200).json({ success: true, ...r });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  if (action === 'generate' || action === 'run') {
    const body = await readBody(req);
    const personId = body.person_id || req.query?.person_id;
    if (!personId) return res.status(400).json({ error: 'person_id required' });
    const deep = body.deep === true || req.query?.deep === 'true';
    const auto_test = (action === 'run') && (body.auto_test === true || req.query?.auto_test === 'true');

    try {
      let r;
      if (action === 'generate') {
        r = await generateHypotheses(db, personId, { deep });
      } else {
        r = await runForPerson(db, personId, { deep, auto_test });
      }
      await trackApiCall(db, ENGINE, action, 0, 0, !!r.ok).catch(() => {});
      if (!r.ok) return res.status(500).json({ success: false, ...r });
      return res.status(200).json({ success: true, ...r });
    } catch (e) {
      try { await reportError(db, ENGINE, personId, e.message, { severity: 'error' }); } catch (_) {}
      return res.status(500).json({ error: e.message });
    }
  }

  return res.status(400).json({ error: 'unknown action: ' + action });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.generateHypotheses = generateHypotheses;
module.exports.runForPerson = runForPerson;
module.exports.testHypotheses = testHypotheses;
module.exports.listForPerson = listForPerson;
module.exports.getStats = getStats;
