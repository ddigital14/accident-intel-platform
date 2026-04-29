/**
 * BEST LEAD SYNTHESIZER — Phase 43 (master endpoint)
 *
 * For a verified victim, this is THE endpoint reps hit. It calls EVERY
 * upstream engine (homegrown-osint-miner, victim-contact-finder,
 * victim-resolver, ai-cross-source-merge, free-osint-extras) and uses
 * Claude Opus 4.7 to produce ONE final, fully-reasoned recommendation:
 *
 *   - best phone / email / address with verification status + confidence
 *   - all alternates seen across the chain
 *   - family / next-of-kin map (with do-not-call notes)
 *   - exact next action a rep should take, including a "do_not" guardrail
 *   - case strength + estimated case value range
 *   - blocking unknowns the rep should chase
 *
 * One call -> all the answers. Reps shouldn't have to dig anywhere else.
 *
 *   GET /api/v1/system/best-lead-synthesizer?secret=ingest-now&action=health
 *   GET ?action=synthesize&person_id=<uuid>      (~90s budget)
 *   GET ?action=batch&limit=N                    (run for N verified victims)
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');
const { enqueueCascade } = require('../system/_cascade');
const { extract, MODELS } = require('../enrich/_ai_router');

// In-process upstream engines
const homegrownOsintMiner = require('../enrich/homegrown-osint-miner');
const victimContactFinder = require('../enrich/victim-contact-finder');
const victimResolver = require('../enrich/victim-resolver');
const aiCrossSourceMerge = require('./ai-cross-source-merge');
const freeOsintExtras = require('../enrich/free-osint-extras');

const SECRET = 'ingest-now';
const TOTAL_BUDGET_MS = 90000;
const MAX_BATCH = 5;

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

function digitsOnly(s) { return String(s || '').replace(/\D+/g, ''); }
function safeJson(s) {
  if (!s) return null;
  let t = String(s).trim();
  if (t.startsWith('```')) t = t.replace(/^```(?:json)?/i, '').replace(/```$/, '').trim();
  const i = t.indexOf('{');
  const j = t.lastIndexOf('}');
  if (i >= 0 && j > i) t = t.slice(i, j + 1);
  try { return JSON.parse(t); } catch (_) { return null; }
}

// ---------------------------------------------------------------------------
// Gather context: pull from DB everything we already know, plus call each
// engine in parallel with a per-engine soft timeout.
// ---------------------------------------------------------------------------
async function gather(db, personId) {
  const start = Date.now();
  const errors = [];
  const trace = {};

  const person = await db('persons').where('id', personId).first();
  if (!person) return { ok: false, error: 'person_not_found' };

  let incident = null;
  if (person.incident_id) {
    incident = await db('incidents').where('id', person.incident_id).first().catch(() => null);
    if (incident) {
      if (!person.city && incident.city) person.city = incident.city;
      if (!person.state && incident.state) person.state = incident.state;
      if (!person.accident_date && incident.accident_date) person.accident_date = incident.accident_date;
    }
  }

  // Existing enrichment_logs and contacts already on file
  const recentLogs = await db('enrichment_logs')
    .where({ person_id: personId })
    .orderBy('created_at', 'desc')
    .limit(50)
    .catch(() => []);

  const contacts = await db('contacts')
    .where({ person_id: personId })
    .catch(() => []);

  // Run engines in parallel, each with its own soft timeout
  const ENGINE_TIMEOUT = 35000;
  const wrap = async (label, fn) => {
    const t0 = Date.now();
    try {
      const v = await Promise.race([
        fn(),
        new Promise(resolve => setTimeout(() => resolve({ ok: false, error: 'soft_timeout' }), ENGINE_TIMEOUT))
      ]);
      trace[label] = { ms: Date.now() - t0, ok: !!(v && v.ok) };
      return v;
    } catch (e) {
      trace[label] = { ms: Date.now() - t0, ok: false, error: e.message };
      errors.push(`${label}:${e.message}`);
      return { ok: false, error: e.message };
    }
  };

  const [osint, contactFinder, resolver, freeExtras] = await Promise.all([
    wrap('osint_miner', () => homegrownOsintMiner.mineOne(db, personId)),
    wrap('contact_finder', () => victimContactFinder.resolveOne(db, personId)),
    wrap('victim_resolver', () => victimResolver.resolveOne(db, personId)),
    wrap('free_osint_extras', () => freeOsintExtras.lookupAll(db, {
      name: person.full_name,
      city: person.city,
      state: person.state
    }))
  ]);

  // ai-cross-source-merge runs at incident-level
  let merge = null;
  if (person.incident_id) {
    merge = await wrap('ai_merge', () => aiCrossSourceMerge.mergeOneIncident(db, person.incident_id));
  }

  return {
    ok: true,
    person, incident, recentLogs, contacts,
    engines: { osint, contactFinder, resolver, freeExtras, merge },
    trace,
    errors,
    gather_ms: Date.now() - start
  };
}

// ---------------------------------------------------------------------------
// Build the prompt for Claude Opus 4.7
// ---------------------------------------------------------------------------
const SYNTH_SYSTEM = `You are the most senior PI-firm intake supervisor and an expert OSINT analyst.

You will receive a packet for ONE accident victim assembled from 5 independent sub-engines. Your job: produce ONE unified recommendation that a personal-injury rep can act on in 60 seconds.

NON-NEGOTIABLES:
- Cite the source label for every claim (use exact source labels from the input: "osint_miner.synthesis.best_phone", "contact_finder.phone", "fec.address", "voter.address", "trestle.cnam", etc.).
- Quantify confidence 0-100 per field — favor 70+ when 2+ independent sources agree, 90+ when 3+ + recent.
- Reject obvious noise: scraper-website domains (thatsthem, radaris, spokeo, peekyou), 555 numbers, news-org generic emails (info@, editor@), funeral home main lines unless next-of-kin context demands a sympathetic call there.
- For verification_status: "verified" requires phone in 2+ independent non-scraper sources OR a successfully-completed Trestle/PDL response; "guessed" = 1 source only; "inferred" = AI synthesis without direct source.
- next_action_for_rep is the WHOLE point. It must include:
    * primary: the single best move (call X, email Y) with the actual number/email/address embedded.
    * secondary: a fallback if primary fails after 48h.
    * do_not: a guardrail that prevents an empathy violation. If the victim is deceased, flag any obvious next-of-kin trauma considerations. If they're hospitalized, flag the family-first rule. If the case is older than 60 days, warn statute-of-limitations clock.
- case_strength_score: 0-100 weighted by injury severity, contact-info completeness, attorney-not-yet-hired signal, and case-value hints.
- estimated_case_value_range: a tight $X-$Y range, not "varies". Anchor on injury severity, fatality, vehicle type, location.
- blocking_unknowns: 1-4 specific facts a rep should chase to lock the case.
- confidence_overall: harmonic-mean-weighted across phone/email/address confidences, penalized when key facts are unknown.

Return JSON only with this exact shape:
{
  "victim_id": "",
  "name": "",
  "incident_summary": "<= 280 chars: what happened, when, where, severity",
  "best_contact": {
    "phone": {"value": "", "confidence_pct": 0, "source": "", "verification_status": "verified|guessed|inferred"},
    "email": {"value": "", "confidence_pct": 0, "source": "", "verification_status": "verified|guessed|inferred"},
    "address": {"value": "", "confidence_pct": 0, "source": "", "verification_status": "verified|guessed|inferred"}
  },
  "all_phones_seen": [{"value": "", "source": ""}],
  "all_emails_seen": [{"value": "", "source": ""}],
  "family": [{"name": "", "relationship": "", "contact_hint": "", "source": ""}],
  "next_action_for_rep": {
    "primary": "",
    "secondary": "",
    "do_not": ""
  },
  "case_strength_score": 0,
  "estimated_case_value_range": "$X-$Y",
  "blocking_unknowns": [""],
  "confidence_overall": 0
}`;

function buildUserPrompt(ctx) {
  const { person, incident, recentLogs, contacts, engines } = ctx;
  // Trim engine outputs to keep prompt under ~25k chars
  const slim = (obj, max = 4500) => JSON.stringify(obj || {}, null, 2).slice(0, max);
  const logsTrim = recentLogs.slice(0, 20).map(l => ({
    field: l.field_name,
    source: l.source,
    confidence: l.confidence,
    new_value: typeof l.new_value === 'string' ? l.new_value.slice(0, 300) : l.new_value,
    when: l.created_at
  }));

  return `VICTIM PROFILE (current DB state):
${JSON.stringify({
  id: person.id,
  full_name: person.full_name,
  phone: person.phone || null,
  email: person.email || null,
  address: person.address || null,
  city: person.city,
  state: person.state,
  injury_severity: person.injury_severity,
  role: person.role,
  qualification_state: person.qualification_state,
  identity_confidence: person.identity_confidence,
  accident_date: person.accident_date
}, null, 2)}

INCIDENT:
${JSON.stringify({
  id: incident?.id, severity: incident?.severity,
  city: incident?.city, state: incident?.state,
  accident_date: incident?.accident_date,
  case_value_estimated: incident?.case_value_estimated,
  vehicles: incident?.vehicles, fatalities: incident?.fatalities
}, null, 2)}

CONTACTS ALREADY ON FILE (n=${contacts.length}):
${slim(contacts.map(c => ({ kind: c.kind, value: c.value, source: c.source, confidence: c.confidence })))}

RECENT ENRICHMENT LOG ENTRIES (n=${logsTrim.length}):
${slim(logsTrim, 3500)}

ENGINE 1 — OSINT MINER (12-signal AI synthesis):
${slim(engines.osint, 4500)}

ENGINE 2 — VICTIM CONTACT FINDER (PDL/Apollo/Trestle/voter/Hunter):
${slim(engines.contactFinder, 4500)}

ENGINE 3 — VICTIM RESOLVER (cross-engine orchestrator):
${slim(engines.resolver, 4500)}

ENGINE 4 — FREE OSINT EXTRAS (OpenStates/OpenCorp/FEC/news/CScore):
${slim(engines.freeExtras, 4000)}

ENGINE 5 — AI CROSS-SOURCE MERGE (incident-level):
${slim(engines.merge, 3500)}

Synthesize. Return JSON only.`;
}

// ---------------------------------------------------------------------------
// Main: synthesize one victim
// ---------------------------------------------------------------------------
async function synthesizeOne(db, personId) {
  const t0 = Date.now();
  const ctx = await gather(db, personId);
  if (!ctx.ok) return { ok: false, error: ctx.error };

  const userPrompt = buildUserPrompt(ctx);
  const r = await extract(db, {
    pipeline: 'best-lead-synthesizer',
    systemPrompt: SYNTH_SYSTEM,
    userPrompt,
    provider: 'claude',          // Force Claude
    tier: 'opus',                // -> Opus 4.7 (Phase 43)
    timeoutMs: 60000,
    responseFormat: 'json',
    temperature: 0
  });

  if (!r.ok) {
    try { await reportError(db, 'best-lead-synthesizer', personId, `synthesis_failed:${r.error}`, { attempts: r.attempts, severity: 'warning' }); } catch (_) {}
    return {
      ok: false,
      error: r.error || 'ai_failed',
      attempts: r.attempts,
      gather_trace: ctx.trace,
      gather_ms: ctx.gather_ms,
      total_ms: Date.now() - t0
    };
  }

  const parsed = r.parsed || safeJson(r.content);
  if (!parsed || typeof parsed !== 'object') {
    return { ok: false, error: 'unparseable_synthesis', content_preview: String(r.content).slice(0, 300), gather_trace: ctx.trace };
  }
  // Patch in IDs
  parsed.victim_id = personId;
  if (!parsed.name) parsed.name = ctx.person.full_name;

  // Persist as the canonical "best contact" row in enrichment_logs
  try {
    await db('enrichment_logs').insert({
      person_id: personId,
      field_name: 'best-lead-synthesizer:final',
      old_value: null,
      new_value: JSON.stringify(parsed).slice(0, 4000),
      source: 'best-lead-synthesizer',
      confidence: parseInt(parsed.confidence_overall || 0),
      verified: (parsed.best_contact?.phone?.verification_status === 'verified'),
      data: JSON.stringify({
        engine: 'best-lead-synthesizer',
        case_strength_score: parsed.case_strength_score,
        case_value_range: parsed.estimated_case_value_range,
        engines_run: Object.keys(ctx.trace),
        engines_ok: Object.keys(ctx.trace).filter(k => ctx.trace[k].ok)
      }).slice(0, 4000),
      created_at: new Date()
    });
  } catch (_) {}

  // Promote best_contact values back onto persons row when confidence high
  try {
    const updates = {};
    const phone = parsed.best_contact?.phone;
    const email = parsed.best_contact?.email;
    const address = parsed.best_contact?.address;
    if (phone?.value && (phone.confidence_pct || 0) >= 75 && phone.verification_status !== 'inferred' && !ctx.person.phone) {
      updates.phone = phone.value;
    }
    if (email?.value && (email.confidence_pct || 0) >= 75 && email.verification_status !== 'inferred' && !ctx.person.email) {
      updates.email = email.value;
    }
    if (address?.value && (address.confidence_pct || 0) >= 70 && !ctx.person.address) {
      updates.address = address.value;
    }
    if (Object.keys(updates).length) {
      updates.updated_at = new Date();
      await db('persons').where('id', personId).update(updates);
    }
  } catch (_) {}

  // Cascade
  try {
    await enqueueCascade(db, {
      person_id: personId,
      incident_id: ctx.person.incident_id,
      trigger_source: 'best-lead-synthesizer',
      trigger_field: 'final_synthesis',
      trigger_value: `score=${parsed.case_strength_score || 0},conf=${parsed.confidence_overall || 0}`,
      priority: (parsed.case_strength_score || 0) >= 70 ? 9 : 5
    });
  } catch (_) {}

  return {
    ok: true,
    synthesis: parsed,
    engine_trace: ctx.trace,
    engines_succeeded: Object.entries(ctx.trace).filter(([_, v]) => v.ok).map(([k]) => k),
    tokens_in: r.tokens_in,
    tokens_out: r.tokens_out,
    model_used: r.model_used,
    provider_used: r.provider_used,
    gather_ms: ctx.gather_ms,
    total_ms: Date.now() - t0
  };
}

async function batchSynthesize(db, { limit = 2 } = {}) {
  const cap = Math.min(MAX_BATCH, Math.max(1, parseInt(limit)));
  // Pick verified victims with the highest identity_confidence that don't yet
  // have a recent best-lead-synthesizer row (last 6 hours).
  const sixHoursAgo = new Date(Date.now() - 6 * 60 * 60 * 1000);

  let candidates = [];
  try {
    candidates = await db.raw(`
      SELECT p.id, p.full_name, p.identity_confidence
      FROM persons p
      WHERE COALESCE(p.qualification_state, '') IN ('verified', 'qualified')
        AND COALESCE(p.role, 'victim') = 'victim'
        AND p.full_name IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM enrichment_logs el
          WHERE el.person_id = p.id
            AND el.source = 'best-lead-synthesizer'
            AND el.created_at > ?
        )
      ORDER BY p.identity_confidence DESC NULLS LAST, p.created_at DESC
      LIMIT ?
    `, [sixHoursAgo, cap]).then(r => r.rows || r);
  } catch (e) {
    // Fallback: simpler query if columns differ
    candidates = await db('persons')
      .whereNotNull('full_name')
      .orderBy('updated_at', 'desc')
      .limit(cap);
  }

  const results = [];
  for (const c of candidates) {
    const t0 = Date.now();
    try {
      const r = await synthesizeOne(db, c.id);
      results.push({
        person_id: c.id,
        name: c.full_name,
        ok: r.ok,
        case_strength: r.synthesis?.case_strength_score,
        confidence_overall: r.synthesis?.confidence_overall,
        primary_action: r.synthesis?.next_action_for_rep?.primary?.slice(0, 200),
        ms: Date.now() - t0,
        error: r.error || null
      });
    } catch (e) {
      results.push({ person_id: c.id, name: c.full_name, ok: false, error: e.message, ms: Date.now() - t0 });
    }
  }
  return {
    ok: true,
    processed: results.length,
    succeeded: results.filter(r => r.ok).length,
    results
  };
}

async function health(db) {
  let synthesisesTotal = 0;
  try {
    const row = await db('enrichment_logs').where('source', 'best-lead-synthesizer').count('* as c').first();
    synthesisesTotal = parseInt(row?.c || 0);
  } catch (_) {}
  return {
    ok: true,
    synthesises_total: synthesisesTotal,
    upstream_engines: ['osint_miner', 'contact_finder', 'victim_resolver', 'ai_merge', 'free_osint_extras'],
    model: MODELS.premium_anth,
    valid_actions: ['health', 'synthesize', 'batch']
  };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const db = getDb();
  const action = (req.query?.action || 'health').toLowerCase();

  try {
    if (action === 'health') {
      const h = await health(db);
      return res.json({ success: true, action: 'health', ...h, timestamp: new Date().toISOString() });
    }
    if (action === 'synthesize') {
      const personId = req.query?.person_id;
      if (!personId) return res.status(400).json({ error: 'person_id required' });
      const r = await synthesizeOne(db, personId);
      return res.json({ success: !!r.ok, ...r, timestamp: new Date().toISOString() });
    }
    if (action === 'batch') {
      const limit = parseInt(req.query?.limit || '2');
      const r = await batchSynthesize(db, { limit });
      return res.json({ success: !!r.ok, ...r, timestamp: new Date().toISOString() });
    }
    return res.status(400).json({ error: 'unknown action', valid: ['health', 'synthesize', 'batch'] });
  } catch (e) {
    try { await reportError(db, 'best-lead-synthesizer', null, e.message); } catch (_) {}
    return res.status(500).json({ success: false, error: e.message });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.synthesizeOne = synthesizeOne;
module.exports.batchSynthesize = batchSynthesize;
module.exports.health = health;
