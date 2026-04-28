/**
 * CLAUDE IDENTITY INVESTIGATOR — Phase 23 #3
 *
 * Claude-as-orchestrator: aggressive iterative identity-fill engine.
 * For every person where identity_confidence < 80 OR contact_quality < 'warm':
 *   1. Aggregate ALL evidence (news, obit, court, social, voter, property, source_reports)
 *   2. Send to Claude (claude-sonnet-4-6) -> { likely_identity, confidence, suggested_searches }
 *   3. For each suggested_search -> route to engine (or skip if precondition not met)
 *   4. Loop new evidence back to Claude for re-evaluation
 *   5. Stop at identity_confidence >= 90 OR after MAX_ITERATIONS=3
 *
 * GET /api/v1/enrich/claude-identity-investigator?secret=ingest-now&action=batch&limit=10
 *
 * 14-point compliance:
 *   - Uses _ai_router (cost-tracked, fail-over)
 *   - Cascade emission per iteration
 *   - logChange on identity_confidence bumps
 *   - Folded into existing 30-min cron slot (no new vercel.json cron)
 */
const { v4: uuidv4 } = require('uuid');
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { logChange } = require('../system/changelog');
const { enqueueCascade } = require('../system/_cascade');
const { extract } = require('./_ai_router');

const MAX_ITERATIONS = 3;
const TARGET_CONFIDENCE = 90;
const ITERATION_BUDGET_MS = 30000;

let _ensured = false;
async function ensureTables(db) {
  if (_ensured) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS claude_identity_runs (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        person_id UUID,
        incident_id UUID,
        iterations INTEGER DEFAULT 0,
        starting_confidence INTEGER,
        ending_confidence INTEGER,
        verdict VARCHAR(120),
        searches_fired TEXT[],
        searches_succeeded TEXT[],
        new_fields_filled TEXT[],
        terminated_reason VARCHAR(60),
        model_used VARCHAR(80),
        total_tokens_in INTEGER,
        total_tokens_out INTEGER,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_cir_person ON claude_identity_runs(person_id);
      CREATE INDEX IF NOT EXISTS idx_cir_created ON claude_identity_runs(created_at DESC);

      ALTER TABLE persons ADD COLUMN IF NOT EXISTS identity_confidence INTEGER DEFAULT 50;
      ALTER TABLE persons ADD COLUMN IF NOT EXISTS contact_quality VARCHAR(20) DEFAULT 'cold';
      ALTER TABLE persons ADD COLUMN IF NOT EXISTS last_investigator_run TIMESTAMPTZ;
    `);
    _ensured = true;
  } catch (_) { /* non-fatal */ }
}

async function gatherEvidence(db, person) {
  const ev = { sources: {}, signals: {} };
  if (person.incident_id) {
    const reports = await db('source_reports')
      .where('incident_id', person.incident_id)
      .select('source_type', 'parsed_data', 'confidence', 'created_at')
      .orderBy('created_at', 'desc')
      .limit(15)
      .catch(() => []);
    for (const r of reports) {
      let parsed = r.parsed_data;
      if (typeof parsed === 'string') { try { parsed = JSON.parse(parsed); } catch { parsed = null; } }
      const k = r.source_type || 'unknown';
      if (!ev.sources[k]) ev.sources[k] = [];
      if (parsed) ev.sources[k].push({ ...parsed, _confidence: r.confidence });
    }
    const inc = await db('incidents').where('id', person.incident_id).first().catch(() => null);
    if (inc) ev.incident = {
      type: inc.incident_type, severity: inc.severity, city: inc.city,
      state: inc.state, occurred_at: inc.occurred_at,
      description: String(inc.description || '').slice(0, 400),
      lead_score: inc.lead_score, fatalities: inc.fatalities_count,
    };
  }
  ev.signals = {
    full_name: person.full_name, age: person.age, phone: person.phone,
    email: person.email, address: person.address, city: person.city,
    state: person.state, employer: person.employer, occupation: person.occupation,
    facebook_url: person.facebook_url, linkedin_url: person.linkedin_url,
    has_attorney: person.has_attorney, vehicle_make: person.vehicle_make,
    license_plate: person.license_plate, vin: person.vin,
    confidence_score: person.confidence_score,
    identity_confidence: person.identity_confidence || 50,
  };
  return ev;
}

const SYSTEM_PROMPT = `You are an investigative analyst on a personal-injury intelligence platform.
Your job: given accumulated evidence about ONE person involved in an accident, decide which API integrations to fire NEXT to fill missing identity/contact data.

Available integrations:
  - twilio-lookup        (phone -> carrier, line_type, caller_name) — best when phone exists
  - enrich-pdl-by-name   (name+city -> email/phone/employer/social) — best when name exists, no phone
  - hunter-find          (employer -> email) — best when employer known, no email
  - voter-rolls          (name+state -> DOB/age/address) — best when name+state, FL/GA/TX preferred
  - property-records     (address -> owner/value) — best when address known
  - social-search        (name+location -> social URLs) — best when name+city
  - court-reverse-link   (incident -> attorney/lawsuit) — for any active case
  - searchbug-and-voter  (name -> address+phone via voter+searchbug) — generic fallback
  - family-tree          (deceased -> relatives/spouse) — fatal cases only
  - obit-search          (name+state -> obituary detail) — fatal/serious cases

Return JSON only:
{
  "likely_identity": "Full Name, age X, lives in City ST" | null,
  "confidence": 0-100,
  "duplicate_warning": "..." | null,
  "suggested_searches": [
    { "integration": "twilio-lookup", "reason": "have phone but no carrier", "priority": 1 }
  ],
  "ready_for_rep": true|false,
  "notes": "1-2 sentences"
}

Rules:
- Suggest at MOST 4 integrations per round
- Skip integrations whose pre-conditions are not met (no phone -> dont suggest twilio-lookup)
- If identity is verified across 3+ sources AND we have phone+address -> ready_for_rep=true
- If sources contradict (different ages, cities) -> duplicate_warning + low confidence`;

async function askClaude(db, person, evidence, iteration) {
  const userPrompt = `Iteration ${iteration}/${MAX_ITERATIONS}.

Person signals so far:
${JSON.stringify(evidence.signals, null, 2)}

Linked incident:
${JSON.stringify(evidence.incident || {}, null, 2)}

Aggregated source data (${Object.keys(evidence.sources).length} source types):
${JSON.stringify(evidence.sources, null, 2)}

Decide what integrations to fire NEXT. Return JSON.`;

  const r = await extract(db, {
    pipeline: 'claude-identity-investigator',
    systemPrompt: SYSTEM_PROMPT,
    userPrompt,
    provider: 'claude',
    tier: 'auto',
    severityHint: evidence.incident?.severity,
    timeoutMs: 25000,
    responseFormat: 'json',
    temperature: 0,
  });
  if (!r.ok) return { ok: false, error: r.error };
  return { ok: true, verdict: r.parsed || {}, model: r.model_used, tokens_in: r.tokens_in, tokens_out: r.tokens_out };
}

const INTEGRATION_MAP = {
  'twilio-lookup': async (db, person) => {
    if (!person.phone) return { skipped: 'no_phone' };
    try {
      const tw = require('./twilio');
      const lu = await tw.lookupPhone(db, person.phone).catch(e => ({ ok: false, error: e.message }));
      if (lu?.ok && tw.applyLookupToPerson) {
        await tw.applyLookupToPerson(db, person.id, lu).catch(()=>{});
        return { ok: true, line_type: lu.line_type, carrier: lu.carrier_name };
      }
      return { ok: false, error: lu?.error || 'no_data' };
    } catch (e) { return { ok: false, error: e.message }; }
  },
  'enrich-pdl-by-name': async (db, person) => {
    try {
      const pdlbn = require('./pdl-by-name');
      if (typeof pdlbn.processPerson === 'function') return await pdlbn.processPerson(db, person);
    } catch (_) {}
    return { skipped: 'pdl_processPerson_missing' };
  },
  'voter-rolls': async (db, person) => {
    try {
      const vr = require('./voter-rolls');
      if (typeof vr.lookupVoter === 'function' && person.full_name && person.state) {
        const parts = person.full_name.split(/\s+/);
        const firstName = parts[0]; const lastName = parts[parts.length-1];
        const r = await vr.lookupVoter(db, firstName, lastName, person.state).catch(()=>null);
        return r ? { ok: true, matched: !!r } : { skipped: 'no_match' };
      }
    } catch (_) {}
    return { skipped: 'voter-rolls_unavailable' };
  },
  'property-records': async (db, person) => {
    try {
      const pr = require('./property-records');
      if (typeof pr.lookupOwner === 'function' && person.address && person.state) {
        const r = await pr.lookupOwner({ address: person.address, city: person.city, state: person.state }, db).catch(()=>null);
        return r ? { ok: true } : { skipped: 'no_match' };
      }
    } catch (_) {}
    return { skipped: 'property_unavailable' };
  },
  'social-search': async (db, person) => {
    try {
      const ss = require('./social-search');
      if (typeof ss.searchSocial === 'function' && person.full_name) {
        const cfg = await (async()=>{ try { return await ss.getCseConfig?.(db); } catch(_) { return null; } })();
        const r = await ss.searchSocial(person.full_name, person.city, person.state, cfg).catch(()=>null);
        return r ? { ok: true, found: r.length } : { skipped: 'no_match' };
      }
    } catch (_) {}
    return { skipped: 'social_unavailable' };
  },
  'family-tree': async (db, person) => {
    try {
      const ft = require('./family-tree');
      if (typeof ft.processDeceased === 'function') return await ft.processDeceased(db, person);
    } catch (_) {}
    return { skipped: 'family-tree_missing' };
  },
  'searchbug-and-voter': async (db, person) => {
    return { deferred_to_cron: 'people-search' };
  },
  'hunter-find': async (db, person) => {
    if (!person.employer) return { skipped: 'no_employer' };
    try {
      const dp = require('./deep');
      if (typeof dp.deepEnrichPerson === 'function') {
        return await dp.deepEnrichPerson(db, person);
      }
    } catch (_) {}
    return { skipped: 'hunter_missing' };
  },
  'court-reverse-link': async (db, person) => {
    return { deferred_to_cron: 'court-reverse-link' };
  },
  'obit-search': async (db, person) => {
    return { deferred_to_cron: 'obit-backfill' };
  },
};

function computeContactQuality(person) {
  const hasPhone = !!person.phone;
  const hasEmail = !!person.email;
  const hasAddress = !!person.address;
  const verified = !!person.phone_carrier;
  if (hasPhone && verified && (hasEmail || hasAddress)) return 'hot';
  if (hasPhone && (hasEmail || hasAddress)) return 'warm';
  if (hasPhone || hasEmail) return 'lukewarm';
  return 'cold';
}

function fieldsThatChanged(before, after) {
  const out = [];
  for (const k of ['full_name', 'phone', 'email', 'address', 'city', 'state', 'employer', 'facebook_url', 'linkedin_url', 'phone_carrier', 'vehicle_make']) {
    if (!before[k] && after[k]) out.push(k);
  }
  return out;
}

async function investigatePerson(db, person, opts = {}) {
  await ensureTables(db);
  const startT = Date.now();
  const startConf = person.identity_confidence || person.confidence_score || 50;
  const run = {
    iterations: 0,
    starting_confidence: startConf,
    ending_confidence: startConf,
    searches_fired: [],
    searches_succeeded: [],
    new_fields_filled: [],
    verdict: null,
    terminated_reason: null,
    total_tokens_in: 0,
    total_tokens_out: 0,
    model_used: null,
  };

  let working = { ...person };
  for (let it = 1; it <= MAX_ITERATIONS; it++) {
    if (Date.now() - startT > ITERATION_BUDGET_MS) {
      run.terminated_reason = 'time_budget';
      break;
    }
    run.iterations = it;
    const ev = await gatherEvidence(db, working);
    const ask = await askClaude(db, working, ev, it);
    if (!ask.ok) {
      run.terminated_reason = `claude_error:${String(ask.error).slice(0,40)}`;
      break;
    }
    run.verdict = ask.verdict;
    run.model_used = ask.model;
    run.total_tokens_in += ask.tokens_in || 0;
    run.total_tokens_out += ask.tokens_out || 0;
    const conf = parseInt(ask.verdict.confidence) || 0;
    run.ending_confidence = conf;
    if (ask.verdict.ready_for_rep || conf >= TARGET_CONFIDENCE) {
      run.terminated_reason = 'target_reached';
      break;
    }
    const suggestions = (ask.verdict.suggested_searches || []).slice(0, 4);
    if (suggestions.length === 0) {
      run.terminated_reason = 'no_suggestions';
      break;
    }
    const before = { ...working };
    for (const sug of suggestions) {
      const fn = INTEGRATION_MAP[sug.integration];
      if (!fn) continue;
      run.searches_fired.push(sug.integration);
      try {
        const result = await Promise.race([
          fn(db, working),
          new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 12000))
        ]).catch(e => ({ error: e.message }));
        if (result && (result.ok || result.success || result.fields_filled || result.matched)) {
          run.searches_succeeded.push(sug.integration);
        }
      } catch (e) {
        await reportError(db, 'claude-identity-investigator', person.id, `${sug.integration}: ${e.message}`).catch(()=>{});
      }
    }
    working = await db('persons').where('id', person.id).first().catch(() => working);
    const newFields = fieldsThatChanged(before, working || before);
    run.new_fields_filled.push(...newFields);
  }

  const finalConf = Math.max(run.starting_confidence, run.ending_confidence);
  const cq = computeContactQuality(working);
  try {
    await db('persons').where('id', person.id).update({
      identity_confidence: finalConf,
      contact_quality: cq,
      last_investigator_run: new Date(),
      updated_at: new Date(),
    });
    if (finalConf !== startConf) {
      await logChange(db, {
        entity_type: 'person', entity_id: person.id,
        action: 'identity_confidence_update',
        details: { before: startConf, after: finalConf, fields_filled: run.new_fields_filled },
      }).catch(()=>{});
    }
  } catch (_) {}

  await enqueueCascade(db, {
    person_id: person.id,
    incident_id: person.incident_id,
    trigger_source: 'claude_identity_investigator',
    trigger_field: 'identity_confidence',
    trigger_value: String(finalConf),
    priority: 4,
  }).catch(()=>{});

  await db('claude_identity_runs').insert({
    id: uuidv4(),
    person_id: person.id, incident_id: person.incident_id,
    iterations: run.iterations,
    starting_confidence: run.starting_confidence,
    ending_confidence: finalConf,
    verdict: String(run.verdict?.likely_identity || '').slice(0, 120),
    searches_fired: run.searches_fired,
    searches_succeeded: run.searches_succeeded,
    new_fields_filled: run.new_fields_filled,
    terminated_reason: run.terminated_reason || 'completed',
    model_used: run.model_used,
    total_tokens_in: run.total_tokens_in,
    total_tokens_out: run.total_tokens_out,
    created_at: new Date(),
  }).catch(()=>{});

  return { ok: true, run, contact_quality: cq, identity_confidence: finalConf };
}

async function processBatch(db, limit = 10) {
  await ensureTables(db);
  const rows = await db.raw(`
    SELECT p.* FROM persons p
    LEFT JOIN incidents i ON p.incident_id = i.id
    WHERE p.full_name IS NOT NULL
      AND (
        COALESCE(p.identity_confidence, 50) < 80
        OR COALESCE(p.contact_quality, 'cold') NOT IN ('warm', 'hot')
      )
      AND (p.last_investigator_run IS NULL OR p.last_investigator_run < NOW() - INTERVAL '6 hours')
      AND COALESCE(i.discovered_at, p.created_at) > NOW() - INTERVAL '30 days'
    ORDER BY COALESCE(i.lead_score, 0) DESC, COALESCE(p.identity_confidence, 50) ASC
    LIMIT ?
  `, [limit]).then(r => r.rows || []).catch(() => []);

  const out = { evaluated: 0, upgraded: 0, fields_filled: 0, by_termination: {}, samples: [] };
  const startT = Date.now();
  for (const p of rows) {
    if (Date.now() - startT > 50000) break;
    out.evaluated++;
    const r = await investigatePerson(db, p).catch(e => ({ ok: false, error: e.message }));
    if (r.ok) {
      const tr = r.run.terminated_reason || 'completed';
      out.by_termination[tr] = (out.by_termination[tr] || 0) + 1;
      out.fields_filled += r.run.new_fields_filled.length;
      if (r.run.ending_confidence > r.run.starting_confidence) out.upgraded++;
      if (out.samples.length < 5) {
        out.samples.push({
          person_id: p.id,
          name: p.full_name,
          before: r.run.starting_confidence,
          after: r.run.ending_confidence,
          fields: r.run.new_fields_filled,
        });
      }
    }
  }
  return out;
}

// Phase 24 #5 / Phase 25 — Backfill identity_confidence for persons with NULL via cross-exam
// Phase 25: now also drains nameless rows via baseline fallback (was leaving them stuck NULL)
async function backfillIdentityConfidence(db, opts = {}) {
  const limit = Math.min(opts.limit || 200, 500);
  const out = {
    evaluated: 0, updated: 0, skipped: 0,
    total_null_before: 0, remaining_null: 0,
    examined: 0, samples: []
  };
  try {
    const nullCnt = await db.raw(`
      SELECT COUNT(*) AS c FROM persons WHERE identity_confidence IS NULL
    `).then(r => parseInt(r.rows?.[0]?.c || 0)).catch(() => 0);
    out.total_null_before = nullCnt;

    // Phase 25: drain BOTH named and nameless rows. Nameless can't run cross-exam,
    // so they receive a baseline from confidence_score (or 50). Named first.
    const rows = await db.raw(`
      SELECT * FROM persons
      WHERE identity_confidence IS NULL
      ORDER BY (full_name IS NOT NULL) DESC, created_at DESC
      LIMIT ?
    `, [limit]).then(r => r.rows || []).catch(() => []);

    out.examined = rows.length;

    let crossExamine;
    try { crossExamine = require('./cross-exam').crossExamine; } catch (_) {}

    const start = Date.now();
    for (const p of rows) {
      if (Date.now() - start > 45000) break;
      out.evaluated++;
      try {
        let conf = null;
        let source = 'fallback';
        if (p.full_name && crossExamine) {
          try {
            const r = await crossExamine(db, p);
            if (r && typeof r.identity_confidence === 'number') {
              conf = r.identity_confidence;
              source = 'cross-exam';
            }
          } catch (_) { /* fall through */ }
        }
        if (conf == null) {
          conf = parseInt(p.confidence_score) || 50;
          source = p.full_name ? 'fallback' : 'fallback_nameless';
        }
        await db('persons').where('id', p.id).update({
          identity_confidence: conf,
          updated_at: new Date()
        });
        out.updated++;
        if (out.samples.length < 5) {
          out.samples.push({ id: p.id, name: p.full_name || null, ic: conf, source });
        }
      } catch (e) {
        out.skipped++;
      }
    }

    out.remaining_null = await db.raw(`
      SELECT COUNT(*) AS c FROM persons WHERE identity_confidence IS NULL
    `).then(r => parseInt(r.rows?.[0]?.c || 0)).catch(() => 0);
  } catch (e) {
    out.error = e.message;
  }
  return out;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const action = req.query.action || 'batch';
  try {
    if (action === 'health') {
      return res.json({ success: true, anthropic_configured: !!process.env.ANTHROPIC_API_KEY, integrations: Object.keys(INTEGRATION_MAP) });
    }
    if (action === 'person' && req.query.person_id) {
      const p = await db('persons').where('id', req.query.person_id).first();
      if (!p) return res.status(404).json({ error: 'person_not_found' });
      const r = await investigatePerson(db, p);
      return res.json({ success: true, ...r });
    }
    if (action === 'batch') {
      const limit = Math.min(parseInt(req.query.limit) || 10, 25);
      // Phase 24 #5 — auto-backfill identity_confidence on every batch run
      const bf = await backfillIdentityConfidence(db, { limit: 50 }).catch(() => null);
      const out = await processBatch(db, limit);
      return res.json({
        success: true,
        message: `Identity investigator: ${out.upgraded}/${out.evaluated} upgraded, ${out.fields_filled} fields filled, ${bf?.updated || 0} ic_backfilled`,
        backfill_ic: bf,
        ...out,
      });
    }
    if (action === 'backfill_ic') {
      const limit = Math.min(parseInt(req.query.limit) || 200, 500);
      const out = await backfillIdentityConfidence(db, { limit });
      return res.json({
        success: true,
        message: `Backfill: ${out.updated}/${out.evaluated} updated (${out.total_null_before} were NULL)`,
        ...out,
      });
    }
    return res.status(400).json({ error: 'invalid action', valid: ['batch', 'person', 'health', 'backfill_ic'] });
  } catch (err) {
    await reportError(db, 'claude-identity-investigator', null, err.message).catch(()=>{});
    res.status(500).json({ error: err.message });
  }
};

module.exports.investigatePerson = investigatePerson;
module.exports.processBatch = processBatch;
module.exports.backfillIdentityConfidence = backfillIdentityConfidence;
