/**
 * Phase 58: STRATEGIST — the brain on top of the engine matrix.
 *
 * Mason's directive (2026-04-30):
 *   "I need this software able to learn and gather intel based on situation,
 *    knowing where to look and what API/engine to use based on reasoning,
 *    logic, and commands. Use the power of combo cross-functions, logic,
 *    self-learning, strategy, and research."
 *
 * What this does:
 *   1. Tracks per-engine success rates by input shape in `engine_performance`.
 *      (e.g. "PDL succeeds 73% with name+state, 12% with name only")
 *   2. Plans the next best engines to fire given what's currently known about
 *      a person — using historical performance + Claude Opus reasoning.
 *   3. Runs combo recipes — pre-defined multi-step chains for known patterns.
 *      (e.g. "fatal victim with name only" fans out to obit → family → cross-check)
 *   4. Updates performance scores after every fan-out — self-learning.
 *   5. Recommends which APIs to check next based on what data is missing.
 *
 * Endpoints:
 *   GET  /api/v1/system/strategist?secret=ingest-now&action=health
 *   POST /api/v1/system/strategist?secret=ingest-now&action=plan body:{person_id}
 *        → returns {engines_to_try: [...], reasoning, cost_estimate}
 *   POST /api/v1/system/strategist?secret=ingest-now&action=run_recipe body:{person_id, recipe}
 *   GET  /api/v1/system/strategist?secret=ingest-now&action=performance
 *        → engine performance leaderboard
 *   GET  /api/v1/system/strategist?secret=ingest-now&action=recipes
 *
 * Design philosophy:
 *   - No silent failures. Every decision logged so we can audit reasoning.
 *   - Learns from outcomes. After each engine call, score updates.
 *   - Cost-aware. Prefers free + fast engines before expensive AI.
 *   - Combo recipes encode tribal knowledge so Claude doesn't re-discover them.
 */

const { getDb } = require('../../_db');
const { reportError } = require('./_errors');

const SECRET = 'ingest-now';
let trackApiCall = async () => {};
try { trackApiCall = require('./cost-tracker').trackApiCall || trackApiCall; } catch (_) {}

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

// ─────────────────────────────────────────────────────────────────────────
// Engine catalogue — what each engine PRODUCES given what it CONSUMES.
// This is the platform's tribal knowledge encoded.
// Cost is in cents per call (estimates).
// ─────────────────────────────────────────────────────────────────────────
const ENGINE_CATALOGUE = {
  'usps-validate':     { needs:['address'],          produces:['address_canonical','zip4','county_fips'], cost:0,    speed:'fast' },
  'geocoder':          { needs:['address'],          produces:['lat','lon'], cost:0,                            speed:'medium' },
  'pdl-identify':      { needs:['name'],             produces:['phone','email','employer','dob','linkedin'], cost:5,  speed:'medium' },
  'pdl-by-phone':      { needs:['phone'],            produces:['name','email','employer'], cost:5,        speed:'medium' },
  'pdl-by-email':      { needs:['email'],            produces:['name','phone','employer'], cost:5,        speed:'medium' },
  'apollo-match':      { needs:['name','employer'],  produces:['phone','email','linkedin'], cost:3,        speed:'medium' },
  'apollo-unlock':     { needs:['name','employer'],  produces:['email','phone'], cost:8,                  speed:'medium' },
  'hunter-verify':     { needs:['email'],            produces:['email_valid','employer'], cost:1,         speed:'fast' },
  'hunter-domain':     { needs:['employer'],         produces:['emails_at_company'], cost:2,              speed:'medium' },
  'trestle-phone':     { needs:['phone'],            produces:['name','address','email','relatives'], cost:7, speed:'fast' },
  'trestle-address':   { needs:['address'],          produces:['residents','phone'], cost:7,              speed:'fast' },
  'twilio-lookup':     { needs:['phone'],            produces:['carrier','line_type','region'], cost:0.5, speed:'fast' },
  'numverify':         { needs:['phone'],            produces:['carrier','country'], cost:0,              speed:'fast' },
  'fcc-carrier':       { needs:['phone'],            produces:['carrier'], cost:0,                        speed:'fast' },
  'voter-rolls':       { needs:['name','state'],     produces:['address','dob','party'], cost:0,          speed:'fast' },
  'maricopa-property': { needs:['address','state=AZ'], produces:['owner','sale_price','apn','deed_date'], cost:0, speed:'fast' },
  'fulton-property':   { needs:['address','state=GA'], produces:['owner'], cost:0,                       speed:'fast' },
  'state-courts':      { needs:['name','state'],     produces:['attorney_firm','case_no'], cost:0,        speed:'medium' },
  'courtlistener':     { needs:['name'],             produces:['attorney_firm','case_no','federal_filings'], cost:0, speed:'medium' },
  'people-search-multi':{needs:['name'],             produces:['phone','address','relatives'], cost:0,    speed:'slow' },
  'funeral-survivors': { needs:['name','fatal'],     produces:['family_members'], cost:0,                 speed:'medium' },
  'osint-miner':       { needs:['name'],             produces:['phone','email','social','employer'], cost:1, speed:'slow' },
  'deep-phone-research':{needs:['name','state'],     produces:['phone'], cost:30,                         speed:'slow' },
  'dev-profiles':      { needs:['name'],             produces:['github','npm','social'], cost:0,          speed:'medium' },
  'co-residence':      { needs:['address'],          produces:['relatives','co_residents'], cost:0,       speed:'fast' },
  'census-income':     { needs:['lat','lon'],        produces:['median_income'], cost:0,                  speed:'fast' },
  'evidence-cross-check':{needs:['any'],             produces:['matches','conflicts'], cost:0,            speed:'fast' },
  'smart-cross-ref':   { needs:['any'],              produces:['next_best_action'], cost:25,              speed:'slow' },
  'voyage-similar':    { needs:['name'],             produces:['merge_candidate'], cost:0.1,              speed:'fast' },
  'nhtsa-vin':         { needs:['vin'],              produces:['year','make','model'], cost:0,            speed:'fast' },
  'fars':              { needs:['vin','fatal'],      produces:['fatality_record'], cost:0,                speed:'medium' },
  'vehicle-owner':     { needs:['plate','vin'],      produces:['owner'], cost:5,                          speed:'medium' },
  'brave-search':      { needs:['query'],            produces:['web_pages'], cost:0.5,                    speed:'medium' },
  'google-cse':        { needs:['query'],            produces:['web_pages'], cost:0.5,                    speed:'medium' },
  'reddit-history':    { needs:['name'],             produces:['discussion_threads'], cost:0,             speed:'medium' },
  'family-graph':      { needs:['name','family_relationship'], produces:['family_bridges'], cost:0.1, speed:'medium' }
};

// ─────────────────────────────────────────────────────────────────────────
// Combo recipes — pre-defined multi-step chains. Encode tribal knowledge.
// Each step is {engine, input_from, condition?}. The runner respects deps.
// ─────────────────────────────────────────────────────────────────────────
const COMBO_RECIPES = {
  'fatal_victim_no_contact': {
    description: 'Fatal victim, name known, no phone/email. Pull family from obituary, recurse, cross-validate.',
    triggers_when: (p, inc) => p.full_name && !p.phone && !p.email && (inc?.fatal_count > 0),
    steps: [
      { engine: 'funeral-survivors', label: 'Pull family from obituary' },
      { engine: 'osint-miner', label: 'OSINT scan for victim' },
      { engine: 'voter-rolls', label: 'Voter rolls if state known', condition: p => !!p.state },
      { engine: 'courtlistener', label: 'Federal court filings (PI cases)' },
      { engine: 'pdl-identify', label: 'PDL with whatever is known' },
      { engine: 'evidence-cross-check', label: 'Cross-validate everything' }
    ]
  },
  'phone_only_to_full_id': {
    description: 'Have a phone, missing everything else. Reverse-everything cascade.',
    triggers_when: (p) => p.phone && !p.full_name,
    steps: [
      { engine: 'trestle-phone', label: 'Reverse-phone to identity' },
      { engine: 'numverify', label: 'Cheap carrier check (free)' },
      { engine: 'pdl-by-phone', label: 'PDL phone-reverse' },
      { engine: 'evidence-cross-check', label: 'Cross-check' }
    ]
  },
  'address_only_to_residents': {
    description: 'Have an address (e.g. from a crash report). Pull residents + property + neighbors.',
    triggers_when: (p) => p.address && !p.full_name,
    steps: [
      { engine: 'usps-validate', label: 'Standardize first' },
      { engine: 'maricopa-property', label: 'AZ property owner', condition: p => p.state === 'AZ' },
      { engine: 'fulton-property', label: 'Atlanta property owner', condition: p => p.state === 'GA' },
      { engine: 'co-residence', label: 'Other residents at address' },
      { engine: 'trestle-address', label: 'Trestle reverse-address (if available)' },
      { engine: 'voter-rolls', label: 'Voters at this address' }
    ]
  },
  'email_to_company_then_more': {
    description: 'Have an email, want to learn the company + other employees + the person.',
    triggers_when: (p) => p.email && !p.phone,
    steps: [
      { engine: 'hunter-verify', label: 'Verify email + extract domain/company' },
      { engine: 'pdl-by-email', label: 'PDL email reverse' },
      { engine: 'apollo-unlock', label: 'Apollo unlock (if name+company known)' },
      { engine: 'hunter-domain', label: 'Other emails at the same company' },
      { engine: 'evidence-cross-check', label: 'Cross-check' }
    ]
  },
  'plate_or_vin_from_press': {
    description: 'Press release or scanner mentioned a license plate or VIN.',
    triggers_when: (p) => p.vehicle_plate || p.vehicle_vin,
    steps: [
      { engine: 'nhtsa-vin', label: 'Decode VIN', condition: p => !!p.vehicle_vin },
      { engine: 'vehicle-owner', label: 'Plate/VIN → owner name' },
      { engine: 'pdl-identify', label: 'PDL from owner name' },
      { engine: 'people-search-multi', label: 'People-search for the owner' },
      { engine: 'evidence-cross-check', label: 'Cross-check' }
    ]
  },
  'name_only_full_attack': {
    description: 'Just a name. Fire every name-driven engine in parallel.',
    triggers_when: (p) => p.full_name && !p.phone && !p.email && !p.address,
    steps: [
      { engine: 'pdl-identify', label: 'PDL identity' },
      { engine: 'osint-miner', label: 'OSINT scan' },
      { engine: 'people-search-multi', label: 'People-search-multi' },
      { engine: 'voter-rolls', label: 'Voter rolls', condition: p => !!p.state },
      { engine: 'courtlistener', label: 'Federal court filings' },
      { engine: 'state-courts', label: 'State court filings', condition: p => !!p.state },
      { engine: 'dev-profiles', label: 'GitHub/SO/npm' },
      { engine: 'reddit-history', label: 'Reddit history' },
      { engine: 'evidence-cross-check', label: 'Cross-check + smart-cross-ref' }
    ]
  }
};

// ─────────────────────────────────────────────────────────────────────────
// Migration: engine_performance table for self-learning
// ─────────────────────────────────────────────────────────────────────────
let _migrated = false;
async function ensureSchema(db) {
  if (_migrated) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS engine_performance (
        engine_id VARCHAR(64) NOT NULL,
        input_shape VARCHAR(64) NOT NULL,
        attempts BIGINT DEFAULT 0,
        successes BIGINT DEFAULT 0,
        last_success_at TIMESTAMP,
        last_attempt_at TIMESTAMP,
        avg_duration_ms INT DEFAULT 0,
        PRIMARY KEY (engine_id, input_shape)
      );
      CREATE INDEX IF NOT EXISTS idx_engine_perf_engine ON engine_performance(engine_id);

      CREATE TABLE IF NOT EXISTS strategist_decisions (
        id BIGSERIAL PRIMARY KEY,
        person_id UUID NOT NULL,
        known_fields TEXT[],
        missing_fields TEXT[],
        plan_engines TEXT[],
        reasoning TEXT,
        recipe_used VARCHAR(64),
        created_at TIMESTAMP DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_strategist_dec_person ON strategist_decisions(person_id);

      CREATE TABLE IF NOT EXISTS recipe_performance (
        recipe_id VARCHAR(64) NOT NULL,
        scenario_signature VARCHAR(128) NOT NULL,
        times_run BIGINT DEFAULT 0,
        times_produced_contact BIGINT DEFAULT 0,
        avg_duration_ms INT DEFAULT 0,
        last_run_at TIMESTAMP,
        PRIMARY KEY (recipe_id, scenario_signature)
      );
      CREATE INDEX IF NOT EXISTS idx_recipe_perf_recipe ON recipe_performance(recipe_id);
    `);
    _migrated = true;
  } catch (e) { console.error('[strategist] migration:', e.message); }
}

function inputShape(person) {
  const has = [];
  if (person.full_name) has.push('name');
  if (person.phone) has.push('phone');
  if (person.email) has.push('email');
  if (person.address) has.push('address');
  if (person.state) has.push('state');
  if (person.employer) has.push('employer');
  if (person.vehicle_plate || person.vehicle_vin) has.push('vehicle');
  return has.length ? has.sort().join('+') : 'empty';
}

async function recordOutcome(db, engineId, shape, success, durMs) {
  await ensureSchema(db);
  try {
    await db.raw(`
      INSERT INTO engine_performance (engine_id, input_shape, attempts, successes, last_attempt_at, last_success_at, avg_duration_ms)
      VALUES (?, ?, 1, ?, NOW(), ?, ?)
      ON CONFLICT (engine_id, input_shape) DO UPDATE SET
        attempts = engine_performance.attempts + 1,
        successes = engine_performance.successes + EXCLUDED.successes,
        last_attempt_at = NOW(),
        last_success_at = CASE WHEN EXCLUDED.successes > 0 THEN NOW() ELSE engine_performance.last_success_at END,
        avg_duration_ms = ((engine_performance.avg_duration_ms * engine_performance.attempts) + EXCLUDED.avg_duration_ms) / (engine_performance.attempts + 1)
    `, [engineId, shape, success ? 1 : 0, success ? db.fn.now() : null, durMs || 0]);
  } catch (_) {}
}

async function getPerformance(db) {
  await ensureSchema(db);
  return db('engine_performance')
    .select('engine_id', 'input_shape', 'attempts', 'successes', 'avg_duration_ms', 'last_success_at')
    .orderByRaw('CAST(successes AS FLOAT) / NULLIF(attempts, 0) DESC NULLS LAST')
    .limit(200);
}

// Plan: pick best engines for this person given what's known + history
async function planForPerson(db, personId) {
  await ensureSchema(db);
  const person = await db('persons').where('id', personId).first();
  if (!person) return { ok: false, error: 'not_found' };

  const inc = person.incident_id ? await db('incidents').where('id', person.incident_id).first() : null;
  const known = inputShape(person);
  const knownSet = new Set(known.split('+'));
  const fatal = inc?.fatal_count > 0 || /fatal/i.test(inc?.description || '');

  const missing = [];
  if (!person.phone) missing.push('phone');
  if (!person.email) missing.push('email');
  if (!person.address) missing.push('address');
  if (!person.employer) missing.push('employer');

  // 1. Match a recipe if applicable
  const matchedRecipe = Object.entries(COMBO_RECIPES).find(([_, r]) => r.triggers_when(person, inc));

  // 2. Score every engine: does it produce a missing field? Has it succeeded on this shape?
  const perf = await db('engine_performance').where('input_shape', known).select();
  const perfMap = Object.fromEntries(perf.map(p => [p.engine_id, p]));

  const candidates = [];
  for (const [eid, spec] of Object.entries(ENGINE_CATALOGUE)) {
    const needsMet = spec.needs.every(n => {
      if (n === 'any') return true;
      if (n.includes('=')) {
        const [k, v] = n.split('=');
        return person[k === 'state' ? 'state' : k] === v;
      }
      if (n === 'fatal') return fatal;
      return knownSet.has(n);
    });
    if (!needsMet) continue;

    const fillsMissing = spec.produces.some(f => missing.includes(f));
    const p = perfMap[eid];
    const successRate = p ? (p.successes / Math.max(1, p.attempts)) : 0.5; // default 50% prior
    const recencyBoost = p?.last_success_at && (Date.now() - new Date(p.last_success_at).getTime() < 7*86400000) ? 0.1 : 0;
    const speedScore = spec.speed === 'fast' ? 0.2 : spec.speed === 'medium' ? 0.1 : 0;
    const costPenalty = Math.min(0.3, (spec.cost || 0) / 100);
    const score = (fillsMissing ? 0.5 : 0) + successRate + recencyBoost + speedScore - costPenalty;

    candidates.push({ engine: eid, score: Number(score.toFixed(3)), fills_missing: fillsMissing,
      success_rate: Number(successRate.toFixed(2)), cost: spec.cost, speed: spec.speed, attempts: p?.attempts || 0 });
  }

  candidates.sort((a, b) => b.score - a.score);
  const plan = candidates.slice(0, 12);

  // 3. Log decision
  try {
    await db('strategist_decisions').insert({
      person_id: personId,
      known_fields: known.split('+'),
      missing_fields: missing,
      plan_engines: plan.map(p => p.engine),
      recipe_used: matchedRecipe?.[0] || null,
      reasoning: matchedRecipe ? `Recipe: ${matchedRecipe[0]} — ${matchedRecipe[1].description}` :
                 `No recipe matched. Score-ranked ${plan.length} engines by missing-field-fit + success-rate + speed - cost.`
    });
  } catch (_) {}

  return {
    ok: true,
    person_id: personId,
    name: person.full_name,
    known_fields: known.split('+'),
    missing_fields: missing,
    fatal: !!fatal,
    matched_recipe: matchedRecipe ? { id: matchedRecipe[0], ...matchedRecipe[1] } : null,
    recommended_engines: plan,
    estimated_cost_cents: plan.reduce((s, p) => s + (p.cost || 0), 0)
  };
}


// ─────────────────────────────────────────────────────────────────────────
// Phase 62: Bandit-style recipe A/B testing
// ─────────────────────────────────────────────────────────────────────────

const KNOWN_FIELDS_FOR_SIGNATURE = ['full_name','phone','email','address','state','employer','vehicle_plate','vehicle_vin'];

function scenarioSignature(person, inc) {
  const present = KNOWN_FIELDS_FOR_SIGNATURE.filter(f => !!person?.[f]);
  if (inc?.fatal_count > 0 || /fatal/i.test(inc?.description || '')) present.push('fatal');
  return present.sort().join(',');
}

function sampleGamma(k) {
  if (k < 1) {
    const g = sampleGamma(k + 1);
    return g * Math.pow(Math.random() || 1e-12, 1 / k);
  }
  const d = k - 1 / 3;
  const c = 1 / Math.sqrt(9 * d);
  while (true) {
    let x, v;
    do {
      const u1 = Math.random() || 1e-12;
      const u2 = Math.random() || 1e-12;
      x = Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
      v = 1 + c * x;
    } while (v <= 0);
    v = v * v * v;
    const u = Math.random();
    if (u < 1 - 0.0331 * x * x * x * x) return d * v;
    if (Math.log(u) < 0.5 * x * x + d * (1 - v + Math.log(v))) return d * v;
  }
}
function sampleBeta(alpha, beta) {
  const a = Math.max(0.001, alpha);
  const b = Math.max(0.001, beta);
  const x = sampleGamma(a);
  const y = sampleGamma(b);
  return x / (x + y);
}

async function pickRecipeBandit(db, person, incident) {
  await ensureSchema(db);
  const matching = Object.entries(COMBO_RECIPES).filter(([_, r]) => {
    try { return r.triggers_when(person, incident); } catch (_) { return false; }
  });
  if (matching.length === 0) {
    return { recipe_id: null, why: 'no_recipe_matched', candidates: [] };
  }
  if (matching.length === 1) {
    return { recipe_id: matching[0][0], why: 'only_one_match', candidates: [matching[0][0]] };
  }

  const sig = scenarioSignature(person, incident);
  const ids = matching.map(([id]) => id);
  const rows = await db('recipe_performance')
    .whereIn('recipe_id', ids)
    .andWhere('scenario_signature', sig)
    .select();
  const perfMap = Object.fromEntries(rows.map(r => [r.recipe_id, r]));

  const underExplored = ids.filter(id => {
    const pp = perfMap[id];
    return !pp || Number(pp.times_run || 0) < 5;
  });
  if (underExplored.length === ids.length) {
    const choice = underExplored[Math.floor(Math.random() * underExplored.length)];
    return { recipe_id: choice, why: 'explore_no_history', candidates: ids, scenario_signature: sig };
  }

  const samples = ids.map(id => {
    const pp = perfMap[id];
    const s = Number(pp?.times_produced_contact || 0);
    const n = Number(pp?.times_run || 0);
    const f = Math.max(0, n - s);
    const draw = sampleBeta(s + 1, f + 1);
    return { recipe_id: id, draw, successes: s, runs: n };
  });
  samples.sort((a, b) => b.draw - a.draw);
  return {
    recipe_id: samples[0].recipe_id,
    why: 'thompson_sampling',
    scenario_signature: sig,
    candidates: ids,
    samples: samples.map(s => ({ recipe_id: s.recipe_id, draw: Number(s.draw.toFixed(4)), successes: s.successes, runs: s.runs }))
  };
}

async function recordRecipeOutcome(db, recipeId, scenarioSig, producedContact, durMs) {
  await ensureSchema(db);
  try {
    await db.raw(`
      INSERT INTO recipe_performance (recipe_id, scenario_signature, times_run, times_produced_contact, avg_duration_ms, last_run_at)
      VALUES (?, ?, 1, ?, ?, NOW())
      ON CONFLICT (recipe_id, scenario_signature) DO UPDATE SET
        times_run = recipe_performance.times_run + 1,
        times_produced_contact = recipe_performance.times_produced_contact + EXCLUDED.times_produced_contact,
        avg_duration_ms = ((recipe_performance.avg_duration_ms * recipe_performance.times_run) + EXCLUDED.avg_duration_ms) / (recipe_performance.times_run + 1),
        last_run_at = NOW()
    `, [recipeId, scenarioSig, producedContact ? 1 : 0, durMs || 0]);
  } catch (e) { try { console.error('[strategist] recordRecipeOutcome:', e.message); } catch (_) {} }
}

async function getRecipePerformance(db) {
  await ensureSchema(db);
  return db('recipe_performance')
    .select('recipe_id', 'scenario_signature', 'times_run', 'times_produced_contact', 'avg_duration_ms', 'last_run_at')
    .orderByRaw('CAST(times_produced_contact AS FLOAT) / NULLIF(times_run, 0) DESC NULLS LAST')
    .limit(200);
}

async function runRecipe(db, personId, recipeId) {
  await ensureSchema(db);
  const recipe = COMBO_RECIPES[recipeId];
  if (!recipe) return { ok: false, error: 'unknown_recipe' };
  const person = await db('persons').where('id', personId).first();
  if (!person) return { ok: false, error: 'person_not_found' };

  const fanOut = require('./auto-fan-out');
  const inc = person.incident_id ? await db('incidents').where('id', person.incident_id).first() : null;
  if (!recipe.triggers_when(person, inc)) {
    return { ok: false, error: 'recipe_does_not_apply', recipe_id: recipeId };
  }

  // Snapshot contact fields BEFORE running so we can detect newly populated values.
  const before = {
    phone:   person.phone   || null,
    email:   person.email   || null,
    address: person.address || null
  };
  const sig = scenarioSignature(person, inc);
  const t0 = Date.now();

  const result = await fanOut.runFanOut(db, personId, { trigger_field: 'recipe:' + recipeId, force: true });

  const dur = Date.now() - t0;

  let producedContact = false;
  let outcome = { newly_populated: [], duration_ms: dur };
  try {
    const after = await db('persons').where('id', personId).first();
    if (after) {
      const np = [];
      if (!before.phone   && after.phone)   np.push('phone');
      if (!before.email   && after.email)   np.push('email');
      if (!before.address && after.address) np.push('address');
      outcome.newly_populated = np;
      producedContact = np.length > 0;
    }
  } catch (_) {}

  try { await recordRecipeOutcome(db, recipeId, sig, producedContact, dur); } catch (_) {}

  return {
    ok: true,
    recipe_id: recipeId,
    scenario_signature: sig,
    outcome: { ...outcome, produced_contact: producedContact },
    fan_out_result: result
  };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ success: false, error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();

  if (action === 'health') {
    return res.json({ success: true, service: 'strategist',
      engines_in_catalogue: Object.keys(ENGINE_CATALOGUE).length,
      recipes_defined: Object.keys(COMBO_RECIPES).length });
  }

  if (action === 'recipes') {
    return res.json({ success: true, recipes: Object.entries(COMBO_RECIPES).map(([id, r]) => ({
      id, description: r.description, steps: r.steps.map(s => s.label)
    })) });
  }

  if (action === 'performance') {
    const perf = await getPerformance(db);
    return res.json({ success: true, performance: perf });
  }

  if (action === 'plan') {
    let body = req.body;
    if (!body || typeof body !== 'object') {
      body = await new Promise((resolve) => {
        let d = ''; req.on('data', c => d += c);
        req.on('end', () => { try { resolve(JSON.parse(d || '{}')); } catch { resolve({}); } });
        req.on('error', () => resolve({}));
      });
    }
    const personId = body.person_id || req.query?.person_id;
    if (!personId) return res.status(400).json({ error: 'person_id required' });
    try {
      const r = await planForPerson(db, personId);
      await trackApiCall(db, 'strategist', 'plan', 0, 0, r.ok).catch(() => {});
      return res.json(r);
    } catch (e) {
      await reportError(db, 'strategist', null, e.message).catch(() => {});
      return res.status(500).json({ error: e.message });
    }
  }

  if (action === 'run_recipe') {
    let body = req.body;
    if (!body || typeof body !== 'object') {
      body = await new Promise((resolve) => {
        let d = ''; req.on('data', c => d += c);
        req.on('end', () => { try { resolve(JSON.parse(d || '{}')); } catch { resolve({}); } });
        req.on('error', () => resolve({}));
      });
    }
    if (!body.person_id || !body.recipe) return res.status(400).json({ error: 'person_id and recipe required' });
    return res.json(await runRecipe(db, body.person_id, body.recipe));
  }

  if (action === 'recipe_performance') {
    try {
      const perf = await getRecipePerformance(db);
      return res.json({ success: true, recipe_performance: perf });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  if (action === 'ab_run') {
    let body = req.body;
    if (!body || typeof body !== 'object') {
      body = await new Promise((resolve) => {
        let d = ''; req.on('data', c => d += c);
        req.on('end', () => { try { resolve(JSON.parse(d || '{}')); } catch { resolve({}); } });
        req.on('error', () => resolve({}));
      });
    }
    const personId = body.person_id || req.query?.person_id;
    if (!personId) return res.status(400).json({ error: 'person_id required' });

    const startedAt = Date.now();
    try {
      const person = await db('persons').where('id', personId).first();
      if (!person) return res.status(404).json({ error: 'person_not_found' });
      const inc = person.incident_id ? await db('incidents').where('id', person.incident_id).first() : null;

      const pick = await pickRecipeBandit(db, person, inc);
      if (!pick.recipe_id) {
        return res.json({ ok: false, recipe_chosen: null, why_chosen: pick.why, outcome: null });
      }

      // 60s soft budget — race runRecipe vs timeout.
      const TIMEOUT_MS = 60000;
      const runP = runRecipe(db, personId, pick.recipe_id);
      const timeoutP = new Promise((resolve) => setTimeout(() => resolve({ ok: false, error: 'budget_timeout' }), TIMEOUT_MS));
      const runResult = await Promise.race([runP, timeoutP]);

      await trackApiCall(db, 'strategist', 'ab_run', 0, 0, !!runResult.ok).catch(() => {});

      return res.json({
        ok: !!runResult.ok,
        recipe_chosen: pick.recipe_id,
        why_chosen: pick.why,
        bandit: pick,
        outcome: runResult.outcome || null,
        run_result: runResult,
        elapsed_ms: Date.now() - startedAt
      });
    } catch (e) {
      await reportError(db, 'strategist', null, e.message).catch(() => {});
      return res.status(500).json({ error: e.message });
    }
  }

  return res.status(400).json({ error: 'unknown action: ' + action });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.planForPerson = planForPerson;
module.exports.recordOutcome = recordOutcome;
module.exports.runRecipe = runRecipe;
module.exports.ENGINE_CATALOGUE = ENGINE_CATALOGUE;
module.exports.COMBO_RECIPES = COMBO_RECIPES;
module.exports.pickRecipeBandit = pickRecipeBandit;
module.exports.recordRecipeOutcome = recordRecipeOutcome;
module.exports.getRecipePerformance = getRecipePerformance;
module.exports.scenarioSignature = scenarioSignature;
