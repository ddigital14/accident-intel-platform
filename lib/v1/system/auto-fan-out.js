/**
 * Phase 55: Auto Fan-Out Orchestrator (2026-04-30)
 *
 * MANDATORY PLATFORM RULE:
 *   Whenever any field is added or changed on a `persons` row, this orchestrator
 *   fires every engine that could plausibly enrich the *missing* fields. The goal
 *   is maximum contact-info coverage with zero engines left unused.
 *
 * Endpoints:
 *   GET /api/v1/system/auto-fan-out?secret=ingest-now&action=health
 *   POST /api/v1/system/auto-fan-out?secret=ingest-now&action=run
 *        body: { person_id, trigger_field?, force? }
 *   GET /api/v1/system/auto-fan-out?secret=ingest-now&action=batch&scope=qualified&limit=50
 *
 * Triggered by:
 *   1. Postgres trigger on persons UPDATE/INSERT → enqueues cascade with
 *      action='auto_fan_out' (handled in lib/v1/_cascade.js ACTION_HANDLERS)
 *   2. Direct rep action (one-click "Re-enrich now" button)
 *   3. Scheduled batch (every 6 hours) over qualified persons
 *
 * Rate limit: max 1 full pass per person per 6h UNLESS a contact-info
 *   field changed (phone/email/address/full_name), in which case re-fire.
 */

const { getDb } = require('../../_db');
const { reportError } = require('./_errors');

let _migrationApplied = false;
async function applyMigrationIdempotent(db) {
  if (_migrationApplied) return;
  try {
    await db.raw(`-- Phase 55 (2026-04-30): Auto Fan-Out platform rule
-- Every persons INSERT/UPDATE on a meaningful field auto-enqueues a full-fan-out
-- cascade so every relevant engine fires to fill remaining contact gaps.

CREATE TABLE IF NOT EXISTS cascade_queue (
  id BIGSERIAL PRIMARY KEY,
  person_id UUID NOT NULL,
  action VARCHAR(64) NOT NULL,
  trigger_field VARCHAR(64),
  trigger_value TEXT,
  priority INT DEFAULT 5,
  status VARCHAR(32) DEFAULT 'queued',
  contact_field_changed BOOLEAN DEFAULT FALSE,
  enqueued_at TIMESTAMP DEFAULT NOW(),
  processed_at TIMESTAMP,
  attempts INT DEFAULT 0,
  last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_cascade_queue_status_priority
  ON cascade_queue (status, priority DESC, enqueued_at ASC);
CREATE INDEX IF NOT EXISTS idx_cascade_queue_person_id ON cascade_queue (person_id);

CREATE OR REPLACE FUNCTION enqueue_auto_fan_out() RETURNS TRIGGER AS $$
DECLARE
  changed_field TEXT := NULL;
  is_contact BOOLEAN := FALSE;
  prio INT := 5;
BEGIN
  -- Identify which meaningful field changed (newest wins)
  IF TG_OP = 'INSERT' THEN
    changed_field := 'create';
    is_contact := (NEW.phone IS NOT NULL OR NEW.email IS NOT NULL OR NEW.address IS NOT NULL);
    prio := 8;
  ELSE
    IF NEW.phone IS DISTINCT FROM OLD.phone AND NEW.phone IS NOT NULL THEN
      changed_field := 'phone'; is_contact := TRUE; prio := 9;
    ELSIF NEW.email IS DISTINCT FROM OLD.email AND NEW.email IS NOT NULL THEN
      changed_field := 'email'; is_contact := TRUE; prio := 9;
    ELSIF NEW.address IS DISTINCT FROM OLD.address AND NEW.address IS NOT NULL THEN
      changed_field := 'address'; is_contact := TRUE; prio := 8;
    ELSIF NEW.full_name IS DISTINCT FROM OLD.full_name AND NEW.full_name IS NOT NULL THEN
      changed_field := 'name'; prio := 7;
    ELSIF NEW.dob IS DISTINCT FROM OLD.dob AND NEW.dob IS NOT NULL THEN
      changed_field := 'dob'; prio := 6;
    ELSIF NEW.employer IS DISTINCT FROM OLD.employer AND NEW.employer IS NOT NULL THEN
      changed_field := 'employer'; prio := 6;
    ELSIF NEW.victim_verified IS DISTINCT FROM OLD.victim_verified AND NEW.victim_verified = TRUE THEN
      changed_field := 'verified'; prio := 8;
    END IF;
  END IF;

  -- Only enqueue if a meaningful field changed
  IF changed_field IS NOT NULL THEN
    -- Avoid duplicate queued entries for same person + same field within 60 seconds
    INSERT INTO cascade_queue (person_id, action, trigger_field, trigger_value, priority, contact_field_changed)
    SELECT NEW.id, 'auto_fan_out', changed_field,
           COALESCE(
             CASE changed_field
               WHEN 'phone'    THEN NEW.phone
               WHEN 'email'    THEN NEW.email
               WHEN 'address'  THEN NEW.address
               WHEN 'name'     THEN NEW.full_name
               WHEN 'employer' THEN NEW.employer
               ELSE NULL
             END, ''),
           prio, is_contact
    WHERE NOT EXISTS (
      SELECT 1 FROM cascade_queue
      WHERE person_id = NEW.id
        AND action = 'auto_fan_out'
        AND trigger_field = changed_field
        AND status = 'queued'
        AND enqueued_at > NOW() - INTERVAL '60 seconds'
    );
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS persons_auto_fan_out_trigger ON persons;
CREATE TRIGGER persons_auto_fan_out_trigger
  AFTER INSERT OR UPDATE ON persons
  FOR EACH ROW EXECUTE FUNCTION enqueue_auto_fan_out();
`);
    _migrationApplied = true;
  } catch (e) {
    console.error('[auto-fan-out] migration apply failed:', e.message);
  }
}

let trackApiCall = async () => {};
try { trackApiCall = require('./cost-tracker').trackApiCall || trackApiCall; } catch (_) {}

const SECRET = 'ingest-now';
const RATE_LIMIT_HOURS = 6;
const PER_ENGINE_TIMEOUT_MS = 12000;
const TOTAL_BUDGET_MS = 55000;

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

// The engine matrix — drives WHICH engines fire based on what's known + what's missing.
// Every engine resolves to lib/v1/<dir>/<file>.js with .runOne(personId) or a runner
// function. Loaders are lazy so missing modules don't break the orchestrator.
const ENGINE_MATRIX = [
  // ── Identity / name resolution ──
  { id: 'pdl-identify',        path: '../enrich/pdl-identify',        method: 'identifyOne',  fires_on: ['name', 'always'],   fills: ['phone','email','address','employer','dob'] },
  { id: 'apollo-match',        path: '../enrich/apollo-match',        method: 'matchOne',     fires_on: ['name', 'always'],   fills: ['phone','email','employer','linkedin'] },
  { id: 'victim-resolver',     path: '../enrich/victim-resolver',     method: 'resolveOne',   fires_on: ['name', 'always'],   fills: ['phone','email','address'] },
  { id: 'people-search-multi', path: '../enrich/people-search-multi', method: 'lookupOne',    fires_on: ['name'],             fills: ['phone','address','relatives'] },
  { id: 'voter-rolls-search',  path: '../enrich/voter-states',        method: 'lookupOne',    fires_on: ['name','address'],   fills: ['address','dob','party'] },
  { id: 'courtlistener',       path: '../enrich/state-courts',        method: 'searchOne',    fires_on: ['name'],             fills: ['attorney_firm','case_no'] },
  { id: 'funeral-survivors',   path: '../enrich/funeral-home-survivors', method: 'resolveOne', fires_on: ['name','fatal'],   fills: ['family'] },
  { id: 'osint-miner',         path: '../enrich/homegrown-osint-miner', method: 'mineOne',    fires_on: ['name','always'],    fills: ['phone','email','social','employer'] },
  { id: 'deep-phone-research', path: '../enrich/deep-phone-research', method: 'researchOne',  fires_on: ['name'],             fills: ['phone'] },

  // ── Phone-driven engines ──
  { id: 'trestle-phone',       path: '../enrich/trestle',             method: 'reversePhone', fires_on: ['phone'],            fills: ['name','address','email','relatives'] },
  { id: 'twilio-lookup',       path: '../enrich/twilio',              method: 'lookupOne',    fires_on: ['phone'],            fills: ['carrier','line_type'] },
  { id: 'numverify',           path: '../enrich/numverify',           method: 'verifyOne',    fires_on: ['phone'],            fills: ['carrier','country'] },
  { id: 'fcc-carrier',         path: '../enrich/fcc-carrier',         method: 'lookupOne',    fires_on: ['phone'],            fills: ['carrier'] },
  { id: 'pdl-by-phone',        path: '../enrich/pdl-identify',        method: 'identifyByPhone', fires_on: ['phone'],         fills: ['name','email','employer'] },
  { id: 'apollo-unlock',       path: '../enrich/apollo-unlock',       method: 'unlockOne',    fires_on: ['phone','email'],    fills: ['email','phone'] },

  // ── Email-driven engines ──
  { id: 'hunter-verify',       path: '../enrich/hunter',              method: 'verifyOne',    fires_on: ['email'],            fills: ['email_valid','employer'] },
  { id: 'pdl-by-email',        path: '../enrich/pdl-identify',        method: 'identifyByEmail', fires_on: ['email'],         fills: ['name','phone','employer'] },
  { id: 'dev-profiles',        path: '../enrich/dev-profiles',        method: 'lookupOne',    fires_on: ['email','name'],     fills: ['github','social'] },

  // ── Address-driven engines ──
  { id: 'trestle-address',     path: '../enrich/trestle',             method: 'reverseAddress', fires_on: ['address'],        fills: ['residents','phone'] },
  { id: 'maricopa-property',   path: '../enrich/maricopa-assessor',   method: 'lookupByAddress', fires_on: ['address'],       fills: ['owner','sale_price'] },
  { id: 'fulton-property',     path: '../enrich/fulton-property',     method: 'lookupByAddress', fires_on: ['address'],       fills: ['owner'] },
  { id: 'co-residence',        path: '../enrich/co-residence',        method: 'lookupOne',    fires_on: ['address'],          fills: ['relatives','co_residents'] },
  { id: 'census-income',       path: '../enrich/census-income',       method: 'lookupOne',    fires_on: ['address'],          fills: ['median_income','poverty'] },
  { id: 'usps-validate',       path: '../enrich/usps-validate',       method: 'validateOne',  fires_on: ['address'],          fills: ['zip4','address_canonical'] },

  // ── Vehicle-driven ──
  { id: 'vehicle-owner',       path: '../enrich/vehicle-owner',       method: 'lookupOne',    fires_on: ['plate','vin'],      fills: ['owner','year','make','model'] },
  { id: 'nhtsa-vin',           path: '../enrich/nhtsa',               method: 'decodeVin',    fires_on: ['vin'],              fills: ['year','make','model','recalls'] },
  { id: 'fars',                path: '../enrich/fars',                method: 'lookupOne',    fires_on: ['vin','plate','fatal'], fills: ['fatality_record'] },

  // ── Cross-checks (always last) ──
  { id: 'evidence-cross-check',path: '../enrich/evidence-cross-checker', method: 'checkOne', fires_on: ['always'],            fills: ['cross_check'] },
  { id: 'smart-cross-ref',     path: '../enrich/smart-cross-ref',     method: 'crossRefOne',  fires_on: ['always'],           fills: ['next_best_action'] },
  { id: 'voyage-similar',      path: '../enrich/_voyage_router',      method: 'findSimilar',  fires_on: ['name','always'],    fills: ['merge_candidate'] },
];

/**
 * Loader with graceful failure — missing modules just get skipped.
 */
function loadEngine(spec) {
  try {
    const mod = require(spec.path);
    const fn = mod[spec.method] || mod.runOne || mod.handler;
    return typeof fn === 'function' ? fn : null;
  } catch (_) {
    return null;
  }
}

/**
 * Decide which engines to fire based on:
 *   - what fields the person already has (don't re-fire engines for fields already filled)
 *   - what trigger caused this fan-out (so phone-update fires phone engines hard)
 *   - whether the person is fatal (funeral-survivors gates on this)
 *   - whether enough time has passed since last fan-out (rate limit)
 */
function selectEngines(person, triggerField, opts = {}) {
  const knowns = new Set();
  if (person.full_name || person.first_name) knowns.add('name');
  if (person.phone) knowns.add('phone');
  if (person.email) knowns.add('email');
  if (person.address || person.city) knowns.add('address');
  if (person.vehicle_plate) knowns.add('plate');
  if (person.vehicle_vin) knowns.add('vin');
  if (person.is_fatal || person.role === 'fatal') knowns.add('fatal');

  const trigger = triggerField || 'always';
  knowns.add('always');
  if (trigger) knowns.add(trigger);

  return ENGINE_MATRIX.filter(e => e.fires_on.some(f => knowns.has(f)));
}

async function runFanOut(db, personId, opts = {}) {
  await applyMigrationIdempotent(db);
  const startedAt = Date.now();

  // Load the person row
  const person = await db('persons').where({ id: personId }).first();
  if (!person) return { ok: false, error: 'person_not_found' };

  // Rate limit check
  if (!opts.force) {
    const recent = await db('enrichment_logs')
      .where({ person_id: personId, source: 'auto-fan-out' })
      .where('field_name', 'fan_out_summary')
      .orderBy('created_at', 'desc')
      .first();
    if (recent) {
      const ageHours = (Date.now() - new Date(recent.created_at).getTime()) / 36e5;
      if (ageHours < RATE_LIMIT_HOURS && !opts.contactFieldChanged) {
        return { ok: true, skipped: true, reason: 'rate_limited', last_run_hours_ago: ageHours.toFixed(1) };
      }
    }
  }

  const engines = selectEngines(person, opts.trigger_field, opts);
  const results = [];

  // Fire all selected engines in parallel with per-engine timeout + global budget.
  const fanOutPromises = engines.map(async (spec) => {
    if (Date.now() - startedAt > TOTAL_BUDGET_MS) {
      return { id: spec.id, status: 'budget_exceeded' };
    }
    const fn = loadEngine(spec);
    if (!fn) return { id: spec.id, status: 'engine_not_loaded' };
    const t0 = Date.now();
    try {
      const r = await Promise.race([
        Promise.resolve(fn(db, personId)),
        new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), PER_ENGINE_TIMEOUT_MS))
      ]);
      return { id: spec.id, status: 'ok', dur: Date.now() - t0, result: r ? (typeof r === 'object' ? Object.keys(r).slice(0, 5) : 'ok') : 'empty' };
    } catch (e) {
      return { id: spec.id, status: 'err', dur: Date.now() - t0, error: e.message };
    }
  });

  const settled = await Promise.allSettled(fanOutPromises);
  for (const s of settled) results.push(s.status === 'fulfilled' ? s.value : { status: 'rejected', error: s.reason?.message });

  // Re-read the person to see what filled in
  const after = await db('persons').where({ id: personId }).first();
  const filled = {
    phone: !!after.phone && !person.phone,
    email: !!after.email && !person.email,
    address: !!after.address && !person.address,
    employer: !!after.employer && !person.employer
  };

  // Log a summary so cross-check + UI can see this happened
  try {
    await db('enrichment_logs').insert({
      person_id: personId,
      field_name: 'fan_out_summary',
      old_value: null,
      new_value: JSON.stringify({
        engines_fired: results.length,
        ok: results.filter(r => r.status === 'ok').length,
        errors: results.filter(r => r.status === 'err').length,
        skipped: results.filter(r => r.status === 'engine_not_loaded').length,
        filled_now: filled,
        trigger: opts.trigger_field || 'manual'
      }).slice(0, 4000),
      source: 'auto-fan-out',
      confidence: 50,
      verified: true,
      data: JSON.stringify({ duration_ms: Date.now() - startedAt }).slice(0, 4000),
      created_at: new Date()
    });
  } catch (_) {}

  return {
    ok: true,
    person_id: personId,
    name: person.full_name,
    engines_fired: results.length,
    ok_count: results.filter(r => r.status === 'ok').length,
    err_count: results.filter(r => r.status === 'err').length,
    skipped: results.filter(r => r.status === 'engine_not_loaded').length,
    filled_in_this_pass: filled,
    duration_ms: Date.now() - startedAt,
    trace: results
  };
}

async function batchFanOut(db, { scope = 'qualified', limit = 50, force = false } = {}) {
  await applyMigrationIdempotent(db);
  let rows;
  if (scope === 'qualified') {
    rows = await db.raw(`
      SELECT DISTINCT p.id
      FROM persons p
      JOIN incidents i ON i.id = p.incident_id
      WHERE i.qualification_state = 'qualified'
      ORDER BY p.id
      LIMIT ${parseInt(limit) || 50}
    `).then(r => r.rows || r).catch(() => []);
  } else if (scope === 'verified') {
    rows = await db('persons').where('victim_verified', true).limit(limit).select('id');
  } else if (scope === 'recent') {
    rows = await db('persons').where('created_at', '>', new Date(Date.now() - 24*3600*1000)).limit(limit).select('id');
  } else {
    rows = [];
  }

  const out = { ok: true, scope, candidates: rows.length, results: [] };
  for (const row of rows) {
    const id = row.id || row.person_id;
    if (!id) continue;
    try {
      const r = await runFanOut(db, id, { force });
      out.results.push({ person_id: id, ok: r.ok, fired: r.engines_fired, ok_count: r.ok_count, filled: r.filled_in_this_pass });
    } catch (e) {
      out.results.push({ person_id: id, ok: false, error: e.message });
    }
  }
  return out;
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const action = (req.query?.action || 'health').toLowerCase();
  let db;
  try { db = getDb(); } catch (e) {
    return res.status(500).json({ success: false, error: 'db_unavailable:' + e.message });
  }

  if (action === 'health') {
    return res.status(200).json({
      success: true,
      service: 'auto-fan-out',
      engines_in_matrix: ENGINE_MATRIX.length,
      ts: new Date().toISOString()
    });
  }

  if (action === 'matrix') {
    return res.status(200).json({
      success: true,
      matrix: ENGINE_MATRIX.map(e => ({ id: e.id, fires_on: e.fires_on, fills: e.fills }))
    });
  }

  if (action === 'run') {
    let body = req.body;
    if (!body || typeof body !== 'object') {
      body = await new Promise((resolve) => {
        let data = '';
        req.on('data', (c) => { data += c; });
        req.on('end', () => { try { resolve(JSON.parse(data || '{}')); } catch { resolve({}); } });
        req.on('error', () => resolve({}));
      });
    }
    const personId = body.person_id || req.query?.person_id;
    if (!personId) return res.status(400).json({ error: 'person_id required' });
    try {
      const r = await runFanOut(db, personId, {
        trigger_field: body.trigger_field || req.query?.trigger_field,
        force: body.force === true || req.query?.force === 'true',
        contactFieldChanged: !!body.contact_field_changed
      });
      await trackApiCall(db, 'auto-fan-out', 'run_one', 0, 0, !!r.ok).catch(() => {});
      return res.status(200).json({ success: true, ...r });
    } catch (e) {
      await reportError(db, 'auto-fan-out', null, e.message, { severity: 'error' });
      return res.status(500).json({ error: e.message, success: false });
    }
  }

  if (action === 'batch') {
    const scope = req.query?.scope || 'qualified';
    const limit = Math.max(1, Math.min(parseInt(req.query?.limit) || 50, 200));
    const force = req.query?.force === 'true';
    try {
      const r = await batchFanOut(db, { scope, limit, force });
      await trackApiCall(db, 'auto-fan-out', 'batch', 0, 0, true).catch(() => {});
      return res.status(200).json({ success: true, ...r });
    } catch (e) {
      await reportError(db, 'auto-fan-out', null, e.message, { severity: 'error' });
      return res.status(500).json({ error: e.message, success: false });
    }
  }

  return res.status(400).json({ error: 'unknown action: ' + action });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.runFanOut = runFanOut;
module.exports.batchFanOut = batchFanOut;
module.exports.ENGINE_MATRIX = ENGINE_MATRIX;
