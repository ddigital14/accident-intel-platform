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
const PER_ENGINE_TIMEOUT_MS = 18000;
const TOTAL_BUDGET_MS = 90000;

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

// The engine matrix — drives WHICH engines fire based on what's known + what's missing.
// Every engine resolves to lib/v1/<dir>/<file>.js with .runOne(personId) or a runner
// function. Loaders are lazy so missing modules don't break the orchestrator.
// Phase 56: Per-engine adapter functions normalize the call to (db, person).
// Each adapter returns a result object or null. Errors are caught by the orchestrator.
//
// fires_on:    array of triggers — name|phone|email|address|plate|vin|fatal|always
// fills:       fields this engine could populate (for logging only)
// adapter:     async (db, person) => any — does the actual call

const ENGINE_MATRIX = [
  // ── Identity / name resolution ──
  { id: 'pdl-identify', fires_on: ['name','always'], fills: ['phone','email','address','employer','dob'],
    adapter: async (db, p) => { const m = require('../enrich/pdl-identify'); return m.run ? m.run(db, 1) : null; } },

  { id: 'apollo-cross-pollinate', fires_on: ['name','always'], fills: ['phone','email','employer','linkedin'],
    adapter: async (db, p) => {
      // apollo-cross-pollinate exports only the handler; we re-create the per-person path inline.
      const apolloMod = require('../enrich/apollo-cross-pollinate');
      // It's a batch handler — invoke via the batch route by simulating req/res
      const fakeReq = { method: 'GET', query: { secret: 'ingest-now', action: 'one', person_id: p.id }, headers: {} };
      const fakeRes = { _data: null, status() { return this; }, json(d) { this._data = d; return this; }, setHeader() {} };
      try { await apolloMod(fakeReq, fakeRes); return fakeRes._data; } catch { return null; }
    } },

  { id: 'apollo-unlock', fires_on: ['name','phone','email','always'], fills: ['email','phone'],
    adapter: async (db, p) => { const m = require('../enrich/apollo-unlock'); return m.unlockPerson ? m.unlockPerson(db, p) : null; } },

  { id: 'victim-resolver', fires_on: ['name','always'], fills: ['phone','email','address'],
    adapter: async (db, p) => { const m = require('../enrich/victim-resolver'); return m.resolveOne ? m.resolveOne(db, p.id) : null; } },

  { id: 'victim-contact-finder', fires_on: ['name','always'], fills: ['phone','email','address'],
    adapter: async (db, p) => { const m = require('../enrich/victim-contact-finder'); return m.resolveOne ? m.resolveOne(db, p.id) : null; } },

  { id: 'osint-miner', fires_on: ['name','always'], fills: ['phone','email','social','employer'],
    adapter: async (db, p) => { const m = require('../enrich/homegrown-osint-miner'); return m.mineOne ? m.mineOne(db, p.id) : null; } },

  { id: 'deep-phone-research', fires_on: ['name'], fills: ['phone'],
    adapter: async (db, p) => { const m = require('../enrich/deep-phone-research'); return m.researchOne ? m.researchOne(db, { person_id: p.id, victim_name: p.full_name, city: p.city, state: p.state }) : null; } },

  { id: 'funeral-survivors', fires_on: ['name','fatal'], fills: ['family'],
    adapter: async (db, p) => {
      const m = require('../enrich/funeral-home-survivors');
      const inc = p.incident_id ? await db('incidents').where('id', p.incident_id).first() : null;
      const isFatal = inc?.fatal_count > 0 || inc?.severity === 'fatal' || /fatal/i.test(inc?.description || '');
      if (!isFatal) return { skipped: 'not_fatal' };
      return m.resolveOne ? m.resolveOne(db, p) : null;
    } },

  { id: 'state-courts', fires_on: ['name'], fills: ['attorney_firm','case_no'],
    adapter: async (db, p) => {
      const m = require('../enrich/state-courts');
      if (!p.full_name || !p.state) return { skipped: 'missing_name_or_state' };
      return m.search ? m.search(p.full_name, p.state, db) : null;
    } },

  { id: 'dev-profiles', fires_on: ['name','email'], fills: ['github','social'],
    adapter: async (db, p) => {
      const m = require('../enrich/dev-profiles');
      if (!p.full_name) return { skipped: 'no_name' };
      return m.find ? m.find(p.full_name, db) : null;
    } },

  // ── Phone-driven ──
  { id: 'trestle-phone', fires_on: ['phone'], fills: ['name','address','email','relatives'],
    adapter: async (db, p) => {
      const m = require('../enrich/trestle');
      if (!p.phone) return { skipped: 'no_phone' };
      return m.reversePhone ? m.reversePhone(p.phone, db) : null;
    } },

  { id: 'trestle-address', fires_on: ['address'], fills: ['residents','phone'],
    adapter: async (db, p) => {
      const m = require('../enrich/trestle');
      if (!p.address) return { skipped: 'no_address' };
      return m.reverseAddress ? m.reverseAddress(p.address, db) : { skipped: 'reverseAddress_unavailable' };
    } },

  // ── Address-driven ──
  { id: 'co-residence', fires_on: ['address'], fills: ['relatives','co_residents'],
    adapter: async (db, p) => {
      const m = require('../enrich/co-residence');
      // findHouseholds is batch-mode; safe no-op for now until per-person variant exists
      return { note: 'batch_only_skipped_per_person' };
    } },

  { id: 'census-income', fires_on: ['address'], fills: ['median_income'],
    adapter: async (db, p) => {
      const m = require('../enrich/census-income');
      if (!(p.lat && p.lon)) return { skipped: 'no_geo' };
      return m.lookup ? m.lookup(p.lat, p.lon, db) : null;
    } },

  { id: 'usps-validate', fires_on: ['address'], fills: ['zip4','address_canonical'],
    adapter: async (db, p) => {
      const m = require('../enrich/usps-validate');
      if (!p.address) return { skipped: 'no_address' };
      const addr = { street: p.address, city: p.city, state: p.state, zip: p.zip };
      return m.uspsValidate ? m.uspsValidate(addr, db) : null;
    } },

  // ── Cross-checks (always) ──
  { id: 'evidence-cross-check', fires_on: ['always'], fills: ['cross_check'],
    adapter: async (db, p) => { const m = require('../enrich/evidence-cross-checker'); return m.checkOne ? m.checkOne(db, p.id) : null; } },

  // ── Phase 60: adversarial cross-check (independent third-party validation) ──
  { id: 'adversarial-cross-check', fires_on: ['always'], fills: ['adversarial_validation'],
    adapter: async (db, p) => { const m = require('./adversarial-cross-check'); return m.validateOne ? m.validateOne(db, p.id) : null; } },

  { id: 'smart-cross-ref', fires_on: ['always'], fills: ['next_best_action'],
    adapter: async (db, p) => { const m = require('../enrich/smart-cross-ref'); return m.runForPerson ? m.runForPerson(db, p.id) : null; } },
];

// Phase 56: matrix entries carry their own adapter — no separate loader needed.
function loadAdapter(spec) { return spec.adapter || null; }

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
    const adapter = loadAdapter(spec);
    if (!adapter) return { id: spec.id, status: 'no_adapter' };
    const t0 = Date.now();
    try {
      const r = await Promise.race([
        Promise.resolve(adapter(db, person)),
        new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), PER_ENGINE_TIMEOUT_MS))
      ]);
      const skipped = (r && typeof r === 'object' && r.skipped) ? r.skipped : null;
      const dur = Date.now() - t0;
      // Phase 58: report outcome to strategist for self-learning
      try {
        const strat = require('./strategist');
        const knownStr = ((person.full_name?'name':'') + (person.phone?'+phone':'') + (person.email?'+email':'') + (person.address?'+address':'') + (person.state?'+state':'')).replace(/^\+/,'') || 'empty';
        await strat.recordOutcome(db, spec.id, knownStr, !skipped, dur);
      } catch (_) {}
      return { id: spec.id, status: skipped ? 'skipped' : 'ok', skipped, dur, result: r ? (typeof r === 'object' ? Object.keys(r).slice(0, 6) : 'ok') : 'empty' };
    } catch (e) {
      const dur = Date.now() - t0;
      try {
        const strat = require('./strategist');
        const knownStr = ((person.full_name?'name':'') + (person.phone?'+phone':'') + (person.email?'+email':'') + (person.address?'+address':'') + (person.state?'+state':'')).replace(/^\+/,'') || 'empty';
        await strat.recordOutcome(db, spec.id, knownStr, false, dur);
      } catch (_) {}
      return { id: spec.id, status: 'err', dur, error: e.message };
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
