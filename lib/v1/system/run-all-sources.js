/**
 * RUN-ALL-SOURCES — Phase 40 Module 3
 *
 * One-click rep action: fire every enrichment source against a single
 * (verified or candidate) victim in sequence, collect per-source results,
 * return final merged contact + qualified status.
 *
 * POST /api/v1/system/run-all-sources?secret=ingest-now&person_id=<uuid>
 * POST /api/v1/system/run-all-sources?secret=ingest-now&victim_name=Heather%20Avery&city=Houston&state=TX
 *
 * Sequential pipeline (60s budget):
 *   1. victim-contact-finder (12 sources composite)
 *   2. apollo-cross-pollinate
 *   3. pdl-identify             (top victim only — burns 1 of 5 free credits)
 *   4. funeral-home-survivors   (if fatal)
 *   5. cross-checker (evidence-cross-checker)
 *   6. ensemble-qualifier
 *
 * Audit row written to enrichment_logs with action='run-all-sources'.
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { trackApiCall } = require('./cost');
const { v4: uuidv4 } = require('uuid');

const finder = require('../enrich/victim-contact-finder');
const apollo = require('../enrich/apollo-cross-pollinate');
const pdlId  = require('../enrich/pdl-identify');
const survivors = require('../enrich/funeral-home-survivors');
const xchecker = require('../enrich/evidence-cross-checker');
const ensemble = require('../enrich/ensemble-qualifier');

const SECRET = 'ingest-now';
const TOTAL_BUDGET_MS = 58000;

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

function makeFakeRes() {
  return {
    _statusCode: 200, _body: null, _headers: {},
    setHeader(k, v) { this._headers[k] = v; return this; },
    status(c) { this._statusCode = c; return this; },
    json(o) { this._body = o; return this; },
    end() { return this; }
  };
}
function callable(mod) {
  if (typeof mod === 'function') return mod;
  if (mod && typeof mod.handler === 'function') return mod.handler;
  if (mod && typeof mod.default === 'function') return mod.default;
  return null;
}

async function runStep(label, mod, query, timeoutMs = 18000) {
  const start = Date.now();
  try {
    const fn = callable(mod);
    if (!fn) return { step: label, ok: false, error: 'no_callable_export', latency_ms: 0 };
    const fakeReq = { method: 'GET', query: { secret: 'ingest-now', ...query }, headers: {}, body: null, url: '' };
    const fakeRes = makeFakeRes();
    await Promise.race([
      fn(fakeReq, fakeRes),
      new Promise((_, rej) => setTimeout(() => rej(new Error('step_timeout_' + timeoutMs)), timeoutMs))
    ]);
    const body = fakeRes._body;
    const ok = fakeRes._statusCode === 200 && body?.success !== false;
    return {
      step: label,
      ok,
      status: fakeRes._statusCode,
      latency_ms: Date.now() - start,
      summary: extractSummary(body),
      error: ok ? null : (body?.error || 'failed')
    };
  } catch (e) {
    return { step: label, ok: false, error: e.message, latency_ms: Date.now() - start };
  }
}

function extractSummary(b) {
  if (!b) return null;
  const out = {};
  for (const k of [
    'final_contact','complete','filled','sources_used','source_count',
    'enriched','matched','phone','email','address','full_name','samples',
    'family_inserted','family_found','obituary_url','identity_confidence',
    'qualification_state','lead_score','promoted','evidence_sum',
    'inserted','updated'
  ]) if (b[k] !== undefined) out[k] = b[k];
  return out;
}

async function ensureLogTable(db) {
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS enrichment_logs (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        person_id UUID,
        incident_id UUID,
        field_name VARCHAR(80),
        old_value TEXT,
        new_value TEXT,
        confidence INTEGER,
        verified BOOLEAN DEFAULT FALSE,
        action VARCHAR(80),
        meta JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
      ALTER TABLE enrichment_logs ADD COLUMN IF NOT EXISTS action VARCHAR(80);
      ALTER TABLE enrichment_logs ADD COLUMN IF NOT EXISTS meta JSONB;
    `);
  } catch (_) {}
}

async function findOrCreatePerson(db, params) {
  const { person_id, victim_name, city, state } = params;
  if (person_id) {
    const p = await db('persons').where('id', person_id).first();
    if (!p) return { error: 'person_not_found' };
    return { person: p, created: false };
  }
  if (!victim_name) return { error: 'person_id or victim_name required' };

  // Try existing match by name+state
  let p = await db('persons')
    .whereRaw('LOWER(full_name) = LOWER(?)', [victim_name])
    .modify(q => state && q.where('state', String(state).toUpperCase()))
    .first();
  if (p) return { person: p, created: false };

  // Create a transient person
  const parts = String(victim_name).trim().split(/\s+/);
  const [row] = await db('persons').insert({
    full_name: victim_name,
    first_name: parts[0] || null,
    last_name: parts.length > 1 ? parts[parts.length - 1] : null,
    city: city || null,
    state: state ? String(state).toUpperCase() : null,
    derived_from: 'run-all-sources',
    identity_confidence: 25,
    created_at: new Date(),
    updated_at: new Date()
  }).returning(['*']);
  return { person: row, created: true };
}

async function runAll(db, person, opts = {}) {
  const t0 = Date.now();
  const personId = person.id;
  const isFatal = !!person.incident_id && await db('incidents')
    .where('id', person.incident_id)
    .first('severity', 'headline')
    .then(i => i && (i.severity === 'fatal' || /killed|fatal|dies|dead|deceased/i.test(i.headline || '')))
    .catch(() => false);

  const results = [];
  const remaining = () => Math.max(2000, TOTAL_BUDGET_MS - (Date.now() - t0));

  // 1. victim-contact-finder
  results.push(await runStep('victim-contact-finder', finder,
    { action: 'resolve', person_id: personId },
    Math.min(20000, remaining())));

  // 2. apollo-cross-pollinate (single person)
  if (Date.now() - t0 < TOTAL_BUDGET_MS - 5000) {
    results.push(await runStep('apollo-cross-pollinate', apollo,
      { action: 'one', person_id: personId },
      Math.min(10000, remaining())));
  }

  // 3. pdl-identify (top victim only — burns 1 of 5 free credits/day)
  if (opts.use_pdl !== false && Date.now() - t0 < TOTAL_BUDGET_MS - 5000) {
    results.push(await runStep('pdl-identify', pdlId,
      { action: 'one', person_id: personId },
      Math.min(8000, remaining())));
  }

  // 4. funeral-home-survivors (fatal only)
  if (isFatal && Date.now() - t0 < TOTAL_BUDGET_MS - 5000) {
    results.push(await runStep('funeral-home-survivors', survivors,
      { action: 'resolve', person_id: personId },
      Math.min(12000, remaining())));
  }

  // 5. evidence-cross-checker
  if (Date.now() - t0 < TOTAL_BUDGET_MS - 3000) {
    results.push(await runStep('evidence-cross-checker', xchecker,
      { action: 'one', person_id: personId },
      Math.min(8000, remaining())));
  }

  // 6. ensemble-qualifier (recompute lead_score for the incident)
  if (person.incident_id && Date.now() - t0 < TOTAL_BUDGET_MS - 2000) {
    results.push(await runStep('ensemble-qualifier', ensemble,
      { action: 'one', incident_id: person.incident_id },
      Math.min(6000, remaining())));
  }

  // Re-load merged person + qualification
  const merged = await db('persons').where('id', personId).first();
  let qualified = null;
  let leadScore = null;
  if (merged?.incident_id) {
    const inc = await db('incidents').where('id', merged.incident_id).first('qualification_state','lead_score');
    if (inc) { qualified = inc.qualification_state; leadScore = inc.lead_score; }
  }

  // Audit log
  try {
    await ensureLogTable(db);
    await db('enrichment_logs').insert({
      person_id: personId,
      incident_id: merged?.incident_id || null,
      field_name: 'run-all-sources',
      action: 'run-all-sources',
      new_value: `${results.filter(r => r.ok).length}/${results.length} sources ok`,
      confidence: merged?.identity_confidence || null,
      verified: !!merged?.victim_verified,
      meta: JSON.stringify({ steps: results.map(r => ({ step: r.step, ok: r.ok, latency_ms: r.latency_ms })) }),
      created_at: new Date()
    });
  } catch (_) {}

  try { await trackApiCall(db, 'system-run-all-sources', 'composite', 0, 0, true); } catch (_) {}

  return {
    person_id: personId,
    person: {
      full_name: merged?.full_name,
      phone: merged?.phone,
      email: merged?.email,
      address: merged?.address,
      city: merged?.city,
      state: merged?.state,
      identity_confidence: merged?.identity_confidence,
      victim_verified: !!merged?.victim_verified
    },
    qualification_state: qualified,
    lead_score: leadScore,
    is_fatal: isFatal,
    sources_run: results.length,
    sources_ok: results.filter(r => r.ok).length,
    steps: results,
    total_latency_ms: Date.now() - t0
  };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const db = getDb();
  try {
    const personId = req.query?.person_id;
    const victim_name = req.query?.victim_name;
    const city = req.query?.city;
    const state = req.query?.state;

    const lookup = await findOrCreatePerson(db, { person_id: personId, victim_name, city, state });
    if (lookup.error) return res.status(400).json({ error: lookup.error });

    const out = await runAll(db, lookup.person, { use_pdl: req.query?.use_pdl !== '0' });
    return res.json({
      success: true,
      created_person: lookup.created,
      ...out,
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    await reportError(db, 'system-run-all-sources', null, e.message);
    res.status(500).json({ success: false, error: e.message });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.runAll = runAll;
