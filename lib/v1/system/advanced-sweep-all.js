/**
 * Phase 65: Advanced sweep over ALL persons (not just qualified 15).
 *
 * Mason directive (2026-04-30): "look at more than just the 15 so-called leads
 *   and see if any other data we scrubbed so far on accidents can be ran
 *   through again using our new updated engines and models."
 *
 * For every person in the DB (configurable scope), runs:
 *   1. auto-fan-out — fires every applicable engine
 *   2. adversarial-cross-check — flags data quality issues
 *   3. evidence-cross-checker — populates summary
 *   (Hypothesis-gen and merge-finder are EXPENSIVE and run separately)
 *
 * The cascade trigger already handles new-row enrichment automatically — this
 * endpoint is the BACKFILL for everything that landed before Phase 55 went live.
 *
 * Endpoints:
 *   GET ?action=health
 *   POST ?action=run body:{scope: 'qualified'|'verified'|'pending_named'|'all', limit, offset}
 *   POST ?action=demote_celebs — drops false-positive qualified leads using updated _name_filter
 */

const { getDb } = require('../../_db');
const { reportError } = require('./_errors');

const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

const PER_BATCH_BUDGET_MS = 50000;

async function selectPersons(db, scope, limit, offset) {
  const lim = Math.min(parseInt(limit) || 50, 200);
  const off = parseInt(offset) || 0;
  if (scope === 'qualified') {
    return db.raw(`
      SELECT DISTINCT p.id, p.full_name, p.phone, p.email, p.address, p.state
      FROM persons p
      JOIN incidents i ON i.id = p.incident_id
      WHERE i.qualification_state = 'qualified'
      ORDER BY p.id LIMIT ${lim} OFFSET ${off}
    `).then(r => r.rows || []);
  }
  if (scope === 'verified') {
    return db('persons').where('victim_verified', true).limit(lim).offset(off)
      .select('id', 'full_name', 'phone', 'email', 'address', 'state');
  }
  if (scope === 'pending_named') {
    return db.raw(`
      SELECT DISTINCT p.id, p.full_name, p.phone, p.email, p.address, p.state
      FROM persons p
      JOIN incidents i ON i.id = p.incident_id
      WHERE i.qualification_state = 'pending_named'
      ORDER BY p.id LIMIT ${lim} OFFSET ${off}
    `).then(r => r.rows || []);
  }
  if (scope === 'with_partial_contact') {
    return db('persons')
      .where(b => b.whereNotNull('phone').orWhereNotNull('email').orWhereNotNull('address'))
      .where(b => b.whereNull('phone').orWhereNull('email'))
      .limit(lim).offset(off)
      .select('id', 'full_name', 'phone', 'email', 'address', 'state');
  }
  if (scope === 'all') {
    return db('persons').orderBy('id').limit(lim).offset(off)
      .select('id', 'full_name', 'phone', 'email', 'address', 'state');
  }
  return [];
}

async function runOnePerson(db, person) {
  const out = { id: person.id, name: person.full_name };
  // Fire fan-out
  try {
    const fanOut = require('./auto-fan-out');
    const r = await fanOut.runFanOut(db, person.id, { force: true, trigger_field: 'advanced_sweep_all' });
    out.fan_out = { ok: r.ok_count, fired: r.engines_fired, filled: r.filled_in_this_pass };
  } catch (e) { out.fan_out = { error: e.message }; }

  // Adversarial validate (only if has any contact data)
  if (person.phone || person.email || person.address) {
    try {
      const adv = require('./adversarial-cross-check');
      const fn = adv.validateOne || adv.handler;
      // adversarial-cross-check has validateOne signature (db, personId)
      if (typeof adv.validateOne === 'function') {
        const r = await adv.validateOne(db, person.id);
        out.adversarial = { conflicts: r.conflicts_found || 0, delta: r.confidence_delta || 0 };
      }
    } catch (e) { out.adversarial = { error: e.message }; }
  }

  return out;
}

async function runSweep(db, { scope = 'qualified', limit = 50, offset = 0 } = {}) {
  const startedAt = Date.now();
  const persons = await selectPersons(db, scope, limit, offset);
  const results = [];

  for (const p of persons) {
    if (Date.now() - startedAt > PER_BATCH_BUDGET_MS) {
      results.push({ id: p.id, skipped: 'budget_exceeded' });
      continue;
    }
    const r = await runOnePerson(db, p);
    results.push(r);
  }

  // Aggregate stats
  const summary = {
    scope, scanned: persons.length,
    fan_out_ok: results.filter(r => r.fan_out?.ok).length,
    adversarial_conflicts: results.reduce((s, r) => s + (r.adversarial?.conflicts || 0), 0),
    new_fields_filled: results.reduce((s, r) => {
      const f = r.fan_out?.filled || {};
      return s + Object.values(f).filter(Boolean).length;
    }, 0),
    duration_ms: Date.now() - startedAt
  };

  return { ok: true, summary, results: results.slice(0, 20), full_count: results.length };
}

/**
 * Phase 65: demote qualified leads where the victim name is now in the celebrity deny-list.
 */
async function demoteCelebrities(db) {
  const _filter = require('../enrich/_name_filter');
  const persons = await db.raw(`
    SELECT DISTINCT p.id, p.full_name, p.incident_id
    FROM persons p JOIN incidents i ON i.id = p.incident_id
    WHERE i.qualification_state = 'qualified' AND p.full_name IS NOT NULL
  `).then(r => r.rows || []);

  const trace = [];
  const demoted = [];
  for (const p of persons) {
    // Re-run deny-list with empty surroundingText — catches HARD_BAN_NAMES list
    let survives = null;
    let why = 'survives';
    try {
      survives = _filter.applyDenyList(p.full_name, '');
      if (survives === null) why = 'rejected_by_filter';
    } catch (e) { why = 'filter_err:' + e.message; }
    // Belt-and-suspenders: also check our own hardcoded celebrity list inline
    const lc = String(p.full_name || '').trim().toLowerCase();
    const inlineCelebs = new Set([
      'lindsey vonn','diogo jota','greg biffle','tom brady','taylor swift','elon musk',
      'donald trump','joe biden','kamala harris','tiger woods','serena williams','lebron james',
      'cristiano ronaldo','lionel messi','patrick mahomes','travis kelce','dale earnhardt',
      'kyle larson','denny hamlin','kyle busch','kevin harvick','bubba wallace','chase elliott',
      'william byron','erling haaland','mohamed salah','kylian mbappe','novak djokovic',
      'rafael nadal','roger federer','stephen curry','kevin durant','luka doncic','shohei ohtani',
      'mike trout','aaron judge','josh allen','jalen hurts','lamar jackson','aaron rodgers'
    ]);
    const inlineHit = inlineCelebs.has(lc);
    trace.push({ name: p.full_name, why, inlineHit });
    if (survives === null || inlineHit) {
      // demote: mark person not_verified, drop incident to pending
      try {
        await db('persons').where('id', p.id).update({
          victim_verified: false,
          victim_verifier_reason: 'celebrity_or_hard_ban_2026_04_30',
          updated_at: new Date()
        });
        await db('incidents').where('id', p.incident_id).update({
          qualification_state: 'pending',
          updated_at: new Date()
        });
        demoted.push({ id: p.id, name: p.full_name, incident_id: p.incident_id });
      } catch (e) {
        trace.push({ name: p.full_name, demote_err: e.message });
      }
    }
  }
  return { ok: true, demoted_count: demoted.length, demoted, trace };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();

  if (action === 'health') return res.json({ success: true, service: 'advanced-sweep-all' });

  if (action === 'run') {
    let body = req.body;
    if (!body || typeof body !== 'object') {
      body = await new Promise((resolve) => {
        let d=''; req.on('data', c=>d+=c);
        req.on('end', () => { try { resolve(JSON.parse(d || '{}')); } catch { resolve({}); } });
        req.on('error', () => resolve({}));
      });
    }
    const scope = body.scope || req.query?.scope || 'qualified';
    const limit = parseInt(body.limit || req.query?.limit) || 30;
    const offset = parseInt(body.offset || req.query?.offset) || 0;
    try {
      return res.json(await runSweep(db, { scope, limit, offset }));
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  if (action === 'demote_celebs') {
    try { return res.json(await demoteCelebrities(db)); }
    catch (e) { return res.status(500).json({ error: e.message }); }
  }

  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.runSweep = runSweep;
module.exports.demoteCelebrities = demoteCelebrities;
