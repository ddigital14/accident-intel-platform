/**
 * CONSTANT CROSS-LOOP — Phase 23 #5
 *
 * Iterates over persons updated in last 30 min. Detects which fields are NEW
 * (not present 30 min ago) and triggers the appropriate engine for each.
 *
 * Aggressive every-tick fan-out — runs every 5 min folded into the existing
 * qualify/notify cron slot.
 *
 * GET /api/v1/system/constant-cross-loop?secret=ingest-now&minutes=30
 *
 * Trigger map:
 *   new full_name      -> court-reverse-link + obit-backfill + pdl-by-name
 *   new phone          -> twilio-lookup
 *   new address        -> property-records + voter-rolls
 *   new employer       -> hunter-find (deep enrich)
 *   new vin/plate      -> vehicle-history
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { enqueueCascade } = require('./_cascade');

let _ensured = false;
async function ensureTable(db) {
  if (_ensured) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS constant_loop_runs (
        id BIGSERIAL PRIMARY KEY,
        run_at TIMESTAMPTZ DEFAULT NOW(),
        persons_scanned INTEGER,
        triggers_fired INTEGER,
        engines JSONB DEFAULT '{}'::jsonb,
        latency_ms INTEGER
      );
      CREATE INDEX IF NOT EXISTS idx_clr_run ON constant_loop_runs(run_at DESC);

      CREATE TABLE IF NOT EXISTS person_signal_history (
        id BIGSERIAL PRIMARY KEY,
        person_id UUID NOT NULL,
        signal_field VARCHAR(60),
        signal_value TEXT,
        first_seen_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_psh_person ON person_signal_history(person_id);
      CREATE INDEX IF NOT EXISTS idx_psh_field ON person_signal_history(signal_field);
    `);
    _ensured = true;
  } catch (_) {}
}

const SIGNAL_FIELDS = [
  'full_name', 'phone', 'email', 'address', 'employer',
  'linkedin_url', 'facebook_url', 'license_plate', 'vin', 'vehicle_make'
];

const SIGNAL_FANOUT = {
  full_name: ['court-reverse-link', 'obit-backfill', 'pdl-by-name'],
  phone: ['twilio-lookup'],
  address: ['property-records', 'voter-rolls'],
  employer: ['hunter-find'],
  email: ['email-verify'],
  linkedin_url: ['social-search'],
  vin: ['vehicle-history'],
  license_plate: ['vehicle-history'],
};

async function detectNewSignals(db, person) {
  const newSignals = [];
  for (const f of SIGNAL_FIELDS) {
    const v = person[f];
    if (!v) continue;
    const seen = await db('person_signal_history')
      .where({ person_id: person.id, signal_field: f, signal_value: String(v).slice(0, 200) })
      .first()
      .catch(() => null);
    if (!seen) {
      newSignals.push({ field: f, value: v });
      await db('person_signal_history').insert({
        person_id: person.id,
        signal_field: f,
        signal_value: String(v).slice(0, 200),
        first_seen_at: new Date(),
      }).catch(() => {});
    }
  }
  return newSignals;
}

async function fireEngine(db, engineKey, person) {
  try {
    switch (engineKey) {
      case 'twilio-lookup': {
        if (!person.phone) return false;
        const tw = require('../enrich/twilio');
        const lu = await tw.lookupPhone(db, person.phone).catch(() => null);
        if (lu?.ok && tw.applyLookupToPerson) {
          await tw.applyLookupToPerson(db, person.id, lu).catch(()=>{});
          return true;
        }
        return false;
      }
      case 'pdl-by-name': {
        const pdl = require('../enrich/pdl-by-name');
        if (typeof pdl.processPerson === 'function') {
          const r = await pdl.processPerson(db, person).catch(() => null);
          return !!(r && (r.ok || r.fields_filled));
        }
        return false;
      }
      case 'voter-rolls': {
        const vr = require('../enrich/voter-rolls');
        if (typeof vr.lookupVoter === 'function' && person.full_name && person.state) {
          const parts = person.full_name.split(/\s+/);
          const r = await vr.lookupVoter(db, parts[0], parts[parts.length-1], person.state).catch(() => null);
          return !!r;
        }
        return false;
      }
      case 'property-records': {
        const pr = require('../enrich/property-records');
        if (typeof pr.lookupOwner === 'function' && person.address && person.state) {
          const r = await pr.lookupOwner({ address: person.address, city: person.city, state: person.state }, db).catch(() => null);
          return !!r;
        }
        return false;
      }
      case 'hunter-find': {
        const dp = require('../enrich/deep');
        if (typeof dp.deepEnrichPerson === 'function') {
          const r = await dp.deepEnrichPerson(db, person).catch(() => null);
          return !!(r && (r.ok || r.fields_filled));
        }
        return false;
      }
      case 'social-search': {
        const ss = require('../enrich/social-search');
        if (typeof ss.searchSocial === 'function' && person.full_name) {
          const cfg = await (ss.getCseConfig ? ss.getCseConfig(db).catch(()=>null) : null);
          const r = await ss.searchSocial(person.full_name, person.city, person.state, cfg).catch(() => null);
          return !!(r && r.length > 0);
        }
        return false;
      }
      case 'court-reverse-link':
      case 'obit-backfill':
      case 'vehicle-history':
      case 'email-verify':
        // Defer to dedicated cron job — emit cascade only
        return false;
      default:
        return false;
    }
  } catch (e) {
    await reportError(db, 'constant-cross-loop', person.id, `${engineKey}: ${e.message}`).catch(()=>{});
    return false;
  }
}

async function processLoop(db, opts = {}) {
  await ensureTable(db);
  const minutes = parseInt(opts.minutes) || 30;
  const startT = Date.now();
  const result = {
    persons_scanned: 0,
    new_signals: 0,
    engines_fired: 0,
    engines_succeeded: 0,
    by_engine: {},
    by_signal: {},
  };

  const persons = await db.raw(
    `SELECT * FROM persons WHERE updated_at > NOW() - (INTERVAL '1 minute' * ?) ORDER BY updated_at DESC LIMIT 100`,
    [minutes]
  ).then(r => r.rows || []).catch(() => []);

  result.persons_scanned = persons.length;
  for (const p of persons) {
    if (Date.now() - startT > 45000) break;
    const newSignals = await detectNewSignals(db, p);
    result.new_signals += newSignals.length;
    for (const sig of newSignals) {
      result.by_signal[sig.field] = (result.by_signal[sig.field] || 0) + 1;
      const engines = SIGNAL_FANOUT[sig.field] || [];
      for (const eng of engines) {
        if (Date.now() - startT > 45000) break;
        result.engines_fired++;
        const ok = await fireEngine(db, eng, p);
        if (ok) result.engines_succeeded++;
        result.by_engine[eng] = result.by_engine[eng] || { fired: 0, success: 0 };
        result.by_engine[eng].fired++;
        if (ok) result.by_engine[eng].success++;
        await enqueueCascade(db, {
          person_id: p.id,
          incident_id: p.incident_id,
          trigger_source: 'constant_cross_loop',
          trigger_field: sig.field,
          trigger_value: String(sig.value).slice(0, 200),
          priority: 6,
        }).catch(()=>{});
      }
    }
  }

  await db('constant_loop_runs').insert({
    persons_scanned: result.persons_scanned,
    triggers_fired: result.engines_fired,
    engines: JSON.stringify(result.by_engine),
    latency_ms: Date.now() - startT,
  }).catch(()=>{});

  return result;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  try {
    const minutes = parseInt(req.query.minutes) || 30;
    const out = await processLoop(db, { minutes });
    res.json({
      success: true,
      message: `Constant loop: ${out.engines_succeeded}/${out.engines_fired} fired across ${out.persons_scanned} persons (${out.new_signals} new signals)`,
      ...out,
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    await reportError(db, 'constant-cross-loop', null, err.message).catch(()=>{});
    res.status(500).json({ error: err.message });
  }
};

module.exports.processLoop = processLoop;
