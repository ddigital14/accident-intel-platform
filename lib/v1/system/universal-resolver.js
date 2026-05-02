/**
 * Phase 74: Universal Resolver — One input field → full identity card.
 *
 * Mason's directive (2026-04-30):
 *   "putting a phone number in a system should spit out a name and address if
 *    data mined correctly. same for address to email to phone."
 *
 * Takes ANY single field (phone / email / address / name + state) and parallel-
 * fires every engine that can produce missing fields. Synthesizes a single
 * consolidated identity card with provenance + conflict flags.
 *
 * No matching person in our DB required — this resolves any input from scratch.
 *
 * HTTP:
 *   GET  /api/v1/system/universal-resolver?secret=ingest-now&action=health
 *   POST /api/v1/system/universal-resolver?secret=ingest-now&action=resolve
 *        body: {phone?, email?, address?, name?, state?, city?}
 */

const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

const PER_ENGINE_TIMEOUT_MS = 12000;
const TOTAL_BUDGET_MS = 28000;  // Vercel 30s function limit, 2s headroom

async function readBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  return new Promise(r => {
    let d=''; req.on('data', c=>d+=c);
    req.on('end', () => { try { r(JSON.parse(d || '{}')); } catch { r({}); } });
    req.on('error', () => r({}));
  });
}

function withTimeout(promise, ms, label) {
  return Promise.race([
    Promise.resolve(promise),
    new Promise((_, rej) => setTimeout(() => rej(new Error(`timeout:${label}`)), ms))
  ]);
}

/**
 * Run every engine that consumes the given input fields. Each returns
 *   { source, ok, ...fields_produced }
 * The synthesizer downstream picks the highest-confidence value for each field.
 */
async function fanResolve(db, input) {
  const startedAt = Date.now();
  const phone = (input.phone || '').toString().replace(/\D+/g, '').slice(-10);
  const email = (input.email || '').toString().toLowerCase().trim();
  const address = (input.address || '').toString().trim();
  const name = (input.name || '').toString().trim();
  const state = (input.state || '').toString().toUpperCase();
  const city = (input.city || '').toString().trim();

  const tasks = [];

  // ── PHONE-DRIVEN ──
  if (phone) {
    const e164 = '+1' + phone;
    tasks.push({
      source: 'numverify',
      run: async () => {
        try {
          const m = require('../enrich/numverify');
          if (m.lookup) return await m.lookup(e164, db);
        } catch (_) {}
        return null;
      }
    });
    tasks.push({
      source: 'opencnam',
      run: async () => {
        try {
          const m = require('../enrich/opencnam');
          return await m.lookupPhone(e164);
        } catch (_) { return null; }
      }
    });
    tasks.push({
      source: 'trestle-phone',
      run: async () => {
        try {
          const m = require('../enrich/trestle');
          if (m.reversePhone) return await m.reversePhone(e164, db);
        } catch (_) {}
        return null;
      }
    });
    tasks.push({
      source: 'twilio-lookup',
      run: async () => {
        try {
          const m = require('../enrich/twilio');
          if (m.lookupPhone) return await m.lookupPhone(e164, db);
        } catch (_) {}
        return null;
      }
    });
  }

  // ── EMAIL-DRIVEN ──
  if (email) {
    tasks.push({
      source: 'hunter-verify',
      run: async () => {
        try {
          const m = require('../enrich/hunter');
          if (m.verify) return await m.verify(email, db);
        } catch (_) {}
        return null;
      }
    });
  }

  // ── ADDRESS-DRIVEN ──
  if (address) {
    tasks.push({
      source: 'usps-validate',
      run: async () => {
        try {
          const m = require('../enrich/usps-validate');
          if (m.uspsValidate) {
            // Parse freeform if needed
            const parts = address.split(',').map(s => s.trim());
            const street = parts[0] || address;
            const tail = parts[parts.length - 1] || '';
            const m1 = tail.match(/^([A-Z]{2})\s*(\d{5})?/i);
            const stateP = m1?.[1] || state;
            const zipP = m1?.[2] || '';
            const cityP = parts.length >= 3 ? parts[parts.length - 2] : (city || '');
            return await m.uspsValidate({ street, city: cityP, state: stateP, zip: zipP }, db);
          }
        } catch (_) {}
        return null;
      }
    });
    if (state === 'AZ' || (!state && /\baz\b|arizona|phoenix|tucson|mesa/i.test(address))) {
      tasks.push({
        source: 'maricopa-property',
        run: async () => {
          try {
            const m = require('../enrich/maricopa-assessor');
            if (m.lookupByAddress) return await m.lookupByAddress(address, db);
          } catch (_) {}
          return null;
        }
      });
    }
  }

  // ── NAME-DRIVEN ──
  if (name) {
    tasks.push({
      source: 'pdl-by-name',
      run: async () => {
        try {
          const m = require('../enrich/pdl-identify');
          if (m.identifyByName) return await m.identifyByName({ name, state, city }, db);
          if (m.identifyOne) return await m.identifyOne({ full_name: name, state, city }, db);
        } catch (_) {}
        return null;
      }
    });
    tasks.push({
      source: 'apollo-match',
      run: async () => {
        try {
          const m = require('../enrich/apollo-cross-pollinate');
          if (m.apolloMatch) {
            const key = await db('system_config').where('key', 'apollo_api_key').first().then(r => r?.value).catch(() => null);
            return await m.apolloMatch({ full_name: name, state }, typeof key === 'string' ? key.replace(/^"|"$/g, '') : key);
          }
        } catch (_) {}
        return null;
      }
    });
    tasks.push({
      source: 'people-search-multi',
      run: async () => {
        try {
          const m = require('../enrich/people-search-multi');
          if (m.lookup) return await m.lookup({ full_name: name, state }, db);
        } catch (_) {}
        return null;
      }
    });
    if (state) {
      tasks.push({
        source: 'voter-rolls',
        run: async () => {
          try {
            const m = require('../enrich/voter-states');
            if (m.lookup) return await m.lookup({ full_name: name, state }, db);
          } catch (_) {}
          return null;
        }
      });
    }
  }

  // Run all in parallel
  const results = await Promise.allSettled(tasks.map(async t => {
    if (Date.now() - startedAt > TOTAL_BUDGET_MS) return { source: t.source, status: 'budget_exceeded' };
    try {
      const r = await withTimeout(t.run(), PER_ENGINE_TIMEOUT_MS, t.source);
      return { source: t.source, status: 'ok', result: r };
    } catch (e) {
      return { source: t.source, status: 'err', error: e.message };
    }
  }));

  return results.map(r => r.status === 'fulfilled' ? r.value : { status: 'rejected', error: r.reason?.message });
}

/**
 * Synthesize: pick the consensus value for each field, flag conflicts.
 * Returns a single identity card { name, phone, email, address, employer, ... } plus provenance.
 */
function synthesize(rawResults) {
  const evidence = {
    name: [], phone: [], email: [], address: [], city: [], state: [], zip: [],
    employer: [], age: [], dob: [], carrier: [], line_type: [], relatives: []
  };

  // Extract fields from each engine result
  for (const r of rawResults) {
    if (r.status !== 'ok' || !r.result) continue;
    const src = r.source;
    const x = r.result;

    // Common patterns across engines
    const push = (key, val) => {
      if (val == null || val === '') return;
      const v = typeof val === 'string' ? val.trim() : val;
      if (!v) return;
      evidence[key]?.push({ source: src, value: v });
    };

    // Trestle phone returns owners array
    if (Array.isArray(x.owners)) {
      for (const o of x.owners) {
        push('name', o.name);
        if (Array.isArray(o.current_addresses)) for (const a of o.current_addresses) push('address', a.full_address || a);
        if (Array.isArray(o.alternate_phones)) for (const p of o.alternate_phones) push('phone', p);
        if (Array.isArray(o.emails)) for (const em of o.emails) push('email', em);
      }
    }

    // OpenCNAM
    if (x.name && r.source === 'opencnam') push('name', x.name);

    // PDL
    if (x.full_name) push('name', x.full_name);
    if (x.work_email) push('email', x.work_email);
    if (x.mobile_phone) push('phone', x.mobile_phone);
    if (x.location_address_line_1) push('address', x.location_address_line_1);
    if (x.location_locality) push('city', x.location_locality);
    if (x.location_region) push('state', x.location_region);
    if (x.location_postal_code) push('zip', x.location_postal_code);
    if (x.job_company_name) push('employer', x.job_company_name);
    if (x.birth_year) push('age', new Date().getFullYear() - x.birth_year);
    if (x.birth_date) push('dob', x.birth_date);

    // Apollo
    if (x.organization?.name) push('employer', x.organization.name);
    if (Array.isArray(x.phone_numbers)) for (const p of x.phone_numbers) push('phone', typeof p === 'string' ? p : p.raw_number);
    if (x.email) push('email', x.email);

    // Generic field mapping
    for (const k of ['name','phone','email','address','city','state','zip','employer','carrier','line_type']) {
      if (x[k] && typeof x[k] === 'string') push(k, x[k]);
    }

    // USPS canonical
    if (x.canonical_address) push('address', x.canonical_address);
    if (x.zip_plus_4) push('zip', x.zip_plus_4);
  }

  // Pick consensus values (most common, or first if tie)
  const consensus = {};
  const conflicts = {};
  for (const [field, votes] of Object.entries(evidence)) {
    if (!votes.length) continue;
    const counts = {};
    for (const v of votes) {
      const key = String(v.value).toLowerCase().trim();
      counts[key] = counts[key] || { value: v.value, sources: [], count: 0 };
      counts[key].sources.push(v.source);
      counts[key].count++;
    }
    const sorted = Object.values(counts).sort((a, b) => b.count - a.count);
    consensus[field] = { value: sorted[0].value, sources: sorted[0].sources, votes: sorted[0].count };
    if (sorted.length > 1) {
      conflicts[field] = sorted.slice(0, 3).map(s => ({ value: s.value, sources: s.sources, votes: s.count }));
    }
  }

  return { consensus, conflicts, total_engines: rawResults.length, ok_engines: rawResults.filter(r => r.status === 'ok').length };
}

async function resolve(db, input) {
  const t0 = Date.now();
  const raw = await fanResolve(db, input);
  const synth = synthesize(raw);
  return {
    ok: true,
    input,
    duration_ms: Date.now() - t0,
    identity: synth.consensus,
    conflicts: synth.conflicts,
    engines_total: synth.total_engines,
    engines_ok: synth.ok_engines,
    raw_results: raw.map(r => ({ source: r.source, status: r.status, error: r.error || null }))
  };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();

  if (action === 'health') return res.json({ success: true, service: 'universal-resolver',
    accepts: ['phone','email','address','name','state','city'] });

  if (action === 'resolve') {
    const body = await readBody(req);
    const input = {
      phone: body.phone || req.query?.phone,
      email: body.email || req.query?.email,
      address: body.address || req.query?.address,
      name: body.name || req.query?.name,
      state: body.state || req.query?.state,
      city: body.city || req.query?.city
    };
    if (!Object.values(input).some(v => v)) return res.status(400).json({ error: 'at least one field required (phone/email/address/name)' });
    try {
      return res.json(await resolve(db, input));
    } catch (e) {
      await reportError(db, 'universal-resolver', null, e.message).catch(()=>{});
      return res.status(500).json({ error: e.message });
    }
  }

  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.resolve = resolve;
module.exports.fanResolve = fanResolve;
module.exports.synthesize = synthesize;
