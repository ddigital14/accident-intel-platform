/**
 * Phase 92: Deep-Dive Narrow Engine
 *
 * For partial-info persons (name only, name+state, etc.), triangulate identity
 * via every name-compatible source: voter rolls, Apollo, PDL, Brave obituaries,
 * Google CSE Facebook. Confidence-weighted fusion fills empty fields when a
 * cluster reaches >=0.75. Saves all candidates for audit.
 */
const { getDb } = require('../../_db');

const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

const APOLLO_KEY = process.env.APOLLO_API_KEY;
const PDL_KEY = process.env.PDL_API_KEY;
const BRAVE_KEY = process.env.BRAVE_API_KEY;
const GOOGLE_CSE_KEY = process.env.GOOGLE_CSE_API_KEY;
const GOOGLE_CSE_ID = process.env.GOOGLE_CSE_ENGINE_ID;

let _candTableEnsured = false;
async function ensureCandidateTable(db) {
  if (_candTableEnsured) return;
  await db.raw(`
    CREATE TABLE IF NOT EXISTS person_identity_candidates (
      id BIGSERIAL PRIMARY KEY,
      person_id UUID NOT NULL,
      candidate JSONB,
      confidence DOUBLE PRECISION,
      sources TEXT[],
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_pic_person ON person_identity_candidates (person_id);
  `).catch(() => {});
  _candTableEnsured = true;
}

function nameTokens(s) { return (s || '').toLowerCase().replace(/[^a-z\s]/g, '').split(/\s+/).filter(Boolean); }
function nameOverlap(a, b) {
  const ta = new Set(nameTokens(a)); const tb = new Set(nameTokens(b));
  if (ta.size === 0 || tb.size === 0) return 0;
  let m = 0; for (const t of ta) if (tb.has(t)) m++;
  return m / Math.max(ta.size, tb.size);
}

async function apolloByName(name, locality) {
  if (!APOLLO_KEY) return null;
  try {
    const r = await fetch('https://api.apollo.io/api/v1/mixed_people/search', {
      method: 'POST', headers: { 'X-Api-Key': APOLLO_KEY, 'Content-Type': 'application/json' },
      body: JSON.stringify({ q_keywords: name, person_locations: locality ? [locality] : undefined, page: 1, per_page: 5 }),
      signal: AbortSignal.timeout(10000)
    });
    if (!r.ok) return null;
    const j = await r.json();
    return (j.people || []).map(p => ({
      source: 'apollo', name: p.name, title: p.title, employer: p.organization?.name,
      city: p.city, state: p.state,
      email: p.email && p.email !== 'email_not_unlocked@domain.com' ? p.email : null,
      linkedin: p.linkedin_url
    }));
  } catch { return null; }
}

async function pdlByName(name, locality) {
  if (!PDL_KEY) return null;
  try {
    const sql = `SELECT * FROM person WHERE full_name='${name.replace(/'/g, "''")}' ${locality ? `AND location_locality='${locality.toLowerCase().replace(/'/g, "''")}'` : ''}`;
    const r = await fetch('https://api.peopledatalabs.com/v5/person/search?size=5', {
      method: 'POST', headers: { 'X-Api-Key': PDL_KEY, 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql }), signal: AbortSignal.timeout(10000)
    });
    if (!r.ok) return null;
    const j = await r.json();
    return (j.data || []).map(p => ({
      source: 'pdl', name: p.full_name, title: p.job_title, employer: p.job_company_name,
      city: p.location_locality, state: p.location_region,
      email: (p.work_email || p.personal_emails?.[0]) || null,
      phone: p.phone_numbers?.[0] || null,
      linkedin: p.linkedin_url,
      age: p.birth_year ? new Date().getFullYear() - p.birth_year : null
    }));
  } catch { return null; }
}

async function braveSearchObituary(name, city, state) {
  if (!BRAVE_KEY) return null;
  const q = `"${name}" obituary ${city || ''} ${state || ''}`.trim();
  try {
    const r = await fetch(`https://api.search.brave.com/res/v1/web/search?q=${encodeURIComponent(q)}&count=5`, {
      headers: { 'Accept': 'application/json', 'X-Subscription-Token': BRAVE_KEY },
      signal: AbortSignal.timeout(10000)
    });
    if (!r.ok) return null;
    const j = await r.json();
    return (j.web?.results || []).slice(0, 5).map(x => ({ source: 'brave-obit', title: x.title, url: x.url, snippet: x.description }));
  } catch { return null; }
}

async function googleCseFacebook(name, city) {
  if (!GOOGLE_CSE_KEY || !GOOGLE_CSE_ID) return null;
  const q = `site:facebook.com "${name}" ${city || ''}`;
  try {
    const r = await fetch(`https://www.googleapis.com/customsearch/v1?key=${GOOGLE_CSE_KEY}&cx=${GOOGLE_CSE_ID}&q=${encodeURIComponent(q)}&num=5`, { signal: AbortSignal.timeout(10000) });
    if (!r.ok) return null;
    const j = await r.json();
    return (j.items || []).slice(0, 5).map(x => ({ source: 'google-cse-fb', title: x.title, url: x.link, snippet: x.snippet }));
  } catch { return null; }
}

async function voterRollsByName(db, name, state) {
  if (!state) return null;
  try {
    const tokens = nameTokens(name);
    if (tokens.length < 2) return null;
    const [first, ...rest] = tokens;
    const last = rest[rest.length - 1];
    const rows = await db.raw(`
      SELECT * FROM voter_records
      WHERE state = ?
        AND LOWER(first_name) LIKE ?
        AND LOWER(last_name) LIKE ?
      LIMIT 5
    `, [state, `${first}%`, `${last}%`]).then(r => r.rows || []).catch(() => []);
    return rows.map(v => ({
      source: 'voter-rolls', name: `${v.first_name} ${v.last_name}`, city: v.city, state: v.state,
      address: [v.street_address, v.city, v.state, v.zip].filter(Boolean).join(', '),
      age: v.birth_year ? new Date().getFullYear() - v.birth_year : null, party: v.party
    }));
  } catch { return null; }
}

function fuseCandidates(person, results) {
  const all = [];
  for (const list of results) if (Array.isArray(list)) for (const c of list) all.push(c);
  if (all.length === 0) return [];
  const filtered = all
    .map(c => ({ ...c, _name_sim: nameOverlap(person.full_name, c.name || c.title || '') }))
    .filter(c => c._name_sim >= 0.5);
  const clusters = {};
  for (const c of filtered) {
    const key = `${(c.city || '').toLowerCase()}|${(c.state || '').toUpperCase()}`;
    if (!clusters[key]) clusters[key] = { city: c.city, state: c.state, sources: new Set(), evidence: [], best_name: c.name };
    clusters[key].sources.add(c.source);
    clusters[key].evidence.push(c);
    if ((c._name_sim || 0) > 0.85) clusters[key].best_name = c.name;
  }
  const SOURCE_WEIGHTS = { 'voter-rolls': 0.35, 'pdl': 0.25, 'apollo': 0.20, 'brave-obit': 0.15, 'google-cse-fb': 0.05 };
  const ranked = Object.values(clusters).map(c => {
    let confidence = 0;
    for (const s of c.sources) confidence += SOURCE_WEIGHTS[s] || 0.1;
    if (person.state && c.state && person.state.toUpperCase() === c.state.toUpperCase()) confidence += 0.15;
    return {
      name: c.best_name, city: c.city, state: c.state,
      sources: Array.from(c.sources), evidence_count: c.evidence.length,
      confidence: Math.min(0.99, confidence), sample: c.evidence.slice(0, 3)
    };
  }).sort((a, b) => b.confidence - a.confidence);
  return ranked.slice(0, 5);
}

async function applyTopCandidate(db, person, top) {
  if (!top || top.confidence < 0.75) return { applied: false, reason: 'below_threshold' };
  const updates = {};
  const sample = top.sample || [];
  const phoneCandidate = sample.find(s => s.phone)?.phone;
  const emailCandidate = sample.find(s => s.email)?.email;
  const addrCandidate = sample.find(s => s.address)?.address;
  if (!person.phone && phoneCandidate) updates.phone = phoneCandidate;
  if (!person.email && emailCandidate) updates.email = emailCandidate;
  if (!person.address && addrCandidate) updates.address = addrCandidate;
  if (!person.city && top.city) updates.city = top.city;
  if (!person.state && top.state) updates.state = top.state;
  if (Object.keys(updates).length === 0) return { applied: false, reason: 'no_new_fields' };
  try {
    await db('persons').where('id', person.id).update(updates);
    for (const [field, value] of Object.entries(updates)) {
      await db('enrichment_logs').insert({
        person_id: person.id, field_name: field, old_value: null,
        new_value: typeof value === 'string' ? value : JSON.stringify(value),
        created_at: new Date()
      }).catch(() => {});
    }
    return { applied: true, fields: Object.keys(updates), confidence: top.confidence };
  } catch (e) {
    return { applied: false, reason: 'db_error', error: e.message };
  }
}

async function processOne(db, person) {
  await ensureCandidateTable(db);
  const locality = person.city || null;
  const state = person.state || null;
  const [apollo, pdl, brave, gcse, voter] = await Promise.all([
    apolloByName(person.full_name, locality),
    pdlByName(person.full_name, locality),
    braveSearchObituary(person.full_name, locality, state),
    googleCseFacebook(person.full_name, locality),
    voterRollsByName(db, person.full_name, state)
  ]);
  const candidates = fuseCandidates(person, [apollo, pdl, brave, gcse, voter]);
  if (candidates.length === 0) return { person_id: person.id, status: 'no_candidates' };
  const top = candidates[0];
  await db('person_identity_candidates').insert({
    person_id: person.id, candidate: JSON.stringify(candidates),
    confidence: top.confidence, sources: top.sources, created_at: new Date()
  }).catch(() => {});
  const apply = await applyTopCandidate(db, person, top);
  return {
    person_id: person.id, name: person.full_name,
    candidates_found: candidates.length, top_confidence: top.confidence, top_sources: top.sources,
    applied: apply.applied, fields_filled: apply.fields || [], apply_reason: apply.reason
  };
}

async function findPartialPersons(db, limit) {
  return db.raw(`
    SELECT * FROM persons
    WHERE full_name IS NOT NULL
      AND (phone IS NULL OR email IS NULL OR address IS NULL)
      AND lead_tier IN ('pending', 'qualified', 'related')
      AND (created_at > NOW() - INTERVAL '30 days' OR victim_verified = TRUE)
    ORDER BY CASE WHEN victim_verified = TRUE THEN 0 ELSE 1 END, created_at DESC
    LIMIT ${parseInt(limit) || 10}
  `).then(r => r.rows || []);
}

module.exports = async function handler(req, res) {
  if (!authed(req)) return res.status(401).json({ error: 'unauthorized' });
  const action = req.query?.action || 'health';
  const db = getDb();
  if (action === 'health') {
    return res.status(200).json({
      ok: true, engine: 'deep-dive-narrow',
      sources_configured: {
        apollo: !!APOLLO_KEY, pdl: !!PDL_KEY, brave: !!BRAVE_KEY,
        google_cse: !!(GOOGLE_CSE_KEY && GOOGLE_CSE_ID), voter_rolls: 'db'
      }
    });
  }
  if (action === 'narrow') {
    const id = req.query?.person_id;
    if (!id) return res.status(400).json({ error: 'person_id required' });
    const p = await db('persons').where('id', id).first();
    if (!p) return res.status(404).json({ error: 'person not found' });
    const result = await processOne(db, p);
    return res.status(200).json({ ok: true, result });
  }
  if (action === 'run') {
    const limit = parseInt(req.query?.limit) || 10;
    const persons = await findPartialPersons(db, limit);
    const results = [];
    let applied = 0, candidates_found = 0;
    for (const p of persons) {
      try {
        const r = await processOne(db, p);
        results.push(r);
        if (r.applied) applied++;
        if (r.candidates_found) candidates_found += r.candidates_found;
      } catch (e) {
        results.push({ person_id: p.id, status: 'error', error: e.message });
      }
    }
    return res.status(200).json({ ok: true, processed: persons.length, applied, candidates_found, results: results.slice(0, 20) });
  }
  return res.status(400).json({ error: 'unknown action', valid: ['health', 'run', 'narrow'] });
};
