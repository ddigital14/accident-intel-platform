/**
 * Phase 67-C: Hunter Domain Search
 *
 * When a person's employer is known (from prior enrichment_logs), enumerate
 * emails at that company. Returns ranked seniority/department contacts so
 * reps can reach coworkers as alternative paths to the victim.
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const SECRET = 'ingest-now';
let trackApiCall = async () => {};
try { trackApiCall = require('../system/cost-tracker').trackApiCall || trackApiCall; } catch (_) {}

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function getHunterKey(db) {
  if (process.env.HUNTER_API_KEY) return process.env.HUNTER_API_KEY;
  try {
    const row = await db('system_config').where('key', 'hunter_api_key').first();
    if (row?.value) return typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
  } catch (_) {}
  return null;
}

function slugifyToDomain(employer) {
  if (!employer) return null;
  const slug = String(employer).toLowerCase().replace(/[^a-z0-9]+/g, '');
  return slug ? slug + '.com' : null;
}

async function findEmployerForPerson(db, personId) {
  // Look in enrichment_logs.new_value for employer/company/domain hints
  const rows = await db('enrichment_logs')
    .where('person_id', personId)
    .whereIn('field_name', ['employer', 'company', 'apollo_cross_pollinate', 'pdl_identify', 'pdl_enrich', 'fan_out_summary'])
    .orderBy('created_at', 'desc')
    .limit(20)
    .select('field_name', 'new_value');
  for (const r of rows) {
    try {
      const j = typeof r.new_value === 'string' ? JSON.parse(r.new_value) : r.new_value;
      if (!j) continue;
      if (j.employer) return { employer: j.employer, source: r.field_name };
      if (j.company) return { employer: j.company, source: r.field_name };
      if (j.organization?.name) return { employer: j.organization.name, source: r.field_name };
      if (j.domain) return { employer: null, domain: j.domain, source: r.field_name };
      if (j.website_url) {
        const m = String(j.website_url).match(/https?:\/\/([^\/]+)/);
        if (m) return { employer: null, domain: m[1].replace(/^www\./, ''), source: r.field_name };
      }
    } catch (_) {}
  }
  return null;
}

async function callHunterDomain(domain, key) {
  const url = `https://api.hunter.io/v2/domain-search?domain=${encodeURIComponent(domain)}&limit=10&type=personal&api_key=${key}`;
  try {
    const r = await fetch(url, { signal: AbortSignal.timeout(15000) });
    const j = await r.json();
    if (!r.ok) return { ok: false, error: j.errors?.[0]?.detail || `http_${r.status}` };
    return { ok: true, ...j.data };
  } catch (e) { return { ok: false, error: e.message }; }
}

async function lookupOne(db, personId) {
  const key = await getHunterKey(db);
  if (!key) return { skipped: 'no_hunter_key' };
  const empInfo = await findEmployerForPerson(db, personId);
  if (!empInfo) return { skipped: 'no_employer_known' };
  let domain = empInfo.domain || slugifyToDomain(empInfo.employer);
  if (!domain) return { skipped: 'no_domain_resolvable' };
  const r = await callHunterDomain(domain, key);
  await trackApiCall(db, 'hunter-domain', 'lookup', 0.02, 1, !!r.ok).catch(() => {});
  if (!r.ok) return r;

  const KEEP_SEN = new Set(['senior', 'executive', 'manager', 'director', 'c_suite', 'owner']);
  const KEEP_DEPT = new Set(['hr', 'executive', 'operations', 'management', 'legal']);

  const emails = (r.emails || []).filter(e =>
    KEEP_SEN.has((e.seniority || '').toLowerCase()) ||
    KEEP_DEPT.has((e.department || '').toLowerCase())
  );

  // Insert relevant coworkers as related persons
  const inserted = [];
  const victim = await db('persons').where('id', personId).first();
  if (!victim) return { ok: false, error: 'victim_not_found' };
  for (const e of emails.slice(0, 5)) {
    if (!e.value) continue;
    try {
      const exists = await db('persons').where('email', e.value).first();
      if (exists) { inserted.push({ email: e.value, status: 'exists' }); continue; }
      const { v4: uuid } = require('uuid');
      const id = uuid();
      await db('persons').insert({
        id,
        incident_id: victim.incident_id,
        role: 'related',
        relationship_to_victim: 'coworker',
        victim_id: victim.id,
        full_name: `${e.first_name || ''} ${e.last_name || ''}`.trim() || 'Unknown coworker',
        first_name: e.first_name || null,
        last_name: e.last_name || null,
        email: e.value,
        created_at: new Date(),
        updated_at: new Date()
      });
      inserted.push({ id, email: e.value, position: e.position });
    } catch (_) {}
  }

  // Log to enrichment_logs (minimal schema)
  try {
    await db('enrichment_logs').insert({
      person_id: personId,
      field_name: 'hunter_domain_emails',
      old_value: null,
      new_value: JSON.stringify({
        domain, employer: empInfo.employer, total_returned: emails.length,
        kept: inserted.length, source: 'hunter-domain', emails: emails.slice(0, 10)
      }).slice(0, 4000),
      created_at: new Date()
    });
  } catch (_) {}

  return { ok: true, domain, employer: empInfo.employer, total: emails.length, inserted };
}

async function batchLookup(db, limit = 10) {
  const rows = await db('persons')
    .whereNull('phone').orWhereNull('email')
    .limit(limit)
    .select('id');
  const out = [];
  for (const r of rows) {
    try { out.push({ id: r.id, ...(await lookupOne(db, r.id)) }); }
    catch (e) { out.push({ id: r.id, error: e.message }); }
  }
  return { ok: true, processed: out.length, results: out };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();

  if (action === 'health') return res.json({ success: true, service: 'hunter-domain' });

  if (action === 'lookup') {
    const pid = req.query?.person_id;
    if (!pid) return res.status(400).json({ error: 'person_id required' });
    return res.json(await lookupOne(db, pid));
  }

  if (action === 'lookup_domain') {
    const domain = req.query?.domain;
    if (!domain) return res.status(400).json({ error: 'domain required' });
    const key = await getHunterKey(db);
    if (!key) return res.json({ skipped: 'no_hunter_key' });
    return res.json(await callHunterDomain(domain, key));
  }

  if (action === 'batch') {
    const limit = Math.min(50, parseInt(req.query?.limit) || 10);
    return res.json(await batchLookup(db, limit));
  }

  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.lookupOne = lookupOne;
module.exports.batchLookup = batchLookup;
