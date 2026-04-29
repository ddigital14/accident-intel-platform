/**
 * Auto-Purchase Missing Data — Phase 44B
 * GET /api/v1/enrich/auto-purchase?secret=ingest-now&action=health|spend_check|run
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');
const { enqueueCascade } = require('../system/_cascade');

const SECRET = 'ingest-now';
const PROVIDER_COST = { apollo_unlock: 0.41, pdl_enrich: 0.10, hunter_find: 0.04 };

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function getBudget(db) {
  try {
    const row = await db('system_config').where({ key: 'auto_purchase_budget_usd_per_day' }).first();
    if (row?.value) {
      const v = typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
      const n = parseFloat(v);
      if (!Number.isNaN(n)) return n;
    }
  } catch (_) {}
  return parseFloat(process.env.AUTO_PURCHASE_BUDGET_USD_PER_DAY || '0');
}

async function spentToday(db) {
  try {
    const r = await db.raw(`SELECT COALESCE(SUM(cost_usd), 0) AS spent FROM system_api_calls WHERE service='auto_purchase' AND created_at > NOW() - INTERVAL '24 hours'`);
    return parseFloat(r.rows?.[0]?.spent || 0);
  } catch (_) { return 0; }
}

async function recordSpend(db, provider, amount) {
  try {
    await db.raw(`
      INSERT INTO system_api_calls (id, pipeline, service, tokens_in, tokens_out, cost_usd, success, created_at)
      VALUES (gen_random_uuid(), 'auto-purchase', 'auto_purchase', 0, 0, ?, TRUE, NOW())
    `, [amount]);
  } catch (_) {
    try { await trackApiCall(db, 'auto-purchase', 'auto_purchase', 0, 0, true); } catch (_) {}
  }
}

async function findCandidates(db, limit = 10) {
  try {
    const r = await db.raw(`
      SELECT p.id AS person_id, p.full_name, p.first_name, p.last_name, p.city, p.state,
             p.email, p.phone, p.employer, p.identity_confidence,
             i.id AS incident_id, i.lead_score, i.qualification_state
        FROM persons p
        LEFT JOIN incidents i ON i.id = p.incident_id
       WHERE COALESCE(p.identity_confidence, 0) >= 60
         AND COALESCE(i.lead_score, 0) >= 80
         AND p.role IN ('victim','driver')
         AND (p.email IS NULL OR p.email = '')
         AND p.full_name IS NOT NULL
       ORDER BY i.lead_score DESC NULLS LAST, p.identity_confidence DESC
       LIMIT ${parseInt(limit)}
    `);
    return r.rows || [];
  } catch (_) { return []; }
}

async function tryApollo(db, person, budgetLeft) {
  if (budgetLeft < PROVIDER_COST.apollo_unlock) return { ok: false, error: 'budget_too_low', skipped: true };
  try {
    const apolloUnlock = require('./apollo-unlock');
    const fn = apolloUnlock.unlockPerson || apolloUnlock.unlock;
    if (typeof fn !== 'function') return { ok: false, error: 'apollo_unlock_unavailable' };
    const r = await fn(db, person);
    await recordSpend(db, 'apollo_unlock', PROVIDER_COST.apollo_unlock);
    return { ok: !!r?.ok, provider: 'apollo_unlock', cost: PROVIDER_COST.apollo_unlock, result: r };
  } catch (e) { return { ok: false, error: e.message }; }
}

async function tryHunter(db, person, budgetLeft) {
  if (budgetLeft < PROVIDER_COST.hunter_find) return { ok: false, error: 'budget_too_low', skipped: true };
  if (!person.employer) return { ok: false, error: 'no_employer' };
  try {
    let key = process.env.HUNTER_API_KEY;
    if (!key) {
      const row = await db('system_config').where({ key: 'hunter_api_key' }).first();
      if (row?.value) key = typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
    }
    if (!key) return { ok: false, error: 'no_hunter_key' };
    const first = (person.first_name || person.full_name?.split(/\s+/)[0] || '').toLowerCase();
    const last  = (person.last_name  || person.full_name?.split(/\s+/).pop() || '').toLowerCase();
    const url = `https://api.hunter.io/v2/email-finder?domain=${encodeURIComponent(person.employer)}&first_name=${encodeURIComponent(first)}&last_name=${encodeURIComponent(last)}&api_key=${key}`;
    const resp = await fetch(url, { signal: AbortSignal.timeout(15000) });
    await recordSpend(db, 'hunter_find', PROVIDER_COST.hunter_find);
    if (!resp.ok) return { ok: false, error: `HTTP ${resp.status}` };
    const data = await resp.json();
    const email = data?.data?.email;
    if (email) {
      try {
        await db('persons').where('id', person.person_id).update({ email, updated_at: new Date() });
        await enqueueCascade(db, person.person_id, 'auto_purchase_hunter').catch(() => {});
      } catch (_) {}
    }
    return { ok: !!email, provider: 'hunter_find', cost: PROVIDER_COST.hunter_find, email };
  } catch (e) { return { ok: false, error: e.message }; }
}

async function runAutoPurchase(db, limit = 10) {
  const budget = await getBudget(db);
  if (!budget || budget <= 0) return { ok: true, enabled: false, reason: 'budget_disabled', budget };
  const spent = await spentToday(db);
  let remaining = Math.max(0, budget - spent);
  if (remaining <= 0) return { ok: true, enabled: true, budget, spent, remaining: 0, reason: 'cap_reached' };
  const candidates = await findCandidates(db, limit);
  const log = [];
  for (const c of candidates) {
    if (remaining < Math.min(PROVIDER_COST.hunter_find, PROVIDER_COST.apollo_unlock)) break;
    let tried = await tryHunter(db, c, remaining);
    if (tried?.cost) remaining -= tried.cost;
    if (!tried?.ok && remaining >= PROVIDER_COST.apollo_unlock) {
      const apo = await tryApollo(db, c, remaining);
      if (apo?.cost) remaining -= apo.cost;
      log.push({ person_id: c.person_id, full_name: c.full_name, hunter: tried, apollo: apo });
    } else {
      log.push({ person_id: c.person_id, full_name: c.full_name, hunter: tried });
    }
  }
  return {
    ok: true, enabled: true, budget,
    spent_before: spent,
    remaining_after: Math.max(0, remaining),
    attempted: log.length, log
  };
}

async function health(db) {
  const budget = await getBudget(db);
  const spent = await spentToday(db);
  return {
    ok: true, enabled: !!(budget && budget > 0),
    budget_usd_per_day: budget,
    spent_24h_usd: Math.round(spent * 10000) / 10000,
    remaining_usd: Math.max(0, budget - spent),
    providers: PROVIDER_COST
  };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  const db = getDb();
  const action = (req.query?.action || 'health').toLowerCase();
  try {
    if (action === 'health') return res.json({ success: true, ...(await health(db)) });
    if (action === 'spend_check') {
      const spent = await spentToday(db);
      const budget = await getBudget(db);
      return res.json({ success: true, budget, spent, remaining: Math.max(0, budget - spent) });
    }
    if (action === 'run') {
      const limit = parseInt(req.query.limit || '10');
      return res.json({ success: true, ...(await runAutoPurchase(db, limit)) });
    }
    return res.status(400).json({ error: 'unknown_action', valid: ['health', 'spend_check', 'run'] });
  } catch (e) {
    try { await reportError(db, 'auto-purchase', null, e.message); } catch (_) {}
    return res.status(500).json({ success: false, error: e.message });
  }
};

module.exports.runAutoPurchase = runAutoPurchase;
module.exports.health = health;
module.exports.getBudget = getBudget;
