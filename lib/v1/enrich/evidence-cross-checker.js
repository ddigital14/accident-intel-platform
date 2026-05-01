/**
 * EVIDENCE CROSS-CHECKER (Phase 38 Wave B)
 *
 * Walks recently enriched persons and validates contact info across sources:
 *   - Phones: digits-only equality. Mismatch -> docks confidence -10.
 *   - Addresses: token-overlap >= 70% required.
 *   - Emails: exact match required for confidence boost.
 *   - Names: Levenshtein distance <= 2 acceptable (middle-initial variants).
 *
 * Emits enqueueCascade weight=25 on confirmed cross-match, weight=-10 on conflict.
 *
 * HTTP entrypoint:
 *   GET /api/v1/enrich/evidence-cross-checker?secret=ingest-now&action=health
 *   GET /api/v1/enrich/evidence-cross-checker?secret=ingest-now&action=batch&limit=30
 *   GET /api/v1/enrich/evidence-cross-checker?secret=ingest-now&action=check&person_id=<uuid>
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');
const { enqueueCascade } = require('../system/_cascade');

const SECRET = 'ingest-now';

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

function digitsOnly(s) { return String(s || '').replace(/\D+/g, ''); }
function normTokens(s) {
  return String(s || '').toLowerCase().replace(/[^a-z0-9 ]+/g, ' ').split(/\s+/).filter(Boolean);
}

function tokenOverlap(a, b) {
  if (!a.length || !b.length) return 0;
  const setB = new Set(b);
  let hit = 0;
  for (const t of a) if (setB.has(t)) hit++;
  return hit / Math.max(a.length, b.length);
}

function levenshtein(a, b) {
  a = String(a || '').toLowerCase();
  b = String(b || '').toLowerCase();
  if (a === b) return 0;
  if (!a.length) return b.length;
  if (!b.length) return a.length;
  const dp = new Array(b.length + 1);
  for (let j = 0; j <= b.length; j++) dp[j] = j;
  for (let i = 1; i <= a.length; i++) {
    let prev = dp[0];
    dp[0] = i;
    for (let j = 1; j <= b.length; j++) {
      const tmp = dp[j];
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      dp[j] = Math.min(dp[j] + 1, dp[j - 1] + 1, prev + cost);
      prev = tmp;
    }
  }
  return dp[b.length];
}

/**
 * Group enrichment_logs entries (last 24h) by person_id and compare values.
 */
async function checkOne(db, personId) {
  const result = {
    person_id: personId,
    phone_sources: 0,
    email_sources: 0,
    address_sources: 0,
    name_sources: 0,
    matches: 0,
    conflicts: 0,
    confidence_delta: 0,
    detail: []
  };

  const logs = await db('enrichment_logs')
    .where('person_id', personId)
    .where('created_at', '>', new Date(Date.now() - 7 * 86400000))
    .orderBy('created_at', 'desc')
    .limit(40);
  if (!logs.length) {
    // Phase 55: log a 'no_evidence_yet' summary so UI doesn't show 'not yet checked' forever.
    try {
      await db('enrichment_logs').insert({
        person_id: personId,
        field_name: 'evidence_cross_check_summary',
        old_value: null,
        new_value: JSON.stringify({ matches: 0, conflicts: 0, detail: ['no_evidence_yet'] }).slice(0, 4000),
        source_url: null,
        source: 'evidence-cross-checker',
        confidence: 30,
        verified: false,
        data: JSON.stringify({ status: 'no_evidence_yet' }).slice(0, 4000),
        created_at: new Date()
      });
    } catch (_) {}
    return { ok: true, ...result, skipped: 'no_logs' };
  }

  // Aggregate by field
  const phones = [];
  const emails = [];
  const addresses = [];
  const names = [];
  for (const l of logs) {
    let parsed = null;
    try { parsed = l.data ? (typeof l.data === 'string' ? JSON.parse(l.data) : l.data) : null; }
    catch (_) {}
    const fields = parsed?.fields || parsed || {};
    if (fields.phones && Array.isArray(fields.phones)) {
      for (const p of fields.phones) phones.push({ p, src: l.source || 'unknown' });
    }
    if (fields.emails && Array.isArray(fields.emails)) {
      for (const e of fields.emails) emails.push({ e: String(e).toLowerCase(), src: l.source || 'unknown' });
    }
    if (fields.addresses && Array.isArray(fields.addresses)) {
      for (const a of fields.addresses) addresses.push({ a: a.address || a, src: l.source || 'unknown' });
    }
    if (fields.full_name || fields.name) {
      names.push({ n: fields.full_name || fields.name, src: l.source || 'unknown' });
    }
  }

  result.phone_sources = phones.length;
  result.email_sources = emails.length;
  result.address_sources = addresses.length;
  result.name_sources = names.length;

  // PHONE: pairwise digits-only compare
  if (phones.length >= 2) {
    const norms = phones.map(x => ({ ...x, d: digitsOnly(x.p) }));
    const dist = new Set(norms.map(x => x.d).filter(Boolean));
    if (dist.size === 1) { result.matches++; result.detail.push('phone_match'); }
    else if (dist.size > 1) { result.conflicts++; result.confidence_delta -= 10; result.detail.push('phone_conflict:' + dist.size); }
  }

  // EMAIL: exact compare
  if (emails.length >= 2) {
    const dist = new Set(emails.map(x => x.e).filter(Boolean));
    if (dist.size === 1) { result.matches++; result.detail.push('email_match'); }
    else if (dist.size > 1) { result.conflicts++; result.confidence_delta -= 10; result.detail.push('email_conflict:' + dist.size); }
  }

  // ADDRESS: token-overlap >= 70%
  if (addresses.length >= 2) {
    const tokens = addresses.map(x => normTokens(x.a));
    let agree = 0, disagree = 0;
    for (let i = 0; i < tokens.length; i++) {
      for (let j = i + 1; j < tokens.length; j++) {
        const ov = tokenOverlap(tokens[i], tokens[j]);
        if (ov >= 0.7) agree++;
        else disagree++;
      }
    }
    if (agree && agree >= disagree) { result.matches++; result.detail.push('address_match:' + agree); }
    else if (disagree > 0) { result.conflicts++; result.confidence_delta -= 10; result.detail.push('address_conflict:' + disagree); }
  }

  // NAME: Levenshtein <=2 acceptable
  if (names.length >= 2) {
    const base = String(names[0].n || '').toLowerCase();
    let agree = 0, disagree = 0;
    for (let i = 1; i < names.length; i++) {
      const d = levenshtein(base, String(names[i].n || '').toLowerCase());
      if (d <= 2) agree++;
      else disagree++;
    }
    if (agree && agree >= disagree) { result.matches++; result.detail.push('name_match:' + agree); }
    else if (disagree > 0) { result.conflicts++; result.confidence_delta -= 5; result.detail.push('name_conflict:' + disagree); }
  }

  // Apply confidence delta to person if any conflicts
  if (result.confidence_delta !== 0) {
    try {
      await db('persons').where('id', personId).update({
        confidence: db.raw('GREATEST(0, LEAST(100, COALESCE(confidence, 50) + ?))', [result.confidence_delta]),
        updated_at: new Date()
      });
    } catch (_) {}
  }

  // Phase 55: Always log a summary row so UI doesn't show "not yet checked" forever.
  // When there's only one source per field, matches=conflicts=0 — log it as 'single_source'.
  if (result.matches === 0 && result.conflicts === 0) {
    try {
      await db('enrichment_logs').insert({
        person_id: personId,
        field_name: 'evidence_cross_check_summary',
        old_value: null,
        new_value: JSON.stringify({
          matches: 0,
          conflicts: 0,
          detail: ['single_source'],
          phone_sources: result.phone_sources || 0,
          email_sources: result.email_sources || 0,
          address_sources: result.address_sources || 0
        }).slice(0, 4000),
        source_url: null,
        source: 'evidence-cross-checker',
        confidence: 50,
        verified: true,
        data: JSON.stringify({ status: 'single_source', cross_engine_conflict: false }).slice(0, 4000),
        created_at: new Date()
      });
    } catch (_) {}
  }

  // Emit cascade with weight reflecting matches vs conflicts
  if (result.matches > 0 || result.conflicts > 0) {
    const weight = result.matches * 25 + result.conflicts * -10;
    try {
      await enqueueCascade(db, {
        person_id: personId,
        trigger_source: 'evidence-cross-checker',
        trigger_field: 'cross_check',
        trigger_value: result.detail.join(','),
        priority: result.matches > result.conflicts ? 7 : 4
      });
    } catch (_) {}
    // Log a summary record for observability
    try {
      await db('enrichment_logs').insert({
        person_id: personId,
        field_name: 'evidence_cross_check_summary',
        old_value: null,
        new_value: JSON.stringify({ matches: result.matches, conflicts: result.conflicts, detail: result.detail }).slice(0, 4000),
        source_url: null,
        source: 'evidence-cross-checker',
        confidence: 60,
        verified: result.conflicts === 0,
        data: JSON.stringify({ weight, fields_checked: result.detail.length, cross_engine_conflict: result.conflicts > 0 }).slice(0, 4000),
        created_at: new Date()
      });
    } catch (_) {}
  }

  return { ok: true, ...result };
}

async function batchCheck(db, { limit = 30, force_qualified = false } = {}) {
  // Phase 53e: force_qualified mode pulls every person attached to a qualified incident,
  // regardless of source-count or freshness — useful for one-shot re-checks.
  let rows;
  if (force_qualified) {
    rows = await db.raw(`
      SELECT DISTINCT p.id AS person_id
      FROM persons p
      JOIN incidents i ON i.id = p.incident_id
      WHERE i.qualification_state = 'qualified'
      ORDER BY p.id
      LIMIT ${parseInt(limit) || 30}
    `).then(r => r.rows || r).catch(() => []);
  } else {
    rows = await db.raw(`
      SELECT person_id, COUNT(DISTINCT source) AS src_count
      FROM enrichment_logs
      WHERE created_at > NOW() - INTERVAL '7 days'
        AND person_id IS NOT NULL
      GROUP BY person_id
      HAVING COUNT(DISTINCT source) >= 2
      ORDER BY MAX(created_at) DESC
      LIMIT ${parseInt(limit) || 30}
    `).then(r => r.rows || r).catch(() => []);
  }

  const out = {
    candidates: Array.isArray(rows) ? rows.length : 0,
    checked: 0,
    matches_total: 0,
    conflicts_total: 0,
    samples: []
  };

  for (const r of (Array.isArray(rows) ? rows : [])) {
    let one;
    try { one = await checkOne(db, r.person_id); }
    catch (e) { continue; }
    if (!one?.ok) continue;
    out.checked++;
    out.matches_total += one.matches || 0;
    out.conflicts_total += one.conflicts || 0;
    if (out.samples.length < 10 && (one.matches || one.conflicts)) {
      out.samples.push({
        person_id: r.person_id,
        matches: one.matches,
        conflicts: one.conflicts,
        detail: one.detail
      });
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
    return res.status(200).json({ success: true, service: 'evidence-cross-checker', ts: new Date().toISOString() });
  }

  if (action === 'check') {
    const personId = req.query?.person_id || req.query?.id;
    if (!personId) return res.status(400).json({ error: 'person_id required' });
    try {
      const r = await checkOne(db, personId);
      await trackApiCall(db, 'evidence-cross-checker', 'check_one', 0, 0, !!r.ok).catch(() => {});
      return res.status(200).json({ success: true, ...r });
    } catch (e) {
      await reportError(db, 'evidence-cross-checker', null, e.message, { severity: 'error' });
      return res.status(500).json({ error: e.message, success: false });
    }
  }

  if (action === 'batch') {
    const limit = Math.max(1, Math.min(parseInt(req.query?.limit) || 30, 100));
    try {
      const r = await batchCheck(db, { limit, force_qualified: req.query?.force === 'qualified' });
      await trackApiCall(db, 'evidence-cross-checker', 'batch', 0, 0, true).catch(() => {});
      return res.status(200).json({ success: true, ...r });
    } catch (e) {
      await reportError(db, 'evidence-cross-checker', null, e.message, { severity: 'error' });
      return res.status(500).json({ error: e.message, success: false });
    }
  }

  return res.status(400).json({ error: 'unknown action: ' + action });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.checkOne = checkOne;
module.exports.batchCheck = batchCheck;
