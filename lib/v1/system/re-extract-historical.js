/**
 * GET /api/v1/system/re-extract-historical?secret=ingest-now
 *
 * Phase 67 Agent B — One-shot maintenance backfill that re-validates EVERY
 * person row currently in the DB against the *current* deny-list (Phase 65/66
 * hardened with 50+ celebrity/officer/unknown bans + CELEBRITY_CONTEXT_RX).
 *
 * Older extractors persisted celebrities, officers, "unknown unknown", and
 * NASCAR drivers as victims. This endpoint cleans them up.
 *
 * Actions:
 *   ?action=health
 *   ?action=audit&limit=N         dry-run, classifies WOULD-be demotions
 *   ?action=run&limit=N           actually demotes (victim_verified=false +
 *                                 reason='deny_list_2026_04_30'); transitions
 *                                 incidents to qualification_state='pending'
 *                                 if all their verified victims are now gone
 *   ?action=stats                 counts by state + recent demotions (7d)
 *
 * NOT in ENGINE_MATRIX — this is one-shot maintenance, not per-person enrich.
 * Wired into router only.
 */

const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { applyDenyList, HARD_BAN_NAMES, CELEBRITY_CONTEXT_RX, OFFICIAL_RX, BYLINE_RX } = require('../enrich/_name_filter');

let trackApiCall = async () => {};
try { trackApiCall = require('./cost-tracker').trackApiCall || trackApiCall; } catch (_) {}
try { if (!trackApiCall || trackApiCall === (async () => {})) trackApiCall = require('./cost').trackApiCall || trackApiCall; } catch (_) {}

const SECRET = 'ingest-now';
const REASON_TAG = 'deny_list_2026_04_30';
const TIME_BUDGET_MS = 50 * 1000;

function authed(req) {
  const s = (req.query && req.query.secret) || (req.headers && req.headers['x-cron-secret']);
  return s === SECRET || s === process.env.CRON_SECRET;
}

/**
 * Classify the *reason* a name fails the current deny-list, so the audit
 * report can break demotions down by category.
 */
function classifyReason(name, surroundingText) {
  const trimmed = String(name || '').trim().replace(/\s+/g, ' ');
  if (!trimmed) return 'empty';
  const norm = trimmed.toLowerCase();
  const text = String(surroundingText || '');

  // Roley tokens like "Officer 1", "Unknown Faulkner Cousin"
  if (HARD_BAN_NAMES.has(norm)) {
    if (/\b(officer|sheriff|deputy|trooper|agent|federal|hpd)\b/.test(norm)) return 'officer';
    if (/\b(unknown|john\s+doe|jane\s+doe|no\s+name)\b/.test(norm)) return 'unknown';
    if (/\b(staff|press|news|editorial|breaking|getty|associated)\b/.test(norm)) return 'byline';
    return 'celebrity';
  }

  if (text && CELEBRITY_CONTEXT_RX.test(text)) {
    const ctxMatch = text.match(CELEBRITY_CONTEXT_RX);
    if (ctxMatch) {
      const ctxIdx = ctxMatch.index;
      const nameIdx = text.toLowerCase().indexOf(norm);
      if (nameIdx >= 0 && Math.abs(nameIdx - ctxIdx) < 300) return 'celebrity';
    }
  }

  if (BYLINE_RX.test(text)) {
    const m = text.match(BYLINE_RX);
    if (m && m[1] && m[1].toLowerCase() === norm) return 'byline';
  }
  if (OFFICIAL_RX.test(text)) {
    const m = text.match(OFFICIAL_RX);
    if (m && m[1] && m[1].toLowerCase() === norm) return 'officer';
  }

  if (/\b(officer|sheriff|deputy|trooper|spokesperson)\b/i.test(trimmed)) return 'officer';
  if (/\b(unknown|john\s+doe|jane\s+doe)\b/i.test(norm)) return 'unknown';
  return 'other';
}

async function _scan(db, { limit, runMode }) {
  const t0 = Date.now();
  const summary = {
    scanned: 0,
    would_demote: 0,
    demoted: 0,
    incidents_pending: 0,
    by_reason: { celebrity: 0, officer: 0, unknown: 0, byline: 0, other: 0, empty: 0 },
    samples: [],
    elapsed_ms: 0,
    timed_out: false,
    next_offset: null
  };

  // Pull persons ordered by created_at DESC (newest first — most likely to
  // have been extracted before each deny-list patch).
  const persons = await db('persons')
    .select('id', 'full_name', 'incident_id', 'victim_verified', 'created_at')
    .whereNotNull('full_name')
    .orderBy('created_at', 'desc')
    .limit(Math.max(1, Math.min(parseInt(limit, 10) || 500, 5000)));

  // Cache parent-incident description lookups.
  const descCache = new Map();
  async function descFor(incidentId) {
    if (!incidentId) return '';
    if (descCache.has(incidentId)) return descCache.get(incidentId);
    let row = null;
    try {
      row = await db('incidents')
        .where('id', incidentId)
        .first('description', 'raw_description');
    } catch (_) { /* raw_description may not exist */ }
    if (!row) {
      try { row = await db('incidents').where('id', incidentId).first('description'); }
      catch (_) {}
    }
    const txt = row ? [row.description, row.raw_description].filter(Boolean).join('\n') : '';
    descCache.set(incidentId, txt);
    return txt;
  }

  // Track which incidents we've demoted from so we can re-check victim count.
  const incidentsToCheck = new Set();
  const demotedIncidentMap = new Map(); // incident_id -> [pid,...]

  for (let i = 0; i < persons.length; i++) {
    if (Date.now() - t0 > TIME_BUDGET_MS) {
      summary.timed_out = true;
      summary.next_offset = persons[i] ? persons[i].created_at : null;
      break;
    }
    const p = persons[i];
    summary.scanned++;
    const text = await descFor(p.incident_id);
    const survived = applyDenyList(p.full_name, text);
    if (survived) continue;

    summary.would_demote++;
    const reason = classifyReason(p.full_name, text);
    summary.by_reason[reason] = (summary.by_reason[reason] || 0) + 1;
    if (summary.samples.length < 25) {
      summary.samples.push({
        id: p.id,
        full_name: p.full_name,
        incident_id: p.incident_id,
        was_verified: !!p.victim_verified,
        reason
      });
    }

    if (!runMode) continue;

    // RUN MODE: actually demote.
    try {
      await db('persons').where('id', p.id).update({
        victim_verified: false,
        victim_verifier_reason: REASON_TAG,
        updated_at: new Date()
      });
      summary.demoted++;
      if (p.incident_id) {
        incidentsToCheck.add(p.incident_id);
        if (!demotedIncidentMap.has(p.incident_id)) demotedIncidentMap.set(p.incident_id, []);
        demotedIncidentMap.get(p.incident_id).push(p.id);
      }

      // Log to enrichment_logs (minimal schema only).
      try {
        await db('enrichment_logs').insert({
          person_id: p.id,
          field_name: 're_extracted_demoted',
          old_value: p.victim_verified ? 'verified' : 'unverified',
          new_value: JSON.stringify({ reason, tag: REASON_TAG, name: p.full_name }),
          created_at: new Date()
        });
      } catch (_) { /* table column drift — silent */ }
    } catch (e) {
      await reportError(db, 're-extract-historical', p.id, 'demote_failed:' + e.message,
        { severity: 'warning' }).catch(() => {});
    }
  }

  // RUN MODE: for every incident that lost a person, see if any verified
  // victims remain. If not, transition qualification_state -> 'pending'.
  if (runMode && incidentsToCheck.size > 0) {
    for (const incidentId of incidentsToCheck) {
      if (Date.now() - t0 > TIME_BUDGET_MS) {
        summary.timed_out = true;
        break;
      }
      try {
        const remaining = await db('persons')
          .where({ incident_id: incidentId, victim_verified: true })
          .count('* as c').first();
        const count = parseInt((remaining && remaining.c) || 0, 10);
        if (count === 0) {
          await db('incidents')
            .where('id', incidentId)
            .whereNot('qualification_state', 'pending')
            .update({ qualification_state: 'pending', updated_at: new Date() });
          summary.incidents_pending++;
        }
      } catch (e) { /* ignore */ }
    }
  }

  summary.elapsed_ms = Date.now() - t0;
  return summary;
}

async function runHealth(db) {
  let personCount = 0;
  let incidentCount = 0;
  try { const r = await db('persons').count('* as c').first(); personCount = parseInt(r.c, 10); } catch (_) {}
  try { const r = await db('incidents').count('* as c').first(); incidentCount = parseInt(r.c, 10); } catch (_) {}
  return {
    ok: true,
    engine: 're-extract-historical',
    phase: 67,
    deny_list_tag: REASON_TAG,
    persons_total: personCount,
    incidents_total: incidentCount,
    hard_ban_size: HARD_BAN_NAMES.size
  };
}

async function runStats(db) {
  const stats = {
    engine: 're-extract-historical',
    by_verification_state: {},
    demoted_last_7d: 0,
    demoted_by_reason_7d: {}
  };
  try {
    const rows = await db('persons')
      .select('victim_verified')
      .count('* as c')
      .groupBy('victim_verified');
    for (const r of rows) {
      const k = r.victim_verified === null ? 'null'
        : (r.victim_verified ? 'verified' : 'unverified');
      stats.by_verification_state[k] = parseInt(r.c, 10);
    }
  } catch (_) {}
  try {
    const since = new Date(Date.now() - 7 * 24 * 3600 * 1000);
    const r = await db('enrichment_logs')
      .where('field_name', 're_extracted_demoted')
      .where('created_at', '>=', since)
      .count('* as c').first();
    stats.demoted_last_7d = parseInt((r && r.c) || 0, 10);

    const samples = await db('enrichment_logs')
      .where('field_name', 're_extracted_demoted')
      .where('created_at', '>=', since)
      .select('new_value')
      .limit(2000);
    for (const s of samples) {
      let parsed = null;
      try { parsed = typeof s.new_value === 'string' ? JSON.parse(s.new_value) : s.new_value; }
      catch (_) {}
      const reason = (parsed && parsed.reason) || 'other';
      stats.demoted_by_reason_7d[reason] = (stats.demoted_by_reason_7d[reason] || 0) + 1;
    }
  } catch (_) {}
  return stats;
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const action = (req.query && req.query.action) || 'health';
  const limit = (req.query && req.query.limit) || 500;
  const db = getDb();

  try {
    if (action === 'health') {
      const r = await runHealth(db);
      return res.status(200).json({ success: true, ...r });
    }
    if (action === 'stats') {
      const r = await runStats(db);
      return res.status(200).json({ success: true, ...r });
    }
    if (action === 'audit') {
      const r = await _scan(db, { limit, runMode: false });
      await trackApiCall(db, 're-extract-historical', 'audit', 0, 0, true).catch(() => {});
      return res.status(200).json({ success: true, action: 'audit', ...r });
    }
    if (action === 'run') {
      const r = await _scan(db, { limit, runMode: true });
      await trackApiCall(db, 're-extract-historical', 'run', 0, 0, true).catch(() => {});
      return res.status(200).json({ success: true, action: 'run', ...r });
    }
    return res.status(400).json({ error: 'unknown action: ' + action });
  } catch (e) {
    await reportError(db, 're-extract-historical', null, e.message,
      { severity: 'error' }).catch(() => {});
    return res.status(500).json({ error: e.message, success: false });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.runHealth = runHealth;
module.exports.runStats = runStats;
module.exports.classifyReason = classifyReason;
