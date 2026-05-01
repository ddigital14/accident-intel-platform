/**
 * Lead qualification + scoring engine
 *
 * QUALIFICATION RULE (per Mason 2026-04-25):
 *   An incident is QUALIFIED if it has at least one person with:
 *     - non-empty full_name (or first_name + last_name) AND
 *     - at least one of: phone, email, address (street, not just city)
 *
 * Otherwise: PENDING (still useful — gets enrichment passes)
 *
 * LEAD SCORE 0-100 = severity_weight × contact_completeness × recency_decay × multi_source_bonus
 */

const SEVERITY_WEIGHTS = {
  fatal: 100, critical: 90, serious: 75, moderate: 55, minor: 35, unknown: 25
};

function isQualifiedPerson(p) {
  const hasName = !!(p.full_name && p.full_name.trim().length > 2)
    || !!(p.first_name && p.last_name);
  if (!hasName) return false;
  const hasContact = !!(
    (p.phone && p.phone.trim()) ||
    (p.email && p.email.trim()) ||
    (p.address && p.address.trim().length > 5)  // street-level, not just city
  );
  return hasContact;
}

function contactCompleteness(persons) {
  if (!persons || persons.length === 0) return 0;
  let bestScore = 0;
  for (const p of persons) {
    if (!p.full_name && !p.last_name) continue;
    let score = 30;  // having a name at all
    if (p.phone) score += 25;
    if (p.email) score += 20;
    if (p.address) score += 15;
    if (p.phone_verified) score += 5;
    if (p.email_verified) score += 5;
    if (p.insurance_company) score += 5;
    if (p.is_injured && p.injury_severity) score += 5;
    if (p.has_attorney === false) score += 10;  // no attorney = warmer lead
    if (p.has_attorney === true) score -= 30;   // already represented
    bestScore = Math.max(bestScore, Math.min(100, score));
  }
  return bestScore;
}

function recencyDecay(occurredAt) {
  if (!occurredAt) return 50;
  const ageHours = (Date.now() - new Date(occurredAt).getTime()) / 3600000;
  if (ageHours < 1) return 100;
  if (ageHours < 6) return 95;
  if (ageHours < 24) return 85;
  if (ageHours < 72) return 75;
  if (ageHours < 168) return 60;     // <1 week
  if (ageHours < 720) return 50;     // <30 days — still warm; statute of limitations 1-2yr typically
  if (ageHours < 2160) return 40;    // <90 days
  if (ageHours < 4320) return 30;    // <180 days
  return 20;
}

function multiSourceBonus(sourceCount) {
  // 1=0%, 2=+10%, 3=+18%, 4+=+25%
  const c = parseInt(sourceCount) || 1;
  if (c >= 4) return 1.25;
  if (c === 3) return 1.18;
  if (c === 2) return 1.10;
  return 1.0;
}

function computeLeadScore(incident, persons) {
  const sevW = SEVERITY_WEIGHTS[incident.severity] || 25;
  const contact = contactCompleteness(persons);
  const recency = recencyDecay(incident.occurred_at || incident.discovered_at);
  const sourceBonus = multiSourceBonus(incident.source_count);
  // weighted product, normalized to 0-100
  const raw = (sevW * 0.40) + (contact * 0.40) + (recency * 0.20);
  let score = Math.min(100, Math.round(raw * sourceBonus));
  // Phase 65: apply pattern-miner historical signal delta if present.
  // patternDelta is attached by callers that have already invoked applySignals().
  if (typeof incident._pattern_delta === 'number') {
    score = Math.max(0, Math.min(100, score + incident._pattern_delta));
  }
  return score;
}

/**
 * Phase 65: enrich an incident with pattern-miner delta before scoring.
 * Async because it queries lead_score_signals.
 */
async function applyPatternDelta(db, incident) {
  try {
    const pm = require('./pattern-miner');
    const r = await pm.applySignals(db, incident.id);
    if (r?.ok && typeof r.adjusted_score_delta === 'number') {
      incident._pattern_delta = r.adjusted_score_delta;
    }
  } catch (_) {}
  return incident;
}

/**
 * Evaluate a single incident — returns {state, score, qualified_persons}
 * Pass in pre-fetched persons array if you have it; otherwise fetches.
 */
async function evaluateIncident(db, incident, persons = null) {
  await applyPatternDelta(db, incident);
  if (!persons) {
    persons = await db('persons').where('incident_id', incident.id).select('*');
  }
  // Phase 67: filter persons through current deny-list — never promote celebs/officers/unknowns
  try {
    const _filter = require('../enrich/_name_filter');
    const surroundingText = (incident.description || incident.raw_description || '');
    persons = persons.filter(p => {
      if (!p.full_name) return true; // partial victims still count for pending_named
      const survives = _filter.applyDenyList(p.full_name, surroundingText);
      return survives !== null;
    });
  } catch (_) {}
  const qualified = persons.filter(isQualifiedPerson);
  const state = qualified.length > 0 ? 'qualified' : (persons.length > 0 ? 'pending_named' : 'pending');
  const score = computeLeadScore(incident, persons);
  return { state, score, qualified_persons: qualified.length, total_persons: persons.length };
}

let _columnsEnsured = false;
async function ensureColumns(db) {
  if (_columnsEnsured) return;
  try {
    await db.raw(`
      ALTER TABLE incidents ADD COLUMN IF NOT EXISTS lead_score INTEGER DEFAULT 0;
      ALTER TABLE incidents ADD COLUMN IF NOT EXISTS qualification_state VARCHAR(20) DEFAULT 'pending';
      ALTER TABLE incidents ADD COLUMN IF NOT EXISTS qualified_at TIMESTAMPTZ;
      ALTER TABLE incidents ADD COLUMN IF NOT EXISTS has_contact_info BOOLEAN DEFAULT FALSE;
      ALTER TABLE incidents ADD COLUMN IF NOT EXISTS notified_at TIMESTAMPTZ;
      CREATE INDEX IF NOT EXISTS idx_incidents_qualification_state ON incidents(qualification_state);
      CREATE INDEX IF NOT EXISTS idx_incidents_lead_score ON incidents(lead_score DESC);
      CREATE INDEX IF NOT EXISTS idx_incidents_qualified_at ON incidents(qualified_at DESC NULLS LAST);
    `);
    _columnsEnsured = true;
  } catch (e) {
    console.error('qualify: ensureColumns failed', e.message);
  }
}

module.exports = {
  ensureColumns,
  evaluateIncident,
  isQualifiedPerson,
  contactCompleteness,
  computeLeadScore,
  SEVERITY_WEIGHTS,
};
