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
  if (ageHours < 24) return 80;
  if (ageHours < 72) return 60;
  if (ageHours < 168) return 40;
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
  return Math.min(100, Math.round(raw * sourceBonus));
}

/**
 * Evaluate a single incident — returns {state, score, qualified_persons}
 * Pass in pre-fetched persons array if you have it; otherwise fetches.
 */
async function evaluateIncident(db, incident, persons = null) {
  if (!persons) {
    persons = await db('persons').where('incident_id', incident.id).select('*');
  }
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
