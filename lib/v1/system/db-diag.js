/**
 * Phase 95: DB Diagnostic — answers "what data do we actually have?"
 *
 * Mason's insight: 11 qualified leads is just the TOP of the funnel. We have
 * 118 persons in DB and 1137 incidents — let's see what's named, unmatched,
 * orphaned, etc. so we know what we can piece together.
 */
const { getDb } = require('../../_db');

const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

module.exports = async function handler(req, res) {
  if (!authed(req)) return res.status(401).json({ error: 'unauthorized' });
  const db = getDb();
  const out = {};

  // Persons: total, by tier, by source, attached vs orphan
  out.persons_total = (await db('persons').count('* as c').first()).c;
  out.persons_by_tier = await db('persons').select('lead_tier').count('* as c').groupBy('lead_tier');
  out.persons_by_source = await db('persons').select('source').count('* as c').groupBy('source').orderBy('c','desc').limit(20);
  out.persons_with_full_name = (await db('persons').whereNotNull('full_name').where(db.raw('length(full_name) >= 5')).count('* as c').first()).c;
  out.persons_with_phone = (await db('persons').whereNotNull('phone').count('* as c').first()).c;
  out.persons_with_email = (await db('persons').whereNotNull('email').count('* as c').first()).c;
  out.persons_with_address = (await db('persons').whereNotNull('address').count('* as c').first()).c;
  out.persons_with_state = (await db('persons').whereNotNull('state').count('* as c').first()).c;
  out.persons_verified = (await db('persons').where('victim_verified', true).count('* as c').first()).c;
  out.persons_orphan = (await db.raw(`
    SELECT COUNT(*) as c FROM persons p WHERE NOT EXISTS (SELECT 1 FROM incidents i WHERE i.id = p.incident_id)
  `)).rows[0].c;

  // Persons WITH NAME but NOT QUALIFIED — Mason's question
  out.named_but_not_qualified = (await db.raw(`
    SELECT COUNT(*) as c FROM persons p
    JOIN incidents i ON i.id = p.incident_id
    WHERE p.full_name IS NOT NULL AND length(p.full_name) >= 5
      AND (i.qualification_state IS NULL OR i.qualification_state != 'qualified')
  `)).rows[0].c;

  // Sample of named-but-not-qualified by state
  out.named_unqualified_by_state = (await db.raw(`
    SELECT i.state, COUNT(DISTINCT p.id) as named_persons
    FROM persons p JOIN incidents i ON i.id = p.incident_id
    WHERE p.full_name IS NOT NULL AND length(p.full_name) >= 5
      AND (i.qualification_state IS NULL OR i.qualification_state != 'qualified')
    GROUP BY i.state ORDER BY named_persons DESC LIMIT 15
  `)).rows;

  // Sample 10 named-but-not-qualified persons
  out.named_unqualified_sample = (await db.raw(`
    SELECT p.id, p.full_name, p.role, p.lead_tier, p.victim_verified,
           p.phone IS NOT NULL as has_phone,
           p.email IS NOT NULL as has_email,
           p.address IS NOT NULL as has_address,
           i.state, i.city, i.severity, i.incident_type, i.lead_score,
           i.qualification_state, i.occurred_at, i.incident_number
    FROM persons p JOIN incidents i ON i.id = p.incident_id
    WHERE p.full_name IS NOT NULL AND length(p.full_name) >= 5
      AND (i.qualification_state IS NULL OR i.qualification_state != 'qualified')
    ORDER BY i.occurred_at DESC NULLS LAST LIMIT 15
  `)).rows;

  // Incidents: total, with persons, without persons
  out.incidents_total = (await db('incidents').count('* as c').first()).c;
  out.incidents_with_persons = (await db.raw(`
    SELECT COUNT(DISTINCT i.id) as c FROM incidents i
    JOIN persons p ON p.incident_id = i.id
  `)).rows[0].c;
  out.incidents_nameless = parseInt(out.incidents_total) - parseInt(out.incidents_with_persons);

  // Nameless incidents broken down by source prefix + severity
  out.nameless_by_source = (await db.raw(`
    SELECT
      CASE
        WHEN incident_number LIKE 'nyc-opendata:%' THEN 'nyc-opendata'
        WHEN incident_number LIKE 'sf-datasf:%' THEN 'sf-datasf'
        WHEN incident_number LIKE 'chicago-socrata:%' THEN 'chicago-socrata'
        WHEN incident_number LIKE 'la-opendata:%' THEN 'la-opendata'
        WHEN incident_number LIKE 'cook-me:%' THEN 'cook-me'
        WHEN incident_number LIKE 'patch:%' THEN 'patch'
        WHEN incident_number LIKE 'sheriff%' THEN 'sheriff'
        WHEN incident_number LIKE 'caringbridge:%' THEN 'caringbridge'
        WHEN incident_number LIKE 'social:%' THEN 'social'
        WHEN incident_number LIKE 'gofundme%' THEN 'gofundme'
        ELSE 'news/other'
      END as src,
      severity,
      COUNT(*) as c
    FROM incidents i
    WHERE NOT EXISTS (SELECT 1 FROM persons p WHERE p.incident_id = i.id)
    GROUP BY src, severity
    ORDER BY c DESC LIMIT 30
  `)).rows;

  // Nameless FATAL/CRITICAL incidents in last 14 days (these are the highest-value targets for crash-news bridging)
  out.nameless_fatal_recent = (await db.raw(`
    SELECT COUNT(*) as c FROM incidents i
    WHERE NOT EXISTS (SELECT 1 FROM persons p WHERE p.incident_id = i.id)
      AND severity IN ('fatal','critical')
      AND occurred_at > NOW() - INTERVAL '14 days'
  `)).rows[0].c;

  // Cross-match opportunity: named persons in state X with nameless fatal incidents in state X
  out.cross_match_opportunity = (await db.raw(`
    SELECT i.state, COUNT(DISTINCT i.id) as nameless_fatals
    FROM incidents i
    WHERE NOT EXISTS (SELECT 1 FROM persons p WHERE p.incident_id = i.id)
      AND severity IN ('fatal','critical')
      AND occurred_at > NOW() - INTERVAL '30 days'
    GROUP BY i.state ORDER BY nameless_fatals DESC LIMIT 10
  `)).rows;

  // Voter roll loaded data
  try {
    out.voter_records_total = (await db('voter_records').count('* as c').first()).c;
    out.voter_records_by_state = await db('voter_records').select('state').count('* as c').groupBy('state').orderBy('c','desc').limit(10);
  } catch (e) {
    out.voter_records_total = 'table_missing_or_error';
  }

  // Property records
  try {
    out.property_records_total = (await db('property_records').count('* as c').first()).c;
  } catch { out.property_records_total = 'table_missing'; }

  // Family graph bridges
  try {
    out.family_bridges_total = (await db('family_bridges').count('* as c').first()).c;
  } catch { out.family_bridges_total = 'table_missing'; }

  // Person identity candidates from deep-dive-narrow
  try {
    out.identity_candidates = (await db('person_identity_candidates').count('* as c').first()).c;
  } catch { out.identity_candidates = 'table_missing'; }

  return res.status(200).json({ ok: true, ...out });
};
