/**
 * Phase 95: DB Diagnostic — answers "what data do we actually have?"
 */
const { getDb } = require('../../_db');

const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function safe(fn) {
  try { return await fn(); } catch (e) { return { error: e.message }; }
}

module.exports = async function handler(req, res) {
  if (!authed(req)) return res.status(401).json({ error: 'unauthorized' });
  const db = getDb();
  const out = {};

  out.persons_total = await safe(async () => (await db('persons').count('* as c').first()).c);
  out.persons_by_tier = await safe(async () => db('persons').select('lead_tier').count('* as c').groupBy('lead_tier'));
  out.persons_by_source = await safe(async () => db('persons').select('source').count('* as c').groupBy('source').orderBy('c','desc').limit(20));
  out.persons_with_full_name = await safe(async () => (await db('persons').whereNotNull('full_name').where(db.raw('length(full_name) >= 5')).count('* as c').first()).c);
  out.persons_with_phone = await safe(async () => (await db('persons').whereNotNull('phone').count('* as c').first()).c);
  out.persons_with_email = await safe(async () => (await db('persons').whereNotNull('email').count('* as c').first()).c);
  out.persons_with_address = await safe(async () => (await db('persons').whereNotNull('address').count('* as c').first()).c);
  out.persons_with_state = await safe(async () => (await db('persons').whereNotNull('state').count('* as c').first()).c);
  out.persons_verified = await safe(async () => (await db('persons').where('victim_verified', true).count('* as c').first()).c);

  out.named_but_not_qualified = await safe(async () => (await db.raw(`
    SELECT COUNT(*) as c FROM persons p
    JOIN incidents i ON i.id = p.incident_id
    WHERE p.full_name IS NOT NULL AND length(p.full_name) >= 5
      AND (i.qualification_state IS NULL OR i.qualification_state != 'qualified')
  `)).rows[0].c);

  out.named_unqualified_by_state = await safe(async () => (await db.raw(`
    SELECT i.state, COUNT(DISTINCT p.id) as named_persons
    FROM persons p JOIN incidents i ON i.id = p.incident_id
    WHERE p.full_name IS NOT NULL AND length(p.full_name) >= 5
      AND (i.qualification_state IS NULL OR i.qualification_state != 'qualified')
    GROUP BY i.state ORDER BY named_persons DESC LIMIT 15
  `)).rows);

  out.named_unqualified_sample = await safe(async () => (await db.raw(`
    SELECT p.id, p.full_name, p.role, p.lead_tier,
           p.phone IS NOT NULL as has_phone,
           p.email IS NOT NULL as has_email,
           p.address IS NOT NULL as has_address,
           i.state, i.city, i.severity, i.lead_score,
           i.qualification_state, i.occurred_at, i.incident_number
    FROM persons p JOIN incidents i ON i.id = p.incident_id
    WHERE p.full_name IS NOT NULL AND length(p.full_name) >= 5
      AND (i.qualification_state IS NULL OR i.qualification_state != 'qualified')
    ORDER BY i.occurred_at DESC NULLS LAST LIMIT 15
  `)).rows);

  out.incidents_total = await safe(async () => (await db('incidents').count('* as c').first()).c);
  out.incidents_with_persons = await safe(async () => (await db.raw(`
    SELECT COUNT(DISTINCT i.id) as c FROM incidents i
    JOIN persons p ON p.incident_id = i.id
  `)).rows[0].c);

  out.nameless_by_source = await safe(async () => (await db.raw(`
    SELECT
      CASE
        WHEN incident_number LIKE 'nyc-opendata:%' THEN 'nyc-opendata'
        WHEN incident_number LIKE 'sf-datasf:%' THEN 'sf-datasf'
        WHEN incident_number LIKE 'chicago-socrata:%' THEN 'chicago-socrata'
        WHEN incident_number LIKE 'la-opendata:%' THEN 'la-opendata'
        WHEN incident_number LIKE 'cook-me:%' THEN 'cook-me'
        WHEN incident_number LIKE 'patch:%' THEN 'patch'
        ELSE 'other'
      END as src, severity, COUNT(*) as c
    FROM incidents i
    WHERE NOT EXISTS (SELECT 1 FROM persons p WHERE p.incident_id = i.id)
    GROUP BY src, severity ORDER BY c DESC LIMIT 30
  `)).rows);

  out.nameless_fatal_recent = await safe(async () => (await db.raw(`
    SELECT COUNT(*) as c FROM incidents i
    WHERE NOT EXISTS (SELECT 1 FROM persons p WHERE p.incident_id = i.id)
      AND severity IN ('fatal','critical')
      AND occurred_at > NOW() - INTERVAL '14 days'
  `)).rows[0].c);

  out.cross_match_opportunity = await safe(async () => (await db.raw(`
    SELECT i.state, COUNT(DISTINCT i.id) as nameless_fatals
    FROM incidents i
    WHERE NOT EXISTS (SELECT 1 FROM persons p WHERE p.incident_id = i.id)
      AND severity IN ('fatal','critical')
      AND occurred_at > NOW() - INTERVAL '30 days'
    GROUP BY i.state ORDER BY nameless_fatals DESC LIMIT 10
  `)).rows);

  out.voter_records_total = await safe(async () => (await db('voter_records').count('* as c').first()).c);
  out.voter_records_by_state = await safe(async () => db('voter_records').select('state').count('* as c').groupBy('state').orderBy('c','desc').limit(10));
  out.property_records_total = await safe(async () => (await db('property_records').count('* as c').first()).c);
  out.family_bridges_total = await safe(async () => (await db('family_bridges').count('* as c').first()).c);
  out.identity_candidates = await safe(async () => (await db('person_identity_candidates').count('* as c').first()).c);

  return res.status(200).json({ ok: true, ...out });
};
