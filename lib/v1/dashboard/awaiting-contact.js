/**
 * Phase 38 v2: rep-handoff view — verified victims with no contact yet.
 * These are real, US, recent accident victims that the auto-resolver couldn't
 * fully populate. Reps work them manually with this prioritized list.
 */
const { getDb } = require('../../_db');

module.exports = async function handler(req, res) {
  const db = getDb();
  if (req.query?.secret !== 'ingest-now') return res.status(401).json({ error: 'unauthorized' });
  const limit = Math.min(parseInt(req.query.limit) || 50, 200);
  try {
    const rows = await db.raw(`
      SELECT
        p.id          AS person_id,
        p.full_name,
        p.victim_role,
        p.phone,
        p.email,
        p.address,
        i.id          AS incident_id,
        i.city,
        i.state,
        i.incident_type,
        i.severity,
        i.address     AS incident_address,
        i.discovered_at,
        i.lead_score,
        i.qualification_state
      FROM persons p
      JOIN incidents i ON i.id = p.incident_id
      WHERE COALESCE(p.victim_verified, false) = true
        AND (p.phone IS NULL OR p.phone = '')
        AND (p.email IS NULL OR p.email = '')
      ORDER BY COALESCE(i.lead_score, 0) DESC, i.discovered_at DESC
      LIMIT ${limit}
    `).then(r => r.rows || r);
    return res.json({
      success: true,
      count: rows.length,
      victims_awaiting_contact: rows.map(r => ({
        person_id: r.person_id,
        name: r.full_name,
        role: r.victim_role,
        incident: {
          id: r.incident_id,
          city: r.city,
          state: r.state,
          type: r.incident_type,
          severity: r.severity,
          headline: (r.incident_address || '').slice(0, 200),
          score: r.lead_score,
          state: r.qualification_state,
          discovered: r.discovered_at
        },
        rep_actions: [
          'Search ' + r.full_name + ' on FastPeopleSearch / TruePeopleSearch',
          'Cross-ref ' + (r.city || '') + ', ' + (r.state || '') + ' on Apollo /people/match',
          'Run PDL Pro Enrichment with min_likelihood=1',
          'Check Maricopa property records if AZ',
          'Google CSE: ' + r.full_name + ' obituary OR linkedin OR facebook'
        ]
      })),
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
};
