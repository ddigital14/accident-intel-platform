/**
 * Phase 46: targeted cleanup — clear known false-positive contacts:
 * 1. Patrick Ramsey: Fort Worth TX namesake phone (incident is Atlanta GA)
 * 2. Teen Girl: sex-offender article misclassified — quarantine
 */
const { getDb } = require('../../_db');

module.exports = async function handler(req, res) {
  if (req.query?.secret !== 'ingest-now') return res.status(401).json({ error: 'unauthorized' });
  const db = getDb();
  const out = { patrick_cleared: 0, teen_girl_quarantined: 0, demoted_orphans: 0, samples: [] };
  try {
    // 1. Patrick Ramsey GA — null any non-GA phone/address
    const patrickFix = await db.raw(`
      UPDATE persons p SET phone = NULL, email = NULL, address = NULL, updated_at = NOW()
      FROM incidents i
      WHERE p.incident_id = i.id
        AND p.full_name ILIKE 'Patrick Ramsey%'
        AND i.state = 'GA'
        AND (
          (p.phone IS NOT NULL AND p.phone NOT LIKE '404%' AND p.phone NOT LIKE '470%' AND p.phone NOT LIKE '678%' AND p.phone NOT LIKE '770%' AND p.phone NOT LIKE '762%')
          OR (p.address IS NOT NULL AND p.address NOT ILIKE '%GA%' AND p.address NOT ILIKE '%Atlanta%' AND p.address NOT ILIKE '%Georgia%' AND p.address NOT ILIKE '%Powder Springs%' AND p.address NOT ILIKE '%Marietta%')
        )
      RETURNING p.id, p.full_name
    `);
    out.patrick_cleared = (patrickFix.rows || patrickFix).length || 0;

    // 2. Teen Girl quarantine
    const teenFix = await db('persons')
      .where('full_name', 'ILIKE', 'Teen Girl%')
      .update({
        victim_verified: false,
        victim_role: 'misclassified',
        victim_verifier_reason: 'misclassified_not_accident_victim',
        updated_at: new Date()
      });
    out.teen_girl_quarantined = teenFix;

    // 3. Demote orphans
    const orphans = await db.raw(`
      SELECT i.id FROM incidents i
      LEFT JOIN persons p ON p.incident_id = i.id AND COALESCE(p.victim_verified, false) = true
      WHERE i.qualification_state = 'qualified'
      GROUP BY i.id
      HAVING COUNT(p.id) = 0
    `);
    for (const o of orphans.rows || orphans) {
      await db('incidents').where({ id: o.id }).update({
        qualification_state: 'pending_unverified', qualified_at: null, updated_at: new Date()
      });
      out.demoted_orphans++;
    }

    return res.json({ success: true, ...out, timestamp: new Date().toISOString() });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
};
