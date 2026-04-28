/**
 * Phase 38: demote any incident currently 'qualified' that has ZERO victim_verified persons.
 * The new rule (RULES.md): qualified requires >=1 verified victim. Old incidents promoted by
 * news-scoring alone get pushed back to pending so reps don't waste time.
 */
const { getDb } = require('../../_db');

module.exports = async function handler(req, res) {
  const db = getDb();
  if (req.query?.secret !== 'ingest-now') return res.status(401).json({ error: 'unauthorized' });
  try {
    // Find qualified incidents with no verified victim
    const orphans = await db.raw(`
      SELECT i.id, i.qualification_state, COUNT(p.id) AS person_count,
             COUNT(p.id) FILTER (WHERE COALESCE(p.victim_verified, false) = true) AS verified_count
      FROM incidents i
      LEFT JOIN persons p ON p.incident_id = i.id
      WHERE i.qualification_state = 'qualified'
      GROUP BY i.id
      HAVING COUNT(p.id) FILTER (WHERE COALESCE(p.victim_verified, false) = true) = 0
    `).then(r => r.rows || r);

    let demoted = 0, demotedNoPerson = 0, demotedUnverifiedPerson = 0;
    for (const row of orphans) {
      const newState = row.person_count > 0 ? 'pending_named' : 'pending';
      await db('incidents').where({ id: row.id }).update({
        qualification_state: newState,
        qualified_at: null,
        updated_at: new Date()
      });
      demoted++;
      if (row.person_count > 0) demotedUnverifiedPerson++;
      else demotedNoPerson++;
    }

    return res.json({
      success: true,
      orphans_found: orphans.length,
      demoted,
      demoted_no_persons: demotedNoPerson,
      demoted_only_unverified_persons: demotedUnverifiedPerson,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
};
