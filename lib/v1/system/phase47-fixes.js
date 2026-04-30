/**
 * Phase 47: 3 high-impact fixes
 * 1. Propagate incident.city/state -> persons.city/state for all persons missing geo
 * 2. Dedup Tancredo Bankhardt (and any other duplicate persons on different incidents)
 * 3. Add a database trigger so future inserts auto-fill from incident
 */
const { getDb } = require('../../_db');

module.exports = async function handler(req, res) {
  if (req.query?.secret !== 'ingest-now') return res.status(401).json({ error: 'unauthorized' });
  const db = getDb();
  const out = { propagated: 0, deduped: 0, trigger_installed: false };
  try {
    // ===== FIX 1: Propagate geo from incidents to persons where person is missing it =====
    const propResult = await db.raw(`
      UPDATE persons p SET
        city = COALESCE(p.city, i.city),
        state = COALESCE(p.state, i.state),
        updated_at = NOW()
      FROM incidents i
      WHERE p.incident_id = i.id
        AND (
          (p.city IS NULL AND i.city IS NOT NULL)
          OR (p.state IS NULL AND i.state IS NOT NULL)
        )
      RETURNING p.id
    `);
    out.propagated = (propResult.rows || propResult).length || 0;

    // ===== FIX 2: Install Postgres trigger for future inserts =====
    try {
      await db.raw(`
        CREATE OR REPLACE FUNCTION aip_persons_inherit_geo()
        RETURNS TRIGGER AS $$
        DECLARE
          inc_city TEXT;
          inc_state TEXT;
        BEGIN
          IF NEW.incident_id IS NOT NULL AND (NEW.city IS NULL OR NEW.state IS NULL) THEN
            SELECT city, state INTO inc_city, inc_state FROM incidents WHERE id = NEW.incident_id;
            IF NEW.city IS NULL THEN NEW.city := inc_city; END IF;
            IF NEW.state IS NULL THEN NEW.state := inc_state; END IF;
          END IF;
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
      `);
      await db.raw(`DROP TRIGGER IF EXISTS persons_inherit_geo_trg ON persons`);
      await db.raw(`
        CREATE TRIGGER persons_inherit_geo_trg
        BEFORE INSERT OR UPDATE ON persons
        FOR EACH ROW EXECUTE FUNCTION aip_persons_inherit_geo()
      `);
      out.trigger_installed = true;
    } catch (e) { out.trigger_error = e.message; }

    // ===== FIX 3: Dedup persons by name (when same name on multiple recent incidents) =====
    // Find names that appear 2+ times across DIFFERENT incidents (likely AI-extracted dupes)
    const dupes = await db.raw(`
      SELECT full_name, COUNT(DISTINCT incident_id) AS incident_count, ARRAY_AGG(id ORDER BY created_at) AS ids
      FROM persons
      WHERE COALESCE(victim_verified, false) = true
        AND full_name IS NOT NULL AND full_name <> ''
      GROUP BY full_name
      HAVING COUNT(DISTINCT incident_id) > 1
    `);
    const dupeRows = dupes.rows || dupes;
    const samples = [];
    for (const dr of dupeRows) {
      // Keep the FIRST (oldest) record, demote the rest to victim_verified=false
      const keep = dr.ids[0];
      const drop = dr.ids.slice(1);
      if (drop.length) {
        const res = await db('persons').whereIn('id', drop).update({
          victim_verified: false,
          victim_role: 'duplicate',
          victim_verifier_reason: 'duplicate_of:' + keep,
          updated_at: new Date()
        });
        out.deduped += res;
        if (samples.length < 5) samples.push({ name: dr.full_name, kept: keep, dropped: drop.length });
      }
    }
    out.dedup_samples = samples;

    return res.json({ success: true, ...out, timestamp: new Date().toISOString() });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
};
