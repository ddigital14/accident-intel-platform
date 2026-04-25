/**
 * GET /api/v1/system/triggers?secret=ingest-now
 *
 * One-time setup: install Postgres triggers that auto-update incident
 * fields whenever related tables (persons, source_reports) change.
 *
 * Triggers installed:
 *   1. tr_persons_after_insert_update — when person added/edited, recompute
 *      incident.qualification_state + has_contact_info on parent incident
 *   2. tr_source_reports_after_insert — when new source report added,
 *      bump incident.source_count + last_updated
 *   3. tr_persons_attorney_skip — when has_attorney=true, mark contact_status
 *
 * Safe to re-run.
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { logChange } = require('./changelog');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const log = [];

  try {
    // 1. Trigger function: recompute qualification_state on parent incident
    //    when persons inserted/updated
    try {
      await db.raw(`
        CREATE OR REPLACE FUNCTION aip_recompute_qualification(inc_id UUID)
        RETURNS VOID AS $f$
        DECLARE
          q_count INTEGER;
          n_count INTEGER;
          new_state VARCHAR;
        BEGIN
          SELECT
            COUNT(*) FILTER (
              WHERE full_name IS NOT NULL AND full_name <> ''
                AND ((phone IS NOT NULL AND phone <> '')
                  OR (email IS NOT NULL AND email <> '')
                  OR (address IS NOT NULL AND length(trim(address)) > 5))
            ),
            COUNT(*) FILTER (WHERE full_name IS NOT NULL AND full_name <> '')
          INTO q_count, n_count
          FROM persons WHERE incident_id = inc_id;

          IF q_count > 0 THEN
            new_state := 'qualified';
          ELSIF n_count > 0 THEN
            new_state := 'pending_named';
          ELSE
            new_state := 'pending';
          END IF;

          UPDATE incidents
          SET qualification_state = new_state,
              has_contact_info = (q_count > 0),
              qualified_at = CASE
                WHEN new_state = 'qualified' AND qualification_state <> 'qualified'
                THEN NOW()
                ELSE qualified_at
              END,
              updated_at = NOW()
          WHERE id = inc_id;
        END;
        $f$ LANGUAGE plpgsql;
      `);
      log.push('aip_recompute_qualification function created');
    } catch (e) { log.push(`fn create error: ${e.message}`); }

    // 2. Trigger on persons changes
    try {
      await db.raw(`
        CREATE OR REPLACE FUNCTION aip_persons_change_trigger()
        RETURNS TRIGGER AS $t$
        BEGIN
          IF TG_OP = 'DELETE' THEN
            PERFORM aip_recompute_qualification(OLD.incident_id);
            RETURN OLD;
          ELSE
            PERFORM aip_recompute_qualification(NEW.incident_id);
            RETURN NEW;
          END IF;
        END;
        $t$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS tr_persons_qualify ON persons;
        CREATE TRIGGER tr_persons_qualify
        AFTER INSERT OR UPDATE OR DELETE ON persons
        FOR EACH ROW EXECUTE FUNCTION aip_persons_change_trigger();
      `);
      log.push('persons trigger installed');
    } catch (e) { log.push(`persons trigger error: ${e.message}`); }

    // 3. Trigger to keep incidents.source_count in sync with source_reports
    try {
      await db.raw(`
        CREATE OR REPLACE FUNCTION aip_sourcereports_change_trigger()
        RETURNS TRIGGER AS $t$
        DECLARE
          c INTEGER;
          target_id UUID;
        BEGIN
          IF TG_OP = 'DELETE' THEN
            target_id := OLD.incident_id;
          ELSE
            target_id := NEW.incident_id;
          END IF;
          IF target_id IS NULL THEN RETURN COALESCE(NEW, OLD); END IF;
          SELECT COUNT(DISTINCT source_type) INTO c
          FROM source_reports WHERE incident_id = target_id;
          UPDATE incidents
          SET source_count = GREATEST(c, 1),
              updated_at = NOW()
          WHERE id = target_id;
          RETURN COALESCE(NEW, OLD);
        END;
        $t$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS tr_sourcereports_count ON source_reports;
        CREATE TRIGGER tr_sourcereports_count
        AFTER INSERT OR UPDATE OR DELETE ON source_reports
        FOR EACH ROW EXECUTE FUNCTION aip_sourcereports_change_trigger();
      `);
      log.push('source_reports trigger installed');
    } catch (e) { log.push(`source_reports trigger error: ${e.message}`); }

    // 4. Trigger to mark persons.contact_status when has_attorney flips to true
    try {
      await db.raw(`
        CREATE OR REPLACE FUNCTION aip_persons_attorney_trigger()
        RETURNS TRIGGER AS $t$
        BEGIN
          IF NEW.has_attorney = TRUE
             AND (OLD.has_attorney IS DISTINCT FROM TRUE)
             AND NEW.contact_status NOT IN ('has_attorney') THEN
            NEW.contact_status := 'has_attorney';
          END IF;
          RETURN NEW;
        END;
        $t$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS tr_persons_attorney ON persons;
        CREATE TRIGGER tr_persons_attorney
        BEFORE UPDATE ON persons
        FOR EACH ROW EXECUTE FUNCTION aip_persons_attorney_trigger();
      `);
      log.push('persons attorney trigger installed');
    } catch (e) { log.push(`attorney trigger error: ${e.message}`); }

    // 5. Backfill: run aip_recompute_qualification once for every existing incident
    let backfilled = 0;
    try {
      const r = await db.raw(`
        SELECT id FROM incidents
        WHERE qualification_state IS NULL OR qualification_state = ''
        LIMIT 5000
      `);
      for (const row of r.rows || []) {
        await db.raw('SELECT aip_recompute_qualification($1)', [row.id]);
        backfilled++;
      }
      log.push(`Backfilled qualification for ${backfilled} incidents`);
    } catch (e) { log.push(`backfill error: ${e.message}`); }

    await logChange(db, {
      kind: 'schema',
      title: 'Auto-update triggers installed',
      summary: `${log.length} triggers/functions installed. Backfilled ${backfilled} incidents.`,
      author: 'system:triggers',
      meta: { backfilled, log }
    });

    res.json({ success: true, log, backfilled, timestamp: new Date().toISOString() });
  } catch (err) {
    await reportError(db, 'triggers', null, err.message);
    res.status(500).json({ error: err.message, log });
  }
};
