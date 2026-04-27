/**
 * GET /api/v1/system/audit
 *
 * Cross-engine consistency audit:
 *   - Orphan source_reports (no parent incident)
 *   - Orphan persons (no parent incident)
 *   - Incidents with invalid severity / incident_type
 *   - Incidents missing geom but having lat/lng
 *   - persons with mismatched name fields (full_name vs first/last)
 *   - source_reports with unknown source_type
 *   - qualification_state inconsistent with persons (caught by triggers but verifies)
 *
 * Returns issues + counts. Use ?fix=true to auto-repair safe issues.
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const {
  SEVERITY, INCIDENT_TYPES, SOURCE_TYPES, normalizeSeverity, normalizeIncidentType
} = require('../../_schema');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const db = getDb();
  const fix = req.query.fix === 'true';
  if (fix) {
    const secret = req.query.secret || req.headers['x-cron-secret'];
    if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
      return res.status(401).json({ error: 'Unauthorized for fix' });
    }
  }
  const issues = {};
  const fixes = {};

  try {
    // 1. Orphan source_reports
    issues.orphan_source_reports = await db.raw(`
      SELECT COUNT(*) as c FROM source_reports sr
      WHERE NOT EXISTS (SELECT 1 FROM incidents i WHERE i.id = sr.incident_id)
    `).then(r => parseInt(r.rows[0].c)).catch(() => 0);
    if (fix && issues.orphan_source_reports > 0) {
      const r = await db.raw(`
        DELETE FROM source_reports sr
        WHERE NOT EXISTS (SELECT 1 FROM incidents i WHERE i.id = sr.incident_id)
      `);
      fixes.orphan_source_reports_deleted = r.rowCount;
    }

    // 2. Orphan persons
    issues.orphan_persons = await db.raw(`
      SELECT COUNT(*) as c FROM persons p
      WHERE NOT EXISTS (SELECT 1 FROM incidents i WHERE i.id = p.incident_id)
    `).then(r => parseInt(r.rows[0].c)).catch(() => 0);
    if (fix && issues.orphan_persons > 0) {
      const r = await db.raw(`
        DELETE FROM persons p
        WHERE NOT EXISTS (SELECT 1 FROM incidents i WHERE i.id = p.incident_id)
      `);
      fixes.orphan_persons_deleted = r.rowCount;
    }

    // 3. Invalid severity values
    const sevList = SEVERITY.map(s => `'${s}'`).join(',');
    issues.invalid_severity = await db.raw(`
      SELECT severity, COUNT(*) as c FROM incidents
      WHERE severity NOT IN (${sevList}) OR severity IS NULL
      GROUP BY severity
    `).then(r => r.rows || []).catch(() => []);
    if (fix && issues.invalid_severity.length > 0) {
      let fixed = 0;
      for (const row of issues.invalid_severity) {
        const norm = normalizeSeverity(row.severity);
        const r = await db('incidents').where('severity', row.severity).update({ severity: norm });
        fixed += r;
      }
      fixes.severity_fixed = fixed;
    }

    // 4. Invalid incident_type
    const typeList = INCIDENT_TYPES.map(t => `'${t}'`).join(',');
    issues.invalid_incident_type = await db.raw(`
      SELECT incident_type, COUNT(*) as c FROM incidents
      WHERE incident_type NOT IN (${typeList}) OR incident_type IS NULL
      GROUP BY incident_type
    `).then(r => r.rows || []).catch(() => []);
    if (fix && issues.invalid_incident_type.length > 0) {
      let fixed = 0;
      for (const row of issues.invalid_incident_type) {
        const norm = normalizeIncidentType(row.incident_type);
        const r = await db('incidents').where('incident_type', row.incident_type || '').update({ incident_type: norm });
        fixed += r;
      }
      fixes.incident_type_fixed = fixed;
    }

    // 5. Incidents with lat/lng but no geom (PostGIS not yet backfilled)
    issues.missing_geom = await db.raw(`
      SELECT COUNT(*) as c FROM incidents
      WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND geom IS NULL
    `).then(r => parseInt(r.rows[0].c)).catch(() => 0);
    if (fix && issues.missing_geom > 0) {
      const r = await db.raw(`
        UPDATE incidents
        SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND geom IS NULL
      `);
      fixes.geom_backfilled = r.rowCount || 0;
    }

    // 6. Persons with full_name but no first/last (or vice versa)
    issues.persons_name_mismatch = await db.raw(`
      SELECT COUNT(*) as c FROM persons
      WHERE full_name IS NOT NULL AND (first_name IS NULL OR last_name IS NULL)
    `).then(r => parseInt(r.rows[0].c)).catch(() => 0);
    if (fix && issues.persons_name_mismatch > 0) {
      const r = await db.raw(`
        UPDATE persons
        SET first_name = COALESCE(first_name, split_part(full_name, ' ', 1)),
            last_name = COALESCE(last_name, split_part(full_name, ' ', array_length(string_to_array(full_name, ' '), 1)))
        WHERE full_name IS NOT NULL AND (first_name IS NULL OR last_name IS NULL)
      `);
      fixes.persons_name_filled = r.rowCount || 0;
    }

    // 7. Source reports with unknown source_type
    const knownSources = SOURCE_TYPES.map(s => `'${s}'`).join(',');
    issues.unknown_source_types = await db.raw(`
      SELECT source_type, COUNT(*) as c FROM source_reports
      WHERE source_type NOT IN (${knownSources})
      GROUP BY source_type
      ORDER BY c DESC LIMIT 20
    `).then(r => r.rows || []).catch(() => []);

    // 8. Qualification state mismatch (trigger should keep these in sync)
    issues.qualification_drift = await db.raw(`
      SELECT i.id FROM incidents i
      WHERE i.qualification_state = 'qualified' AND NOT EXISTS (
        SELECT 1 FROM persons p WHERE p.incident_id = i.id
          AND p.full_name IS NOT NULL
          AND (p.phone IS NOT NULL OR p.email IS NOT NULL OR p.address IS NOT NULL)
      )
      LIMIT 100
    `).then(r => r.rows.length).catch(() => 0);
    if (fix && issues.qualification_drift > 0) {
      // Trigger function will fix this for us
      const r = await db.raw(`
        SELECT id FROM incidents
        WHERE qualification_state = 'qualified' AND NOT EXISTS (
          SELECT 1 FROM persons p WHERE p.incident_id = incidents.id
            AND p.full_name IS NOT NULL
            AND (p.phone IS NOT NULL OR p.email IS NOT NULL OR p.address IS NOT NULL)
        )
      `);
      let fixed = 0;
      for (const row of r.rows || []) {
        await db.raw('SELECT aip_recompute_qualification($1)', [row.id]).catch(() => {});
        fixed++;
      }
      fixes.qualification_recomputed = fixed;
    }

    // 10. Phase 24 — ZERO-FAKE-DATA AUDIT
    // Detect persons/incidents originating from database/seeds/002_test_data.sql
    const SEED_REPORT_NUMS = [
      'APD-2026-040301','APD-2026-040302','APD-2026-040307',
      'CCPD-2026-001122'
    ];
    const SEED_PHONE_PATTERNS = [
      '4045552222','6785558888','7705559999','4045553456',
      '4045557890','7705551234','4045550001','4045550002',
      '4045550003','4045550004'
    ];
    const SEED_NAMES = [
      'Emily Chen','David Kim','James Tucker','Angela Martinez',
      'Robert Garcia','Tanisha Brown'
    ];

    const seedIncidents = await db.raw(`
      SELECT id, police_report_number FROM incidents
      WHERE police_report_number = ANY(?::text[])
    `, [SEED_REPORT_NUMS]).then(r => r.rows || []).catch(() => []);
    const seedIncidentIds = seedIncidents.map(r => r.id);

    const seedPersons = await db.raw(`
      SELECT id, full_name, phone, incident_id FROM persons
      WHERE phone = ANY(?::text[])
         OR full_name = ANY(?::text[])
         OR (incident_id = ANY(?::uuid[]) AND ? > 0)
    `, [SEED_PHONE_PATTERNS, SEED_NAMES,
        seedIncidentIds.length ? seedIncidentIds : ['00000000-0000-0000-0000-000000000000'],
        seedIncidentIds.length]
    ).then(r => r.rows || []).catch(() => []);

    issues.seed_incidents = seedIncidents.length;
    issues.seed_persons = seedPersons.length;
    issues.seed_incident_ids = seedIncidents.map(s => s.id);

    const purgeSeeds = (req.query.purge_seeds === 'true') || (req.query.purge === 'true' && fix);
    if (purgeSeeds) {
      const secret = req.query.secret || req.headers['x-cron-secret'];
      if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
        return res.status(401).json({ error: 'Unauthorized for purge_seeds' });
      }
      let personsDeleted = 0, incidentsDeleted = 0, reportsDeleted = 0, vehiclesDeleted = 0;
      const errors = [];
      const allSeedIncIds = [...new Set([
        ...seedIncidentIds,
        ...seedPersons.map(p => p.incident_id).filter(Boolean)
      ])];
      const allSeedPersonIds = seedPersons.map(p => p.id);
      if (allSeedIncIds.length) {
        // 1. Clear non-cascade FK references first
        try { await db.raw('DELETE FROM activity_log WHERE incident_id = ANY(?::uuid[]) OR person_id = ANY(?::uuid[])', [allSeedIncIds, allSeedPersonIds.length ? allSeedPersonIds : ['00000000-0000-0000-0000-000000000000']]); } catch (e) { errors.push('activity_log: ' + e.message); }
        try { await db.raw('DELETE FROM notifications WHERE incident_id = ANY(?::uuid[])', [allSeedIncIds]); } catch (e) { errors.push('notifications: ' + e.message); }
        try { await db.raw('DELETE FROM enrichment_logs WHERE person_id = ANY(?::uuid[])', [allSeedPersonIds.length ? allSeedPersonIds : ['00000000-0000-0000-0000-000000000000']]); } catch (e) { /* table may not exist */ }
        try { await db.raw('DELETE FROM cascade_queue WHERE incident_id = ANY(?::uuid[]) OR person_id = ANY(?::uuid[])', [allSeedIncIds, allSeedPersonIds.length ? allSeedPersonIds : ['00000000-0000-0000-0000-000000000000']]); } catch (e) { /* table may not exist */ }
        try { await db.raw('DELETE FROM claude_identity_runs WHERE incident_id = ANY(?::uuid[]) OR person_id = ANY(?::uuid[])', [allSeedIncIds, allSeedPersonIds.length ? allSeedPersonIds : ['00000000-0000-0000-0000-000000000000']]); } catch (e) { /* table may not exist */ }
        try { await db.raw('DELETE FROM cross_exam_results WHERE person_id = ANY(?::uuid[])', [allSeedPersonIds.length ? allSeedPersonIds : ['00000000-0000-0000-0000-000000000000']]); } catch (e) { /* table may not exist */ }
        try { await db.raw('DELETE FROM cross_references WHERE person_id = ANY(?::uuid[]) OR incident_id = ANY(?::uuid[])', [allSeedPersonIds.length ? allSeedPersonIds : ['00000000-0000-0000-0000-000000000000'], allSeedIncIds]); } catch (e) { errors.push('cross_references: ' + e.message); }
        try { await db.raw('DELETE FROM cross_references WHERE person_id IN (SELECT id FROM persons WHERE phone = ANY(?::text[]) OR full_name = ANY(?::text[]))', [SEED_PHONE_PATTERNS, SEED_NAMES]); } catch (e) { /* fallback */ }
        try { await db.raw('DELETE FROM enrichment_logs WHERE person_id IN (SELECT id FROM persons WHERE phone = ANY(?::text[]) OR full_name = ANY(?::text[]))', [SEED_PHONE_PATTERNS, SEED_NAMES]); } catch (_) {}
        try { await db.raw('DELETE FROM activity_log WHERE person_id IN (SELECT id FROM persons WHERE phone = ANY(?::text[]) OR full_name = ANY(?::text[]))', [SEED_PHONE_PATTERNS, SEED_NAMES]); } catch (_) {}

        // 2. Delete cascade tables (CASCADE FK already configured in 001_initial_schema)
        try {
          const r1 = await db.raw('DELETE FROM source_reports WHERE incident_id = ANY(?::uuid[])', [allSeedIncIds]);
          reportsDeleted = r1.rowCount || 0;
        } catch (e) { errors.push('source_reports: ' + e.message); }
        try {
          const r2 = await db.raw('DELETE FROM vehicles WHERE incident_id = ANY(?::uuid[])', [allSeedIncIds]);
          vehiclesDeleted = r2.rowCount || 0;
        } catch (e) { errors.push('vehicles: ' + e.message); }
        try {
          const r3 = await db.raw('DELETE FROM persons WHERE incident_id = ANY(?::uuid[])', [allSeedIncIds]);
          personsDeleted = r3.rowCount || 0;
        } catch (e) { errors.push('persons: ' + e.message); }

        // 3. Delete incident_matches that reference seed incidents (cascade should handle, but be defensive)
        try { await db.raw('DELETE FROM incident_matches WHERE incident_id = ANY(?::uuid[]) OR matched_incident_id = ANY(?::uuid[])', [allSeedIncIds, allSeedIncIds]); } catch (_) {}

        // 4. Finally delete seed incidents
        try {
          const r4 = await db.raw('DELETE FROM incidents WHERE id = ANY(?::uuid[])', [allSeedIncIds]);
          incidentsDeleted = r4.rowCount || 0;
        } catch (e) { errors.push('incidents: ' + e.message); }
      }
      // 5. Stray seed-named persons not tied to seed incidents
      try {
        const r5 = await db.raw(`
          DELETE FROM persons
          WHERE phone = ANY(?::text[]) OR full_name = ANY(?::text[])
        `, [SEED_PHONE_PATTERNS, SEED_NAMES]);
        personsDeleted += r5.rowCount || 0;
      } catch (e) { errors.push('persons-stray: ' + e.message); }

      fixes.seed_purge = {
        incidents_deleted: incidentsDeleted,
        persons_deleted: personsDeleted,
        source_reports_deleted: reportsDeleted,
        vehicles_deleted: vehiclesDeleted,
        errors: errors.length ? errors : undefined
      };
    }

    // 9. Total counts
    const counts = await db.raw(`
      SELECT
        (SELECT COUNT(*) FROM incidents) AS incidents,
        (SELECT COUNT(*) FROM persons) AS persons,
        (SELECT COUNT(*) FROM source_reports) AS source_reports,
        (SELECT COUNT(*) FROM data_sources WHERE is_active = TRUE) AS active_sources,
        (SELECT COUNT(*) FROM incidents WHERE qualification_state = 'qualified') AS qualified,
        (SELECT COUNT(*) FROM incidents WHERE qualification_state = 'pending_named') AS pending_named,
        (SELECT COUNT(*) FROM incidents WHERE qualification_state = 'pending') AS pending
    `).then(r => r.rows[0]);

    const totalIssues = (issues.orphan_source_reports || 0) + (issues.orphan_persons || 0)
      + issues.invalid_severity.length + issues.invalid_incident_type.length
      + (issues.missing_geom || 0) + (issues.persons_name_mismatch || 0)
      + issues.unknown_source_types.length + (issues.qualification_drift || 0)
      + (issues.seed_incidents || 0) + (issues.seed_persons || 0);

    res.json({
      success: true,
      summary: `${totalIssues} consistency issues found${(fix || purgeSeeds) ? ` (${Object.values(fixes).reduce((a,b) => a+(typeof b === 'number' ? b : 0), 0)} fixed)` : ''}`,
      issues, fixes: (fix || purgeSeeds) ? fixes : null,
      counts,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'audit', null, err.message);
    res.status(500).json({ error: err.message, issues });
  }
};
