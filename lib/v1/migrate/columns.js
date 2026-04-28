/**
 * Database Migration: Add missing columns for enrichment integrations
 * POST /api/v1/migrate/columns?secret=migrate-now
 *
 * Adds columns needed by NumVerify, Tracerfy, and FARS integrations.
 * Safe to run multiple times (uses IF NOT EXISTS).
 */
const { getDb } = require('../../_db');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const secret = req.headers['x-cron-secret'] || req.query.secret;
  if (secret !== process.env.CRON_SECRET && secret !== 'migrate-now') {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const db = getDb();
  const results = { added: [], skipped: [], errors: [] };

  // Columns to add to persons table
  const personColumns = [
    // NumVerify fields
    { name: 'phone_verified', type: 'BOOLEAN DEFAULT NULL' },
    { name: 'phone_carrier', type: 'VARCHAR(100) DEFAULT NULL' },
    { name: 'phone_line_type', type: 'VARCHAR(50) DEFAULT NULL' },
    { name: 'phone_location', type: 'VARCHAR(200) DEFAULT NULL' },
    // Tracerfy skip trace fields
    { name: 'phone_dnc', type: 'BOOLEAN DEFAULT NULL' },
    { name: 'email_verified', type: 'BOOLEAN DEFAULT NULL' },
    { name: 'mailing_address', type: 'VARCHAR(255) DEFAULT NULL' },
    { name: 'mailing_city', type: 'VARCHAR(100) DEFAULT NULL' },
    { name: 'mailing_state', type: 'VARCHAR(10) DEFAULT NULL' },
    { name: 'mailing_zip', type: 'VARCHAR(20) DEFAULT NULL' },
    { name: 'date_of_birth', type: 'DATE DEFAULT NULL' },
    { name: 'deceased', type: 'BOOLEAN DEFAULT FALSE' },
    { name: 'litigator', type: 'BOOLEAN DEFAULT NULL' },
    { name: 'property_owner', type: 'BOOLEAN DEFAULT NULL' },
    // Hunter.io fields
    { name: 'linkedin_url', type: 'VARCHAR(255) DEFAULT NULL' },
    // General enrichment
    { name: 'employer', type: 'VARCHAR(200) DEFAULT NULL' },
    { name: 'occupation', type: 'VARCHAR(200) DEFAULT NULL' },
    { name: 'household_income_range', type: 'VARCHAR(50) DEFAULT NULL' },
    { name: 'insurance_company', type: 'VARCHAR(200) DEFAULT NULL' },
    { name: 'policy_limits', type: 'VARCHAR(50) DEFAULT NULL' },
    { name: 'attorney_name', type: 'VARCHAR(200) DEFAULT NULL' },
    { name: 'enrichment_score', type: 'INTEGER DEFAULT 0' },
    { name: 'enrichment_sources', type: 'TEXT[] DEFAULT NULL' },
    { name: 'last_enriched_at', type: 'TIMESTAMPTZ DEFAULT NULL' },
    { name: 'do_not_contact', type: 'BOOLEAN DEFAULT FALSE' },
  ];

  // Columns to add to incidents table
  const incidentColumns = [
    { name: 'weather_conditions', type: 'VARCHAR(200) DEFAULT NULL' },
  ];

  // Add person columns
  for (const col of personColumns) {
    try {
      const exists = await db.raw(`
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'persons' AND column_name = ?
      `, [col.name]);

      if (exists.rows && exists.rows.length > 0) {
        results.skipped.push(`persons.${col.name}`);
      } else {
        await db.raw(`ALTER TABLE persons ADD COLUMN IF NOT EXISTS "${col.name}" ${col.type}`);
        results.added.push(`persons.${col.name}`);
      }
    } catch (err) {
      // IF NOT EXISTS handles duplicates, but catch any other errors
      if (err.message.includes('already exists')) {
        results.skipped.push(`persons.${col.name}`);
      } else {
        results.errors.push(`persons.${col.name}: ${err.message}`);
      }
    }
  }

  // Add incident columns
  for (const col of incidentColumns) {
    try {
      const exists = await db.raw(`
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'incidents' AND column_name = ?
      `, [col.name]);

      if (exists.rows && exists.rows.length > 0) {
        results.skipped.push(`incidents.${col.name}`);
      } else {
        await db.raw(`ALTER TABLE incidents ADD COLUMN IF NOT EXISTS "${col.name}" ${col.type}`);
        results.added.push(`incidents.${col.name}`);
      }
    } catch (err) {
      if (err.message.includes('already exists')) {
        results.skipped.push(`incidents.${col.name}`);
      } else {
        results.errors.push(`incidents.${col.name}: ${err.message}`);
      }
    }
  }

  // Phase 25 — perf indexes (idempotent CREATE INDEX IF NOT EXISTS)
  // Trigger via ?action=indexes or runs implicitly via 'all'
  const wantIndexes = req.query.action === 'indexes' || req.query.action === 'all' || req.query.indexes === '1';
  if (wantIndexes) {
    const indexes = [
      "CREATE INDEX IF NOT EXISTS idx_inc_qstate ON incidents (qualification_state)",
      "CREATE INDEX IF NOT EXISTS idx_inc_discovered ON incidents (discovered_at DESC)",
      "CREATE INDEX IF NOT EXISTS idx_inc_occurred ON incidents (occurred_at DESC)",
      "CREATE INDEX IF NOT EXISTS idx_inc_lead_score ON incidents (lead_score DESC NULLS LAST)",
      "CREATE INDEX IF NOT EXISTS idx_inc_qstate_lead ON incidents (qualification_state, lead_score DESC NULLS LAST)",
      "CREATE INDEX IF NOT EXISTS idx_inc_assigned ON incidents (assigned_to)",
      "CREATE INDEX IF NOT EXISTS idx_inc_state_city ON incidents (state, city)",
      "CREATE INDEX IF NOT EXISTS idx_persons_incident ON persons (incident_id)",
      "CREATE INDEX IF NOT EXISTS idx_persons_full_name ON persons (full_name) WHERE full_name IS NOT NULL",
      "CREATE INDEX IF NOT EXISTS idx_persons_phone ON persons (phone) WHERE phone IS NOT NULL",
      "CREATE INDEX IF NOT EXISTS idx_persons_email ON persons (email) WHERE email IS NOT NULL",
      "CREATE INDEX IF NOT EXISTS idx_persons_ic ON persons (identity_confidence)",
      "CREATE INDEX IF NOT EXISTS idx_persons_contact_quality ON persons (contact_quality)",
      "CREATE INDEX IF NOT EXISTS idx_sources_incident ON source_reports (incident_id)",
      "CREATE INDEX IF NOT EXISTS idx_sources_ref ON source_reports (source_reference)",
      "CREATE INDEX IF NOT EXISTS idx_sources_type_created ON source_reports (source_type, created_at DESC)",
      "CREATE INDEX IF NOT EXISTS idx_errors_pipeline_created ON system_errors (pipeline, created_at DESC)",
      "CREATE INDEX IF NOT EXISTS idx_xref_person ON cross_references (person_id)",
      "CREATE INDEX IF NOT EXISTS idx_xref_incident ON cross_references (incident_id)",
      // Phase 29: GIN trgm for fuzzy victim-name search inside parsed_data JSON blobs
      "CREATE EXTENSION IF NOT EXISTS pg_trgm",
      "CREATE INDEX IF NOT EXISTS idx_sources_parsed_trgm ON source_reports USING GIN ((parsed_data::text) gin_trgm_ops)",
    ];
    results.indexes = { added: 0, errors: [] };
    for (const sql of indexes) {
      try { await db.raw(sql); results.indexes.added++; }
      catch (e) { results.indexes.errors.push(`${sql.slice(20,70)}: ${e.message}`); }
    }
  }

  res.json({
    success: true,
    message: `Migration complete: ${results.added.length} columns added, ${results.skipped.length} already existed${results.indexes ? `, ${results.indexes.added} indexes ensured` : ''}`,
    ...results,
    timestamp: new Date().toISOString()
  });
};
