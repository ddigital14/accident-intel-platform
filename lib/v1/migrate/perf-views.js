/**
 * Phase 33: materialized view for dashboard read paths + composite indexes.
 * POST /api/v1/migrate/perf-views?secret=migrate-now
 */
const { getDb } = require('../../_db');

module.exports = async function handler(req, res) {
  const secret = req.headers['x-cron-secret'] || req.query.secret;
  if (secret !== process.env.CRON_SECRET && secret !== 'migrate-now') return res.status(401).json({ error: 'Unauthorized' });
  const db = getDb();
  const results = { ok: [], errors: [] };
  const stmts = [
    // Phase 33: ensure new columns exist before referencing in MV
    `ALTER TABLE incidents ADD COLUMN IF NOT EXISTS predicted_value_min BIGINT`,
    `ALTER TABLE incidents ADD COLUMN IF NOT EXISTS predicted_value_likely BIGINT`,
    `ALTER TABLE incidents ADD COLUMN IF NOT EXISTS predicted_value_max BIGINT`,
    `ALTER TABLE incidents ADD COLUMN IF NOT EXISTS case_strength_score INT`,
    `ALTER TABLE incidents ADD COLUMN IF NOT EXISTS case_complexity TEXT`,
    `ALTER TABLE incidents ADD COLUMN IF NOT EXISTS recommended_action TEXT`,
    `ALTER TABLE incidents ADD COLUMN IF NOT EXISTS prediction_reasoning TEXT`,
    `ALTER TABLE incidents ADD COLUMN IF NOT EXISTS metro_heat_score INT DEFAULT 0`,
    `ALTER TABLE incidents ADD COLUMN IF NOT EXISTS recycled_count INT DEFAULT 0`,
    `ALTER TABLE incidents ADD COLUMN IF NOT EXISTS recycled_at TIMESTAMPTZ`,
    `ALTER TABLE incidents ADD COLUMN IF NOT EXISTS block_group_income INT`,
    `ALTER TABLE incidents ADD COLUMN IF NOT EXISTS case_value_modifier NUMERIC`,
    `ALTER TABLE incidents ADD COLUMN IF NOT EXISTS employer TEXT`,
    `ALTER TABLE incidents ADD COLUMN IF NOT EXISTS comp_carrier TEXT`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS engagement_score INT DEFAULT 0`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS name_rarity INT`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS has_attorney BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS attorney_name TEXT`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS deceased BOOLEAN`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS dob_year INT`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS merged_into BIGINT`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS fraud_risk BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS fraud_reason TEXT`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS has_voter_searched BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS has_dev_profile_searched BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS has_archive_searched BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS has_state_court_searched BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS has_business_searched BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS has_reddit_searched BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS has_relatives_searched BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS has_usps_validated BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS has_fl_county_searched BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS has_llc_searched BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS bidir_resync_at TIMESTAMPTZ`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS family_tree_expanded BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS relationship_to_victim TEXT`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS policy_limits_estimated BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS policy_limits_min INT`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS policy_limits_max INT`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS carrier_market_rank INT`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS insurance_carrier TEXT`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS crm_exported_at TIMESTAMPTZ`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS apollo_exported_at TIMESTAMPTZ`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS common_room_exported_at TIMESTAMPTZ`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS ghl_exported_at TIMESTAMPTZ`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS has_contradiction BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS line_type TEXT`,
    `ALTER TABLE persons ADD COLUMN IF NOT EXISTS carrier TEXT`,
    `ALTER TABLE vehicles ADD COLUMN IF NOT EXISTS has_salvage_searched BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE vehicles ADD COLUMN IF NOT EXISTS on_salvage_listing BOOLEAN DEFAULT FALSE`,
    `ALTER TABLE vehicles ADD COLUMN IF NOT EXISTS vehicle_owner_inferred BOOLEAN DEFAULT FALSE`,
    `CREATE TABLE IF NOT EXISTS lead_notes (id SERIAL PRIMARY KEY, incident_id BIGINT, person_id BIGINT, body TEXT, author_id BIGINT, created_at TIMESTAMPTZ DEFAULT NOW())`,
    `CREATE TABLE IF NOT EXISTS sms_log (id SERIAL PRIMARY KEY, incident_id BIGINT, person_id BIGINT, direction TEXT, status TEXT, body TEXT, twilio_sid TEXT, created_at TIMESTAMPTZ DEFAULT NOW())`,
    `CREATE TABLE IF NOT EXISTS email_log (id SERIAL PRIMARY KEY, incident_id BIGINT, person_id BIGINT, status TEXT, event TEXT, subject TEXT, created_at TIMESTAMPTZ DEFAULT NOW())`,
    `CREATE TABLE IF NOT EXISTS form_submissions (id SERIAL PRIMARY KEY, person_id BIGINT, form_id TEXT, payload JSONB, created_at TIMESTAMPTZ DEFAULT NOW())`,
    `CREATE TABLE IF NOT EXISTS person_relationships (id SERIAL PRIMARY KEY, person_a_id BIGINT, person_b_id BIGINT, relationship TEXT, confidence INT, source TEXT, created_at TIMESTAMPTZ DEFAULT NOW(), UNIQUE (person_a_id, person_b_id))`,

    // Composite indexes for hot paths
    `CREATE INDEX IF NOT EXISTS idx_enrich_logs_person_time ON enrichment_logs (person_id, created_at DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_inc_qual_assigned ON incidents (qualification_state, assigned_to)`,
    `CREATE INDEX IF NOT EXISTS idx_inc_assigned_lead ON incidents (assigned_to, lead_score DESC NULLS LAST)`,
    `DO $$ BEGIN
       IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='sms_log') THEN
         EXECUTE 'CREATE INDEX IF NOT EXISTS idx_sms_log_inc_time ON sms_log (incident_id, created_at DESC)';
       ELSIF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='messages') THEN
         EXECUTE 'CREATE INDEX IF NOT EXISTS idx_messages_inc_time ON messages (incident_id, created_at DESC)';
       END IF;
     END $$`,
    `DO $$ DECLARE col TEXT; BEGIN
       SELECT column_name INTO col FROM information_schema.columns WHERE table_schema='public' AND table_name='cascade_queue' AND column_name IN ('subject_type','entity_type','kind') LIMIT 1;
       IF col IS NOT NULL THEN
         EXECUTE 'CREATE INDEX IF NOT EXISTS idx_cascade_subj ON cascade_queue (' || col || ', created_at DESC)';
       END IF;
     END $$`,
    `CREATE INDEX IF NOT EXISTS idx_inc_pred_value ON incidents (predicted_value_likely DESC NULLS LAST) WHERE predicted_value_likely IS NOT NULL`,
    // Materialized view: dashboard summary
    `DROP MATERIALIZED VIEW IF EXISTS mv_dashboard_summary`,
    `CREATE MATERIALIZED VIEW mv_dashboard_summary AS
       SELECT
         i.id, i.description, i.severity, i.city, i.state, i.lead_score, i.qualification_state,
         i.predicted_value_likely, i.case_strength_score, i.recommended_action,
         i.assigned_to, i.assigned_at, i.created_at, i.occurred_at,
         p.full_name, p.phone, p.email, p.identity_confidence, p.engagement_score, p.has_attorney,
         u.email AS rep_email
       FROM incidents i
       LEFT JOIN persons p ON p.incident_id = i.id
       LEFT JOIN users u ON u.id = i.assigned_to
       WHERE i.created_at > NOW() - INTERVAL '60 days'`,
    `CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_dash_id ON mv_dashboard_summary (id)`,
    `CREATE INDEX IF NOT EXISTS idx_mv_dash_qual_score ON mv_dashboard_summary (qualification_state, lead_score DESC NULLS LAST)`,
    `CREATE INDEX IF NOT EXISTS idx_mv_dash_state_city ON mv_dashboard_summary (state, city)`
  ];
  for (const sql of stmts) {
    try { await db.raw(sql); results.ok.push(sql.slice(0, 80)); }
    catch (e) { results.errors.push(`${sql.slice(0, 60)}: ${e.message}`); }
  }
  res.json({ success: results.errors.length === 0, ok: results.ok.length, errors: results.errors });
};
