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
    // Composite indexes for hot paths
    `CREATE INDEX IF NOT EXISTS idx_enrich_logs_person_time ON enrichment_logs (person_id, created_at DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_persons_inc_qual ON persons (incident_id, qualification_state)`,
    `CREATE INDEX IF NOT EXISTS idx_inc_assigned_lead ON incidents (assigned_to, lead_score DESC NULLS LAST)`,
    `CREATE INDEX IF NOT EXISTS idx_sms_log_inc_time ON sms_log (incident_id, created_at DESC)`,
    `CREATE INDEX IF NOT EXISTS idx_cascade_subj_kind ON cascade_queue (subject_type, subject_id, created_at DESC)`,
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
