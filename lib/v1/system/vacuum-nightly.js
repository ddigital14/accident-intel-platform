/**
 * Nightly VACUUM ANALYZE. Keeps indexes hot, query planner fresh.
 * Cron: 4am daily.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('./cost');
const { reportError } = require('./_errors');

const HOT_TABLES = ['incidents','persons','source_reports','enrichment_logs','cascade_queue','sms_log','email_log','vehicles'];

async function run(db) {
  const ok = []; const errs = [];
  for (const t of HOT_TABLES) {
    try { await db.raw(`VACUUM ANALYZE ${t}`); ok.push(t); }
    catch (e) { errs.push(`${t}: ${e.message}`); }
  }
  await trackApiCall(db, 'system-vacuum-nightly', 'sql', 0, 0, errs.length === 0).catch(() => {});
  return { tables: HOT_TABLES.length, ok: ok.length, errors: errs };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'vacuum-nightly', tables: HOT_TABLES });
    const out = await run(db);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'vacuum-nightly', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
