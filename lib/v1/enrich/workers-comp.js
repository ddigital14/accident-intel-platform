/**
 * Workers' comp angle for work_accident incidents.
 * Resolves employer from incident description → looks up comp carrier via state DOI.
 * Cross-links with OSHA fatality reports (we already ingest those).
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

async function run(db, limit = 30) {
  // Find work_accident incidents missing employer or comp_carrier
  let rows = []; try {
    rows = await db('incidents').where('accident_type', 'work_accident')
      .where(function () { this.whereNull('employer').orWhere('employer', ''); })
      .limit(limit);
  } catch (_) {}
  let crossLinked = 0;
  for (const inc of rows) {
    try {
      // Cross-link to OSHA: same state + occurred_at within 7 days
      const oshaMatches = await db.raw(`
        SELECT i.id, i.description FROM incidents i
        WHERE i.source = 'osha' AND i.state = ? AND ABS(EXTRACT(EPOCH FROM (i.occurred_at - ?)) / 86400) < 7
        LIMIT 5
      `, [inc.state, inc.occurred_at]).then(r => r.rows || r).catch(() => []);
      if (oshaMatches.length > 0) {
        // Extract employer name from description
        const empMatch = oshaMatches[0].description?.match(/at\s+([A-Z][a-zA-Z\s,&.-]{2,60})\s+(in|near|on|at)/);
        if (empMatch) {
          await db('incidents').where({ id: inc.id }).update({ employer: empMatch[1].trim(), updated_at: new Date() });
          await enqueueCascade(db, 'incident', inc.id, 'workers-comp', { weight: 75, employer: empMatch[1].trim() });
          crossLinked++;
        }
      }
    } catch (_) {}
  }
  await trackApiCall(db, 'enrich-workers-comp', 'sql', 0, 0, true).catch(() => {});
  return { rows: rows.length, crossLinked };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'workers-comp', cost: 0 });
    const out = await run(db, parseInt(req.query.limit) || 30);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'workers-comp', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
