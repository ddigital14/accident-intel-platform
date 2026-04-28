/**
 * Confidence trail viz endpoint — returns ordered list of all engines that fired
 * for a specific person, with weights, timestamps, and field changes.
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');

async function run(db, personId) {
  const trail = await db('enrichment_logs').where('person_id', personId).orderBy('created_at', 'asc').limit(100).catch(() => []);
  const cascades = await db('cascade_queue').where('subject_id', personId).where('subject_type', 'person').orderBy('created_at', 'asc').limit(100).catch(() => []);
  const sources = await db('source_reports').whereRaw('parsed_data::text LIKE ?', [`%${personId}%`]).limit(20).catch(() => []);
  return { person_id: personId, enrichment_logs: trail.length, cascade_events: cascades.length, source_reports: sources.length, trail: trail.slice(0, 30), cascades: cascades.slice(0, 30) };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'confidence-trail' });
    const id = parseInt(req.query.person_id);
    if (!id) return res.status(400).json({ error: 'need person_id' });
    const out = await run(db, id);
    return res.json(out);
  } catch (err) { await reportError(db, 'confidence-trail', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
