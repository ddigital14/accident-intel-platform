/**
 * Atomic lead claim. Race-safe — uses WHERE assigned_to IS NULL guard with RETURNING.
 * Prevents two reps both successfully claiming the same lead.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('./cost');
const { reportError } = require('./_errors');
const { enqueueCascade } = require('./_cascade');

async function claim(db, { incident_id, rep_id, rep_email }) {
  if (!incident_id || (!rep_id && !rep_email)) return { ok: false, error: 'need incident_id and rep_id|rep_email' };
  // Resolve rep
  let rep = null;
  if (rep_id) rep = await db('users').where({ id: rep_id }).first();
  else if (rep_email) rep = await db('users').where({ email: rep_email }).first();
  if (!rep) return { ok: false, error: 'rep_not_found' };
  // Atomic claim: only succeeds if assigned_to is currently NULL
  const updated = await db.raw(
    `UPDATE incidents SET assigned_to = ?, assigned_at = NOW(), qualification_state = COALESCE(qualification_state, 'qualified')
     WHERE id = ? AND assigned_to IS NULL
     RETURNING id, assigned_to, assigned_at`,
    [rep.id, incident_id]
  ).then(r => r.rows || r).catch(() => []);
  if (!updated || !updated[0]) {
    // Find who beat us
    const cur = await db('incidents').where({ id: incident_id }).first();
    return { ok: false, error: 'already_claimed', claimed_by: cur?.assigned_to, claimed_at: cur?.assigned_at };
  }
  await enqueueCascade(db, 'incident', incident_id, 'atomic-claim', { weight: 5, rep: rep.email });
  await trackApiCall(db, 'system-atomic-claim', 'success', 0, 0, true).catch(() => {});
  return { ok: true, claimed_by: rep.email, claimed_at: updated[0].assigned_at };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'atomic-claim' });
    if (req.method === 'POST') {
      const body = await new Promise(r => { let d = ''; req.on('data', c => d += c); req.on('end', () => { try { r(JSON.parse(d)); } catch (_) { r({}); } }); });
      const out = await claim(db, body);
      return res.status(out.ok ? 200 : 409).json(out);
    }
    return res.status(400).json({ error: 'POST {incident_id, rep_email|rep_id}' });
  } catch (err) { await reportError(db, 'atomic-claim', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.claim = claim;
