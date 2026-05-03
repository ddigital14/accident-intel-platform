/**
 * Phase 95: Promote-Named-Pending
 *
 * The qualification logic correctly classifies persons as `pending_named` when
 * they have a name but no contact info. We have 53 such persons sitting idle.
 * This engine targets them with aggressive enrichment:
 *   1. Fire auto-fan-out on each (Apollo/PDL/Trestle/obit/funeral/courtListener/etc.)
 *   2. Re-run qualify to promote any that picked up contact info
 *   3. Report which ones we still couldn't fill
 */
const { getDb } = require('../../_db');

const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function findNamedPending(db, limit) {
  return (await db.raw(`
    SELECT p.id, p.full_name, p.role, p.lead_tier,
           p.phone, p.email, p.address,
           i.state, i.city, i.severity, i.lead_score,
           i.qualification_state, i.occurred_at, i.id as incident_id,
           i.incident_number
    FROM persons p JOIN incidents i ON i.id = p.incident_id
    WHERE p.full_name IS NOT NULL
      AND length(p.full_name) >= 5
      AND (p.full_name ~ ' ')
      AND (i.qualification_state IS NULL OR i.qualification_state != 'qualified')
      AND (p.phone IS NULL OR p.email IS NULL OR p.address IS NULL)
    ORDER BY
      CASE i.severity
        WHEN 'fatal' THEN 1
        WHEN 'critical' THEN 2
        WHEN 'serious' THEN 3
        ELSE 4
      END,
      i.occurred_at DESC NULLS LAST
    LIMIT ${parseInt(limit) || 50}
  `)).rows;
}

async function runEnrichment(db, person) {
  // Fire auto-fan-out for this person
  const m = require('./auto-fan-out');
  const fakeReq = { method: 'POST', query: { secret: 'ingest-now', action: 'run' }, headers: {}, body: { person_id: person.id, force: true } };
  const fakeRes = { _data: null, _status: 200, status(s) { this._status = s; return this; }, json(d) { this._data = d; return this; }, setHeader() {} };
  try {
    await m(fakeReq, fakeRes);
    return fakeRes._data;
  } catch (e) {
    return { error: e.message };
  }
}

async function reQualifyOne(db, incidentId) {
  // Re-fetch persons + check qualification
  const persons = await db('persons').where('incident_id', incidentId);
  let qualified = false;
  for (const p of persons) {
    const hasName = (p.full_name && p.full_name.trim().length > 2);
    const hasContact = (p.phone && p.phone.trim()) || (p.email && p.email.trim()) || (p.address && p.address.trim().length > 5);
    if (hasName && hasContact) { qualified = true; break; }
  }
  if (qualified) {
    await db('incidents').where('id', incidentId).update({ qualification_state: 'qualified' });
    return true;
  }
  return false;
}

module.exports = async function handler(req, res) {
  if (!authed(req)) return res.status(401).json({ error: 'unauthorized' });
  const action = req.query?.action || 'health';
  const db = getDb();

  if (action === 'health') {
    const named = (await db.raw(`
      SELECT COUNT(*) as c FROM persons p
      JOIN incidents i ON i.id = p.incident_id
      WHERE p.full_name IS NOT NULL AND length(p.full_name) >= 5
        AND (i.qualification_state IS NULL OR i.qualification_state != 'qualified')
    `)).rows[0].c;
    return res.status(200).json({ ok: true, engine: 'promote-named-pending', named_pending_count: parseInt(named) });
  }

  if (action === 'list') {
    const limit = parseInt(req.query?.limit) || 60;
    const persons = await findNamedPending(db, limit);
    return res.status(200).json({ ok: true, count: persons.length, persons: persons.slice(0, 60) });
  }

  if (action === 'run') {
    const limit = parseInt(req.query?.limit) || 10;
    const persons = await findNamedPending(db, limit);
    const results = [];
    let promoted = 0, contacts_added = 0;
    for (const p of persons) {
      try {
        const before = { phone: !!p.phone, email: !!p.email, address: !!p.address };
        const enrichResult = await runEnrichment(db, p);
        // Re-fetch person to see what's new
        const after = await db('persons').where('id', p.id).first();
        const filled = [];
        if (!before.phone && after.phone) filled.push('phone');
        if (!before.email && after.email) filled.push('email');
        if (!before.address && after.address) filled.push('address');
        if (filled.length > 0) contacts_added++;
        const becameQualified = await reQualifyOne(db, p.incident_id);
        if (becameQualified) promoted++;
        results.push({
          person_id: p.id,
          name: p.full_name,
          state: p.state,
          severity: p.severity,
          fields_filled: filled,
          promoted_to_qualified: becameQualified,
          engines_fired: enrichResult?.engines_fired || enrichResult?.fired || 0,
          engines_ok: enrichResult?.ok_count || 0
        });
      } catch (e) {
        results.push({ person_id: p.id, error: e.message });
      }
    }
    return res.status(200).json({
      ok: true,
      processed: persons.length,
      contacts_added,
      promoted,
      results: results.slice(0, 20)
    });
  }

  return res.status(400).json({ error: 'unknown action', valid: ['health','list','run'] });
};
