/**
 * GET /api/v1/dashboard/my-assignments
 *
 * Returns current user's assigned incidents.
 *
 * Phase 23 #2: When user has 0 assigned, fallback to top-10 qualified
 * UNASSIGNED leads with `claimable: true` flag so MyLeads can show a
 * "Claim" button.
 *
 * POST /api/v1/dashboard/my-assignments?action=claim&incidentId=<uuid>
 *   - Assigns the incident to the current user (forces auto-assign for them)
 */
const { getDb } = require('../../_db');
const { requireAuth } = require('../../_auth');
const { v4: uuidv4 } = require('uuid');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const user = requireAuth(req, res);
  if (!user) return;
  const db = getDb();

  // POST claim flow
  if (req.method === 'POST' || req.query.action === 'claim') {
    const incidentId = req.query.incidentId || (req.body && req.body.incidentId);
    if (!incidentId) return res.status(400).json({ error: 'incidentId required' });
    try {
      const inc = await db('incidents').where('id', incidentId).first();
      if (!inc) return res.status(404).json({ error: 'incident_not_found' });
      if (inc.assigned_to && inc.assigned_to !== user.id) {
        return res.status(409).json({ error: 'already_assigned' });
      }
      await db('incidents').where('id', incidentId).update({
        assigned_to: user.id,
        assigned_at: new Date(),
        status: 'assigned',
        updated_at: new Date(),
      });
      await db('activity_log').insert({
        id: uuidv4(),
        user_id: user.id,
        incident_id: incidentId,
        action: 'self_claim',
        details: JSON.stringify({ via: 'my-leads-fallback' }),
        created_at: new Date(),
      }).catch(() => {});
      return res.json({ success: true, claimed: incidentId });
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  }

  try {
    const incidents = await db('incidents as i')
      .leftJoin('metro_areas as ma', 'i.metro_area_id', 'ma.id')
      .where('i.assigned_to', user.id)
      .select('i.*', 'ma.name as metro_area_name')
      .orderBy('i.discovered_at', 'desc');

    let mode = 'assigned';
    let fallback = [];
    if (incidents.length === 0) {
      mode = 'fallback_qualified_unassigned';
      // Top-10 qualified unassigned leads
      fallback = await db('incidents as i')
        .leftJoin('metro_areas as ma', 'i.metro_area_id', 'ma.id')
        .where('i.qualification_state', 'qualified')
        .whereNull('i.assigned_to')
        .where('i.discovered_at', '>', new Date(Date.now() - 30 * 24 * 3600 * 1000))
        .select('i.*', 'ma.name as metro_area_name')
        .orderBy('i.lead_score', 'desc')
        .orderBy('i.discovered_at', 'desc')
        .limit(10);
      // If still empty, drop to pending_named
      if (fallback.length === 0) {
        mode = 'fallback_pending_named';
        fallback = await db('incidents as i')
          .leftJoin('metro_areas as ma', 'i.metro_area_id', 'ma.id')
          .where('i.qualification_state', 'pending_named')
          .whereNull('i.assigned_to')
          .where('i.discovered_at', '>', new Date(Date.now() - 30 * 24 * 3600 * 1000))
          .select('i.*', 'ma.name as metro_area_name')
          .orderBy('i.lead_score', 'desc')
          .limit(10);
      }
    }

    const all = [...incidents, ...fallback];
    const ids = all.map(i => i.id);
    const persons = ids.length > 0
      ? await db('persons').whereIn('incident_id', ids)
          .select('incident_id', 'full_name', 'phone', 'email', 'is_injured', 'injury_severity',
            'insurance_company', 'contact_status', 'has_attorney', 'role',
            'identity_confidence', 'contact_quality')
      : [];

    const personsByIncident = {};
    persons.forEach(p => {
      if (!personsByIncident[p.incident_id]) personsByIncident[p.incident_id] = [];
      personsByIncident[p.incident_id].push(p);
    });

    const enrichInc = (i, claimable) => ({
      ...i,
      persons: personsByIncident[i.id] || [],
      claimable: !!claimable,
    });

    res.json({
      data: incidents.map(i => enrichInc(i, false)),
      fallback: fallback.map(i => enrichInc(i, true)),
      mode,
      assigned_count: incidents.length,
      fallback_count: fallback.length,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
