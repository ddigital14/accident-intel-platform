/**
 * GET /api/v1/system/auto-assign?secret=ingest-now
 *
 * Auto-distributes qualified leads to reps based on:
 *   1. user.assigned_metros (UUID[]) matches incident.metro_area_id
 *   2. user.specialization includes incident.incident_type (auto/motorcycle/truck/work_injury)
 *   3. user.max_daily_leads not yet hit today
 *   4. Lowest current load (round-robin by today's count)
 *
 * Skips:
 *   - already-assigned incidents
 *   - non-qualified incidents
 *   - incidents older than 24h (stale)
 *
 * Cron: every 10 minutes
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('./_errors');

function specializationMatches(userSpec, incidentType) {
  if (!userSpec || userSpec.length === 0) return true; // no specialization = all
  const map = {
    'car_accident': 'auto',
    'motorcycle_accident': 'motorcycle',
    'truck_accident': 'truck',
    'pedestrian': 'auto',
    'bicycle': 'auto',
    'work_accident': 'work_injury',
    'slip_fall': 'work_injury'
  };
  const required = map[incidentType] || 'auto';
  return userSpec.includes(required) || userSpec.includes('all');
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const results = { candidates: 0, assigned: 0, no_rep_available: 0, errors: [] };

  try {
    // Active reps + their daily load
    const reps = await db('users')
      .where('is_active', true)
      .whereIn('role', ['rep', 'manager'])
      .select('id','first_name','last_name','assigned_metros','max_daily_leads','specialization');

    const today = new Date(); today.setHours(0,0,0,0);
    const repLoad = {};
    for (const r of reps) {
      const c = await db('incidents')
        .where('assigned_to', r.id)
        .where('assigned_at', '>=', today)
        .count('* as count').first();
      repLoad[r.id] = parseInt(c?.count || 0);
    }

    // Qualified, unassigned incidents
    // Phase 23 #2: widen window to 30d so existing qualified leads still get assigned
    const lookbackHours = parseInt(req.query.hours) || (24 * 30);
    const candidates = await db('incidents')
      .where('qualification_state', 'qualified')
      .whereNull('assigned_to')
      .where('discovered_at', '>', new Date(Date.now() - lookbackHours * 3600 * 1000))
      .select('id','metro_area_id','incident_type','lead_score','severity')
      .orderBy('lead_score', 'desc')
      .limit(100);
    results.candidates = candidates.length;

    for (const inc of candidates) {
      try {
        // Find best rep
        const eligible = reps.filter(r => {
          if (repLoad[r.id] >= (r.max_daily_leads || 50)) return false;
          if (r.assigned_metros && r.assigned_metros.length > 0 && inc.metro_area_id) {
            if (!r.assigned_metros.includes(inc.metro_area_id)) return false;
          }
          if (!specializationMatches(r.specialization, inc.incident_type)) return false;
          return true;
        });

        if (eligible.length === 0) {
          results.no_rep_available++;
          continue;
        }

        // Pick rep with lowest load
        eligible.sort((a, b) => (repLoad[a.id] || 0) - (repLoad[b.id] || 0));
        const chosen = eligible[0];

        await db('incidents').where('id', inc.id).update({
          assigned_to: chosen.id,
          assigned_at: new Date(),
          status: 'assigned',
          updated_at: new Date()
        });

        await db('activity_log').insert({
          id: uuidv4(),
          user_id: chosen.id,
          incident_id: inc.id,
          action: 'auto_assigned',
          details: JSON.stringify({
            lead_score: inc.lead_score,
            severity: inc.severity,
            rep_load_today: repLoad[chosen.id] + 1
          }),
          created_at: new Date()
        }).catch(() => {});

        repLoad[chosen.id] = (repLoad[chosen.id] || 0) + 1;
        results.assigned++;
      } catch (e) {
        results.errors.push(`${inc.id}: ${e.message}`);
        await reportError(db, 'auto-assign', inc.id, e.message);
      }
    }

    res.json({
      success: true,
      message: `Auto-assign: ${results.assigned} assigned, ${results.no_rep_available} no rep, ${results.candidates} candidates`,
      ...results,
      active_reps: reps.length,
      rep_load: repLoad,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'auto-assign', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
