const { getDb } = require('../../_db');
const { requireAuth } = require('../../_auth');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const user = requireAuth(req, res);
  if (!user) return;
  const db = getDb();

  try {
    const { minutes = 1440, metro, type, state: stateFilter = 'all_verified', limit = 100 } = req.query;
    const since = new Date(Date.now() - parseInt(minutes) * 60 * 1000);

    let query = db('incidents as i')
      .leftJoin('metro_areas as ma', 'i.metro_area_id', 'ma.id')
      .where('i.discovered_at', '>=', since)
      .select('i.id', 'i.incident_type', 'i.severity', 'i.status', 'i.priority',
        'i.address', 'i.city', 'i.state', 'i.latitude', 'i.longitude',
        'i.description', 'i.discovered_at', 'i.occurred_at', 'i.source_count',
        'i.confidence_score', 'i.lead_score', 'i.qualification_state',
        'i.qualified_at', 'i.has_contact_info',
        'i.injuries_count', 'i.fatalities_count', 'i.ems_dispatched',
        'i.helicopter_dispatched', 'i.police_report_number', 'i.tags',
        'ma.name as metro_area')
      .orderBy('i.lead_score', 'desc')
      .orderBy('i.discovered_at', 'desc');

    // qualification state filter — Phase 45: default 'all_verified' shows fatal AND non-fatal
    if (stateFilter === 'qualified') query = query.where('i.qualification_state', 'qualified');
    else if (stateFilter === 'pending') query = query.whereIn('i.qualification_state', ['pending','pending_named']);
    else if (stateFilter === 'pending_named') query = query.where('i.qualification_state', 'pending_named');
    else if (stateFilter === 'all') {} // no filter
    else if (stateFilter === 'all_verified') {
      // Show qualified + any incident with at least 1 verified victim (covers non-fatal injury cases)
      query = query.where(function () {
        this.where('i.qualification_state', 'qualified')
          .orWhereExists(function () {
            this.select('1').from('persons as p')
              .whereRaw('p.incident_id = i.id')
              .where('p.victim_verified', true);
          });
      });
    }
    else query = query.where('i.qualification_state', stateFilter);

    if (metro) query = query.where('i.metro_area_id', metro);
    if (type) query = query.where('i.incident_type', type);

    const incidents = await query.limit(Math.min(500, parseInt(limit)));

    // For each incident, attach person summary (so reps can see contact info at a glance)
    if (incidents.length) {
      const ids = incidents.map(i => i.id);
      const persons = await db('persons')
        .whereIn('incident_id', ids)
        .select('id','incident_id','full_name','first_name','last_name','phone','email','address','is_injured','injury_severity','has_attorney','contact_status','enrichment_score');
      const personsByInc = {};
      for (const p of persons) (personsByInc[p.incident_id] ||= []).push(p);
      for (const inc of incidents) inc.persons = personsByInc[inc.id] || [];
    }

    res.json({ data: incidents, since: since.toISOString(), state_filter: stateFilter });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
