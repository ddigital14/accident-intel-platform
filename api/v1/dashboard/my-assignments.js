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
    const incidents = await db('incidents as i')
      .leftJoin('metro_areas as ma', 'i.metro_area_id', 'ma.id')
      .where('i.assigned_to', user.id)
      .select('i.*', 'ma.name as metro_area_name')
      .orderBy('i.discovered_at', 'desc');

    const ids = incidents.map(i => i.id);
    const persons = ids.length > 0
      ? await db('persons').whereIn('incident_id', ids)
          .select('incident_id', 'full_name', 'phone', 'is_injured', 'injury_severity',
            'insurance_company', 'contact_status', 'has_attorney', 'role')
      : [];

    const personsByIncident = {};
    persons.forEach(p => {
      if (!personsByIncident[p.incident_id]) personsByIncident[p.incident_id] = [];
      personsByIncident[p.incident_id].push(p);
    });

    const enriched = incidents.map(i => ({ ...i, persons: personsByIncident[i.id] || [] }));
    res.json({ data: enriched });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
