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
    const { minutes = 60, metro, type } = req.query;
    const since = new Date(Date.now() - parseInt(minutes) * 60 * 1000);

    let query = db('incidents as i')
      .leftJoin('metro_areas as ma', 'i.metro_area_id', 'ma.id')
      .where('i.discovered_at', '>=', since)
      .select('i.id', 'i.incident_type', 'i.severity', 'i.status', 'i.priority',
        'i.address', 'i.city', 'i.state', 'i.latitude', 'i.longitude',
        'i.description', 'i.discovered_at', 'i.source_count', 'i.confidence_score',
        'i.injuries_count', 'i.fatalities_count', 'i.ems_dispatched',
        'i.helicopter_dispatched', 'i.police_report_number', 'ma.name as metro_area')
      .orderBy('i.discovered_at', 'desc');

    if (metro) query = query.where('i.metro_area_id', metro);
    if (type) query = query.where('i.incident_type', type);

    const incidents = await query.limit(100);
    res.json({ data: incidents, since: since.toISOString() });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
