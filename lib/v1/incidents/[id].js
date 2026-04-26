const { getDb } = require('../../_db');
const { requireAuth } = require('../../_auth');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, PATCH, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const user = requireAuth(req, res);
  if (!user) return;
  const db = getDb();
  const { id } = req.query;

  if (req.method === 'GET') {
    try {
      const incident = await db('incidents as i')
        .leftJoin('metro_areas as ma', 'i.metro_area_id', 'ma.id')
        .leftJoin('users as u', 'i.assigned_to', 'u.id')
        .where('i.id', id)
        .select('i.*', 'ma.name as metro_area_name', db.raw("u.first_name || ' ' || u.last_name as assigned_to_name"))
        .first();
      if (!incident) return res.status(404).json({ error: 'Incident not found' });

      const [persons, vehicles, sourceReports] = await Promise.all([
        db('persons').where({ incident_id: id }).orderBy('role'),
        db('vehicles').where({ incident_id: id }),
        db('source_reports as sr').leftJoin('data_sources as ds', 'sr.data_source_id', 'ds.id')
          .where('sr.incident_id', id).select('sr.*', 'ds.name as source_name', 'ds.type as source_type_name')
          .orderBy('sr.fetched_at', 'desc')
      ]);

      await db('activity_log').insert({ user_id: user.id, incident_id: id, action: 'viewed' });
      res.json({ ...incident, persons, vehicles, sourceReports });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  } else if (req.method === 'PATCH') {
    try {
      const allowed = ['status', 'severity', 'priority', 'description', 'notes', 'tags', 'assigned_to'];
      const updates = {};
      allowed.forEach(f => { if (req.body[f] !== undefined) updates[f] = req.body[f]; });
      if (updates.assigned_to) updates.assigned_at = new Date();

      const [updated] = await db('incidents').where({ id }).update(updates).returning('*');
      await db('activity_log').insert({ user_id: user.id, incident_id: id, action: 'updated', details: JSON.stringify(updates) });
      res.json(updated);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  } else {
    res.status(405).json({ error: 'Method not allowed' });
  }
};
