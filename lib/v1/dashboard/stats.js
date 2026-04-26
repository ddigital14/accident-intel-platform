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
    const { metro, dateFrom, dateTo, period = 'today' } = req.query;
    let startDate;
    const now = new Date();

    switch (period) {
      case 'today': startDate = new Date(now.getFullYear(), now.getMonth(), now.getDate()); break;
      case 'week': startDate = new Date(now - 7 * 86400000); break;
      case 'month': startDate = new Date(now - 30 * 86400000); break;
      default: startDate = dateFrom ? new Date(dateFrom) : new Date(now.getFullYear(), now.getMonth(), now.getDate());
    }

    let baseQuery = db('incidents').where('discovered_at', '>=', startDate);
    if (dateTo) baseQuery = baseQuery.where('discovered_at', '<=', dateTo);
    if (metro) baseQuery = baseQuery.where('metro_area_id', metro);

    const [totals, byType, bySeverity, byMetro, recentHighPriority] = await Promise.all([
      baseQuery.clone().select(
        db.raw('COUNT(*) as total_incidents'),
        db.raw("COUNT(*) FILTER (WHERE status = 'new') as new_incidents"),
        db.raw("COUNT(*) FILTER (WHERE status = 'assigned') as assigned_incidents"),
        db.raw('SUM(COALESCE(injuries_count, 0)) as total_injuries'),
        db.raw('SUM(COALESCE(fatalities_count, 0)) as total_fatalities'),
        db.raw('AVG(confidence_score) as avg_confidence'),
        db.raw("COUNT(*) FILTER (WHERE severity IN ('fatal','critical','serious')) as high_severity_count"),
        db.raw('COUNT(DISTINCT assigned_to) as active_reps')
      ).first(),
      baseQuery.clone().select('incident_type', db.raw('COUNT(*) as count')).groupBy('incident_type').orderBy('count', 'desc'),
      baseQuery.clone().select('severity', db.raw('COUNT(*) as count')).groupBy('severity'),
      db('incidents as i').leftJoin('metro_areas as ma', 'i.metro_area_id', 'ma.id')
        .where('i.discovered_at', '>=', startDate).select('ma.name as metro', db.raw('COUNT(*) as count')).groupBy('ma.name').orderBy('count', 'desc'),
      baseQuery.clone().where('priority', '<=', 3).orderBy('discovered_at', 'desc').limit(10)
        .select('id', 'incident_type', 'severity', 'city', 'state', 'description', 'discovered_at', 'priority')
    ]);

    res.json({ totals, byType, bySeverity, byMetro, recentHighPriority });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
