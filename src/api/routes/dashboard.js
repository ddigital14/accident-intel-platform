const router = require('express').Router();
const db = require('../../config/database');
const { authMiddleware } = require('../middleware/auth');

router.use(authMiddleware);

// GET /dashboard/stats - Overview statistics
router.get('/stats', async (req, res) => {
  try {
    const { metro, dateFrom, dateTo, period = 'today' } = req.query;
    let startDate;
    const now = new Date();

    switch (period) {
      case 'today': startDate = new Date(now.setHours(0, 0, 0, 0)); break;
      case 'week': startDate = new Date(now.setDate(now.getDate() - 7)); break;
      case 'month': startDate = new Date(now.setMonth(now.getMonth() - 1)); break;
      default: startDate = dateFrom ? new Date(dateFrom) : new Date(now.setHours(0, 0, 0, 0));
    }

    let baseQuery = db('incidents').where('discovered_at', '>=', startDate);
    if (dateTo) baseQuery = baseQuery.where('discovered_at', '<=', dateTo);
    if (metro) baseQuery = baseQuery.where('metro_area_id', metro);

    const [totals, byType, bySeverity, byMetro, byHour, recentHighPriority] = await Promise.all([
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
        .where('i.discovered_at', '>=', startDate)
        .select('ma.name as metro', db.raw('COUNT(*) as count'))
        .groupBy('ma.name').orderBy('count', 'desc'),

      baseQuery.clone().select(
        db.raw("EXTRACT(HOUR FROM discovered_at) as hour"),
        db.raw('COUNT(*) as count')
      ).groupBy(db.raw("EXTRACT(HOUR FROM discovered_at)")).orderBy('hour'),

      baseQuery.clone().where('priority', '<=', 3).orderBy('discovered_at', 'desc').limit(10)
        .select('id', 'incident_type', 'severity', 'city', 'state', 'description', 'discovered_at', 'priority')
    ]);

    res.json({ totals, byType, bySeverity, byMetro, byHour, recentHighPriority });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /dashboard/feed - Real-time incident feed (last N minutes)
router.get('/feed', async (req, res) => {
  try {
    const { minutes = 30, metro, type } = req.query;
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
});

// GET /dashboard/my-assignments
router.get('/my-assignments', async (req, res) => {
  try {
    const incidents = await db('incidents as i')
      .leftJoin('metro_areas as ma', 'i.metro_area_id', 'ma.id')
      .where('i.assigned_to', req.user.id)
      .whereIn('i.status', ['assigned', 'in_progress'])
      .select('i.*', 'ma.name as metro_area_name')
      .orderBy('i.priority', 'asc')
      .orderBy('i.discovered_at', 'desc');

    res.json({ data: incidents, total: incidents.length });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /dashboard/metro-areas
router.get('/metro-areas', async (req, res) => {
  try {
    const metros = await db('metro_areas').where('is_active', true).orderBy('name');
    res.json({ data: metros });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
