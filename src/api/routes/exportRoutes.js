const router = require('express').Router();
const db = require('../../config/database');
const { authMiddleware } = require('../middleware/auth');

router.use(authMiddleware);

// GET /export/incidents - Export incidents as CSV/JSON
router.get('/incidents', async (req, res) => {
  try {
    const { format = 'json', dateFrom, dateTo, metro, type, status, limit = 1000 } = req.query;

    let query = db('incidents as i')
      .leftJoin('metro_areas as ma', 'i.metro_area_id', 'ma.id')
      .select('i.incident_number', 'i.incident_type', 'i.severity', 'i.status',
        'i.address', 'i.city', 'i.state', 'i.zip', 'i.latitude', 'i.longitude',
        'i.occurred_at', 'i.discovered_at', 'i.description',
        'i.vehicles_involved', 'i.persons_involved', 'i.injuries_count', 'i.fatalities_count',
        'i.police_report_number', 'i.police_department', 'i.confidence_score',
        'i.source_count', 'ma.name as metro_area');

    if (dateFrom) query = query.where('i.discovered_at', '>=', dateFrom);
    if (dateTo) query = query.where('i.discovered_at', '<=', dateTo);
    if (metro) query = query.where('i.metro_area_id', metro);
    if (type) query = query.where('i.incident_type', type);
    if (status) query = query.where('i.status', status);

    const incidents = await query.orderBy('i.discovered_at', 'desc').limit(parseInt(limit));

    if (format === 'csv') {
      const fields = Object.keys(incidents[0] || {});
      const csvHeader = fields.join(',');
      const csvRows = incidents.map(row => fields.map(f => {
        const val = row[f];
        if (val === null || val === undefined) return '';
        const str = String(val);
        return str.includes(',') || str.includes('"') || str.includes('\n') ? `"${str.replace(/"/g, '""')}"` : str;
      }).join(','));

      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename=incidents_export.csv');
      res.send([csvHeader, ...csvRows].join('\n'));
    } else {
      res.json({ data: incidents, total: incidents.length });
    }

    await db('activity_log').insert({
      user_id: req.user.id, action: 'exported',
      details: JSON.stringify({ format, count: incidents.length, filters: { dateFrom, dateTo, metro, type, status } })
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /export/persons - Export persons data
router.get('/persons', async (req, res) => {
  try {
    const { format = 'json', incidentId, injured, hasAttorney, limit = 1000 } = req.query;

    let query = db('persons as p')
      .leftJoin('incidents as i', 'p.incident_id', 'i.id')
      .select('p.full_name', 'p.phone', 'p.email', 'p.role', 'p.is_injured',
        'p.injury_severity', 'p.injury_description', 'p.transported_to',
        'p.insurance_company', 'p.insurance_type', 'p.policy_limits',
        'p.has_attorney', 'p.attorney_name', 'p.contact_status',
        'i.incident_type', 'i.city as incident_city', 'i.state as incident_state',
        'i.occurred_at', 'i.police_report_number');

    if (incidentId) query = query.where('p.incident_id', incidentId);
    if (injured === 'true') query = query.where('p.is_injured', true);
    if (hasAttorney === 'false') query = query.where(function () {
      this.where('p.has_attorney', false).orWhereNull('p.has_attorney');
    });

    const persons = await query.orderBy('p.created_at', 'desc').limit(parseInt(limit));

    if (format === 'csv') {
      const fields = Object.keys(persons[0] || {});
      const csvHeader = fields.join(',');
      const csvRows = persons.map(row => fields.map(f => {
        const val = row[f];
        if (val === null || val === undefined) return '';
        const str = String(val);
        return str.includes(',') || str.includes('"') ? `"${str.replace(/"/g, '""')}"` : str;
      }).join(','));
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename=persons_export.csv');
      res.send([csvHeader, ...csvRows].join('\n'));
    } else {
      res.json({ data: persons, total: persons.length });
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
