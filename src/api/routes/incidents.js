const router = require('express').Router();
const db = require('../../config/database');
const { authMiddleware, requireRole } = require('../middleware/auth');

router.use(authMiddleware);

// GET /incidents - Live feed with filters
router.get('/', async (req, res) => {
  try {
    const {
      page = 1, limit = 50, type, severity, status, metro, city, state,
      dateFrom, dateTo, minConfidence, assigned, hasAttorney, search,
      sortBy = 'discovered_at', sortDir = 'desc', priority
    } = req.query;

    let query = db('incidents as i')
      .leftJoin('metro_areas as ma', 'i.metro_area_id', 'ma.id')
      .leftJoin('users as u', 'i.assigned_to', 'u.id')
      .select(
        'i.*',
        'ma.name as metro_area_name',
        db.raw("u.first_name || ' ' || u.last_name as assigned_to_name")
      );

    // Filters
    if (type) query = query.where('i.incident_type', type);
    if (severity) query = query.where('i.severity', severity);
    if (status) query = query.where('i.status', status);
    if (metro) query = query.where('i.metro_area_id', metro);
    if (city) query = query.whereILike('i.city', `%${city}%`);
    if (state) query = query.where('i.state', state);
    if (dateFrom) query = query.where('i.discovered_at', '>=', dateFrom);
    if (dateTo) query = query.where('i.discovered_at', '<=', dateTo);
    if (minConfidence) query = query.where('i.confidence_score', '>=', parseFloat(minConfidence));
    if (assigned === 'me') query = query.where('i.assigned_to', req.user.id);
    if (assigned === 'unassigned') query = query.whereNull('i.assigned_to');
    if (priority) query = query.where('i.priority', '<=', parseInt(priority));

    if (search) {
      query = query.where(function () {
        this.whereILike('i.description', `%${search}%`)
          .orWhereILike('i.address', `%${search}%`)
          .orWhereILike('i.police_report_number', `%${search}%`)
          .orWhereILike('i.incident_number', `%${search}%`);
      });
    }

    // Filter by attorney status through subquery
    if (hasAttorney === 'false') {
      query = query.whereNotExists(function () {
        this.select(db.raw(1)).from('persons as p')
          .whereRaw('p.incident_id = i.id').where('p.has_attorney', true);
      });
    }

    // Count total for pagination
    const countQuery = query.clone().clearSelect().clearOrder().count('* as total').first();

    // Apply sort and pagination
    const allowedSorts = ['discovered_at', 'occurred_at', 'severity', 'confidence_score', 'priority', 'source_count'];
    const sortField = allowedSorts.includes(sortBy) ? `i.${sortBy}` : 'i.discovered_at';
    query = query.orderBy(sortField, sortDir === 'asc' ? 'asc' : 'desc')
      .limit(Math.min(parseInt(limit), 200))
      .offset((parseInt(page) - 1) * parseInt(limit));

    const [incidents, countResult] = await Promise.all([query, countQuery]);
    const total = parseInt(countResult?.total || 0);

    // Attach persons summary for each incident
    const incidentIds = incidents.map(i => i.id);
    const persons = incidentIds.length > 0
      ? await db('persons').whereIn('incident_id', incidentIds)
        .select('incident_id', 'full_name', 'phone', 'is_injured', 'injury_severity',
          'insurance_company', 'policy_limits', 'contact_status', 'has_attorney', 'role')
      : [];

    const personsByIncident = {};
    persons.forEach(p => {
      if (!personsByIncident[p.incident_id]) personsByIncident[p.incident_id] = [];
      personsByIncident[p.incident_id].push(p);
    });

    const enrichedIncidents = incidents.map(i => ({
      ...i,
      persons: personsByIncident[i.id] || []
    }));

    res.json({
      data: enrichedIncidents,
      pagination: {
        page: parseInt(page), limit: parseInt(limit),
        total, totalPages: Math.ceil(total / parseInt(limit))
      }
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /incidents/:id - Full incident detail
router.get('/:id', async (req, res) => {
  try {
    const incident = await db('incidents as i')
      .leftJoin('metro_areas as ma', 'i.metro_area_id', 'ma.id')
      .leftJoin('users as u', 'i.assigned_to', 'u.id')
      .where('i.id', req.params.id)
      .select('i.*', 'ma.name as metro_area_name',
        db.raw("u.first_name || ' ' || u.last_name as assigned_to_name"))
      .first();

    if (!incident) return res.status(404).json({ error: 'Incident not found' });

    const [persons, vehicles, sourceReports, activityLog] = await Promise.all([
      db('persons').where({ incident_id: incident.id }).orderBy('role'),
      db('vehicles').where({ incident_id: incident.id }),
      db('source_reports as sr')
        .leftJoin('data_sources as ds', 'sr.data_source_id', 'ds.id')
        .where('sr.incident_id', incident.id)
        .select('sr.*', 'ds.name as source_name', 'ds.type as source_type_name')
        .orderBy('sr.fetched_at', 'desc'),
      db('activity_log as al')
        .leftJoin('users as u', 'al.user_id', 'u.id')
        .where('al.incident_id', incident.id)
        .select('al.*', db.raw("u.first_name || ' ' || u.last_name as user_name"))
        .orderBy('al.created_at', 'desc')
        .limit(50)
    ]);

    // Log view
    await db('activity_log').insert({
      user_id: req.user.id, incident_id: incident.id, action: 'viewed'
    });

    res.json({ ...incident, persons, vehicles, sourceReports, activityLog });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PATCH /incidents/:id - Update incident
router.patch('/:id', async (req, res) => {
  try {
    const allowed = ['status', 'severity', 'priority', 'description', 'notes', 'tags', 'assigned_to'];
    const updates = {};
    allowed.forEach(f => { if (req.body[f] !== undefined) updates[f] = req.body[f]; });

    if (updates.assigned_to) updates.assigned_at = new Date();

    const [updated] = await db('incidents').where({ id: req.params.id }).update(updates).returning('*');

    await db('activity_log').insert({
      user_id: req.user.id, incident_id: req.params.id,
      action: 'updated', details: JSON.stringify(updates)
    });

    // Notify via WebSocket
    const io = req.app.get('io');
    if (io) io.to('all-incidents').emit('incident:updated', updated);

    res.json(updated);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /incidents/:id/assign - Assign to rep
router.post('/:id/assign', async (req, res) => {
  try {
    const { userId } = req.body;
    const [updated] = await db('incidents').where({ id: req.params.id })
      .update({ assigned_to: userId, assigned_at: new Date(), status: 'assigned' })
      .returning('*');

    await db('activity_log').insert({
      user_id: req.user.id, incident_id: req.params.id,
      action: 'assigned', details: JSON.stringify({ assigned_to: userId })
    });

    res.json(updated);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /incidents/:id/note - Add a note
router.post('/:id/note', async (req, res) => {
  try {
    const { note } = req.body;
    const incident = await db('incidents').where({ id: req.params.id }).first();
    const existingNotes = incident.notes || '';
    const timestamp = new Date().toISOString();
    const newNotes = `${existingNotes}\n[${timestamp}] ${req.user.email}: ${note}`.trim();

    await db('incidents').where({ id: req.params.id }).update({ notes: newNotes });
    await db('activity_log').insert({
      user_id: req.user.id, incident_id: req.params.id,
      action: 'noted', details: JSON.stringify({ note })
    });

    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
