const { getDb } = require('../../_db');
const { requireAuth } = require('../../_auth');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const user = requireAuth(req, res);
  if (!user) return;
  const db = getDb();

  if (req.method === 'GET') {
    try {
      const {
        page = 1, limit = 50, type, severity, status, metro, city, state,
        dateFrom, dateTo, minConfidence, assigned, hasAttorney, search,
        sortBy = 'discovered_at', sortDir = 'desc', priority,
        qualification_state, has_contact_info
      } = req.query;

      let query = db('incidents as i')
        .leftJoin('metro_areas as ma', 'i.metro_area_id', 'ma.id')
        .leftJoin('users as u', 'i.assigned_to', 'u.id')
        .select('i.*', 'ma.name as metro_area_name', db.raw("u.first_name || ' ' || u.last_name as assigned_to_name"));

      if (type) query = query.where('i.incident_type', type);
      if (severity) query = query.where('i.severity', severity);
      if (status) query = query.where('i.status', status);
      if (metro) query = query.where('i.metro_area_id', metro);
      if (city) query = query.whereILike('i.city', `%${city}%`);
      if (state) query = query.where('i.state', state);
      if (dateFrom) query = query.where('i.discovered_at', '>=', dateFrom);
      if (dateTo) query = query.where('i.discovered_at', '<=', dateTo);
      if (minConfidence) query = query.where('i.confidence_score', '>=', parseFloat(minConfidence));
      if (assigned === 'me') query = query.where('i.assigned_to', user.id);
      if (assigned === 'unassigned') query = query.whereNull('i.assigned_to');
      if (priority) query = query.where('i.priority', '<=', parseInt(priority));

      if (search) {
        query = query.where(function () {
          this.whereILike('i.description', `%${search}%`)
            .orWhereILike('i.address', `%${search}%`)
            .orWhereILike('i.police_report_number', `%${search}%`);
        });
      }

      if (hasAttorney === 'false') {
        query = query.whereNotExists(function () {
          this.select(db.raw(1)).from('persons as p')
            .whereRaw('p.incident_id = i.id').where('p.has_attorney', true);
        });
      }

      // Qualification state filter
      if (qualification_state === 'qualified') {
        query = query.where('i.qualification_state', 'qualified');
      } else if (qualification_state === 'pending_named') {
        query = query.where('i.qualification_state', 'pending_named');
      } else if (qualification_state === 'pending') {
        query = query.where('i.qualification_state', 'pending');
      } else if (qualification_state === 'has_name') {
        // Has at least one named person
        query = query.whereExists(function () {
          this.select(db.raw(1)).from('persons as p')
            .whereRaw('p.incident_id = i.id')
            .whereNotNull('p.full_name')
            .where('p.full_name', '<>', '');
        });
      }

      // Has contact info filter — at least one person with name + (phone OR email OR address)
      if (has_contact_info === 'true') {
        query = query.whereExists(function () {
          this.select(db.raw(1)).from('persons as p')
            .whereRaw('p.incident_id = i.id')
            .whereNotNull('p.full_name')
            .where('p.full_name', '<>', '')
            .where(function () {
              this.whereNotNull('p.phone').andWhere('p.phone', '<>', '')
                .orWhere(function () { this.whereNotNull('p.email').andWhere('p.email', '<>', ''); })
                .orWhere(function () { this.whereNotNull('p.address').andWhere('p.address', '<>', ''); });
            });
        });
      }

      const countQuery = query.clone().clearSelect().clearOrder().count('* as total').first();
      const allowedSorts = ['discovered_at', 'occurred_at', 'severity', 'confidence_score', 'priority', 'source_count'];
      const sortField = allowedSorts.includes(sortBy) ? `i.${sortBy}` : 'i.discovered_at';
      query = query.orderBy(sortField, sortDir === 'asc' ? 'asc' : 'desc')
        .limit(Math.min(parseInt(limit), 200))
        .offset((parseInt(page) - 1) * parseInt(limit));

      const [incidents, countResult] = await Promise.all([query, countQuery]);
      const total = parseInt(countResult?.total || 0);

      const incidentIds = incidents.map(i => i.id);
      const persons = incidentIds.length > 0
        ? await db('persons').whereIn('incident_id', incidentIds)
          .select('id', 'incident_id', 'full_name', 'first_name', 'last_name',
            'phone', 'phone_secondary', 'email', 'address', 'city', 'state', 'zip',
            'age', 'is_injured', 'injury_severity', 'transported_to',
            'insurance_company', 'policy_limits', 'contact_status', 'has_attorney',
            'attorney_name', 'attorney_firm', 'role', 'enrichment_score', 'confidence_score')
        : [];

      const personsByIncident = {};
      persons.forEach(p => {
        if (!personsByIncident[p.incident_id]) personsByIncident[p.incident_id] = [];
        personsByIncident[p.incident_id].push(p);
      });

      const enrichedIncidents = incidents.map(i => ({ ...i, persons: personsByIncident[i.id] || [] }));

      res.json({
        data: enrichedIncidents,
        pagination: { page: parseInt(page), limit: parseInt(limit), total, totalPages: Math.ceil(total / parseInt(limit)) }
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  } else {
    res.status(405).json({ error: 'Method not allowed' });
  }
};
