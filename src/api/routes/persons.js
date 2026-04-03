const router = require('express').Router();
const db = require('../../config/database');
const { authMiddleware } = require('../middleware/auth');

router.use(authMiddleware);

// GET /persons - Search persons across all incidents
router.get('/', async (req, res) => {
  try {
    const { page = 1, limit = 50, search, injured, contactStatus, incidentId, hasAttorney } = req.query;

    let query = db('persons as p')
      .leftJoin('incidents as i', 'p.incident_id', 'i.id')
      .select('p.*', 'i.incident_type', 'i.severity as incident_severity',
        'i.city as incident_city', 'i.state as incident_state', 'i.occurred_at');

    if (search) {
      query = query.where(function () {
        this.whereILike('p.full_name', `%${search}%`)
          .orWhereILike('p.phone', `%${search}%`)
          .orWhereILike('p.email', `%${search}%`);
      });
    }
    if (injured === 'true') query = query.where('p.is_injured', true);
    if (contactStatus) query = query.where('p.contact_status', contactStatus);
    if (incidentId) query = query.where('p.incident_id', incidentId);
    if (hasAttorney === 'false') query = query.where(function () {
      this.where('p.has_attorney', false).orWhereNull('p.has_attorney');
    });

    const total = await query.clone().clearSelect().clearOrder().count('* as total').first();
    const persons = await query
      .orderBy('p.created_at', 'desc')
      .limit(Math.min(parseInt(limit), 200))
      .offset((parseInt(page) - 1) * parseInt(limit));

    res.json({ data: persons, pagination: { page: parseInt(page), limit: parseInt(limit), total: parseInt(total?.total || 0) } });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PATCH /persons/:id - Update person (contact status, notes, etc.)
router.patch('/:id', async (req, res) => {
  try {
    const allowed = ['contact_status', 'contact_attempts', 'last_contact_at', 'phone', 'phone_secondary',
      'email', 'address', 'insurance_company', 'insurance_policy_number', 'insurance_type',
      'policy_limits', 'has_attorney', 'attorney_name', 'attorney_firm', 'attorney_phone',
      'injury_description', 'transported_to', 'metadata'];
    const updates = {};
    allowed.forEach(f => { if (req.body[f] !== undefined) updates[f] = req.body[f]; });

    const [updated] = await db('persons').where({ id: req.params.id }).update(updates).returning('*');

    await db('activity_log').insert({
      user_id: req.user.id, person_id: req.params.id,
      incident_id: updated.incident_id, action: 'updated',
      details: JSON.stringify(updates)
    });

    res.json(updated);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /persons/:id/contact - Log contact attempt
router.post('/:id/contact', async (req, res) => {
  try {
    const { method, outcome, notes } = req.body;
    const person = await db('persons').where({ id: req.params.id }).first();

    await db('persons').where({ id: req.params.id }).update({
      contact_attempts: (person.contact_attempts || 0) + 1,
      last_contact_at: new Date(),
      contact_status: outcome || person.contact_status
    });

    await db('activity_log').insert({
      user_id: req.user.id, person_id: req.params.id,
      incident_id: person.incident_id, action: 'contacted',
      details: JSON.stringify({ method, outcome, notes })
    });

    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
