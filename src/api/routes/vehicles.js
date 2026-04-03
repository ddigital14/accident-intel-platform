const router = require('express').Router();
const db = require('../../config/database');
const { authMiddleware } = require('../middleware/auth');

router.use(authMiddleware);

router.get('/', async (req, res) => {
  try {
    const { incidentId, commercial, search, page = 1, limit = 50 } = req.query;
    let query = db('vehicles as v').leftJoin('incidents as i', 'v.incident_id', 'i.id').select('v.*', 'i.city', 'i.state', 'i.occurred_at');

    if (incidentId) query = query.where('v.incident_id', incidentId);
    if (commercial === 'true') query = query.where('v.is_commercial', true);
    if (search) {
      query = query.where(function () {
        this.whereILike('v.make', `%${search}%`).orWhereILike('v.model', `%${search}%`)
          .orWhereILike('v.license_plate', `%${search}%`).orWhereILike('v.vin', `%${search}%`);
      });
    }

    const vehicles = await query.orderBy('v.created_at', 'desc').limit(parseInt(limit)).offset((parseInt(page) - 1) * parseInt(limit));
    res.json({ data: vehicles });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.patch('/:id', async (req, res) => {
  try {
    const allowed = ['year', 'make', 'model', 'color', 'license_plate', 'license_state', 'vin', 'damage_severity', 'damage_description', 'towed', 'tow_company', 'insurance_company', 'insurance_policy', 'is_commercial', 'dot_number', 'carrier_name', 'carrier_mc_number'];
    const updates = {};
    allowed.forEach(f => { if (req.body[f] !== undefined) updates[f] = req.body[f]; });
    const [updated] = await db('vehicles').where({ id: req.params.id }).update(updates).returning('*');
    res.json(updated);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
