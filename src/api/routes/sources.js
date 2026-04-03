const router = require('express').Router();
const db = require('../../config/database');
const { authMiddleware, requireRole } = require('../middleware/auth');

router.use(authMiddleware);

router.get('/', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const sources = await db('data_sources').orderBy('type').orderBy('name');
    res.json({ data: sources });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/:id/stats', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const stats = await db('source_reports').where('data_source_id', req.params.id)
      .select(
        db.raw('COUNT(*) as total_reports'),
        db.raw("COUNT(*) FILTER (WHERE is_verified = true) as verified"),
        db.raw('AVG(confidence) as avg_confidence'),
        db.raw('MAX(fetched_at) as last_fetched')
      ).first();
    res.json(stats);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.patch('/:id', requireRole('admin'), async (req, res) => {
  try {
    const { is_active, polling_interval_seconds, config } = req.body;
    const updates = {};
    if (is_active !== undefined) updates.is_active = is_active;
    if (polling_interval_seconds) updates.polling_interval_seconds = polling_interval_seconds;
    if (config) updates.config = config;
    const [updated] = await db('data_sources').where({ id: req.params.id }).update(updates).returning('*');
    res.json(updated);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
