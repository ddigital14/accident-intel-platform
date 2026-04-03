const router = require('express').Router();
const db = require('../../config/database');
const { authMiddleware, requireRole } = require('../middleware/auth');

router.use(authMiddleware);

router.get('/', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const users = await db('users').select('id', 'email', 'first_name', 'last_name', 'role', 'phone', 'assigned_metros', 'specialization', 'is_active', 'last_login_at').orderBy('last_name');
    res.json({ data: users });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/:id/stats', requireRole('admin', 'manager'), async (req, res) => {
  try {
    const stats = await db('incidents').where('assigned_to', req.params.id)
      .select(
        db.raw('COUNT(*) as total_assigned'),
        db.raw("COUNT(*) FILTER (WHERE status = 'contacted') as contacted"),
        db.raw("COUNT(*) FILTER (WHERE status = 'in_progress') as in_progress"),
        db.raw("COUNT(*) FILTER (WHERE status = 'closed') as closed")
      ).first();

    const recentActivity = await db('activity_log').where('user_id', req.params.id).orderBy('created_at', 'desc').limit(20);
    res.json({ stats, recentActivity });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
