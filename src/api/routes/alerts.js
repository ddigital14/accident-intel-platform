const router = require('express').Router();
const db = require('../../config/database');
const { authMiddleware } = require('../middleware/auth');

router.use(authMiddleware);

router.get('/rules', async (req, res) => {
  try {
    const rules = await db('alert_rules').where({ user_id: req.user.id }).orderBy('created_at', 'desc');
    res.json({ data: rules });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/rules', async (req, res) => {
  try {
    const { name, conditions, notify_email, notify_sms, notify_dashboard } = req.body;
    const [rule] = await db('alert_rules').insert({
      user_id: req.user.id, name, conditions: JSON.stringify(conditions),
      notify_email, notify_sms, notify_dashboard
    }).returning('*');
    res.status(201).json(rule);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.get('/notifications', async (req, res) => {
  try {
    const { unreadOnly } = req.query;
    let query = db('notifications').where({ user_id: req.user.id });
    if (unreadOnly === 'true') query = query.where('is_read', false);
    const notifications = await query.orderBy('created_at', 'desc').limit(100);
    res.json({ data: notifications });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

router.post('/notifications/read', async (req, res) => {
  try {
    const { ids } = req.body;
    if (ids && ids.length) {
      await db('notifications').whereIn('id', ids).where('user_id', req.user.id).update({ is_read: true, read_at: new Date() });
    } else {
      await db('notifications').where('user_id', req.user.id).where('is_read', false).update({ is_read: true, read_at: new Date() });
    }
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

module.exports = router;
