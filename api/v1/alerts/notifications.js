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
      const { unreadOnly } = req.query;
      let query = db('notifications').where({ user_id: user.id });
      if (unreadOnly === 'true') query = query.where('is_read', false);
      const notifications = await query.orderBy('created_at', 'desc').limit(100);
      res.json({ data: notifications });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  } else if (req.method === 'POST') {
    // Mark notifications as read
    try {
      const { ids } = req.body;
      if (ids && ids.length) {
        await db('notifications')
          .whereIn('id', ids)
          .where('user_id', user.id)
          .update({ is_read: true, read_at: new Date() });
      } else {
        await db('notifications')
          .where('user_id', user.id)
          .where('is_read', false)
          .update({ is_read: true, read_at: new Date() });
      }
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  } else {
    res.status(405).json({ error: 'Method not allowed' });
  }
};
