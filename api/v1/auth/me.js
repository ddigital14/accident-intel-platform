const { getDb } = require('../../_db');
const { requireAuth } = require('../../_auth');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const user = requireAuth(req, res);
  if (!user) return;

  try {
    const db = getDb();
    const dbUser = await db('users').where({ id: user.id })
      .select('id', 'email', 'first_name', 'last_name', 'role', 'phone', 'assigned_metros', 'specialization', 'settings')
      .first();
    res.json({ user: dbUser });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
