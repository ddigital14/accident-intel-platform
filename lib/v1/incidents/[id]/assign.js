const { getDb } = require('../../../_db');
const { requireAuth } = require('../../../_auth');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const user = requireAuth(req, res);
  if (!user) return;
  const db = getDb();
  const { id } = req.query;

  try {
    const { userId } = req.body;
    const assignTo = userId || user.id;

    const [updated] = await db('incidents').where({ id }).update({
      assigned_to: assignTo,
      assigned_at: new Date(),
      status: 'assigned'
    }).returning('*');

    await db('activity_log').insert({
      user_id: user.id,
      incident_id: id,
      action: 'assigned',
      details: JSON.stringify({ assigned_to: assignTo })
    });

    res.json(updated);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
