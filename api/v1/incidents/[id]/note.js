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
    const { note } = req.body;
    if (!note || !note.trim()) return res.status(400).json({ error: 'Note content required' });

    const currentNotes = await db('incidents').where({ id }).select('notes').first();
    const existingNotes = currentNotes?.notes || '';
    const timestamp = new Date().toISOString();
    const newNote = existingNotes
      ? existingNotes + '\n---\n[' + timestamp + ' - ' + user.email + '] ' + note.trim()
      : '[' + timestamp + ' - ' + user.email + '] ' + note.trim();

    const [updated] = await db('incidents').where({ id }).update({ notes: newNote }).returning('*');
    await db('activity_log').insert({ user_id: user.id, incident_id: id, action: 'note_added', details: JSON.stringify({ note: note.trim() }) });
    res.json(updated);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
