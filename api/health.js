const { getDb } = require('./_db');

module.exports = async function handler(req, res) {
  try {
    const db = getDb();
    await db.raw('SELECT 1');
    res.json({ status: 'ok', timestamp: new Date().toISOString(), database: 'connected' });
  } catch (err) {
    res.status(500).json({ status: 'error', error: err.message });
  }
};
