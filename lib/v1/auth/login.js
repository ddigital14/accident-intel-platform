const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { getDb } = require('../../_db');

module.exports = async function handler(req, res) {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const { email, password } = req.body;
    const db = getDb();
    const user = await db('users').where({ email, is_active: true }).first();

    if (!user || !(await bcrypt.compare(password, user.password_hash))) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    await db('users').where({ id: user.id }).update({ last_login_at: new Date() });

    const token = jwt.sign(
      { id: user.id, email: user.email, role: user.role, metros: user.assigned_metros },
      process.env.JWT_SECRET,
      { expiresIn: '12h' }
    );

    res.json({
      token,
      user: {
        id: user.id, email: user.email, firstName: user.first_name,
        lastName: user.last_name, role: user.role
      }
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
