const router = require('express').Router();
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const db = require('../../config/database');
const { authMiddleware } = require('../middleware/auth');

// POST /auth/login
router.post('/login', async (req, res) => {
  try {
    const { email, password } = req.body;
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
});

// POST /auth/register (admin only creates users)
router.post('/register', authMiddleware, async (req, res) => {
  try {
    if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });

    const { email, password, firstName, lastName, role, phone, assignedMetros, specialization } = req.body;
    const hash = await bcrypt.hash(password, 12);

    const [user] = await db('users').insert({
      email, password_hash: hash, first_name: firstName, last_name: lastName,
      role: role || 'rep', phone, assigned_metros: assignedMetros, specialization
    }).returning(['id', 'email', 'first_name', 'last_name', 'role']);

    res.status(201).json({ user });
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ error: 'Email already exists' });
    res.status(500).json({ error: err.message });
  }
});

// GET /auth/me
router.get('/me', authMiddleware, async (req, res) => {
  try {
    const user = await db('users').where({ id: req.user.id }).select('id', 'email', 'first_name', 'last_name', 'role', 'phone', 'assigned_metros', 'specialization', 'settings').first();
    res.json({ user });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
