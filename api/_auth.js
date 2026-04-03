/**
 * Shared auth helper for serverless functions
 */
const jwt = require('jsonwebtoken');

function verifyToken(req) {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return null;
  }
  try {
    return jwt.verify(authHeader.split(' ')[1], process.env.JWT_SECRET);
  } catch {
    return null;
  }
}

function requireAuth(req, res) {
  const user = verifyToken(req);
  if (!user) {
    res.status(401).json({ error: 'Authentication required' });
    return null;
  }
  return user;
}

module.exports = { verifyToken, requireAuth };
