/**
 * ACCIDENT INTEL PLATFORM - Main Server
 * Real-time accident and personal injury intelligence system
 */

require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const path = require('path');
const cron = require('node-cron');
const { logger } = require('./utils/logger');

const app = express();
const server = http.createServer(app);

// WebSocket for real-time updates
const io = new Server(server, {
  cors: {
    origin: process.env.CORS_ORIGIN || '*',
    methods: ['GET', 'POST']
  }
});

// Make io accessible to routes
app.set('io', io);

// ============================================================================
// MIDDLEWARE
// ============================================================================

app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors());
app.use(compression());
app.use(morgan('combined', { stream: { write: msg => logger.info(msg.trim()) } }));
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 min
  max: 1000,
  message: { error: 'Too many requests, please try again later.' }
});
app.use('/api/', limiter);

// ============================================================================
// API ROUTES
// ============================================================================

const PREFIX = process.env.API_PREFIX || '/api/v1';

app.use(`${PREFIX}/auth`, require('./api/routes/auth'));
app.use(`${PREFIX}/incidents`, require('./api/routes/incidents'));
app.use(`${PREFIX}/persons`, require('./api/routes/persons'));
app.use(`${PREFIX}/vehicles`, require('./api/routes/vehicles'));
app.use(`${PREFIX}/dashboard`, require('./api/routes/dashboard'));
app.use(`${PREFIX}/sources`, require('./api/routes/sources'));
app.use(`${PREFIX}/users`, require('./api/routes/users'));
app.use(`${PREFIX}/alerts`, require('./api/routes/alerts'));
app.use(`${PREFIX}/export`, require('./api/routes/exportRoutes'));

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString(), uptime: process.uptime() });
});

// Serve frontend in production
if (process.env.NODE_ENV === 'production') {
  app.use(express.static(path.join(__dirname, '../frontend/build')));
  app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, '../frontend/build/index.html'));
  });
}

// Error handler
app.use((err, req, res, next) => {
  logger.error('Unhandled error:', err);
  res.status(err.status || 500).json({
    error: process.env.NODE_ENV === 'production' ? 'Internal server error' : err.message,
    ...(process.env.NODE_ENV !== 'production' && { stack: err.stack })
  });
});

// ============================================================================
// WEBSOCKET
// ============================================================================

const { authMiddleware } = require('./api/middleware/auth');

io.use((socket, next) => {
  const token = socket.handshake.auth.token;
  if (!token) return next(new Error('Authentication required'));
  try {
    const jwt = require('jsonwebtoken');
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    socket.user = decoded;
    next();
  } catch (err) {
    next(new Error('Invalid token'));
  }
});

io.on('connection', (socket) => {
  logger.info(`WebSocket connected: ${socket.user.email}`);

  // Join metro area rooms
  if (socket.user.metros) {
    socket.user.metros.forEach(metro => socket.join(`metro:${metro}`));
  }
  socket.join('all-incidents');

  socket.on('subscribe:metro', (metroId) => socket.join(`metro:${metroId}`));
  socket.on('unsubscribe:metro', (metroId) => socket.leave(`metro:${metroId}`));

  socket.on('disconnect', () => {
    logger.info(`WebSocket disconnected: ${socket.user.email}`);
  });
});

// ============================================================================
// SCHEDULED JOBS
// ============================================================================

// Import ingestion runner
const { runIngestionCycle } = require('./ingestion/runner');
const { runEnrichmentCycle } = require('./ingestion/processors/enrichment');
const { runDeduplication } = require('./ingestion/processors/deduplication');

// Run data ingestion every 30 seconds
cron.schedule('*/30 * * * * *', async () => {
  try {
    await runIngestionCycle(io);
  } catch (err) {
    logger.error('Ingestion cycle error:', err);
  }
});

// Run enrichment every 2 minutes
cron.schedule('*/2 * * * *', async () => {
  try {
    await runEnrichmentCycle(io);
  } catch (err) {
    logger.error('Enrichment cycle error:', err);
  }
});

// Run deduplication every 5 minutes
cron.schedule('*/5 * * * *', async () => {
  try {
    await runDeduplication();
  } catch (err) {
    logger.error('Deduplication cycle error:', err);
  }
});

// ============================================================================
// START SERVER
// ============================================================================

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  logger.info(`🚨 Accident Intel Platform running on port ${PORT}`);
  logger.info(`   Environment: ${process.env.NODE_ENV || 'development'}`);
  logger.info(`   API: http://localhost:${PORT}${PREFIX}`);
  logger.info(`   WebSocket: ws://localhost:${PORT}`);
});

module.exports = { app, server, io };
