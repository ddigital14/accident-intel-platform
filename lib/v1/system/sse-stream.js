/**
 * Phase 34: Server-Sent Events stream. Replaces realtime-feed long-poll on Pro.
 * Works by holding the response open and sending periodic "events".
 * Vercel Pro has 60s function timeout — we send pings every 25s and close at 55s,
 * client reconnects automatically (EventSource native behavior).
 */
const { getDb } = require('../../_db');
const { setCursor, getCursor } = require('./kv-cursor');

module.exports = async function handler(req, res) {
  const db = getDb();
  const minScore = parseInt(req.query?.min_score) || 70;
  const userId = req.query?.user_id;
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.write(`: connected\n\n`);

  let lastSeen = null;
  try { const k = await getCursor(`sse:lastseen:${userId || 'all'}`); if (k) lastSeen = new Date(k); } catch (_) {}
  if (!lastSeen) lastSeen = new Date(Date.now() - 5 * 60 * 1000);

  const start = Date.now();
  const TIMEOUT_MS = 55000; // close before Vercel 60s

  while (Date.now() - start < TIMEOUT_MS) {
    try {
      const rows = await db('incidents')
        .leftJoin('persons', 'persons.incident_id', 'incidents.id')
        .where('incidents.created_at', '>', lastSeen)
        .where('incidents.lead_score', '>=', minScore)
        .orderBy('incidents.created_at', 'asc')
        .limit(20)
        .select('incidents.id','incidents.description','incidents.severity','incidents.city','incidents.state','incidents.lead_score','incidents.created_at','persons.full_name','persons.phone');
      for (const r of rows) {
        res.write(`event: lead\ndata: ${JSON.stringify(r)}\n\n`);
        if (new Date(r.created_at) > lastSeen) lastSeen = new Date(r.created_at);
      }
      if (userId) await setCursor(`sse:lastseen:${userId}`, lastSeen.toISOString(), 600).catch(() => {});
      res.write(`event: ping\ndata: {"t":${Date.now()}}\n\n`);
    } catch (e) {
      res.write(`event: error\ndata: ${JSON.stringify({ msg: e.message })}\n\n`);
    }
    await new Promise(r => setTimeout(r, 3000));
  }
  res.write(`event: reconnect\ndata: {}\n\n`);
  res.end();
};
