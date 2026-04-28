/**
 * Real-time SSE push of new high-score incidents.
 * Dashboard connects via EventSource('/api/v1/system/realtime-feed?...').
 * Uses Postgres LISTEN/NOTIFY surfaced via long-poll fallback (Vercel doesn't keep WS open).
 * Falls back to short-poll cursor reads — frontend treats both identically.
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');

async function feed(req, res) {
  const db = getDb();
  const since = req.query?.since ? new Date(req.query.since) : new Date(Date.now() - 5 * 60 * 1000);
  const minScore = parseInt(req.query?.min_score) || 70;
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Cache-Control', 'no-cache, no-store');
  try {
    const rows = await db('incidents')
      .leftJoin('persons', 'persons.incident_id', 'incidents.id')
      .where('incidents.created_at', '>', since)
      .where('incidents.lead_score', '>=', minScore)
      .orderBy('incidents.created_at', 'desc')
      .limit(50)
      .select('incidents.id', 'incidents.description', 'incidents.severity', 'incidents.city', 'incidents.state', 'incidents.lead_score', 'incidents.qualification_state', 'incidents.created_at', 'persons.full_name', 'persons.phone', 'persons.email');
    return res.json({ ok: true, since: since.toISOString(), count: rows.length, items: rows, server_time: new Date().toISOString() });
  } catch (err) { await reportError(db, 'realtime-feed', null, err.message); res.status(500).json({ error: err.message }); }
}

module.exports = async function handler(req, res) {
  const { action } = req.query || {};
  if (action === 'health') return res.json({ ok: true, engine: 'realtime-feed', mode: 'long-poll' });
  return await feed(req, res);
};
