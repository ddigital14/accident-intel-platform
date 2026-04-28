/**
 * Async transcription queue. Scanner ingest pushes audio URLs to queue,
 * cron-triggered worker drains queue in batches. Decouples slow Whisper from fast scanner cron.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { getModelForTask } = require('./model-registry');
const { trackApiCall } = require('./cost');
const { reportError } = require('./_errors');

async function ensureTable(db) {
  await db.raw(`CREATE TABLE IF NOT EXISTS transcription_queue (
    id SERIAL PRIMARY KEY,
    audio_url TEXT NOT NULL,
    metro TEXT,
    state TEXT,
    enqueued_at TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    transcript TEXT,
    error TEXT,
    status TEXT DEFAULT 'queued'
  )`).catch(() => {});
  await db.raw(`CREATE INDEX IF NOT EXISTS idx_tq_status ON transcription_queue (status, enqueued_at) WHERE status IN ('queued', 'in_progress')`).catch(() => {});
}

async function enqueue(db, audio_url, metro, state) {
  await ensureTable(db);
  await db('transcription_queue').insert({ audio_url, metro, state, status: 'queued' });
}

async function drainOne(db) {
  await ensureTable(db);
  const claim = await db.raw(`UPDATE transcription_queue SET status = 'in_progress', started_at = NOW()
    WHERE id = (SELECT id FROM transcription_queue WHERE status = 'queued' ORDER BY enqueued_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED)
    RETURNING *`).then(r => r.rows || r).catch(() => []);
  if (!claim || !claim[0]) return null;
  const job = claim[0];
  const model = await getModelForTask('transcription', 'whisper-1');
  try {
    const audioRes = await fetch(job.audio_url, { timeout: 15000 });
    if (!audioRes.ok) throw new Error(`audio_fetch_${audioRes.status}`);
    const buf = await audioRes.buffer();
    const fd = new (require('form-data'))();
    fd.append('file', buf, { filename: 'a.mp3' });
    fd.append('model', model);
    const r = await fetch('https://api.openai.com/v1/audio/transcriptions', {
      method: 'POST', headers: { Authorization: `Bearer ${process.env.OPENAI_API_KEY}`, ...fd.getHeaders() }, body: fd, timeout: 60000
    });
    const data = await r.json();
    await db('transcription_queue').where({ id: job.id }).update({ status: 'done', finished_at: new Date(), transcript: data.text || null });
    await trackApiCall(db, 'system-transcription-queue', model, 0, 0, true).catch(() => {});
    return { id: job.id, ok: true, length: (data.text || '').length };
  } catch (e) {
    await db('transcription_queue').where({ id: job.id }).update({ status: 'failed', finished_at: new Date(), error: e.message });
    return { id: job.id, ok: false, error: e.message };
  }
}

async function drainBatch(db, limit = 10) {
  let processed = 0;
  for (let i = 0; i < limit; i++) {
    const r = await drainOne(db);
    if (!r) break;
    processed++;
  }
  return { processed };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { action } = req.query || {};
    if (action === 'health') {
      await ensureTable(db);
      const c = await db('transcription_queue').select(db.raw('status, count(*)::int')).groupBy('status').catch(() => []);
      return res.json({ ok: true, engine: 'transcription-queue', queue_stats: c });
    }
    if (action === 'drain' || !action) { const out = await drainBatch(db, parseInt(req.query.limit) || 10); return res.json({ success: true, ...out }); }
    if (action === 'enqueue' && req.method === 'POST') {
      const body = await new Promise(r => { let d = ''; req.on('data', c => d += c); req.on('end', () => { try { r(JSON.parse(d)); } catch (_) { r({}); } }); });
      await enqueue(db, body.audio_url, body.metro, body.state);
      return res.json({ success: true });
    }
    return res.status(400).json({ error: 'unknown action' });
  } catch (err) { await reportError(db, 'transcription-queue', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.enqueue = enqueue;
module.exports.drainBatch = drainBatch;
