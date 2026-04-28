/**
 * Active-learning queue. Confidence band 50–70 surfaced to dashboard for one-click human label.
 * Each label trains the threshold tuner.
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');

async function ensureTable(db) {
  await db.raw(`CREATE TABLE IF NOT EXISTS active_learning_queue (
    id SERIAL PRIMARY KEY,
    person_id BIGINT,
    confidence INT,
    reason TEXT,
    user_label TEXT,
    labeled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
  )`).catch(() => {});
}

async function enqueue(db, limit = 30) {
  await ensureTable(db);
  let rows = []; try {
    rows = await db('persons').whereBetween('identity_confidence', [50, 70]).whereNotNull('full_name')
      .where('full_name', '!=', '').limit(limit);
  } catch (_) {}
  let queued = 0;
  for (const p of rows) {
    try {
      await db('active_learning_queue').insert({ person_id: p.id, confidence: p.identity_confidence, reason: 'mid_band' }).onConflict('id').ignore();
      queued++;
    } catch (_) {}
  }
  return { rows: rows.length, queued };
}

async function label(db, id, userLabel) {
  await db('active_learning_queue').where({ id }).update({ user_label: userLabel, labeled_at: new Date() });
  const r = await db('active_learning_queue').where({ id }).first();
  if (r?.person_id && userLabel === 'correct') {
    await db('persons').where({ id: r.person_id }).update({ identity_confidence: db.raw('GREATEST(identity_confidence, 90)') });
  } else if (r?.person_id && userLabel === 'wrong') {
    await db('persons').where({ id: r.person_id }).update({ identity_confidence: db.raw('LEAST(identity_confidence, 30)') });
  }
  return r;
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { action, id, label_value } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'active-learning', band: '50-70' });
    if (action === 'list') { const rows = await db('active_learning_queue').whereNull('labeled_at').orderBy('created_at', 'desc').limit(25); return res.json({ rows }); }
    if (action === 'label' && id && label_value) { const r = await label(db, parseInt(id), label_value); return res.json({ success: true, row: r }); }
    if (action === 'enqueue' || !action) { const out = await enqueue(db, parseInt(req.query.limit) || 30); return res.json({ success: true, ...out }); }
    return res.status(400).json({ error: 'unknown action' });
  } catch (err) { await reportError(db, 'active-learning', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.enqueue = enqueue;
module.exports.label = label;
