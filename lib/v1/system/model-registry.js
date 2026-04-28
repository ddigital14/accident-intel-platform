/**
 * Runtime AI model registry. getModelForTask(taskName) returns the model
 * string, reading from `model_registry` table with a 60-second in-process
 * cache. Flipping current_model in the dashboard propagates everywhere within
 * 60s without code changes or redeploys.
 *
 * Tasks we know about (used as defaults if registry row missing):
 *   vision_extraction      → gpt-4o
 *   transcription          → whisper-1
 *   light_extraction       → gpt-4o-mini
 *   heavy_extraction       → gpt-4o
 *   cross_reasoning        → claude-sonnet-4-6
 *   premium_reasoning      → claude-opus-4-6
 *   embedding              → text-embedding-3-small
 *   classification         → gpt-4o-mini
 */
const { getDb } = require('../../_db');

const DEFAULTS = {
  // Phase 31: model upgrades — push fatal/cross-reasoning to opus-4-6 for highest fidelity.
  vision_extraction: 'gpt-4o',
  transcription: 'whisper-1',
  light_extraction: 'gpt-4o-mini',
  heavy_extraction: 'gpt-4o',
  cross_reasoning: 'claude-opus-4-6',           // upgraded from sonnet-4-6
  premium_reasoning: 'claude-opus-4-6',
  embedding: 'text-embedding-3-small',
  classification: 'gpt-4o-mini',
  scanner_extraction: 'gpt-4o-mini',
  obit_ner: 'claude-sonnet-4-6',                // upgraded from gpt-4o-mini for richer NER
  news_extraction: 'gpt-4o-mini',
  fatal_extraction: 'claude-opus-4-6',          // upgraded from gpt-4o for fatal reads
  identity_validation: 'claude-opus-4-6',       // upgraded from sonnet-4-6
  spanish_extraction: 'gpt-4o',                 // new task for Spanish NER
  fraud_detection: 'claude-opus-4-6',           // new task for fraud filter context analysis
  family_tree_ner: 'claude-opus-4-6'            // new task for surviving-family extraction
};

const _cache = new Map(); // taskName -> { model, fetched_at }
const TTL = 60_000;

async function ensureTable(db) {
  await db.raw(`CREATE TABLE IF NOT EXISTS model_registry (
    task_name TEXT PRIMARY KEY,
    current_model TEXT NOT NULL,
    fallback_model TEXT,
    notes TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
  )`).catch(() => {});
}

async function seed(db) {
  await ensureTable(db);
  for (const [task, model] of Object.entries(DEFAULTS)) {
    try {
      await db.raw(
        `INSERT INTO model_registry (task_name, current_model, fallback_model)
         VALUES (?, ?, ?) ON CONFLICT (task_name) DO NOTHING`,
        [task, model, model]
      );
    } catch (_) {}
  }
}

async function getModelForTask(taskName, fallback) {
  const now = Date.now();
  const cached = _cache.get(taskName);
  if (cached && now - cached.fetched_at < TTL) return cached.model;
  let model = fallback || DEFAULTS[taskName] || 'gpt-4o-mini';
  try {
    const db = getDb();
    const row = await db('model_registry').where({ task_name: taskName }).first();
    if (row?.current_model) model = row.current_model;
  } catch (_) {}
  _cache.set(taskName, { model, fetched_at: now });
  return model;
}

async function setModelForTask(db, taskName, model, notes) {
  await ensureTable(db);
  await db.raw(
    `INSERT INTO model_registry (task_name, current_model, notes, updated_at)
     VALUES (?, ?, ?, NOW())
     ON CONFLICT (task_name) DO UPDATE SET current_model = EXCLUDED.current_model, notes = COALESCE(EXCLUDED.notes, model_registry.notes), updated_at = NOW()`,
    [taskName, model, notes || null]
  );
  _cache.delete(taskName);
}

async function listAll(db) {
  await ensureTable(db);
  await seed(db);
  return await db('model_registry').orderBy('task_name');
}

function clearCache() { _cache.clear(); }

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { action, task, model, notes } = req.query || {};
    if (action === 'health' || action === 'list') {
      const rows = await listAll(db);
      return res.json({ ok: true, engine: 'model-registry', tasks: rows, defaults: DEFAULTS });
    }
    if (action === 'seed') { await seed(db); return res.json({ success: true }); }
    if (action === 'set' && task && model) {
      await setModelForTask(db, task, model, notes);
      return res.json({ success: true, task, model });
    }
    if (action === 'get' && task) {
      const m = await getModelForTask(task);
      return res.json({ task, model: m });
    }
    if (action === 'clear-cache') { clearCache(); return res.json({ success: true, cleared: true }); }
    return res.status(400).json({ error: 'unknown action', valid: ['list', 'seed', 'set', 'get', 'clear-cache'] });
  } catch (err) { res.status(500).json({ error: err.message }); }
};
module.exports.getModelForTask = getModelForTask;
module.exports.setModelForTask = setModelForTask;
module.exports.seed = seed;
module.exports.DEFAULTS = DEFAULTS;
