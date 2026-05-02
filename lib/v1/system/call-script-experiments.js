/**
 * Phase 87: Call Script A/B Experiments.
 *
 * Tracks which call-script variant (primary vs fallback vs custom) converts
 * better. Reps log call outcomes; we aggregate winner stats and feed signal
 * back into the strategist's `engine_performance` table so the self-learning
 * loop knows which script direction works.
 *
 * Outcome buckets:
 *   conversion: converted | callback_scheduled | reached_victim
 *   failure:    wrong_number | rejected | no_answer | voicemail (treated as
 *               inconclusive — they don't count against, but they don't count
 *               toward conversion either)
 *
 * Winner detection:
 *   primary wins iff conversion_rate(primary) >= 1.2 * conversion_rate(other)
 *   AND samples(primary) >= 10 AND samples(other) >= 10.
 *   confidence: high if 30+ samples per variant, medium 10-29, low <10.
 */
const { getDb } = require('../../_db');

const SECRET = 'ingest-now';
function authed(req) {
  const s = (req.query && req.query.secret) || (req.headers && req.headers['x-cron-secret']);
  return s === SECRET || s === process.env.CRON_SECRET;
}

const CONVERSION_OUTCOMES = ['converted', 'callback_scheduled', 'reached_victim'];
const FAILURE_OUTCOMES = ['wrong_number', 'rejected', 'no_answer', 'voicemail'];
const VALID_VARIANTS = ['primary', 'fallback', 'custom'];

// ─────────────────────────────────────────────────────────────────────────
// Self-applying migration
// ─────────────────────────────────────────────────────────────────────────
let _migrated = false;
async function ensureSchema(db) {
  if (_migrated) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS call_script_attempts (
        id BIGSERIAL PRIMARY KEY,
        person_id UUID NOT NULL,
        rep_id UUID,
        variant_used VARCHAR(20) NOT NULL CHECK (variant_used IN ('primary','fallback','custom')),
        script_text TEXT,
        outcome VARCHAR(40),
        call_duration_seconds INT,
        notes TEXT,
        created_at TIMESTAMP DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_csa_person ON call_script_attempts(person_id);
      CREATE INDEX IF NOT EXISTS idx_csa_outcome ON call_script_attempts(outcome);
    `);
    _migrated = true;
  } catch (e) {
    _migrated = true;
  }
}

// ─────────────────────────────────────────────────────────────────────────
// Logging
// ─────────────────────────────────────────────────────────────────────────
async function logAttempt(db, payload) {
  await ensureSchema(db);
  const {
    person_id,
    rep_id = null,
    variant_used,
    script_text = null,
    outcome = null,
    call_duration_seconds = null,
    notes = null
  } = payload || {};

  if (!person_id) return { ok: false, error: 'person_id required' };
  if (!variant_used || !VALID_VARIANTS.includes(variant_used)) {
    return { ok: false, error: `variant_used must be one of ${VALID_VARIANTS.join('|')}` };
  }
  try {
    const [row] = await db('call_script_attempts')
      .insert({
        person_id,
        rep_id,
        variant_used,
        script_text: script_text ? String(script_text).slice(0, 8000) : null,
        outcome: outcome ? String(outcome).slice(0, 40) : null,
        call_duration_seconds: call_duration_seconds != null ? parseInt(call_duration_seconds) : null,
        notes: notes ? String(notes).slice(0, 4000) : null,
        created_at: new Date()
      })
      .returning(['id', 'created_at']);
    return { ok: true, id: row.id, created_at: row.created_at, variant_used };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ─────────────────────────────────────────────────────────────────────────
// Stats aggregation
// ─────────────────────────────────────────────────────────────────────────
function emptyVariant() {
  return { count: 0, conversions: 0, failures: 0, conversion_rate: 0, avg_duration: 0, total_duration: 0 };
}

async function computeStats(db) {
  await ensureSchema(db);
  const rows = await db('call_script_attempts').select('variant_used', 'outcome', 'call_duration_seconds');

  const by_variant = { primary: emptyVariant(), fallback: emptyVariant(), custom: emptyVariant() };
  const by_outcome = {};
  let total = 0;

  for (const r of rows) {
    total += 1;
    const v = by_variant[r.variant_used];
    if (!v) continue;
    v.count += 1;
    if (r.outcome) by_outcome[r.outcome] = (by_outcome[r.outcome] || 0) + 1;
    if (CONVERSION_OUTCOMES.includes(r.outcome)) v.conversions += 1;
    else if (FAILURE_OUTCOMES.includes(r.outcome)) v.failures += 1;
    if (r.call_duration_seconds != null) v.total_duration += r.call_duration_seconds;
  }
  for (const k of Object.keys(by_variant)) {
    const v = by_variant[k];
    v.conversion_rate = v.count > 0 ? +(v.conversions / v.count).toFixed(4) : 0;
    v.avg_duration = v.count > 0 ? Math.round(v.total_duration / v.count) : 0;
    delete v.total_duration;
  }

  // Winner detection — only variants with >=10 samples are eligible
  const variants = ['primary', 'fallback', 'custom'];
  const eligible = variants
    .map(k => ({ k, ...by_variant[k] }))
    .filter(v => v.count >= 10)
    .sort((a, b) => b.conversion_rate - a.conversion_rate);

  let winner = 'inconclusive';
  if (eligible.length === 1 && eligible[0].conversion_rate > 0) {
    winner = eligible[0].k;
  } else if (eligible.length >= 2) {
    const top = eligible[0], second = eligible[1];
    // top wins if its rate is at least 1.2x second's, OR if second has 0 conversion
    if (
      (second.conversion_rate > 0 && top.conversion_rate >= 1.2 * second.conversion_rate) ||
      (second.conversion_rate === 0 && top.conversion_rate > 0)
    ) {
      winner = top.k;
    }
  }

  // Confidence — based on minimum sample count among variants with any data
  const sampled = sorted.filter(v => v.count > 0);
  const minSamples = sampled.length ? Math.min(...sampled.map(v => v.count)) : 0;
  let confidence = 'low';
  if (minSamples >= 30) confidence = 'high';
  else if (minSamples >= 10) confidence = 'medium';

  return {
    total_attempts: total,
    by_variant,
    winner,
    confidence,
    by_outcome
  };
}

// ─────────────────────────────────────────────────────────────────────────
// Feedback loop into engine_performance
// ─────────────────────────────────────────────────────────────────────────
async function pushPerformanceToStrategist(db) {
  const stats = await computeStats(db);
  const engines = [
    { engine_id: 'call-script-primary', v: stats.by_variant.primary },
    { engine_id: 'call-script-fallback', v: stats.by_variant.fallback },
    { engine_id: 'call-script-custom', v: stats.by_variant.custom }
  ];
  const updated = [];
  for (const { engine_id, v } of engines) {
    if (!v || v.count === 0) continue;
    try {
      await db.raw(`
        CREATE TABLE IF NOT EXISTS engine_performance (
          engine_id VARCHAR(64) NOT NULL,
          input_shape VARCHAR(64) NOT NULL,
          attempts BIGINT DEFAULT 0,
          successes BIGINT DEFAULT 0,
          last_success_at TIMESTAMP,
          last_attempt_at TIMESTAMP,
          avg_duration_ms INT DEFAULT 0,
          PRIMARY KEY (engine_id, input_shape)
        );
      `);
      await db.raw(`
        INSERT INTO engine_performance (engine_id, input_shape, attempts, successes, last_attempt_at, last_success_at, avg_duration_ms)
        VALUES (?, 'ab_test', ?, ?, NOW(), NOW(), ?)
        ON CONFLICT (engine_id, input_shape) DO UPDATE SET
          attempts = EXCLUDED.attempts,
          successes = EXCLUDED.successes,
          last_attempt_at = NOW(),
          last_success_at = CASE WHEN EXCLUDED.successes > 0 THEN NOW() ELSE engine_performance.last_success_at END,
          avg_duration_ms = EXCLUDED.avg_duration_ms
      `, [engine_id, v.count, v.conversions, (v.avg_duration || 0) * 1000]);
      updated.push({ engine_id, attempts: v.count, successes: v.conversions, conversion_rate: v.conversion_rate });
    } catch (e) {
      updated.push({ engine_id, error: e.message });
    }
  }
  return { ok: true, winner: stats.winner, confidence: stats.confidence, updated };
}

// ─────────────────────────────────────────────────────────────────────────
// HTTP handler
// ─────────────────────────────────────────────────────────────────────────
async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'content-type, x-cron-secret');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }

  const action = String((req.query && req.query.action) || 'health').toLowerCase();

  try {
    if (action === 'health') {
      return res.json({
        success: true,
        service: 'call-script-experiments',
        actions: ['health', 'log', 'stats', 'performance_per_engine'],
        valid_variants: VALID_VARIANTS,
        conversion_outcomes: CONVERSION_OUTCOMES,
        failure_outcomes: FAILURE_OUTCOMES
      });
    }

    if (action === 'log') {
      const body = (req.body && typeof req.body === 'object') ? req.body : {};
      const payload = {
        person_id: body.person_id || (req.query && req.query.person_id),
        rep_id: body.rep_id || (req.query && req.query.rep_id) || null,
        variant_used: body.variant_used || (req.query && req.query.variant_used),
        script_text: body.script_text || (req.query && req.query.script_text) || null,
        outcome: body.outcome || (req.query && req.query.outcome) || null,
        call_duration_seconds: body.call_duration_seconds != null ? body.call_duration_seconds : (req.query && req.query.call_duration_seconds),
        notes: body.notes || (req.query && req.query.notes) || null
      };
      const result = await logAttempt(db, payload);
      return res.status(result.ok ? 200 : 400).json(result);
    }

    if (action === 'stats') {
      const stats = await computeStats(db);
      return res.json({ ok: true, ...stats });
    }

    if (action === 'performance_per_engine') {
      const out = await pushPerformanceToStrategist(db);
      return res.json(out);
    }

    return res.status(400).json({ error: 'unknown action', valid: ['health', 'log', 'stats', 'performance_per_engine'] });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.logAttempt = logAttempt;
module.exports.computeStats = computeStats;
module.exports.pushPerformanceToStrategist = pushPerformanceToStrategist;
module.exports.ensureSchema = ensureSchema;
module.exports.CONVERSION_OUTCOMES = CONVERSION_OUTCOMES;
module.exports.FAILURE_OUTCOMES = FAILURE_OUTCOMES;
module.exports.VALID_VARIANTS = VALID_VARIANTS;
