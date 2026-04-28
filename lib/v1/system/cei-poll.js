/**
 * Phase 35: Cross-Engine Intelligence (CEI) poll orchestrator.
 *
 * Single unified loop that runs every 1 min on Pro:
 *   1. Inventory recent engine activity (what fired in last N min)
 *   2. Detect cross-engine opportunities (engine A found X, engine B needing X never ran)
 *   3. Score engine effectiveness (success rate per engine per task type)
 *   4. Optimize model selection (suggest model_registry bumps based on outcomes)
 *   5. Learn patterns (cei_patterns table — winning enrichment chains)
 *   6. Auto-trigger under-utilized engines (queue missing-engine fires)
 *   7. Surface anomalies (engines with sudden failure-rate spikes)
 *
 * Pattern discovered in CaseFlow project. Adapted for AIP's 60+ engines.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('./cost');
const { reportError } = require('./_errors');

async function ensureTables(db) {
  await db.raw(`CREATE TABLE IF NOT EXISTS cei_patterns (
    id SERIAL PRIMARY KEY,
    pattern_key TEXT UNIQUE,
    chain JSONB,
    state TEXT,
    win_count INT DEFAULT 0,
    total_count INT DEFAULT 0,
    last_seen TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
  )`).catch(() => {});
  await db.raw(`CREATE TABLE IF NOT EXISTS cei_anomalies (
    id SERIAL PRIMARY KEY,
    engine TEXT,
    kind TEXT,
    severity TEXT,
    detail JSONB,
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
  )`).catch(() => {});
  await db.raw(`CREATE TABLE IF NOT EXISTS cei_engine_scores (
    engine TEXT PRIMARY KEY,
    success_count INT DEFAULT 0,
    failure_count INT DEFAULT 0,
    avg_latency_ms INT DEFAULT 0,
    last_success_at TIMESTAMPTZ,
    last_failure_at TIMESTAMPTZ,
    effectiveness_score NUMERIC DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT NOW()
  )`).catch(() => {});
}

// 1. Inventory: which engines fired in the last 5 min
async function inventoryActivity(db, minutes = 5) {
  const rows = await db.raw(`
    SELECT service, COUNT(*) AS calls,
      SUM(CASE WHEN success THEN 1 ELSE 0 END) AS ok_calls,
      SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS bad_calls
    FROM system_api_calls
    WHERE created_at > NOW() - INTERVAL '${parseInt(minutes)} minutes'
    GROUP BY service
    ORDER BY calls DESC
  `).then(r => r.rows || r).catch(() => []);
  return rows;
}

// 2. Detect cross-engine opportunities
async function detectOpportunities(db) {
  // Pattern: persons with full_name + phone but no Twilio Lookup ever
  let opportunities = [];
  try {
    const noLookup = await db.raw(`
      SELECT id FROM persons
      WHERE full_name IS NOT NULL AND full_name <> ''
        AND phone IS NOT NULL AND phone <> ''
        AND (carrier IS NULL OR carrier = '')
        AND (line_type IS NULL OR line_type = '')
      LIMIT 25
    `).then(r => r.rows || r).catch(() => []);
    if (noLookup.length > 0) opportunities.push({ engine: 'twilio-lookup', kind: 'missing_run', count: noLookup.length, ids: noLookup.map(r => r.id) });

    // Pattern: persons with full_name but no name-rarity score
    const noRarity = await db.raw(`
      SELECT id FROM persons
      WHERE full_name IS NOT NULL AND full_name <> ''
        AND (name_rarity IS NULL OR name_rarity = 0)
      LIMIT 25
    `).then(r => r.rows || r).catch(() => []);
    if (noRarity.length > 0) opportunities.push({ engine: 'name-rarity', kind: 'missing_run', count: noRarity.length });

    // Pattern: fatal incidents without family-tree expansion
    const noFamily = await db.raw(`
      SELECT i.id FROM incidents i
      LEFT JOIN persons p ON p.incident_id = i.id
      WHERE i.severity = 'fatal'
        AND p.id IS NOT NULL
        AND (p.family_tree_expanded IS NULL OR p.family_tree_expanded = false)
      GROUP BY i.id LIMIT 15
    `).then(r => r.rows || r).catch(() => []);
    if (noFamily.length > 0) opportunities.push({ engine: 'fatal-family-tree', kind: 'missing_run', count: noFamily.length });

    // Pattern: incidents with no predicted value but enough context
    const noPrediction = await db.raw(`
      SELECT id FROM incidents
      WHERE description IS NOT NULL
        AND severity IS NOT NULL
        AND (predicted_value_likely IS NULL OR predicted_value_likely = 0)
      LIMIT 20
    `).then(r => r.rows || r).catch(() => []);
    if (noPrediction.length > 0) opportunities.push({ engine: 'predictive-at-source', kind: 'missing_run', count: noPrediction.length });
  } catch (_) {}
  return opportunities;
}

// 3. Score engines based on system_api_calls
async function scoreEngines(db, hours = 24) {
  const rows = await db.raw(`
    SELECT service AS engine,
      SUM(CASE WHEN success THEN 1 ELSE 0 END) AS s,
      SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS f,
      COUNT(*) AS total
    FROM system_api_calls
    WHERE created_at > NOW() - INTERVAL '${parseInt(hours)} hours'
      AND service IS NOT NULL
    GROUP BY service
    HAVING COUNT(*) >= 5
  `).then(r => r.rows || r).catch(() => []);
  let updated = 0;
  for (const r of rows) {
    const total = parseInt(r.total);
    const s = parseInt(r.s);
    const eff = total > 0 ? s / total : 0;
    try {
      await db.raw(`
        INSERT INTO cei_engine_scores (engine, success_count, failure_count, effectiveness_score, last_success_at, updated_at)
        VALUES (?, ?, ?, ?, NOW(), NOW())
        ON CONFLICT (engine) DO UPDATE SET
          success_count = EXCLUDED.success_count,
          failure_count = EXCLUDED.failure_count,
          effectiveness_score = EXCLUDED.effectiveness_score,
          updated_at = NOW()
      `, [r.engine, s, parseInt(r.f), eff]);
      updated++;
    } catch (_) {}
  }
  return updated;
}

// 4. Detect anomalies: engines with sudden failure spikes
async function detectAnomalies(db) {
  const recent = await db.raw(`
    SELECT service AS engine,
      SUM(CASE WHEN success THEN 1 ELSE 0 END) AS s_recent,
      SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS f_recent
    FROM system_api_calls
    WHERE created_at > NOW() - INTERVAL '15 minutes'
    GROUP BY service
    HAVING SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) >= 5
  `).then(r => r.rows || r).catch(() => []);
  let flagged = 0;
  for (const r of recent) {
    const f = parseInt(r.f_recent);
    const s = parseInt(r.s_recent);
    if (f > s) {
      try {
        await db('cei_anomalies').insert({
          engine: r.engine,
          kind: 'failure_spike',
          severity: f > 20 ? 'critical' : 'warning',
          detail: JSON.stringify({ recent_failures: f, recent_successes: s, window: '15min' })
        });
        flagged++;
      } catch (_) {}
    }
  }
  return flagged;
}

// 5. Learn patterns from successful enrichment chains
async function learnPatterns(db) {
  // Group recent successful identifications by person_id, see which engine sequences led to qualified status
  const winners = await db.raw(`
    SELECT p.id AS person_id, p.location_region AS state,
      ARRAY_AGG(DISTINCT el.source ORDER BY el.source) AS chain
    FROM persons p
    JOIN enrichment_logs el ON el.person_id = p.id
    WHERE p.qualification_state = 'qualified'
      AND p.identity_confidence >= 80
      AND p.created_at > NOW() - INTERVAL '7 days'
    GROUP BY p.id, p.location_region
    HAVING COUNT(DISTINCT el.source) >= 2
    LIMIT 100
  `).then(r => r.rows || r).catch(() => []);
  let learned = 0;
  for (const w of winners) {
    const chain = w.chain || [];
    const key = `${w.state || 'XX'}::${chain.slice(0, 5).join('->')}`;
    try {
      await db.raw(`
        INSERT INTO cei_patterns (pattern_key, chain, state, win_count, total_count, last_seen)
        VALUES (?, ?, ?, 1, 1, NOW())
        ON CONFLICT (pattern_key) DO UPDATE SET
          win_count = cei_patterns.win_count + 1,
          total_count = cei_patterns.total_count + 1,
          last_seen = NOW()
      `, [key, JSON.stringify(chain), w.state]);
      learned++;
    } catch (_) {}
  }
  return learned;
}

// 6. Auto-trigger under-utilized engines via cascade enqueue
async function autoTrigger(db, opportunities) {
  let triggered = 0;
  try {
    const { enqueueCascade } = require('./_cascade');
    for (const opp of opportunities) {
      if (opp.kind !== 'missing_run' || !opp.ids) continue;
      for (const id of opp.ids.slice(0, 5)) {
        await enqueueCascade(db, 'person', id, `cei-auto-${opp.engine}`, { weight: 5, reason: 'cei_opportunity' }).catch(() => {});
        triggered++;
      }
    }
  } catch (_) {}
  return triggered;
}

async function run(db) {
  await ensureTables(db);
  const start = Date.now();
  const activity = await inventoryActivity(db, 5);
  const opportunities = await detectOpportunities(db);
  const scored = await scoreEngines(db, 24);
  const anomalies = await detectAnomalies(db);
  const patternsLearned = await learnPatterns(db);
  const triggered = await autoTrigger(db, opportunities);
  const took = Date.now() - start;
  await trackApiCall(db, 'system-cei-poll', 'cei', 0, 0, true).catch(() => {});
  return {
    duration_ms: took,
    active_engines: activity.length,
    opportunities: opportunities.length,
    scored_engines: scored,
    anomalies_flagged: anomalies,
    patterns_learned: patternsLearned,
    auto_triggered: triggered,
    activity_top: activity.slice(0, 5),
    opportunities_detail: opportunities
  };
}

async function recommendations(db) {
  await ensureTables(db);
  const top = await db('cei_patterns').orderBy('win_count', 'desc').limit(10);
  const failing = await db('cei_engine_scores').where('effectiveness_score', '<', 0.5).orderBy('failure_count', 'desc').limit(10);
  const open = await db('cei_anomalies').whereNull('resolved_at').orderBy('detected_at', 'desc').limit(20);
  return { top_patterns: top, failing_engines: failing, open_anomalies: open };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { action } = req.query || {};
    if (action === 'health') {
      await ensureTables(db);
      const cs = await db('cei_engine_scores').count('* as n').first();
      const cp = await db('cei_patterns').count('* as n').first();
      return res.json({ ok: true, engine: 'cei-poll', scored_engines: parseInt(cs?.n || 0), learned_patterns: parseInt(cp?.n || 0) });
    }
    if (action === 'recommendations') return res.json(await recommendations(db));
    const out = await run(db);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'cei-poll', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
module.exports.recommendations = recommendations;
