/**
 * Predictive case-value at-source. Estimates $ value the moment incident is created,
 * BEFORE downstream enrichment finishes. Enables "shoot first, ask questions later"
 * for high-value incidents that need rep attention immediately.
 *
 * Uses Claude (model_registry: cross_reasoning → opus-4-6) on description + context.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { getModelForTask } = require('../system/model-registry');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

async function predict(incident, db) {
  const model = await getModelForTask('cross_reasoning', 'claude-opus-4-6');
  const prompt = `Estimate the personal-injury case value range for this incident. Return strict JSON:
{
  "estimated_value_min": number (USD),
  "estimated_value_likely": number,
  "estimated_value_max": number,
  "case_strength_score": 0-100,
  "complexity": "simple|moderate|complex|catastrophic",
  "key_drivers": ["..."],
  "rec_action": "immediate_outreach|standard_outreach|low_priority|skip",
  "reasoning": "1-2 sentences"
}

Incident:
- Type: ${incident.accident_type || 'unknown'}
- Severity: ${incident.severity || 'unknown'}
- City/State: ${incident.city || ''}, ${incident.state || ''}
- Description: ${(incident.description || '').slice(0, 800)}
- Block-group income: ${incident.block_group_income || 'unknown'}
- Metro heat (competitor activity): ${incident.metro_heat_score || 0}`;

  let body = null, ok = false;
  try {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model, max_tokens: 600, messages: [{ role: 'user', content: prompt }] }),
      timeout: 55000
    });
    if (r.ok) { body = await r.json(); ok = true; }
  } catch (_) {}
  await trackApiCall(db, 'enrich-predictive-at-source', model, 0, 0, ok).catch(() => {});
  if (!body?.content?.[0]?.text) return null;
  try {
    const m = body.content[0].text.match(/\{[\s\S]*\}/);
    return m ? JSON.parse(m[0]) : null;
  } catch (_) { return null; }
}

async function batch(db, limit = 15) {
  let rows = []; try {
    rows = await db('incidents')
      .where(function () { this.whereNull('predicted_value_likely').orWhere('predicted_value_likely', 0); })
      .where('created_at', '>', db.raw("NOW() - INTERVAL '24 hours'"))
      .orderBy('created_at', 'desc').limit(limit);
  } catch (_) {}
  let scored = 0, immediate = 0;
  for (const inc of rows) {
    const r = await predict(inc, db);
    if (!r) continue;
    try {
      await db('incidents').where({ id: inc.id }).update({
        predicted_value_min: r.estimated_value_min || 0,
        predicted_value_likely: r.estimated_value_likely || 0,
        predicted_value_max: r.estimated_value_max || 0,
        case_strength_score: r.case_strength_score || 0,
        case_complexity: r.complexity,
        recommended_action: r.rec_action,
        prediction_reasoning: r.reasoning,
        updated_at: new Date()
      });
      if (r.rec_action === 'immediate_outreach') {
        immediate++;
        await enqueueCascade(db, 'incident', inc.id, 'predictive-at-source', { weight: 95, value: r.estimated_value_likely });
      }
      scored++;
    } catch (_) {}
  }
  return { rows: rows.length, scored, immediate };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'predictive-at-source', model: await getModelForTask('cross_reasoning') });
    if (action === 'batch') { const out = await batch(db, parseInt(req.query.limit) || 15); return res.json({ success: true, ...out }); }
    if (req.query?.incident_id) {
      const inc = await db('incidents').where({ id: req.query.incident_id }).first();
      if (!inc) return res.status(404).json({ error: 'not_found' });
      const r = await predict(inc, db);
      return res.json({ success: !!r, prediction: r });
    }
    return res.status(400).json({ error: 'need incident_id or action=batch|health' });
  } catch (err) { await reportError(db, 'predictive-at-source', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.predict = predict;
