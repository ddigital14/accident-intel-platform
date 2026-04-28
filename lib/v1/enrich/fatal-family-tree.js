/**
 * Family-tree expansion when victim is fatal.
 * Reads obituaries → Claude NER extracts surviving family (spouse, parents, adult children) →
 * inserts each as a separate person record linked to same incident with relationship code.
 * In a fatal case, the family is the actual lead (deceased can't sign retainer).
 */
const fetch = require('node-fetch');
const { getModelForTask } = require('../system/model-registry');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

async function extractFamily(obit, db) {
  const model = await getModelForTask('premium_reasoning', 'claude-opus-4-6');
  const url = 'https://api.anthropic.com/v1/messages';
  const prompt = `Extract surviving family members from this obituary. Return strict JSON:
{
  "deceased_full_name": "string",
  "survivors": [
    {"full_name": "string", "relationship": "spouse|parent|child|sibling|grandchild|other", "city": "string|null", "state": "string|null", "is_minor": false}
  ],
  "preceded_in_death_by": ["names"],
  "service_location": "string|null",
  "service_date": "string|null"
}
Return only adult survivors (age 18+). Skip if uncertain.

OBITUARY:
${obit}`;
  let body = null, ok = false;
  try {
    const r = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model, max_tokens: 1500, messages: [{ role: 'user', content: prompt }] }),
      timeout: 25000
    });
    if (r.ok) { body = await r.json(); ok = true; }
  } catch (_) {}
  await trackApiCall(db, 'enrich-fatal-family-tree', model, 0, 0, ok).catch(() => {});
  if (!body?.content?.[0]?.text) return null;
  try { return JSON.parse(body.content[0].text.match(/\{[\s\S]*\}/)?.[0] || '{}'); } catch (_) { return null; }
}

async function run(db, limit = 5) {
  let rows = []; try {
    rows = await db('source_reports')
      .where('source_type', 'in', ['obituary', 'tributes', 'funeral_home'])
      .where(function () { this.whereNull('family_tree_expanded').orWhere('family_tree_expanded', false); })
      .orderBy('created_at', 'desc').limit(limit);
  } catch (_) {}
  let expanded = 0, addedSurvivors = 0;
  for (const r of rows) {
    const data = typeof r.parsed_data === 'string' ? JSON.parse(r.parsed_data || '{}') : r.parsed_data;
    const obit = data?.full_text || data?.text || data?.summary || data?.title || '';
    if (!obit || obit.length < 100) continue;
    const fam = await extractFamily(obit, db);
    if (!fam || !Array.isArray(fam.survivors)) continue;
    // Find linked incident by deceased name
    let incidentId = null;
    try {
      const m = await db('persons').where({ full_name: fam.deceased_full_name }).whereNotNull('incident_id').first();
      incidentId = m?.incident_id;
    } catch (_) {}
    for (const s of fam.survivors) {
      if (!s.full_name || s.is_minor) continue;
      try {
        await db('persons').insert({
          full_name: s.full_name,
          relationship_to_victim: s.relationship,
          incident_id: incidentId,
          location_locality: s.city,
          location_region: s.state,
          source: 'fatal-family-tree',
          created_at: new Date()
        }).onConflict(['full_name', 'incident_id']).ignore().catch(() => {});
        addedSurvivors++;
        if (incidentId) await enqueueCascade(db, 'person', null, 'fatal-family-tree', { weight: 75, related_incident: incidentId, rel: s.relationship });
      } catch (_) {}
    }
    try { await db('source_reports').where({ id: r.id }).update({ family_tree_expanded: true }); } catch (_) {}
    expanded++;
  }
  return { processed: rows.length, expanded, addedSurvivors };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'fatal-family-tree', model: await getModelForTask('premium_reasoning') });
    const out = await run(db, parseInt(req.query.limit) || 5);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'fatal-family-tree', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
