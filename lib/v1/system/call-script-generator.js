/**
 * Phase 86: Rep Call Script Generator.
 * 
 * Takes a victim's lead context (name, severity, discrepancy_note, location)
 * and uses Claude Sonnet to draft a personalized opening line that handles
 * the "phone belongs to spouse/family" case gracefully.
 * 
 * Mason directive: when phone resolves to a household member, the rep should
 * have a tailored opening that acknowledges this without sounding scripted.
 */
const { getDb } = require('../../_db');
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function getAnthropicKey(db) {
  if (process.env.ANTHROPIC_API_KEY) return process.env.ANTHROPIC_API_KEY;
  try {
    const row = await db('system_config').where('key', 'anthropic_api_key').first();
    if (row?.value) return typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
  } catch (_) {}
  return null;
}

async function generateScript(db, personId) {
  const person = await db('persons').where('id', personId).first();
  if (!person) return { ok: false, error: 'not_found' };
  const inc = person.incident_id ? await db('incidents').where('id', person.incident_id).first() : null;

  // Fetch the latest discrepancy note
  let discrepancyNote = null;
  try {
    const dn = await db('enrichment_logs')
      .where('person_id', personId)
      .where('field_name', 'discrepancy_note')
      .orderBy('created_at', 'desc').first();
    if (dn) {
      const parsed = typeof dn.new_value === 'string' ? JSON.parse(dn.new_value) : dn.new_value;
      discrepancyNote = parsed?.note || null;
    }
  } catch (_) {}

  const ctx = {
    victim_name: person.full_name || 'the person involved',
    severity: inc?.severity || 'unknown',
    city_state: `${person.city || inc?.city || '?'}, ${person.state || inc?.state || '?'}`,
    has_discrepancy: !!discrepancyNote,
    discrepancy_note: discrepancyNote,
    is_fatal: (inc?.severity === 'fatal' || inc?.fatalities_count > 0)
  };

  const key = await getAnthropicKey(db);
  if (!key) return { ok: false, error: 'no_anthropic_key' };

  const prompt = `You are coaching a personal-injury intake rep on the phone. Generate a SHORT (2-3 sentence) opening line for the call.

Context:
- Victim: ${ctx.victim_name}
- Accident: ${ctx.severity} severity in ${ctx.city_state}${ctx.is_fatal ? ' (fatal)' : ''}
${ctx.has_discrepancy ? `- IMPORTANT: ${ctx.discrepancy_note}` : '- Phone is registered to the victim directly.'}

Rules for the opening line:
- Warm, professional, not pushy
- If the phone is registered to someone else (spouse/family), acknowledge that gracefully without being awkward
- If the case is fatal, reference "the accident" not "your injury" and offer condolences
- Never start with "Hi, this is..." — too cold. Start with something more conversational.
- Mention the rep's firm only as "[firm]" placeholder
- Output JSON: {"primary_line":"...","fallback_line":"...","approach_notes":"one sentence on tone/timing"}`;

  try {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': key, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
      body: JSON.stringify({
        model: 'claude-sonnet-4-6',
        max_tokens: 400,
        messages: [{ role: 'user', content: prompt }]
      }),
      signal: AbortSignal.timeout(15000)
    });
    if (!r.ok) return { ok: false, status: r.status };
    const d = await r.json();
    const out = d.content?.[0]?.text || '';
    const m = out.match(/\{[\s\S]*\}/);
    if (!m) return { ok: false, error: 'no_json', raw: out.slice(0, 300) };
    const parsed = JSON.parse(m[0]);

    try {
      await db('enrichment_logs').insert({
        person_id: personId,
        field_name: 'rep_call_script',
        old_value: null,
        new_value: JSON.stringify({ ...parsed, generated_at: new Date().toISOString(), source: 'call-script-generator' }).slice(0, 4000),
        created_at: new Date()
      });
    } catch (_) {}

    return { ok: true, person_id: personId, victim: ctx.victim_name, ...parsed, context: ctx };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

async function batch(db, limit = 10) {
  const persons = await db('persons')
    .whereNotNull('phone')
    .where('victim_verified', true)
    .limit(limit)
    .select('id');
  const results = [];
  for (const p of persons) {
    try { results.push(await generateScript(db, p.id)); }
    catch (e) { results.push({ id: p.id, error: e.message }); }
  }
  return { ok: true, scanned: persons.length, generated: results.filter(r => r.ok).length, results };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();
  if (action === 'health') return res.json({ success: true, service: 'call-script-generator' });
  if (action === 'generate') {
    const pid = req.query?.person_id;
    if (!pid) return res.status(400).json({ error: 'person_id required' });
    return res.json(await generateScript(db, pid));
  }
  if (action === 'batch') {
    const limit = Math.min(20, parseInt(req.query?.limit) || 5);
    return res.json(await batch(db, limit));
  }
  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.generateScript = generateScript;
module.exports.batch = batch;
