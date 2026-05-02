/**
 * Phase 77 #4: Address Sonnet Extractor.
 * Claude Sonnet 4.6 reads raw_description + description and extracts any
 * concrete street address mentioned. Unlocks geocoder + USPS for ~50% of
 * NYC OpenData rows that have street info but no full address column.
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
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

async function extractAddress(db, personId) {
  const person = await db('persons').where('id', personId).first();
  if (!person) return { ok: false, error: 'not_found' };
  if (person.address) return { ok: true, skipped: 'address_already_set' };
  const inc = person.incident_id ? await db('incidents').where('id', person.incident_id).first() : null;
  const text = ((inc?.raw_description || '') + ' ' + (inc?.description || '')).slice(0, 3000);
  if (!text.trim()) return { ok: true, skipped: 'no_text' };

  const key = await getAnthropicKey(db);
  if (!key) return { ok: false, error: 'no_anthropic_key' };

  const prompt = `Extract any concrete street address mentioned in this accident description. Return JSON only:
{"street": "1234 Maple St", "city": "Brooklyn", "state": "NY", "zip": "11201", "confidence": 0.85}

If only a partial address is mentioned (intersection, neighborhood), return what's available. If nothing concrete, return {"street": null, "confidence": 0}.

Description:
${text}`;

  try {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': key,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json'
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-6',
        max_tokens: 200,
        messages: [{ role: 'user', content: prompt }]
      }),
      signal: AbortSignal.timeout(15000)
    });
    if (!r.ok) return { ok: false, status: r.status };
    const d = await r.json();
    const out = d.content?.[0]?.text || '';
    const m = out.match(/\{[\s\S]*\}/);
    if (!m) return { ok: false, error: 'no_json_in_response' };
    const parsed = JSON.parse(m[0]);
    if (!parsed.street || (parsed.confidence || 0) < 0.5) return { ok: true, skipped: 'low_confidence', parsed };

    // Persist
    const updates = { updated_at: new Date() };
    if (parsed.street && !person.address) updates.address = parsed.street;
    if (parsed.city && !person.city) updates.city = parsed.city;
    if (parsed.state && !person.state) updates.state = parsed.state;
    if (parsed.zip && !person.zip) updates.zip = parsed.zip;
    await db('persons').where('id', personId).update(updates);

    try {
      await db('enrichment_logs').insert({
        person_id: personId,
        field_name: 'address',
        old_value: null,
        new_value: JSON.stringify({ ...parsed, source: 'address-sonnet-extractor' }).slice(0, 4000),
        created_at: new Date()
      });
    } catch (_) {}

    return { ok: true, person_id: personId, extracted: parsed, fields_filled: Object.keys(updates).filter(k => k !== 'updated_at') };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

async function batch(db, limit = 20) {
  const persons = await db.raw(`
    SELECT p.id FROM persons p JOIN incidents i ON i.id = p.incident_id
    WHERE p.address IS NULL AND (i.raw_description IS NOT NULL OR i.description IS NOT NULL)
    ORDER BY p.created_at DESC LIMIT ${parseInt(limit) || 20}
  `).then(r => r.rows || []);
  const results = [];
  for (const p of persons) {
    try { results.push(await extractAddress(db, p.id)); }
    catch (e) { results.push({ id: p.id, error: e.message }); }
  }
  const filled = results.filter(r => r.fields_filled?.length).length;
  return { ok: true, scanned: persons.length, filled };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();
  if (action === 'health') return res.json({ success: true, service: 'address-sonnet-extractor' });
  if (action === 'extract') {
    const pid = req.query?.person_id;
    if (!pid) return res.status(400).json({ error: 'person_id required' });
    return res.json(await extractAddress(db, pid));
  }
  if (action === 'batch') {
    const limit = Math.min(50, parseInt(req.query?.limit) || 10);
    return res.json(await batch(db, limit));
  }
  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.extractAddress = extractAddress;
module.exports.batch = batch;
