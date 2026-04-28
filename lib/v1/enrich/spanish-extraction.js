/**
 * Spanish-language NER for victim names from Hispanic-market sources.
 * Uses gpt-4o (or whatever model_registry has for fatal_extraction) with bilingual prompt.
 * Routes Spanish-language news + GoFundMe campaigns through this instead of English-only extractors.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { getModelForTask } = require('../system/model-registry');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');

function isSpanish(text) {
  if (!text) return false;
  // Quick heuristic: count Spanish stopwords + diacritics
  const spanish_markers = /\b(el|la|los|las|de|que|en|un|una|por|para|con|sin|fue|fueron|murió|falleció|accidente|atropello|choque|herido|muerto|familia|hospital|policía|esposa|hijo|hija|madre|padre)\b/gi;
  const matches = (text.match(spanish_markers) || []).length;
  return matches >= 4;
}

async function extract(text, db) {
  if (!isSpanish(text)) return { skipped: 'not_spanish' };
  const model = await getModelForTask('fatal_extraction', 'gpt-4o');
  const url = 'https://api.openai.com/v1/chat/completions';
  const prompt = `Extract victim names from this Spanish-language accident report. Return strict JSON:
{
  "victims": [
    {"full_name": "string (preserve accents)", "age": number|null, "city": "string|null", "state": "string|null", "severity": "fatal|injured|unknown", "relation": "victim|family"}
  ],
  "incident_type": "string in English",
  "occurred_at_iso": "string|null"
}

REPORT (Spanish):
${text.slice(0, 4000)}`;
  let body = null, ok = false;
  try {
    const r = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${process.env.OPENAI_API_KEY}` },
      body: JSON.stringify({ model, response_format: { type: 'json_object' }, messages: [{ role: 'user', content: prompt }] }),
      timeout: 25000
    });
    if (r.ok) { body = await r.json(); ok = true; }
  } catch (_) {}
  await trackApiCall(db, 'enrich-spanish-extraction', model, 0, 0, ok).catch(() => {});
  if (!body?.choices?.[0]?.message?.content) return null;
  try { return JSON.parse(body.choices[0].message.content); } catch (_) { return null; }
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'spanish-extraction', model: await getModelForTask('fatal_extraction') });
    if (req.method === 'POST') {
      const body = await new Promise(r => { let d = ''; req.on('data', c => d += c); req.on('end', () => { try { r(JSON.parse(d)); } catch (_) { r({}); } }); });
      const out = await extract(body.text || '', db);
      return res.json({ success: true, ...out });
    }
    return res.status(400).json({ error: 'POST {text}' });
  } catch (err) { await reportError(db, 'spanish-extraction', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.extract = extract;
module.exports.isSpanish = isSpanish;
