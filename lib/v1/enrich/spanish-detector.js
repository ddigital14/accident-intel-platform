/**
 * Phase 50: Spanish-language detector + translator engine.
 *
 * Companion to spanish-extraction.js but operating at the source_reports layer:
 *   1. Detect Spanish-language news/RSS articles via word + diacritic heuristics.
 *   2. Send full article to Claude Opus 4.7 for translation + bilingual victim extraction.
 *   3. Insert translated record into source_reports with original_language='es'.
 *   4. Stamp meta + bump CEI counter so the engine learns.
 *
 * Unlocks Mexican-American + Latin community accident victims that English-only
 * extractors silently dropped on the floor.
 *
 * GET  /api/v1/enrich/spanish-detector?secret=ingest-now&action=health
 * POST /api/v1/enrich/spanish-detector?secret=ingest-now&action=translate { text, source_url? }
 * GET  /api/v1/enrich/spanish-detector?secret=ingest-now&action=batch&limit=10
 */
const { getDb } = require('../../_db');
const { extractJson } = require('./_ai_router');
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');
const { bumpCounter } = require('../system/_cei_telemetry');
const { v4: uuidv4 } = require('uuid');

const ENGINE = 'spanish-detector';

// -- Detection -------------------------------------------------------------
const SPANISH_WORDS = /\b(el|la|los|las|de|que|en|un|una|por|para|con|sin|fue|fueron|murio|murió|fallecio|falleció|fallecido|fallecida|accidente|atropello|atropellado|atropellada|choque|chocó|herido|herida|muerto|muerta|familia|hospital|policia|policía|esposa|esposo|hijo|hija|madre|padre|carretera|calle|victima|víctima|conductor|conductora|camion|camión|peaton|peatón|motociclista)\b/gi;
const ACCENTED = /[áéíóúñÁÉÍÓÚÑ¿¡]/g;

function detect(text) {
  if (!text || typeof text !== 'string') return { is_spanish: false, score: 0 };
  const words = (text.match(SPANISH_WORDS) || []).length;
  const accents = (text.match(ACCENTED) || []).length;
  const score = words * 2 + accents;
  // 4+ Spanish words OR (2+ words AND 3+ accent marks) → Spanish
  const is_spanish = words >= 4 || (words >= 2 && accents >= 3);
  return { is_spanish, score, spanish_words: words, accent_marks: accents };
}

// -- Translation + extraction (Claude Opus 4.7) ----------------------------
async function translateAndExtract(db, text, sourceUrl) {
  const det = detect(text);
  if (!det.is_spanish) return { ok: false, skipped: 'not_spanish', detection: det };

  const sys = 'You are a bilingual news translator and forensic extractor. Translate Spanish accident reports to clear English. PRESERVE all proper names verbatim - never anglicize Spanish names. Return JSON only.';
  const userPrompt = `Spanish accident article:
"""
${String(text).slice(0, 6000)}
"""

Translate to English and extract structured data. Return JSON:
{
  "english_text": "full English translation, preserving Spanish names verbatim",
  "original_language": "es",
  "incident_type": "car_accident|truck_accident|motorcycle_accident|pedestrian|bicycle|other",
  "severity": "fatal|serious|moderate|minor|unknown",
  "city": "string|null",
  "state": "two-letter US|null",
  "occurred_at": "ISO 8601|null",
  "victims_extracted": [
    { "full_name": "string (preserve accents)", "role": "driver|passenger|pedestrian|cyclist|family", "age": number|null, "city": "string|null", "state": "string|null", "severity": "fatal|injured|unknown" }
  ],
  "summary_en": "1-sentence English summary"
}`;

  const t0 = Date.now();
  const parsed = await extractJson(db, {
    pipeline: ENGINE,
    systemPrompt: sys,
    userPrompt,
    tier: 'opus',
    provider: 'claude',
    severityHint: /muri|fallec|fatal|muerto|muerta/i.test(text) ? 'fatal' : 'unknown',
    timeoutMs: 50000,
  });

  if (!parsed) return { ok: false, error: 'translation_failed', detection: det };

  // Insert translated record into source_reports if source_url provided
  let inserted_report_id = null;
  if (sourceUrl) {
    try {
      const id = uuidv4();
      await db('source_reports').insert({
        id,
        source_type: 'translated_es',
        source_reference: sourceUrl,
        raw_data: JSON.stringify({ original_text: String(text).slice(0, 3500), language: 'es' }),
        parsed_data: JSON.stringify(parsed),
        contributed_fields: ['victims', 'translation', 'severity'],
        confidence: 60,
        is_verified: false,
        fetched_at: new Date(),
        processed_at: new Date(),
        created_at: new Date(),
        meta: JSON.stringify({ engine: ENGINE, original_language: 'es', detection: det })
      }).onConflict('source_reference').ignore();
      inserted_report_id = id;
    } catch (e) {
      // gracefully degrade if meta column or unique-conflict not present
      try { await reportError(db, ENGINE, sourceUrl, `insert source_report failed: ${e.message}`); } catch (_) {}
    }
  }

  const latency_ms = Date.now() - t0;
  await bumpCounter(db, ENGINE, true, latency_ms).catch(() => {});

  return {
    ok: true,
    detection: det,
    parsed,
    inserted_report_id,
    latency_ms,
    victims_extracted_count: (parsed.victims_extracted || []).length
  };
}

// -- Batch: scan recent source_reports, translate any Spanish ones ---------
async function batchTranslate(db, limit = 10) {
  const candidates = await db.raw(`
    SELECT id, source_reference, raw_data, parsed_data, created_at
    FROM source_reports
    WHERE created_at > NOW() - INTERVAL '24 hours'
      AND COALESCE(meta->>'engine','') <> 'spanish-detector'
      AND (raw_data::text ~ '[áéíóúñÁÉÍÓÚÑ]' OR raw_data::text ~* '\\b(murió|fallecido|atropellado|accidente fatal|herido)\\b')
    ORDER BY created_at DESC
    LIMIT ${parseInt(limit, 10) || 10}
  `).catch(() => ({ rows: [] }));

  const results = { scanned: 0, spanish_detected: 0, translated: 0, errors: 0 };
  for (const row of (candidates.rows || [])) {
    results.scanned++;
    let text = '';
    try {
      const raw = typeof row.raw_data === 'string' ? JSON.parse(row.raw_data) : row.raw_data;
      text = (raw?.item?.title || '') + '\n' + (raw?.item?.description || raw?.original_text || raw?.content || '');
    } catch (_) { text = String(row.raw_data || '').slice(0, 3000); }
    const det = detect(text);
    if (!det.is_spanish) continue;
    results.spanish_detected++;
    const r = await translateAndExtract(db, text, row.source_reference);
    if (r.ok) results.translated++; else results.errors++;
  }
  return results;
}

// -- HTTP handler ----------------------------------------------------------
module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const action = req.query?.action || 'health';

  try {
    if (action === 'health') {
      return res.json({
        success: true,
        engine: ENGINE,
        message: 'Spanish detector + Claude Opus 4.7 translator online',
        capabilities: ['detect', 'translate', 'extract_victims', 'insert_translated_report'],
        timestamp: new Date().toISOString()
      });
    }

    if (action === 'detect') {
      const text = req.query?.text || (req.body?.text || '');
      return res.json({ success: true, detection: detect(String(text)) });
    }

    if (action === 'translate') {
      let body = req.body;
      if (!body && req.method === 'POST') {
        body = await new Promise(r => { let d = ''; req.on('data', c => d += c); req.on('end', () => { try { r(JSON.parse(d)); } catch (_) { r({}); } }); });
      }
      body = body || {};
      const text = body.text || req.query?.text || '';
      const url = body.source_url || req.query?.source_url || null;
      if (!text) return res.status(400).json({ error: 'text required (POST body or ?text=)' });
      const out = await translateAndExtract(db, String(text), url);
      return res.json({ success: !!out.ok, ...out });
    }

    if (action === 'batch') {
      const limit = parseInt(req.query?.limit || '10', 10);
      const out = await batchTranslate(db, limit);
      return res.json({ success: true, message: `Spanish batch: ${out.translated}/${out.scanned} translated`, ...out });
    }

    return res.status(400).json({ error: 'unknown action', allowed: ['health', 'detect', 'translate', 'batch'] });
  } catch (err) {
    await reportError(db, ENGINE, null, err.message).catch(() => {});
    return res.status(500).json({ error: err.message });
  }
};

module.exports.detect = detect;
module.exports.translateAndExtract = translateAndExtract;
module.exports.batchTranslate = batchTranslate;
