/**
 * Phase 50b: Multi-language detector + translator engine.
 *
 * Extends the Phase-50 spanish-detector to 10 languages covering the
 * highest-incidence non-English-speaking communities in the U.S.
 *
 *   es Spanish        - Mexican-American, Latin
 *   fr French         - Haitian (FL), Quebecois
 *   ht Haitian Creole - South FL Haitian community
 *   vi Vietnamese     - TX/CA Vietnamese
 *   tl Tagalog        - CA/NV/HI Filipino
 *   ko Korean         - LA, NJ Korean
 *   zh Mandarin       - NYC, SF Chinese
 *   ru Russian        - NY (Brighton Beach)
 *   pt Portuguese     - Brazilian (FL, MA)
 *   ar Arabic         - MI, NY Arabic-speaking
 *
 * Each language is detected via word + script heuristics, then routed to
 * Claude Opus 4.7 for translation + bilingual victim extraction.
 */
const { getDb } = require('../../_db');
const { extractJson } = require('./_ai_router');
const { reportError } = require('../system/_errors');
const { bumpCounter } = require('../system/_cei_telemetry');
const { v4: uuidv4 } = require('uuid');

const ENGINE = 'multilang-detector';

const LANG_PATTERNS = {
  es: {
    name: 'Spanish',
    words: /\b(el|la|los|las|de|que|en|un|una|por|para|con|sin|fue|fueron|murio|murió|fallecio|falleció|fallecido|fallecida|accidente|atropello|atropellado|atropellada|choque|chocó|herido|herida|muerto|muerta|familia|hospital|policia|policía|esposa|esposo|hijo|hija|madre|padre|carretera|calle|victima|víctima|conductor|conductora|camion|camión|peaton|peatón|motociclista)\b/gi,
    chars: /[áéíóúñ¿¡]/g,
    fatalRegex: /muri|fallec|fatal|muerto|muerta/i,
  },
  fr: {
    name: 'French',
    words: /\b(le|la|les|des|une?|au|du|qui|que|et|dans|sur|pour|avec|sans|ils|elles|il|elle|son|sa|ses|accident|blessé|blessée|décédé|décédée|mort|morte|tué|tuée|victime|conducteur|conductrice|piéton|cycliste|hôpital|police|familles?|enfants?|fille|fils|mère|père|époux|épouse)\b/gi,
    chars: /[àâçéèêëîïôûùüÿœæ]/g,
    fatalRegex: /décéd|tué|mort/i,
  },
  ht: {
    name: 'Haitian Creole',
    words: /\b(aksidan|viktim|mouri|blese|chofè|chofe|machin|wout|lopital|lapolis|fanmi|pitit|manman|papa|madanm|mari|mwen|ou|li|nou|yo|ki|nan|sou|pou|ak|epi|men|tonbe|frape|kanpe|lanmò)\b/gi,
    chars: /[èéò]/g,
    fatalRegex: /mouri|lanmò/i,
  },
  vi: {
    name: 'Vietnamese',
    words: /\b(tai\s?n[ạa]n|n[ạa]n\s?nh[âa]n|t[ưu]\s?vong|ch[ếe]t|b[ịi]\s?th[ưu][ơo]ng|xe\s?h[ơo]i|xe\s?t[ảa]i|xe\s?m[áa]y|ng[ưu][ồo]i\s?l[áa]i|c[ảa]nh\s?s[áa]t|b[ệe]nh\s?vi[ệe]n|gia\s?[đd][ìi]nh)\b/gi,
    chars: /[ăâđêôơưạảấầẩẫậắằẳẵặẹẻẽếềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵỷỹ]/g,
    fatalRegex: /t[ưu]\s?vong|ch[ếe]t/i,
  },
  tl: {
    name: 'Tagalog',
    words: /\b(aksidente|nasaktan|biktima|namatay|nasawi|namatayan|pumanaw|sumakabilang|sasakyan|kotse|trak|motorsiklo|drayber|pulisya|ospital|pamilya|asawa|anak|ina|ama)\b/gi,
    chars: /[ñ]/g,
    fatalRegex: /namatay|nasawi|pumanaw/i,
  },
  ko: {
    name: 'Korean',
    words: /(사고|피해자|사망|부상|운전자|보행자|병원|경찰|가족|남편|아내|아들|딸|어머니|아버지|충돌|차량)/g,
    chars: /[㄰-㆏가-힯]/g,
    fatalRegex: /사망/,
  },
  zh: {
    name: 'Mandarin Chinese',
    words: /(事故|受害者|死亡|受伤|司机|行人|医院|警察|家人|丈夫|妻子|儿子|女儿|母亲|父亲|车祸|碰撞)/g,
    chars: /[一-鿿]/g,
    fatalRegex: /死亡|身亡/,
  },
  ru: {
    name: 'Russian',
    words: /(авари|жертв|погиб|умер|пострадавш|водител|пешеход|больниц|полици|семь|сын|дочь|муж|жена|мать|отец|столкновен)/gi,
    chars: /[А-яЁё]/g,
    fatalRegex: /погиб|умер/i,
  },
  pt: {
    name: 'Portuguese',
    words: /\b(o|a|os|as|um|uma|de|que|em|por|para|com|sem|foi|foram|morreu|morreram|faleceu|faleceram|acidente|atropelad[oa]|colisão|ferid[oa]|mort[oa]|família|hospital|polícia|esposa|marido|filho|filha|mãe|pai|estrada|rua|vítima|condutor|pedestre|motociclista|caminhão)\b/gi,
    chars: /[áàâãéêíóôõúç]/g,
    fatalRegex: /morr|falec/i,
  },
  ar: {
    name: 'Arabic',
    words: /(حادث|ضحي[ةه]|توفي|قتل|جرح|سائق|مشاة|مستشفى|شرطة|عائل[ةه]|زوج|زوجة|ابن|ابنة|أم|أب|اصطدام|سيارة|شاحنة)/g,
    chars: /[؀-ۿ]/g,
    fatalRegex: /توفي|قتل/,
  },
};

function detect(text) {
  if (!text || typeof text !== 'string') return { lang: null, score: 0, scores: {} };
  const scores = {};
  let best = { lang: null, score: 0 };
  for (const [lang, def] of Object.entries(LANG_PATTERNS)) {
    const w = (text.match(def.words) || []).length;
    const c = (text.match(def.chars) || []).length;
    const score = w * 2 + c;
    scores[lang] = { words: w, chars: c, score };
    const passes =
      w >= 4 ||
      (w >= 2 && c >= 3) ||
      (['ko', 'zh', 'ar', 'ru'].includes(lang) && c >= 6);
    if (passes && score > best.score) best = { lang, score };
  }
  if (!best.lang) return { lang: null, score: 0, scores };
  const def = LANG_PATTERNS[best.lang];
  return {
    lang: best.lang,
    name: def.name,
    score: best.score,
    fatal_signal: def.fatalRegex.test(text),
    scores,
  };
}

function detectSpanish(text) {
  const d = detect(text);
  const s = d.scores?.es || { words: 0, chars: 0, score: 0 };
  return {
    is_spanish: d.lang === 'es',
    score: s.score,
    spanish_words: s.words,
    accent_marks: s.chars,
  };
}

async function translateAndExtract(db, text, sourceUrl, forceLang) {
  const det = forceLang
    ? { lang: forceLang, name: LANG_PATTERNS[forceLang]?.name || forceLang, score: 0, fatal_signal: false, scores: {} }
    : detect(text);
  if (!det.lang) return { ok: false, skipped: 'not_foreign_language', detection: det };

  const langName = det.name || det.lang;
  const sys = `You are a bilingual news translator and forensic extractor specializing in ${langName}. Translate the article to clear English. PRESERVE all proper names verbatim - never anglicize ${langName} names, never transliterate beyond what's standard. Return JSON only.`;
  const userPrompt = `${langName} accident article:
"""
${String(text).slice(0, 6000)}
"""

Translate to English and extract structured data. Return JSON:
{
  "english_text": "full English translation, preserving original-language names verbatim",
  "original_language": "${det.lang}",
  "incident_type": "car_accident|truck_accident|motorcycle_accident|pedestrian|bicycle|other",
  "severity": "fatal|serious|moderate|minor|unknown",
  "city": "string|null",
  "state": "two-letter US|null",
  "occurred_at": "ISO 8601|null",
  "victims_extracted": [
    { "full_name": "string (preserve original-script characters and accents)", "role": "driver|passenger|pedestrian|cyclist|family", "age": number|null, "city": "string|null", "state": "string|null", "severity": "fatal|injured|unknown" }
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
    severityHint: det.fatal_signal ? 'fatal' : 'unknown',
    timeoutMs: 50000,
  });

  if (!parsed) {
    await bumpCounter(db, ENGINE, false, Date.now() - t0).catch(() => {});
    return { ok: false, error: 'translation_failed', detection: det };
  }

  let inserted_report_id = null;
  if (sourceUrl) {
    try {
      const id = uuidv4();
      await db('source_reports').insert({
        id,
        source_type: `translated_${det.lang}`,
        source_reference: sourceUrl,
        raw_data: JSON.stringify({ original_text: String(text).slice(0, 3500), language: det.lang }),
        parsed_data: JSON.stringify(parsed),
        contributed_fields: ['victims', 'translation', 'severity'],
        confidence: 60,
        is_verified: false,
        fetched_at: new Date(),
        processed_at: new Date(),
        created_at: new Date(),
        meta: JSON.stringify({ engine: ENGINE, original_language: det.lang, detection: det })
      }).onConflict('source_reference').ignore();
      inserted_report_id = id;
    } catch (e) {
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

async function batchTranslate(db, limit) {
  limit = parseInt(limit, 10) || 10;
  const candidates = await db.raw(`
    SELECT id, source_reference, raw_data, parsed_data, created_at
    FROM source_reports
    WHERE created_at > NOW() - INTERVAL '24 hours'
      AND COALESCE(meta->>'engine','') NOT IN ('multilang-detector','spanish-detector')
      AND (
        raw_data::text ~ '[áéíóúñçãâêôõÁÉÍÓÚÑ]'
        OR raw_data::text ~ '[一-鿿]'
        OR raw_data::text ~ '[가-힯]'
        OR raw_data::text ~ '[؀-ۿ]'
        OR raw_data::text ~ '[А-яЁё]'
        OR raw_data::text ~* '\\b(murió|fallecido|atropellado|accidente fatal|herido|décédé|aksidan|tai nạn|aksidente)\\b'
      )
    ORDER BY created_at DESC
    LIMIT ${limit}
  `).catch(() => ({ rows: [] }));

  const results = { scanned: 0, foreign_detected: 0, translated: 0, errors: 0, by_lang: {} };
  for (const row of (candidates.rows || [])) {
    results.scanned++;
    let text = '';
    try {
      const raw = typeof row.raw_data === 'string' ? JSON.parse(row.raw_data) : row.raw_data;
      text = (raw?.item?.title || '') + '\n' + (raw?.item?.description || raw?.original_text || raw?.content || '');
    } catch (_) { text = String(row.raw_data || '').slice(0, 3000); }
    const det = detect(text);
    if (!det.lang) continue;
    results.foreign_detected++;
    results.by_lang[det.lang] = (results.by_lang[det.lang] || 0) + 1;
    const r = await translateAndExtract(db, text, row.source_reference);
    if (r.ok) results.translated++; else results.errors++;
  }
  return results;
}

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
        message: 'Multi-language detector + Claude Opus 4.7 translator online',
        languages: Object.keys(LANG_PATTERNS).map(k => ({ code: k, name: LANG_PATTERNS[k].name })),
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
      const forceLang = body.lang || req.query?.lang || null;
      if (!text) return res.status(400).json({ error: 'text required (POST body or ?text=)' });
      const out = await translateAndExtract(db, String(text), url, forceLang);
      return res.json({ success: !!out.ok, ...out });
    }

    if (action === 'batch') {
      const limit = parseInt(req.query?.limit || '10', 10);
      const out = await batchTranslate(db, limit);
      return res.json({ success: true, message: `multilang batch: ${out.translated}/${out.scanned} translated`, ...out });
    }

    return res.status(400).json({ error: 'unknown action', allowed: ['health', 'detect', 'translate', 'batch'] });
  } catch (err) {
    await reportError(db, ENGINE, null, err.message).catch(() => {});
    return res.status(500).json({ error: err.message });
  }
};

module.exports.detect = detect;
module.exports.detectSpanish = detectSpanish;
module.exports.translateAndExtract = translateAndExtract;
module.exports.batchTranslate = batchTranslate;
module.exports.LANG_PATTERNS = LANG_PATTERNS;
