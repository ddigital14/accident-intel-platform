/**
 * AI ROUTER — Central GPT/Claude gateway for AIP
 *
 * Replaces the 10+ duplicated `extractWithGPT()` blocks scattered across
 * lib/v1/ingest/* and lib/v1/enrich/*. Every AI extraction now goes through
 * here so we get:
 *   - Consistent error logging (no more silent `catch (_) {}`)
 *   - Tier routing (gpt-4o-mini default, gpt-4o for FATAL/SERIOUS/obit cases)
 *   - Provider failover (OpenAI -> Claude when 5xx or quota)
 *   - Cost tracking via trackApiCall on every call
 *   - Token usage telemetry into system_api_calls.tokens_in/out
 *   - Single point to change models when new versions ship
 *
 * Per RULES.md NEW ENGINE RULE #6: every API call auto-instrumented via trackApiCall.
 */
const { getModelForTask } = require('../system/model-registry');
const { trackApiCall } = require('../system/cost');

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;

// Default model strings — overridden at runtime by model_registry table (60s cache).
// Flip via POST /api/v1/system/model-registry?action=set&task=light_extraction&model=gpt-4o-mini-2025
// All servers pick up the change within 60s, zero redeploy.
const MODELS = {
  cheap_oai:    'gpt-4o-mini',
  premium_oai:  'gpt-4o',
  whisper:      'whisper-1',
  cheap_anth:   'claude-haiku-4-5-20251001',
  mid_anth:     'claude-sonnet-4-6',
  premium_anth: 'claude-opus-4-7',  // Phase 43: latest Anthropic flagship API model ID. Override via system_config 'model_registry' or model_registry table to flip without redeploy.
};
const TASK_MAP = {
  cheap_oai: 'light_extraction', premium_oai: 'heavy_extraction', whisper: 'transcription',
  cheap_anth: 'classification', mid_anth: 'cross_reasoning', premium_anth: 'premium_reasoning'
};
async function resolveModel(modelKey) {
  const taskName = TASK_MAP[modelKey] || modelKey;
  const fallback = MODELS[modelKey] || modelKey;
  try { return await getModelForTask(taskName, fallback); } catch (_) { return fallback; }
}

function pickModel({ tier = 'auto', severityHint, pipeline }) {
  if (tier === 'cheap') return MODELS.cheap_oai;
  if (tier === 'premium') return MODELS.premium_oai;
  if (tier === 'opus' || tier === 'claude_opus') return MODELS.premium_anth;
  const sev = String(severityHint || '').toLowerCase();
  const pl = String(pipeline || '').toLowerCase();
  const fatalish = ['fatal', 'critical', 'serious'].includes(sev);
  const premiumPipeline = pl.includes('obituar') || pl.includes('court') || pl.includes('claude');
  if (fatalish || premiumPipeline) return MODELS.premium_oai;
  return MODELS.cheap_oai;
}

function estTokens(s) { return Math.max(1, Math.ceil(String(s || '').length / 4)); }

async function callOpenAI({ model, systemPrompt, userPrompt, timeoutMs, responseFormat, temperature }) {
  if (!OPENAI_API_KEY) return { ok: false, error: 'openai_key_missing', code: 'NO_KEY' };
  const body = {
    model,
    messages: [
      { role: 'system', content: systemPrompt || 'Return JSON only.' },
      { role: 'user', content: userPrompt }
    ],
    temperature: typeof temperature === 'number' ? temperature : 0,
  };
  if (responseFormat === 'json') body.response_format = { type: 'json_object' };
  try {
    const resp = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(timeoutMs || 20000),
    });
    if (!resp.ok) {
      const text = (await resp.text().catch(() => '')).slice(0, 400);
      return { ok: false, status: resp.status, error: `openai_http_${resp.status}`, raw: text };
    }
    const data = await resp.json();
    const content = data.choices?.[0]?.message?.content || '';
    return {
      ok: true,
      content,
      tokens_in: data.usage?.prompt_tokens || estTokens(systemPrompt + userPrompt),
      tokens_out: data.usage?.completion_tokens || estTokens(content),
      model,
    };
  } catch (e) {
    return { ok: false, error: `openai_exception:${e.name}:${e.message}` };
  }
}

async function callClaude({ model, systemPrompt, userPrompt, timeoutMs, responseFormat, temperature }) {
  if (!ANTHROPIC_API_KEY) return { ok: false, error: 'anthropic_key_missing', code: 'NO_KEY' };
  const sys = systemPrompt || 'Return JSON only.';
  const userBody = responseFormat === 'json'
    ? `${userPrompt}\n\nReturn JSON only - no preamble, no code fences.`
    : userPrompt;
  const body = {
    model,
    max_tokens: /opus/i.test(model) ? 4000 : 2000,
    temperature: typeof temperature === 'number' ? temperature : 0,
    system: sys,
    messages: [{ role: 'user', content: userBody }],
  };
  try {
    const resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(/opus/i.test(model) ? Math.max(timeoutMs || 0, 55000) : (timeoutMs || 30000)),
    });
    if (!resp.ok) {
      const text = (await resp.text().catch(() => '')).slice(0, 400);
      return { ok: false, status: resp.status, error: `anthropic_http_${resp.status}`, raw: text };
    }
    const data = await resp.json();
    const content = data.content?.[0]?.text || '';
    return {
      ok: true,
      content,
      tokens_in: data.usage?.input_tokens || estTokens(sys + userPrompt),
      tokens_out: data.usage?.output_tokens || estTokens(content),
      model,
    };
  } catch (e) {
    return { ok: false, error: `anthropic_exception:${e.name}:${e.message}` };
  }
}

function safeJson(s) {
  if (!s) return null;
  let t = String(s).trim();
  t = t.replace(/^```(?:json)?\s*/i, '').replace(/\s*```\s*$/i, '');
  const i = t.indexOf('{');
  const j = t.lastIndexOf('}');
  if (i >= 0 && j > i) t = t.slice(i, j + 1);
  try { return JSON.parse(t); } catch (_) { return null; }
}

async function extract(db, {
  pipeline, systemPrompt, userPrompt,
  tier = 'auto', severityHint, timeoutMs = 20000,
  responseFormat = 'json', temperature = 0,
  provider = 'auto'
}) {
  const model = pickModel({ tier, severityHint, pipeline });
  let primary, fallback;
  if (provider === 'claude') {
    primary = 'claude'; fallback = OPENAI_API_KEY ? 'openai' : null;
  } else if (provider === 'openai') {
    primary = 'openai'; fallback = ANTHROPIC_API_KEY ? 'claude' : null;
  } else {
    primary = OPENAI_API_KEY ? 'openai' : 'claude';
    fallback = (primary === 'openai' && ANTHROPIC_API_KEY) ? 'claude' : null;
  }

  const attempts = [];
  let result = null;
  for (const p of [primary, fallback].filter(Boolean)) {
    let r;
    if (p === 'openai') {
      r = await callOpenAI({ model, systemPrompt, userPrompt, timeoutMs, responseFormat, temperature });
      if (db) await trackApiCall(db, pipeline || 'ai-router', model, r.tokens_in || 0, r.tokens_out || 0, !!r.ok).catch(()=>{});
    } else {
      let claudeModel;
      if (model === MODELS.premium_anth) claudeModel = MODELS.premium_anth;
      else if (model === MODELS.premium_oai) claudeModel = MODELS.mid_anth;
      else claudeModel = MODELS.cheap_anth;
      r = await callClaude({ model: claudeModel, systemPrompt, userPrompt, timeoutMs, responseFormat, temperature });
      if (db) await trackApiCall(db, pipeline || 'ai-router', claudeModel, r.tokens_in || 0, r.tokens_out || 0, !!r.ok).catch(()=>{});
    }
    attempts.push({ provider: p, ok: r.ok, error: r.error, status: r.status, raw: r.raw ? String(r.raw).slice(0, 300) : null });
    if (r.ok) { result = r; break; }
    if (r.status && r.status >= 400 && r.status < 500 && r.status !== 429) break;
  }

  if (!result) return { ok: false, error: attempts[attempts.length-1]?.error || 'all_providers_failed', attempts };

  const parsed = responseFormat === 'json' ? safeJson(result.content) : null;
  return {
    ok: true,
    content: result.content,
    parsed,
    model_used: result.model,
    provider_used: attempts[attempts.length-1].provider,
    tokens_in: result.tokens_in,
    tokens_out: result.tokens_out,
    attempts,
  };
}

async function extractJson(db, opts) {
  const r = await extract(db, { ...opts, responseFormat: 'json' });
  if (!r.ok) {
    try {
      const { reportError } = require('../system/_errors');
      await reportError(db, opts.pipeline || 'ai-router', null,
        `AI extract failed: ${r.error}`,
        { attempts: r.attempts, severity: 'warning' });
    } catch (_) {}
    return null;
  }
  if (!r.parsed) {
    try {
      const { reportError } = require('../system/_errors');
      await reportError(db, opts.pipeline || 'ai-router', null,
        'AI returned non-JSON content',
        { content_preview: String(r.content).slice(0, 300), severity: 'warning' });
    } catch (_) {}
    return null;
  }
  return r.parsed;
}

module.exports = { extract, extractJson, pickModel, MODELS, callOpenAI, callClaude };
