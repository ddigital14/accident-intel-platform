/**
 * GET /api/v1/system/test-gpt?secret=ingest-now[&provider=openai|claude][&tier=cheap|premium]
 *
 * P0 debug endpoint - runs ONE sample extraction through the AI router and
 * returns the full response (or error). Use this to confirm OPENAI_API_KEY /
 * ANTHROPIC_API_KEY work end-to-end without going through a full ingest job.
 */
const { extract } = require('../enrich/_ai_router');
const { getDb } = require('../../_db');

const SAMPLE_ARTICLE = `Two people were killed and three others injured in a multi-vehicle crash on I-285 northbound near Atlanta on Saturday afternoon. Georgia State Patrol identified the deceased as Marcus Williams, 42, of Atlanta and Linda Chen, 38, of Decatur. Three injured passengers were transported to Grady Memorial Hospital. The crash occurred around 3:15 PM at mile marker 32. State troopers say a tractor-trailer crossed the median and struck two passenger vehicles head-on.`;

const SYS = 'Extract crash data as strict JSON only. Empty list if not a crash.';
const USER = `Extract crash details. Return JSON only:
{ "is_crash": true|false, "city": "...", "state": "two-letter",
  "incident_type": "...", "severity": "fatal|serious|moderate|minor|unknown",
  "fatalities_count": number|null, "injuries_count": number|null,
  "victims": [{"full_name":"","age":null,"role":"","is_injured":true,"injury_severity":""}] }

Article:
"""
${SAMPLE_ARTICLE}
"""`;

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const db = getDb();
  const tier = req.query.tier || 'cheap';
  const provider = req.query.provider || 'auto';

  const out = {
    success: true,
    timestamp: new Date().toISOString(),
    env: {
      OPENAI_API_KEY: !!process.env.OPENAI_API_KEY,
      ANTHROPIC_API_KEY: !!process.env.ANTHROPIC_API_KEY,
    },
    sample_text_length: SAMPLE_ARTICLE.length,
    latency_ms: {},
  };

  if (provider === 'auto' || provider === 'openai') {
    const t0 = Date.now();
    const r = await extract(db, {
      pipeline: 'test-gpt', systemPrompt: SYS, userPrompt: USER,
      tier, timeoutMs: 25000, responseFormat: 'json', provider: 'openai',
    });
    out.latency_ms.openai = Date.now() - t0;
    out.openai = {
      ok: r.ok, model: r.model_used,
      tokens_in: r.tokens_in, tokens_out: r.tokens_out,
      parsed: r.parsed,
      content_preview: r.content ? String(r.content).slice(0, 600) : null,
      error: r.error, attempts: r.attempts,
    };
  }

  if (provider === 'auto' || provider === 'claude') {
    const t0 = Date.now();
    const r = await extract(db, {
      pipeline: 'test-gpt', systemPrompt: SYS, userPrompt: USER,
      tier, timeoutMs: 25000, responseFormat: 'json', provider: 'claude',
    });
    out.latency_ms.claude = Date.now() - t0;
    out.claude = {
      ok: r.ok, model: r.model_used,
      tokens_in: r.tokens_in, tokens_out: r.tokens_out,
      parsed: r.parsed,
      content_preview: r.content ? String(r.content).slice(0, 600) : null,
      error: r.error, attempts: r.attempts,
    };
  }

  res.json(out);
};
