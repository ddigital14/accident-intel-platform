/**
 * Cross-Intelligence Orchestrator — Phase 44B (the brain)
 * Decides which engine to fire NEXT for a verified victim. Uses Claude Opus
 * 4.7 for the reasoning step.
 *
 * GET /api/v1/system/cross-intel-orchestrator?secret=ingest-now&action=plan&person_id=<uuid>
 * GET /api/v1/system/cross-intel-orchestrator?secret=ingest-now&action=plan&victim_name=...&city=...&state=...
 * GET /api/v1/system/cross-intel-orchestrator?secret=ingest-now&action=execute&person_id=<uuid>&max_cost_usd=0.50
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { trackApiCall } = require('./cost');
const { enqueueCascade } = require('./_cascade');

const SECRET = 'ingest-now';
const ANTHROPIC_URL = 'https://api.anthropic.com/v1/messages';
const OPUS_MODEL = 'claude-opus-4-5-20250929';
const HTTP_TIMEOUT_MS = 15000;

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function getKey(db, name) {
  const envName = name.toUpperCase();
  if (process.env[envName]) return process.env[envName];
  try {
    const row = await db('system_config').where({ key: name.toLowerCase() }).first();
    if (row?.value) return typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
  } catch (_) {}
  return null;
}

const ENGINE_CATALOG = [
  { id: 'pdl_identify',          route: 'enrich/pdl-identify',          provider: 'pdl',      cost: 0.10, fills: ['email','phone','employer'],     gates: ['has_name'] },
  { id: 'pdl_by_name',           route: 'enrich/pdl-by-name',           provider: 'pdl',      cost: 0.10, fills: ['email','phone'],                 gates: ['has_name'] },
  { id: 'apollo_unlock',         route: 'enrich/apollo-unlock',         provider: 'apollo',   cost: 0.41, fills: ['email','phone'],                 gates: ['has_name','has_employer'] },
  { id: 'apollo_cross_pollinate',route: 'enrich/apollo-cross-pollinate',provider: 'apollo',   cost: 0.05, fills: ['employer','title'],              gates: ['has_name'] },
  { id: 'voyage_news_rerank',    route: 'enrich/voyage-router',         provider: 'voyageai', cost: 0.02, fills: ['news_articles','context'],       gates: ['has_name'] },
  { id: 'brave_search',          route: 'enrich/brave-search',          provider: 'brave',    cost: 0.005,fills: ['social_links','articles'],       gates: ['has_name'] },
  { id: 'property_records',      route: 'enrich/property-records',      provider: 'free',     cost: 0,    fills: ['address','owner_name','family'], gates: ['has_address','has_state'] },
  { id: 'multi_county_property', route: 'enrich/property-records',      provider: 'free',     cost: 0,    fills: ['property_owner'],                gates: ['has_state','wired_county'] },
  { id: 'fatal_family_tree',     route: 'enrich/fatal-family-tree',     provider: 'free',     cost: 0.01, fills: ['family','next_of_kin'],          gates: ['fatality'] },
  { id: 'twilio_sim_check',      route: 'enrich/twilio',                provider: 'twilio',   cost: 0.008,fills: ['phone_intelligence'],            gates: ['has_phone'] },
  { id: 'archive_search',        route: 'enrich/archive-search',        provider: 'free',     cost: 0,    fills: ['historical_news'],               gates: ['has_name'] },
  { id: 'voter_rolls',           route: 'enrich/voter-rolls',           provider: 'free',     cost: 0,    fills: ['address','dob'],                 gates: ['has_name','has_state'] },
  { id: 'state_courts',          route: 'enrich/state-courts',          provider: 'free',     cost: 0,    fills: ['attorney','case_number'],        gates: ['has_name','has_state'] },
  { id: 'funeral_home_survivors',route: 'enrich/funeral-home-survivors',provider: 'free',     cost: 0,    fills: ['family','next_of_kin'],          gates: ['fatality','has_name'] },
  { id: 'plate_ocr_vision',      route: 'enrich/plate-ocr-vision',      provider: 'anthropic',cost: 0.009,fills: ['plates','vehicle_info'],         gates: ['has_image'] },
  { id: 'hunter_email_finder',   route: 'enrich/auto-purchase',         provider: 'hunter',   cost: 0.04, fills: ['email'],                         gates: ['has_name','has_employer'] },
  { id: 'homegrown_osint_miner', route: 'enrich/homegrown-osint-miner', provider: 'free',     cost: 0.01, fills: ['email','phone','social'],        gates: ['has_name'] }
];

const WIRED_COUNTIES_BY_STATE = {
  TX: ['HOUSTON','PASADENA','SPRING','KATY','HUMBLE','AUSTIN','PFLUGERVILLE','MANOR'],
  IL: ['CHICAGO','CICERO','EVANSTON','SKOKIE'],
  GA: ['ATLANTA','SANDY SPRINGS','ROSWELL','JOHNS CREEK','ALPHARETTA','MILTON'],
  FL: ['MIAMI','HIALEAH','MIAMI BEACH','HOMESTEAD','CORAL GABLES','NORTH MIAMI'],
  AZ: ['PHOENIX','MESA','CHANDLER','GLENDALE','SCOTTSDALE','TEMPE','GILBERT','SURPRISE'],
  CA: ['LOS ANGELES','LONG BEACH','GLENDALE','SANTA MONICA','PASADENA','TORRANCE','INGLEWOOD','BEVERLY HILLS','COMPTON','BURBANK']
};

function applicableGates(person, incident) {
  const out = new Set();
  if (person?.full_name) out.add('has_name');
  if (person?.employer) out.add('has_employer');
  if (person?.phone) out.add('has_phone');
  if (person?.address) out.add('has_address');
  if (person?.state) out.add('has_state');
  if (incident?.fatal) out.add('fatality');
  const cityUp = (person?.city || incident?.city || '').toUpperCase();
  const stateUp = (person?.state || incident?.state || '').toUpperCase();
  if (WIRED_COUNTIES_BY_STATE[stateUp]?.includes(cityUp)) out.add('wired_county');
  if (person?._has_image) out.add('has_image');
  return out;
}

async function gatherContext(db, query) {
  let person = null;
  if (query.person_id) {
    person = await db('persons').where('id', query.person_id).first();
  } else if (query.victim_name) {
    person = await db('persons')
      .whereRaw('LOWER(full_name) = LOWER(?)', [query.victim_name])
      .modify(qb => {
        if (query.state) qb.andWhere(function () { this.whereRaw('LOWER(state)=LOWER(?)',[query.state]).orWhereNull('state'); });
        if (query.city)  qb.andWhere(function () { this.whereRaw('LOWER(city)=LOWER(?)',[query.city]).orWhereNull('city'); });
      })
      .orderBy('updated_at', 'desc')
      .first();
  }
  if (!person && query.victim_name) {
    person = { id: null, full_name: query.victim_name, city: query.city, state: query.state, role: 'victim' };
  }
  if (!person) return { person: null };

  let incident = null;
  if (person.incident_id) {
    incident = await db('incidents').where('id', person.incident_id).first().catch(() => null);
  }

  let history = [];
  try {
    history = await db('enrichment_logs')
      .where('person_id', person.id)
      .select('source', 'source_url', 'field_name', 'created_at')
      .orderBy('created_at', 'desc').limit(50);
  } catch (_) {}

  let hasImage = false;
  if (person.incident_id) {
    try {
      const r = await db.raw(`
        SELECT 1 FROM source_reports
         WHERE incident_id = ?
           AND (image_url IS NOT NULL OR (raw_data IS NOT NULL AND raw_data->>'image_url' IS NOT NULL))
         LIMIT 1
      `, [person.incident_id]);
      hasImage = (r.rows || []).length > 0;
    } catch (_) {}
  }
  person._has_image = hasImage;

  const budget24h = {};
  try {
    const r = await db.raw(`
      SELECT pipeline, COALESCE(SUM(cost_usd),0) AS spent, COUNT(*) AS calls
        FROM system_api_calls
       WHERE created_at > NOW() - INTERVAL '24 hours'
       GROUP BY pipeline
    `);
    for (const row of (r.rows || [])) budget24h[row.pipeline] = { spent: parseFloat(row.spent), calls: parseInt(row.calls) };
  } catch (_) {}

  return { person, incident, history, budget24h };
}

function buildBaseline(ctx) {
  const { person, incident, history } = ctx;
  if (!person) return { actions: [], reason: 'no_person' };
  const gates = applicableGates(person, incident);
  const triedSources = new Set((history || []).map(h => (h.source || '').toLowerCase()));

  const candidates = [];
  for (const e of ENGINE_CATALOG) {
    const gateOk = e.gates.every(g => gates.has(g));
    if (!gateOk) continue;
    const tried = triedSources.has(e.id) || triedSources.has(e.provider);
    if (tried && e.cost > 0.01) continue;

    const missing = [];
    if (e.fills.includes('email') && !person.email) missing.push('email');
    if (e.fills.includes('phone') && !person.phone) missing.push('phone');
    if (e.fills.includes('address') && !person.address) missing.push('address');
    if (e.fills.includes('family')) missing.push('family');
    if (e.fills.includes('property_owner') && !person.address) missing.push('property_owner');

    const yieldScore = missing.length || (e.fills.length - 1);
    candidates.push({
      id: e.id, route: e.route, provider: e.provider,
      estimated_cost_usd: e.cost,
      fills: e.fills,
      missing_fields_addressed: missing,
      yield_score: yieldScore,
      tried_recently: tried
    });
  }

  candidates.sort((a, b) => {
    if ((a.estimated_cost_usd || 0) === 0 && (b.estimated_cost_usd || 0) > 0) return -1;
    if ((b.estimated_cost_usd || 0) === 0 && (a.estimated_cost_usd || 0) > 0) return 1;
    const aRatio = a.yield_score / Math.max(0.001, a.estimated_cost_usd);
    const bRatio = b.yield_score / Math.max(0.001, b.estimated_cost_usd);
    return bRatio - aRatio;
  });

  return {
    person_id: person.id, full_name: person.full_name,
    city: person.city, state: person.state,
    incident_id: incident?.id || null,
    lead_score: incident?.lead_score || null,
    fatal: !!incident?.fatal,
    gates_satisfied: [...gates],
    tried_recently: [...triedSources],
    actions: candidates.slice(0, 8)
  };
}

async function aiReason(db, baseline) {
  const key = await getKey(db, 'anthropic_api_key');
  if (!key || !baseline.actions?.length) return baseline;
  const prompt = `You are the Cross-Intelligence Orchestrator for an accident-intel platform. A verified victim is below with current context, missing fields, and candidate next-engine actions. Pick the top 3 actions in order. Re-rank using:
1. yield (most missing fields filled)
2. cost ($0 free wins ties)
3. specificity (engines that match the victim's state/city beat generic ones)
4. avoid duplicates already tried

Return ONLY JSON: {"ranked":[{"id":"...","reason":"...","estimated_yield":"high|med|low"}],"plan_summary":"<25 words>"}.

CONTEXT:
${JSON.stringify(baseline, null, 2)}`;
  try {
    const resp = await fetch(ANTHROPIC_URL, {
      method: 'POST',
      headers: {
        'x-api-key': key,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json'
      },
      body: JSON.stringify({
        model: OPUS_MODEL,
        max_tokens: 1024,
        messages: [{ role: 'user', content: prompt }]
      }),
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS)
    });
    if (!resp.ok) {
      await trackApiCall(db, 'cross-intel-orchestrator', 'claude_opus', 0, 0, false).catch(() => {});
      return { ...baseline, ai_skipped: true, ai_error: `HTTP ${resp.status}` };
    }
    const data = await resp.json();
    const text = data?.content?.[0]?.text || '';
    const inT = data?.usage?.input_tokens || 0;
    const outT = data?.usage?.output_tokens || 0;
    await trackApiCall(db, 'cross-intel-orchestrator', 'claude_opus', inT, outT, true).catch(() => {});
    let parsed = null;
    try { const m = text.match(/\{[\s\S]*\}/); parsed = m ? JSON.parse(m[0]) : null; } catch (_) {}
    if (!parsed) return { ...baseline, ai_skipped: true, ai_raw: text.slice(0, 500) };

    const ranked = parsed.ranked || [];
    const idIndex = new Map(ranked.map((r, i) => [r.id, { idx: i, reason: r.reason, estimated_yield: r.estimated_yield }]));
    const reordered = [...baseline.actions].sort((a, b) => {
      const A = idIndex.get(a.id); const B = idIndex.get(b.id);
      if (A && !B) return -1; if (B && !A) return 1;
      if (!A && !B) return 0;
      return A.idx - B.idx;
    }).map(a => {
      const meta = idIndex.get(a.id);
      return meta ? { ...a, ai_reason: meta.reason, estimated_yield: meta.estimated_yield } : a;
    });
    return { ...baseline, actions: reordered, plan_summary: parsed.plan_summary || null, ai_used: true };
  } catch (e) {
    return { ...baseline, ai_skipped: true, ai_error: e.message };
  }
}

async function plan(db, query) {
  const ctx = await gatherContext(db, query);
  if (!ctx.person) return { ok: false, error: 'no_person_match' };
  const baseline = buildBaseline(ctx);
  const ranked = await aiReason(db, baseline);
  return { ok: true, ...ranked, generated_at: new Date().toISOString() };
}

async function execute(db, query, maxCostUsd = 0.50) {
  const planRes = await plan(db, query);
  if (!planRes.ok) return planRes;
  let spent = 0;
  const log = [];
  for (const a of (planRes.actions || [])) {
    if (spent + a.estimated_cost_usd > maxCostUsd) {
      log.push({ id: a.id, skipped: 'budget_exceeded', would_cost: a.estimated_cost_usd, remaining: maxCostUsd - spent });
      continue;
    }
    try {
      const mod = require('../' + a.route);
      const fn = (typeof mod === 'function') ? mod : (mod.handler || mod.default);
      if (!fn) { log.push({ id: a.id, error: 'no_handler' }); continue; }
      const fakeReq = {
        method: 'GET',
        query: { secret: SECRET, action: 'health', person_id: planRes.person_id, victim_name: planRes.full_name, city: planRes.city, state: planRes.state },
        headers: { 'x-cron-secret': SECRET }
      };
      const fakeRes = {
        _payload: null, _status: 200,
        status(c) { this._status = c; return this; },
        json(p) { this._payload = p; return this; },
        setHeader() {}, end() {}
      };
      await fn(fakeReq, fakeRes);
      spent += a.estimated_cost_usd;
      log.push({ id: a.id, ran: true, status: fakeRes._status, cost_added: a.estimated_cost_usd });
    } catch (e) {
      log.push({ id: a.id, error: e.message });
    }
  }
  if (planRes.person_id) {
    await enqueueCascade(db, planRes.person_id, 'cross_intel_executed').catch(() => {});
  }
  return { ok: true, plan: planRes, executed: log, spent_usd: Math.round(spent * 10000) / 10000 };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  const db = getDb();
  const action = (req.query?.action || 'plan').toLowerCase();
  try {
    if (action === 'plan') {
      const out = await plan(db, req.query || {});
      return res.json({ success: !!out.ok, ...out });
    }
    if (action === 'execute') {
      const max = parseFloat(req.query.max_cost_usd || '0.50');
      const out = await execute(db, req.query || {}, max);
      return res.json({ success: !!out.ok, ...out });
    }
    return res.status(400).json({ error: 'unknown_action', valid: ['plan','execute'] });
  } catch (e) {
    try { await reportError(db, 'cross-intel-orchestrator', null, e.message); } catch (_) {}
    return res.status(500).json({ success: false, error: e.message });
  }
};
module.exports.plan = plan;
module.exports.execute = execute;
module.exports.ENGINE_CATALOG = ENGINE_CATALOG;
