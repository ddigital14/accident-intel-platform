/**
 * REP PRE-CALL BRIEF — Phase 41 Module 4
 *
 * Generate a 100-word call brief for a personal injury intake rep, tailored
 * to the specific verified victim. Covers:
 *   1) what happened (one sentence)
 *   2) victim status (recent fatality / critical / recovering)
 *   3) sensitivity flags (recent death = wait 24-48h; family already retained)
 *   4) opening talking point (lead with empathy, not the case)
 *   5) likely best-fit case angle (premises, auto, wrongful death, comm vehicle)
 *
 * Cached in enrichment_logs (source='rep-call-brief') for 24 hours so
 * repeated dashboard clicks don't re-burn Claude tokens.
 *
 * HTTP shape:
 *   GET /api/v1/dashboard/rep-call-brief?secret=ingest-now&person_id=<uuid>
 *   GET /api/v1/dashboard/rep-call-brief?secret=ingest-now&victim_name=<n>&city=<c>&state=<s>
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { extract } = require('../enrich/_ai_router');

const SECRET = 'ingest-now';
const CACHE_TTL_HOURS = 24;
const AI_TIMEOUT_MS = 28000;

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

function safeParseJson(v) {
  if (!v) return null;
  if (typeof v === 'object') return v;
  try { return JSON.parse(v); } catch (_) { return null; }
}

async function findPerson(db, { person_id, victim_name, city, state }) {
  if (person_id) {
    return await db('persons').where('id', person_id).first();
  }
  if (!victim_name) return null;
  const q = db('persons').whereRaw('LOWER(full_name) = LOWER(?)', [victim_name]);
  if (state) q.andWhere(function () { this.whereRaw('LOWER(state) = LOWER(?)', [state]).orWhereNull('state'); });
  if (city) q.andWhere(function () { this.whereRaw('LOWER(city) = LOWER(?)', [city]).orWhereNull('city'); });
  return await q.orderBy('updated_at', 'desc').first();
}

async function gatherContext(db, person) {
  const ctx = {
    victim: {
      id: person.id,
      full_name: person.full_name,
      first_name: person.first_name,
      last_name: person.last_name,
      role: person.role,
      age: person.age,
      city: person.city,
      state: person.state,
      injury_severity: person.injury_severity,
      transported_to: person.transported_to,
      employer: person.employer,
      has_attorney: person.has_attorney,
      attorney_name: person.attorney_name,
      attorney_firm: person.attorney_firm,
      contact_status: person.contact_status,
      phone: person.phone ? '(on file)' : null,
      email: person.email ? '(on file)' : null,
      victim_verified: !!person.victim_verified
    },
    incident: null,
    family: [],
    history: []
  };

  if (person.incident_id) {
    const inc = await db('incidents').where('id', person.incident_id).first();
    if (inc) {
      ctx.incident = {
        type: inc.incident_type,
        severity: inc.severity,
        occurred_at: inc.occurred_at,
        city: inc.city,
        state: inc.state,
        address: inc.address,
        intersection: inc.intersection,
        description: inc.description ? String(inc.description).slice(0, 500) : null,
        fatalities_count: inc.fatalities_count,
        injuries_count: inc.injuries_count,
        responding_agencies: inc.responding_agencies
      };
    }
  }

  // Family
  try {
    const fam = await db('persons')
      .where('victim_id', person.id)
      .select('full_name', 'relationship_to_victim', 'derived_from')
      .limit(8);
    ctx.family = fam.map(f => ({
      name: f.full_name,
      relationship: f.relationship_to_victim,
      source: f.derived_from
    }));
  } catch (_) {}

  // Last 8 enrichment events as a signal trail
  try {
    const logs = await db('enrichment_logs')
      .where('person_id', person.id)
      .orderBy('created_at', 'desc')
      .limit(8)
      .select('source', 'field_name', 'created_at');
    ctx.history = logs.map(l => ({ source: l.source, field: l.field_name, at: l.created_at }));
  } catch (_) {}

  return ctx;
}

const SYSTEM_PROMPT =
  'You are an experienced personal injury intake coach writing a SHORT pre-call brief ' +
  'for a rep about to phone a verified accident victim or their family. Write 90-110 words ' +
  'plain prose (no bullet points, no headings, no markdown). Be specific to THIS case. ' +
  'Tone: warm, professional, sober. Never sound salesy or transactional. Acknowledge fatalities ' +
  'with respect. Always advise empathy first, business second.';

function userPromptFor(ctx) {
  return (
    'Case context (JSON):\n' + JSON.stringify(ctx, null, 2) + '\n\n' +
    'Write the 90-110 word pre-call brief covering: (1) what happened in one sentence, ' +
    '(2) the victim\'s current status (recent fatality / critical / recovering / unknown), ' +
    '(3) sensitivity flags (e.g., recent death — wait 24-48 hours, family already retained, ' +
    'witness only, etc.), (4) suggested opening talking point that does NOT lead with the case ' +
    '(establish empathy first), and (5) the most plausible case angle (premises liability, auto, ' +
    'wrongful death, commercial vehicle, motorcycle, pedestrian, work injury). Plain prose only.'
  );
}

async function getCached(db, personId) {
  if (!personId) return null;
  try {
    const row = await db('enrichment_logs')
      .where('person_id', personId)
      .where('action', 'rep-call-brief')
      .where('created_at', '>', db.raw('NOW() - INTERVAL \'' + CACHE_TTL_HOURS + ' hours\''))
      .orderBy('created_at', 'desc')
      .first();
    if (!row) return null;
    const data = safeParseJson(row.meta) || {};
    if (!data.brief) return null;
    return { brief: data.brief, model: data.model, cached_at: row.created_at, tokens_in: data.tokens_in, tokens_out: data.tokens_out };
  } catch (_) { return null; }
}

async function generate(db, person, opts = {}) {
  const ctx = await gatherContext(db, person);
  const userPrompt = userPromptFor(ctx);

  const r = await extract(db, {
    pipeline: 'dashboard-rep-call-brief',
    systemPrompt: SYSTEM_PROMPT,
    userPrompt,
    provider: 'auto',
    tier: 'auto',
    severityHint: ctx.incident?.severity || person.injury_severity,
    timeoutMs: AI_TIMEOUT_MS,
    temperature: 0.3,
    responseFormat: 'text'
  });

  if (!r.ok) {
    return { ok: false, error: r.error || 'ai_failed', attempts: r.attempts };
  }
  const brief = String(r.content || '').trim();
  if (!brief || brief.length < 50) {
    return { ok: false, error: 'brief_too_short', length: brief.length };
  }

  // Cache for 24h
  try {
    await db('enrichment_logs').insert({
      person_id: person.id,
      field_name: 'rep-call-brief',
      old_value: null,
      new_value: brief.slice(0, 4000),
      action: 'rep-call-brief',
      confidence: 80,
      verified: true,
      meta: JSON.stringify({
        brief,
        model: r.model_used,
        provider: r.provider_used,
        tokens_in: r.tokens_in,
        tokens_out: r.tokens_out,
        word_count: brief.split(/\s+/).length
      }).slice(0, 4000),
      created_at: new Date()
    });
  } catch (_) {}

  return {
    ok: true,
    brief,
    word_count: brief.split(/\s+/).length,
    model: r.model_used,
    provider: r.provider_used,
    tokens_in: r.tokens_in,
    tokens_out: r.tokens_out,
    fresh: true
  };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const db = getDb();
  const force = req.query?.force === '1' || req.query?.force === 'true';

  try {
    const person = await findPerson(db, {
      person_id: req.query?.person_id,
      victim_name: req.query?.victim_name,
      city: req.query?.city,
      state: req.query?.state
    });
    if (!person) {
      return res.status(404).json({ success: false, error: 'person_not_found', hint: 'pass person_id or victim_name+city+state' });
    }

    if (!force) {
      const cached = await getCached(db, person.id);
      if (cached) {
        return res.json({
          success: true,
          person_id: person.id,
          victim_name: person.full_name,
          ...cached,
          cached: true,
          timestamp: new Date().toISOString()
        });
      }
    }

    const r = await generate(db, person);
    if (!r.ok) {
      return res.status(502).json({
        success: false,
        person_id: person.id,
        victim_name: person.full_name,
        error: r.error,
        timestamp: new Date().toISOString()
      });
    }

    return res.json({
      success: true,
      person_id: person.id,
      victim_name: person.full_name,
      ...r,
      cached: false,
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    try { await reportError(db, 'dashboard-rep-call-brief', null, e.message); } catch (_) {}
    res.status(500).json({ success: false, error: e.message });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.generate = generate;
