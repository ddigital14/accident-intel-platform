/**
 * Phase 96b: Claude Agent-Loop Research Engine
 *
 * Mason's right approach: every accident is different. Claude reasons per-case
 * about which aggregator / search / API to hit next based on what's already
 * known and what's been found. Iterates 6-10 times until contact info surfaces
 * or we exhaust productive paths.
 *
 * TOOLBOX (Claude can call any of these per iteration):
 *   - brave_search(query)               — general web
 *   - google_cse(query)                  — different index, different bias
 *   - fetch_url(url)                     — any URL → text
 *   - search_legacy_com(name, state)    — obituary aggregator
 *   - search_findagrave(name, state)    — burial registry
 *   - search_gofundme(name)              — fundraisers w/ organizer info
 *   - search_caringbridge(name)          — hospitalized victims
 *   - apollo_by_name(name, city, state) — commercial people-data
 *   - pdl_identify(name, city, state)   — commercial people-data
 *   - trestle_by_phone(phone)            — reverse phone (when phone surfaces)
 *   - hunter_by_employer(name, domain)  — work email when employer known
 *   - courtlistener_by_name(name)        — civil PI filings
 *   - write_field(field, value, evidence_url) — persist finding to DB
 *   - finish(summary)                    — end the loop
 *
 * Claude decides which tools to call, reads the returns, decides next steps,
 * persists findings via write_field. Each persisted field cascades via the
 * existing auto-fan-out trigger.
 *
 * Endpoints:
 *   GET ?action=health
 *   POST ?action=research&person_id=X&max_steps=8
 */
const { getDb } = require('../../_db');

const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
const MODEL = 'claude-opus-4-7';
const FALLBACK_MODEL = 'claude-sonnet-4-6';

// Rate limiter — Brave free tier is 1 req/sec, hit 429 if we burst
let _lastBraveCall = 0;
async function braveRateLimit() {
  const now = Date.now();
  const since = now - _lastBraveCall;
  if (since < 1100) await new Promise(r => setTimeout(r, 1100 - since));
  _lastBraveCall = Date.now();
}

async function fetchWithRetry(url, options, maxRetry = 2) {
  for (let i = 0; i <= maxRetry; i++) {
    const r = await fetch(url, options);
    if (r.ok) return r;
    if (r.status === 429 && i < maxRetry) {
      await new Promise(res => setTimeout(res, 2000 * (i + 1)));
      continue;
    }
    return r;
  }
}

// ─── Key resolvers ──────────────────────────────────────────────────────────
async function resolveKey(db, key, configKey) {
  const env = process.env[key];
  if (env) return env;
  try {
    const row = await db('system_config').where({ key: configKey }).first();
    if (row?.value) return typeof row.value === 'string' ? row.value : (row.value.api_key || row.value.key);
  } catch { /* skip */ }
  return null;
}

// ─── TOOLBOX ────────────────────────────────────────────────────────────────
async function tool_brave_search(args, ctx) {
  const key = await resolveKey(ctx.db, 'BRAVE_API_KEY', 'brave_api_key');
  if (!key) return { error: 'no_brave_key' };
  await braveRateLimit();
  try {
    const r = await fetchWithRetry(`https://api.search.brave.com/res/v1/web/search?q=${encodeURIComponent(args.query)}&count=10&country=us&safesearch=off`, {
      headers: { 'Accept': 'application/json', 'X-Subscription-Token': key },
      signal: AbortSignal.timeout(10000)
    });
    if (!r.ok) return { error: `http_${r.status}` };
    const j = await r.json();
    const results = [];
    for (const arr of [j.web?.results, j.news?.results, j.discussions?.results]) {
      if (!Array.isArray(arr)) continue;
      for (const x of arr) {
        if (x.url) results.push({ title: x.title || '', url: x.url, snippet: (x.description || '').slice(0, 250) });
      }
    }
    return { results: results.slice(0, 8), total: results.length };
  } catch (e) { return { error: e.message }; }
}

async function tool_google_cse(args, ctx) {
  const cseKey = await resolveKey(ctx.db, 'GOOGLE_CSE_API_KEY', 'google_cse');
  let cseId = process.env.GOOGLE_CSE_ID || process.env.GOOGLE_CSE_ENGINE_ID;
  if (!cseId) {
    try {
      const row = await ctx.db('system_config').where({ key: 'google_cse' }).first();
      if (row?.value) cseId = row.value.cse_id || row.value.cx;
    } catch { /* skip */ }
  }
  if (!cseKey || !cseId) return { error: 'no_cse_key' };
  try {
    const r = await fetch(`https://www.googleapis.com/customsearch/v1?key=${cseKey}&cx=${cseId}&q=${encodeURIComponent(args.query)}&num=8`, {
      signal: AbortSignal.timeout(8000)
    });
    if (!r.ok) return { error: `http_${r.status}` };
    const j = await r.json();
    return { results: (j.items || []).map(x => ({ title: x.title, url: x.link, snippet: (x.snippet || '').slice(0, 250) })) };
  } catch (e) { return { error: e.message }; }
}

async function tool_fetch_url(args, ctx) {
  try {
    const r = await fetch(args.url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; AccidentCommandCenter/1.0)' },
      signal: AbortSignal.timeout(8000),
      redirect: 'follow'
    });
    if (!r.ok) return { error: `http_${r.status}`, url: args.url };
    const html = await r.text();
    const text = html
      .replace(/<script[\s\S]*?<\/script>/gi, ' ')
      .replace(/<style[\s\S]*?<\/style>/gi, ' ')
      .replace(/<[^>]+>/g, ' ')
      .replace(/&nbsp;/g, ' ')
      .replace(/&amp;/g, '&')
      .replace(/&quot;/g, '"')
      .replace(/&#39;/g, "'")
      .replace(/\s+/g, ' ')
      .trim()
      .slice(0, 6000);
    return { text, url: args.url, length: text.length };
  } catch (e) { return { error: e.message, url: args.url }; }
}

async function tool_apollo_by_name(args, ctx) {
  const key = await resolveKey(ctx.db, 'APOLLO_API_KEY', 'apollo_api_key');
  if (!key) return { error: 'no_apollo_key' };
  const parts = (args.name || '').trim().split(/\s+/);
  if (parts.length < 2) return { error: 'name_too_short' };
  const body = {
    first_name: parts[0],
    last_name: parts[parts.length - 1],
    reveal_personal_emails: true
  };
  if (args.city) body.city = args.city;
  if (args.state) body.state = args.state;
  try {
    const r = await fetch('https://api.apollo.io/v1/people/match', {
      method: 'POST',
      headers: { 'Cache-Control': 'no-cache', 'Content-Type': 'application/json', 'X-Api-Key': key },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(10000)
    });
    if (!r.ok) return { error: `http_${r.status}` };
    const j = await r.json();
    const p = j.person || null;
    if (!p) return { match: null };
    return {
      match: {
        name: p.name, title: p.title, employer: p.organization?.name,
        city: p.city, state: p.state, email: p.email, linkedin: p.linkedin_url,
        phone: p.mobile_phone || p.phone_numbers?.[0]?.sanitized_number
      }
    };
  } catch (e) { return { error: e.message }; }
}

async function tool_pdl_identify(args, ctx) {
  const key = await resolveKey(ctx.db, 'PDL_API_KEY', 'pdl_api_key');
  if (!key) return { error: 'no_pdl_key' };
  const parts = (args.name || '').trim().split(/\s+/);
  if (parts.length < 2) return { error: 'name_too_short' };
  const params = new URLSearchParams();
  params.append('first_name', parts[0]);
  params.append('last_name', parts[parts.length - 1]);
  if (args.city) params.append('locality', args.city);
  if (args.state) params.append('region', args.state);
  params.append('min_likelihood', '2');
  try {
    const r = await fetch(`https://api.peopledatalabs.com/v5/person/identify?${params}`, {
      headers: { 'X-API-Key': key, 'Accept': 'application/json' },
      signal: AbortSignal.timeout(10000)
    });
    if (!r.ok) return { error: `http_${r.status}` };
    const j = await r.json();
    if (j.status !== 200 || !Array.isArray(j.matches) || j.matches.length === 0) return { match: null };
    const p = j.matches[0].data;
    return {
      match: {
        name: p.full_name, employer: p.job_company_name, title: p.job_title,
        city: p.location_locality, state: p.location_region,
        email: p.personal_emails?.[0] || p.work_email,
        phone: p.mobile_phone || p.phone_numbers?.[0],
        address: p.location_street_address,
        linkedin: p.linkedin_url, age: p.birth_year ? new Date().getFullYear() - p.birth_year : null
      },
      likelihood: j.matches[0].match_score
    };
  } catch (e) { return { error: e.message }; }
}

async function tool_search_legacy(args, ctx) {
  // Legacy.com obit search via Brave site: query
  const q = `site:legacy.com "${args.name}" ${args.state || ''}`;
  return tool_brave_search({ query: q }, ctx);
}

async function tool_search_findagrave(args, ctx) {
  const q = `site:findagrave.com "${args.name}" ${args.state || ''}`;
  return tool_brave_search({ query: q }, ctx);
}

async function tool_search_gofundme(args, ctx) {
  const q = `site:gofundme.com "${args.name}" ${args.state || ''}`;
  return tool_brave_search({ query: q }, ctx);
}

async function tool_search_caringbridge(args, ctx) {
  const q = `site:caringbridge.org "${args.name}" ${args.state || ''}`;
  return tool_brave_search({ query: q }, ctx);
}

async function tool_search_obituary_aggregators(args, ctx) {
  // tributearchive, dignitymemorial, ever loved, funeralhome, frazerconsultants
  const q = `(site:tributearchive.com OR site:dignitymemorial.com OR site:everloved.com OR site:funeralhomes.com) "${args.name}" ${args.state || ''}`;
  return tool_brave_search({ query: q }, ctx);
}

async function tool_search_local_news(args, ctx) {
  const q = `"${args.name}" ${args.city || ''} ${args.state || ''} news accident OR crash OR killed`;
  return tool_brave_search({ query: q }, ctx);
}

async function tool_courtlistener_by_name(args, ctx) {
  // CourtListener has a public free search
  try {
    const r = await fetch(`https://www.courtlistener.com/api/rest/v3/search/?q=${encodeURIComponent(args.name)}&type=r&format=json`, {
      headers: { 'Accept': 'application/json' },
      signal: AbortSignal.timeout(8000)
    });
    if (!r.ok) return { error: `http_${r.status}` };
    const j = await r.json();
    return { results: (j.results || []).slice(0, 5).map(x => ({ caption: x.caseName, court: x.court, date: x.dateFiled, url: x.absolute_url ? `https://www.courtlistener.com${x.absolute_url}` : null })) };
  } catch (e) { return { error: e.message }; }
}

async function tool_write_field(args, ctx) {
  // Persist a discovered field to the person row
  const allowed = ['phone','email','address','city','state','age','employer','occupation','attorney_firm','attorney_name','insurance_company','linkedin_url'];
  const field = args.field;
  const value = args.value;
  if (!allowed.includes(field)) return { error: `field_not_allowed: ${field}` };
  if (!value || (typeof value === 'string' && value.length < 3)) return { error: 'value_too_short' };
  try {
    const upd = { [field]: value };
    await ctx.db('persons').where('id', ctx.person.id).update(upd);
    await ctx.db('enrichment_logs').insert({
      person_id: ctx.person.id, field_name: field,
      old_value: ctx.person[field] || null,
      new_value: typeof value === 'string' ? value : JSON.stringify(value),
      created_at: new Date()
    }).catch(() => {});
    ctx.person[field] = value;  // reflect for subsequent tool calls
    ctx._fields_written.push({ field, value, evidence: args.evidence_url || null });
    return { ok: true, written: { field, value } };
  } catch (e) { return { error: e.message }; }
}

async function tool_add_family_member(args, ctx) {
  const { v4: uuid } = require('uuid');
  if (!args.name || args.name.length < 5 || !/\s/.test(args.name)) return { error: 'name_invalid' };
  try {
    const exists = await ctx.db('persons').where({ incident_id: ctx.person.incident_id, full_name: args.name }).first();
    if (exists) return { ok: false, reason: 'already_exists' };
    await ctx.db('persons').insert({
      id: uuid(),
      incident_id: ctx.person.incident_id,
      full_name: args.name,
      role: 'family',
      relationship_to_victim: args.relationship || 'family',
      victim_id: ctx.person.id,
      city: args.city || ctx.person.city,
      state: args.state || ctx.person.state,
      victim_verified: false,
      lead_tier: 'related',
      source: 'research-agent',
      created_at: new Date()
    });
    ctx._family_added.push(args);
    return { ok: true };
  } catch (e) { return { error: e.message }; }
}


async function tool_legacy_direct(args, ctx) {
  // Hit Legacy.com search directly — they have a public obituary search
  const parts = (args.name || '').trim().split(/\s+/);
  if (parts.length < 2) return { error: 'name_too_short' };
  const first = parts[0], last = parts[parts.length - 1];
  const url = `https://www.legacy.com/obituaries/search?firstName=${encodeURIComponent(first)}&lastName=${encodeURIComponent(last)}${args.state ? '&state='+args.state : ''}`;
  return tool_fetch_url({ url }, ctx);
}

async function tool_findagrave_direct(args, ctx) {
  const parts = (args.name || '').trim().split(/\s+/);
  if (parts.length < 2) return { error: 'name_too_short' };
  const first = parts[0], last = parts[parts.length - 1];
  const url = `https://www.findagrave.com/memorial/search?firstname=${encodeURIComponent(first)}&lastname=${encodeURIComponent(last)}${args.state ? '&location='+args.state : ''}`;
  return tool_fetch_url({ url }, ctx);
}

async function tool_truepeoplesearch(args, ctx) {
  // TruePeopleSearch direct - very effective for US name+state
  const name = (args.name || '').trim();
  const cs = args.state ? `${args.city || ''} ${args.state}`.trim() : '';
  const url = `https://www.truepeoplesearch.com/results?name=${encodeURIComponent(name)}${cs ? '&citystatezip='+encodeURIComponent(cs) : ''}`;
  return tool_fetch_url({ url }, ctx);
}

const TOOLS = {
  brave_search: { fn: tool_brave_search, params: { query: 'string' } },
  google_cse: { fn: tool_google_cse, params: { query: 'string' } },
  fetch_url: { fn: tool_fetch_url, params: { url: 'string' } },
  apollo_by_name: { fn: tool_apollo_by_name, params: { name: 'string', city: 'string?', state: 'string?' } },
  pdl_identify: { fn: tool_pdl_identify, params: { name: 'string', city: 'string?', state: 'string?' } },
  search_legacy_com: { fn: tool_search_legacy, params: { name: 'string', state: 'string?' } },
  search_findagrave: { fn: tool_search_findagrave, params: { name: 'string', state: 'string?' } },
  search_gofundme: { fn: tool_search_gofundme, params: { name: 'string', state: 'string?' } },
  search_caringbridge: { fn: tool_search_caringbridge, params: { name: 'string', state: 'string?' } },
  search_obituary_aggregators: { fn: tool_search_obituary_aggregators, params: { name: 'string', state: 'string?' } },
  search_local_news: { fn: tool_search_local_news, params: { name: 'string', city: 'string?', state: 'string?' } },
  courtlistener_by_name: { fn: tool_courtlistener_by_name, params: { name: 'string' } },
  write_field: { fn: tool_write_field, params: { field: 'string', value: 'any', evidence_url: 'string?' } },
  add_family_member: { fn: tool_add_family_member, params: { name: 'string', relationship: 'string?', city: 'string?', state: 'string?' } },
  legacy_direct_search: { fn: tool_legacy_direct, params: { name: 'string', state: 'string?' } },
  findagrave_direct_search: { fn: tool_findagrave_direct, params: { name: 'string', state: 'string?' } },
  truepeoplesearch: { fn: tool_truepeoplesearch, params: { name: 'string', city: 'string?', state: 'string?' } }
};

// ─── Anthropic tool-use loop ────────────────────────────────────────────────
function buildToolDefinitions() {
  return Object.entries(TOOLS).map(([name, def]) => {
    const properties = {};
    const required = [];
    for (const [k, t] of Object.entries(def.params)) {
      const optional = t.endsWith('?');
      properties[k] = { type: optional ? 'string' : (t === 'any' ? 'string' : 'string') };
      if (!optional) required.push(k);
    }
    return {
      name,
      description: `${name} — ${def.description || ''}`,
      input_schema: { type: 'object', properties, required }
    };
  });
}

async function callClaude(messages, tools) {
  const tries = [MODEL, FALLBACK_MODEL];
  for (const m of tries) {
    try {
      const r = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'x-api-key': ANTHROPIC_KEY,
          'anthropic-version': '2023-06-01',
          'content-type': 'application/json'
        },
        body: JSON.stringify({ model: m, max_tokens: 2000, tools, messages }),
        signal: AbortSignal.timeout(28000)
      });
      if (!r.ok) {
        const txt = await r.text().catch(() => '');
        if (m === FALLBACK_MODEL) return { error: `http_${r.status}: ${txt.slice(0,200)}` };
        continue;
      }
      return await r.json();
    } catch (e) {
      if (m === FALLBACK_MODEL) return { error: e.message };
    }
  }
  return { error: 'all_models_failed' };
}

async function runAgent(db, person, maxSteps = 6) {
  const ctx = { db, person, _fields_written: [], _family_added: [], _tool_calls: [] };
  const tools = buildToolDefinitions();

  const systemPrompt = `You are an OSINT researcher trying to find contact info (phone, email, address) for a victim of a car accident, plus their immediate family members. You have ${maxSteps} tool-call rounds.

PERSON SO FAR:
- Name: ${person.full_name}
- Role: ${person.role || 'victim'}
- Age: ${person.age || 'unknown'}
- City: ${person.city || 'unknown'}
- State: ${person.state || 'unknown'}
- Phone: ${person.phone || 'MISSING'}
- Email: ${person.email || 'MISSING'}
- Address: ${person.address || 'MISSING'}
- Employer: ${person.employer || 'unknown'}

INCIDENT CONTEXT:
${(person._incident_text || '').slice(0, 1000)}

YOUR JOB:
1. Decide which tools to call to find missing contact info.
2. For DECEASED victims: focus on obituaries (legacy.com, findagrave, tributearchive) — they list next-of-kin with cities. Then add_family_member for each, then research the family member (they may be in Apollo/PDL with phone/email).
3. For SURVIVORS: try GoFundMe (organizer is often family), Caringbridge, Apollo/PDL by name+city.
4. When you find a phone/email/address, call write_field to persist it.
5. After 4-6 productive tool calls, summarize and stop.

ANTI-PATTERNS:
- Don't search the same query twice.
- Don't write fake or guessed values — only write fields you found in the search results.
- If a name is too generic (e.g., "John Smith"), prefer family-member research.

Begin.`;

  const messages = [{ role: 'user', content: systemPrompt }];

  for (let step = 0; step < maxSteps; step++) {
    const resp = await callClaude(messages, tools);
    if (resp.error) {
      ctx._tool_calls.push({ step, error: resp.error });
      break;
    }
    messages.push({ role: 'assistant', content: resp.content });
    const toolUses = (resp.content || []).filter(b => b.type === 'tool_use');
    if (toolUses.length === 0) {
      // Claude finished — captured text
      const finalText = (resp.content || []).filter(b => b.type === 'text').map(b => b.text).join('\n');
      ctx._final_summary = finalText.slice(0, 2000);
      break;
    }
    const toolResults = [];
    for (const tu of toolUses) {
      const tool = TOOLS[tu.name];
      let result;
      if (!tool) result = { error: `unknown_tool: ${tu.name}` };
      else result = await tool.fn(tu.input || {}, ctx);
      ctx._tool_calls.push({ step, tool: tu.name, input: tu.input, result_keys: Object.keys(result || {}) });
      toolResults.push({
        type: 'tool_result',
        tool_use_id: tu.id,
        content: JSON.stringify(result).slice(0, 4000)
      });
    }
    messages.push({ role: 'user', content: toolResults });
  }

  return ctx;
}

module.exports = async function handler(req, res) {
  if (!authed(req)) return res.status(401).json({ error: 'unauthorized' });
  const action = req.query?.action || 'health';
  const db = getDb();

  if (action === 'health') {
    return res.status(200).json({
      ok: true, engine: 'research-agent',
      model: MODEL, fallback: FALLBACK_MODEL,
      tools: Object.keys(TOOLS),
      anthropic_configured: !!ANTHROPIC_KEY
    });
  }

  if (action === 'research') {
    const id = req.query?.person_id;
    if (!id) return res.status(400).json({ error: 'person_id required' });
    const maxSteps = Math.min(10, parseInt(req.query?.max_steps) || 6);
    const persons = (await db.raw(`
      SELECT p.*, i.raw_description, i.description, i.severity, i.occurred_at
      FROM persons p JOIN incidents i ON i.id = p.incident_id WHERE p.id = ?
    `, [id])).rows;
    if (persons.length === 0) return res.status(404).json({ error: 'person not found' });
    const person = persons[0];
    person._incident_text = person.raw_description || person.description || '';
    const ctx = await runAgent(db, person, maxSteps);
    return res.status(200).json({
      ok: true,
      person_id: id,
      name: person.full_name,
      tool_calls: ctx._tool_calls.length,
      fields_written: ctx._fields_written,
      family_added: ctx._family_added,
      tool_call_log: ctx._tool_calls,
      final_summary: ctx._final_summary || null
    });
  }

  return res.status(400).json({ error: 'unknown action', valid: ['health','research'] });
};
