/**
 * AI OBITUARY PARSER — Phase 41 Module 2
 *
 * For each fatal incident's verified victim, find the obituary URL (via Google
 * CSE) or use any text we've already cached, then call Claude Sonnet 4.6 to
 * extract a STRUCTURED family tree: deceased details, survivors with
 * relationships, preceded-in-death, funeral home, services.
 *
 * Higher yield than the regex parser in funeral-home-survivors.js (which we
 * keep — AI runs first; regex is the safety net).
 *
 * Inserts each survivor into persons with relationship_to_victim + victim_id
 * FK. Funeral home becomes a row in the new funeral_homes table.
 */
const { v4: uuidv4 } = require('uuid');
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { extractJson } = require('./_ai_router');
const { applyDenyList } = require('./_name_filter');
const { enqueueCascade } = require('../system/_cascade');

const SECRET = 'ingest-now';
const HTTP_TIMEOUT_MS = 12000;
const AI_TIMEOUT_MS = 32000;

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

let _columnsEnsured = false;
async function ensureColumns(db) {
  if (_columnsEnsured) return;
  try {
    await db.raw(
      'ALTER TABLE persons ADD COLUMN IF NOT EXISTS relationship_to_victim VARCHAR(40); ' +
      'ALTER TABLE persons ADD COLUMN IF NOT EXISTS victim_id UUID; ' +
      'ALTER TABLE persons ADD COLUMN IF NOT EXISTS derived_from VARCHAR(60); ' +
      'CREATE TABLE IF NOT EXISTS funeral_homes (' +
      '  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),' +
      '  name VARCHAR(200) NOT NULL,' +
      '  city VARCHAR(100),' +
      '  state VARCHAR(2),' +
      '  phone VARCHAR(40),' +
      '  website VARCHAR(300),' +
      '  metadata JSONB DEFAULT \'{}\',' +
      '  created_at TIMESTAMPTZ DEFAULT NOW(),' +
      '  updated_at TIMESTAMPTZ DEFAULT NOW()' +
      '); ' +
      'CREATE INDEX IF NOT EXISTS idx_funeral_homes_name ON funeral_homes(LOWER(name)); ' +
      'CREATE INDEX IF NOT EXISTS idx_funeral_homes_geo ON funeral_homes(state, city);'
    );
    _columnsEnsured = true;
  } catch (e) {
    console.error('ai-obituary-parser ensureColumns:', e.message);
  }
}

async function loadCseCfg(db) {
  try {
    const row = await db('system_config').where('key', 'google_cse').first();
    let key = process.env.GOOGLE_CSE_API_KEY;
    let cx = process.env.GOOGLE_CSE_ID;
    if (row?.value) {
      const v = typeof row.value === 'string' ? JSON.parse(row.value) : row.value;
      key = v.api_key || key;
      cx = v.cse_id || cx;
    }
    if (!key || !cx) return null;
    return { key, cx };
  } catch (_) { return null; }
}

async function cseSearch(cfg, q) {
  const url = 'https://www.googleapis.com/customsearch/v1?key=' + encodeURIComponent(cfg.key) +
    '&cx=' + encodeURIComponent(cfg.cx) + '&q=' + encodeURIComponent(q) + '&num=5';
  const r = await fetch(url, { signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
  if (!r.ok) return [];
  const j = await r.json();
  return (j.items || []).map(i => ({ link: i.link, title: i.title, snippet: i.snippet || '' }));
}

async function fetchPageText(url) {
  try {
    const r = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; AIP-ObitBot/1.0)',
        'Accept': 'text/html,application/xhtml+xml'
      },
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS),
      redirect: 'follow'
    });
    if (!r.ok) return null;
    const html = await r.text();
    return html
      .replace(/<script[\s\S]*?<\/script>/gi, ' ')
      .replace(/<style[\s\S]*?<\/style>/gi, ' ')
      .replace(/<[^>]+>/g, ' ')
      .replace(/&nbsp;/g, ' ')
      .replace(/&amp;/g, '&')
      .replace(/\s+/g, ' ')
      .trim()
      .slice(0, 9000);
  } catch (_) { return null; }
}

const SYSTEM_PROMPT =
  'You are parsing a published obituary into structured data. Return JSON only - no preamble. ' +
  'Be precise about relationships - a "son" is a child, "brother" is a sibling. ' +
  'Survivors are LIVING family members the obituary names; preceded_in_death are deceased relatives. ' +
  'Use null for unknown values. Never invent names or dates not present in the text.';

function userPromptFor(text, victimName) {
  return (
    'Obituary text for ' + victimName + ':\n"""\n' + text + '\n"""\n\n' +
    'Return JSON of this exact shape:\n' +
    '{"deceased": {"full_name": "...", "age": <int or null>, "dod": "<YYYY-MM-DD or null>", ' +
    '"dob": "<YYYY-MM-DD or null>", "hometown": "<City, ST or null>", "employer": "<string or null>", ' +
    '"military_service": "<branch or null>"},\n' +
    '"survivors": [{"full_name": "...", "relationship": "spouse|child|parent|sibling|grandparent|grandchild|other", ' +
    '"city": "<string or null>", "state": "<2-letter or null>", "deceased_or_living": "living"}],\n' +
    '"preceded_in_death": [{"full_name": "...", "relationship": "..."}],\n' +
    '"funeral_home": {"name": "<string or null>", "city": "<string or null>", "state": "<2-letter or null>", ' +
    '"phone": "<string or null>", "website": "<string or null>"},\n' +
    '"services": [{"type": "visitation|funeral|memorial|graveside|celebration", "date": "<YYYY-MM-DD or null>", ' +
    '"location": "<string or null>"}]}\n\n' +
    'If the text is not actually an obituary, return {"deceased": {"full_name": null}, "survivors": [], "preceded_in_death": [], "funeral_home": {"name": null}, "services": []}.'
  );
}

async function findObituaryText(db, victim) {
  const fullName = victim.full_name || [victim.first_name, victim.last_name].filter(Boolean).join(' ').trim();
  if (!fullName) return { ok: false, reason: 'no_name' };
  const city = victim.city || '';
  const state = victim.state || '';

  // Existing logs from funeral-home-survivors may have already cached an obit URL
  const cfg = await loadCseCfg(db);
  if (!cfg) return { ok: false, reason: 'no_cse_config', name: fullName };

  const q = '"' + fullName + '" obituary ' + (city ? '"' + city + '"' : '') + ' ' + (state ? '"' + state + '"' : '');
  let items = [];
  try { items = await cseSearch(cfg, q.trim()); } catch (_) {}
  if (!items.length) return { ok: false, reason: 'no_results', name: fullName };

  for (const item of items.slice(0, 3)) {
    const url = item.link;
    if (!url) continue;
    const text = await fetchPageText(url);
    if (!text || text.length < 200) continue;
    if (!new RegExp(fullName.split(/\s+/)[0], 'i').test(text)) continue;
    return { ok: true, url, text, name: fullName };
  }
  return { ok: false, reason: 'no_obit_text', name: fullName };
}

async function upsertFuneralHome(db, fh) {
  if (!fh || !fh.name) return null;
  try {
    const existing = await db('funeral_homes')
      .whereRaw('LOWER(name) = LOWER(?)', [fh.name])
      .where('state', fh.state || null)
      .first();
    if (existing) return existing.id;
    const id = uuidv4();
    await db('funeral_homes').insert({
      id,
      name: String(fh.name).slice(0, 200),
      city: fh.city ? String(fh.city).slice(0, 100) : null,
      state: fh.state && String(fh.state).length === 2 ? String(fh.state).toUpperCase() : null,
      phone: fh.phone ? String(fh.phone).slice(0, 40) : null,
      website: fh.website ? String(fh.website).slice(0, 300) : null,
      metadata: JSON.stringify({ source: 'ai-obituary-parser' }),
      created_at: new Date(),
      updated_at: new Date()
    });
    return id;
  } catch (_) { return null; }
}

async function parseOne(db, victim) {
  await ensureColumns(db);
  const found = await findObituaryText(db, victim);
  if (!found.ok) return { ok: false, error: found.reason, victim_id: victim.id };

  const parsed = await extractJson(db, {
    pipeline: 'enrich-ai-obituary-parser',
    systemPrompt: SYSTEM_PROMPT,
    userPrompt: userPromptFor(found.text, found.name),
    provider: 'auto',
    tier: 'auto',
    severityHint: 'fatal',
    timeoutMs: AI_TIMEOUT_MS,
    temperature: 0
  });

  if (!parsed || typeof parsed !== 'object') {
    return { ok: false, error: 'ai_no_parse', victim_id: victim.id, url: found.url };
  }

  const out = { ok: true, victim_id: victim.id, victim_name: found.name, url: found.url, family_inserted: 0, samples: [] };

  // Funeral home
  let funeralHomeId = null;
  if (parsed.funeral_home && parsed.funeral_home.name) {
    funeralHomeId = await upsertFuneralHome(db, parsed.funeral_home);
    out.funeral_home = parsed.funeral_home.name;
  }

  // Survivors → persons
  const survivors = Array.isArray(parsed.survivors) ? parsed.survivors : [];
  for (const s of survivors.slice(0, 20)) {
    const rawName = (s?.full_name || '').toString().trim();
    if (!rawName) continue;
    const safeName = applyDenyList(rawName, found.text);
    if (!safeName) continue;
    const exists = await db('persons')
      .where('victim_id', victim.id)
      .whereRaw('LOWER(full_name) = LOWER(?)', [safeName])
      .first();
    if (exists) continue;
    try {
      const parts = safeName.split(/\s+/);
      const insertRow = {
        id: uuidv4(),
        full_name: safeName,
        first_name: parts[0] || null,
        last_name: parts.length > 1 ? parts[parts.length - 1] : null,
        city: s.city || victim.city || null,
        state: (s.state && String(s.state).length === 2) ? String(s.state).toUpperCase() : (victim.state || null),
        incident_id: victim.incident_id || null,
        victim_id: victim.id,
        relationship_to_victim: String(s.relationship || 'other').toLowerCase().slice(0, 30),
        derived_from: 'ai-obituary-parser',
        victim_verified: false,
        role: 'family_member',
        identity_confidence: 55,
        confidence_score: 55,
        metadata: JSON.stringify({ obit_url: found.url, funeral_home_id: funeralHomeId, source: 'ai-obituary-parser' }),
        created_at: new Date(),
        updated_at: new Date()
      };
      let ret;
      try {
        ret = await db('persons').insert(insertRow).returning(['id']);
      } catch (_) {
        delete insertRow.role;
        ret = await db('persons').insert(insertRow).returning(['id']);
      }
      const newId = ret?.[0]?.id || ret?.[0] || insertRow.id;
      out.family_inserted++;
      if (out.samples.length < 8) out.samples.push({ name: safeName, relationship: insertRow.relationship_to_victim });
      try {
        await enqueueCascade(db, {
          person_id: newId,
          incident_id: victim.incident_id || null,
          trigger_source: 'ai-obituary-parser',
          trigger_field: 'family_added',
          trigger_value: insertRow.relationship_to_victim,
          priority: 5
        });
      } catch (_) {}
    } catch (_) {}
  }

  // Update the deceased victim row with extra fields if we got them
  try {
    const upd = {};
    if (parsed.deceased) {
      if (parsed.deceased.age && Number.isInteger(parsed.deceased.age) && !victim.age) upd.age = parsed.deceased.age;
      if (parsed.deceased.employer && !victim.employer) upd.employer = String(parsed.deceased.employer).slice(0, 200);
      if (Object.keys(upd).length) {
        upd.updated_at = new Date();
        await db('persons').where('id', victim.id).update(upd);
      }
    }
  } catch (_) {}

  // Log result for cross-engine traceability
  try {
    await db('enrichment_logs').insert({
      person_id: victim.id,
      field_name: 'ai-obituary-parser',
      old_value: null,
      new_value: JSON.stringify({ family_inserted: out.family_inserted, funeral_home: out.funeral_home || null }).slice(0, 4000),
      action: 'ai-obituary-parser',
      confidence: 80,
      verified: true,
      meta: JSON.stringify({
        deceased: parsed.deceased || null,
        survivors_count: survivors.length,
        preceded_count: Array.isArray(parsed.preceded_in_death) ? parsed.preceded_in_death.length : 0,
        services: parsed.services || []
      }).slice(0, 4000),
      created_at: new Date()
    });
  } catch (_) {}

  return out;
}

async function batchParse(db, limit = 5) {
  await ensureColumns(db);
  const victims = await db('persons as p')
    .leftJoin('incidents as i', 'p.incident_id', 'i.id')
    .where('p.victim_verified', true)
    .where(function () {
      this.where('i.severity', 'fatal').orWhere('p.injury_severity', 'fatal');
    })
    .whereNotExists(function () {
      this.select('*').from('enrichment_logs as el')
        .whereRaw('el.person_id = p.id')
        .where('el.action', 'ai-obituary-parser');
    })
    .select('p.id', 'p.full_name', 'p.first_name', 'p.last_name', 'p.city', 'p.state', 'p.incident_id', 'p.age', 'p.employer')
    .limit(limit);

  const out = { candidates: victims.length, processed: 0, with_family: 0, total_inserted: 0, samples: [], errors: [] };
  for (const v of victims) {
    try {
      const r = await parseOne(db, v);
      out.processed++;
      if (r.ok && r.family_inserted > 0) {
        out.with_family++;
        out.total_inserted += r.family_inserted;
        if (out.samples.length < 6) out.samples.push({ victim: r.victim_name, family: r.family_inserted, url: r.url });
      } else if (!r.ok) {
        out.errors.push({ victim_id: v.id, error: r.error });
      }
    } catch (e) {
      out.errors.push({ victim_id: v.id, error: e.message?.slice(0, 200) });
      try { await reportError(db, 'enrich-ai-obituary-parser', v.id, e.message); } catch (_) {}
    }
  }
  return out;
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const db = getDb();
  await ensureColumns(db);
  const action = (req.query?.action || 'health').toLowerCase();

  try {
    if (action === 'health') {
      const families = await db('persons').where('derived_from', 'ai-obituary-parser').count('* as c').first().then(r => parseInt(r.c || 0));
      const funeralHomes = await db('funeral_homes').count('* as c').first().then(r => parseInt(r.c || 0));
      return res.json({
        success: true,
        action: 'health',
        ai_obit_family_total: families,
        funeral_homes_total: funeralHomes,
        valid_actions: ['health', 'parse', 'batch'],
        timestamp: new Date().toISOString()
      });
    }
    if (action === 'parse') {
      const personId = req.query?.person_id;
      if (!personId) return res.status(400).json({ error: 'person_id required' });
      const v = await db('persons').where('id', personId).first();
      if (!v) return res.status(404).json({ error: 'person_not_found' });
      const r = await parseOne(db, v);
      return res.json({ success: !!r.ok, ...r, timestamp: new Date().toISOString() });
    }
    if (action === 'batch') {
      const limit = Math.min(15, parseInt(req.query?.limit || '5'));
      const out = await batchParse(db, limit);
      return res.json({ success: true, action: 'batch', ...out, timestamp: new Date().toISOString() });
    }
    res.status(400).json({ error: 'unknown action', valid: ['health', 'parse', 'batch'] });
  } catch (e) {
    try { await reportError(db, 'enrich-ai-obituary-parser', null, e.message); } catch (_) {}
    res.status(500).json({ success: false, error: e.message });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.parseOne = parseOne;
module.exports.batchParse = batchParse;
module.exports.ensureColumns = ensureColumns;
