/**
 * AI NEWS EXTRACTOR — Phase 41 Module 1
 *
 * For each news incident's source article text, call Claude Sonnet 4.6 to
 * extract a STRUCTURED list of victims, drivers, passengers, pedestrians,
 * cyclists. Skips journalists/officials/witnesses by prompt instruction,
 * then double-checks each name through applyDenyList for safety.
 */
const { v4: uuidv4 } = require('uuid');
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { extractJson } = require('./_ai_router');
const { applyDenyList } = require('./_name_filter');
const { enqueueCascade } = require('../system/_cascade');

const SECRET = 'ingest-now';
const TIMEOUT_MS = 30000;

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

function safeParseJson(v) {
  if (!v) return null;
  if (typeof v === 'object') return v;
  try { return JSON.parse(v); } catch (_) { return null; }
}

async function buildArticleText(db, incidentId) {
  const inc = await db('incidents').where('id', incidentId).first();
  if (!inc) return { text: '', incident: null };
  const reports = await db('source_reports')
    .where('incident_id', incidentId)
    .orderBy('created_at', 'desc')
    .limit(8)
    .select('raw_data', 'parsed_data', 'source_type', 'source_reference');
  const chunks = [];
  if (inc.description) chunks.push(String(inc.description));
  if (inc.raw_description) chunks.push(String(inc.raw_description));
  for (const r of reports) {
    const raw = safeParseJson(r.raw_data) || {};
    const it = raw.item || raw;
    if (it.title) chunks.push(String(it.title));
    if (it.description) chunks.push(String(it.description));
    if (it.content) chunks.push(String(it.content));
    if (it.body) chunks.push(String(it.body));
    if (r.source_reference) chunks.push('Source: ' + String(r.source_reference));
  }
  const text = chunks.join('\n\n').replace(/\s+\n/g, '\n').trim();
  return { text: text.slice(0, 6500), incident: inc };
}

function normalizeRole(r) {
  const v = String(r || '').toLowerCase().replace(/[^a-z]/g, '');
  if (!v) return 'victim';
  if (v.includes('motor')) return 'driver';
  if (v.includes('cyc')) return 'cyclist';
  if (v.includes('pedes') || v.includes('walk')) return 'pedestrian';
  if (v.includes('pass')) return 'passenger';
  if (v.includes('driv')) return 'driver';
  return 'victim';
}

function normalizeSeverity(s) {
  const v = String(s || '').toLowerCase();
  if (!v) return null;
  if (v.includes('dec') || v.includes('death') || v === 'fatal' || v.includes('killed')) return 'fatal';
  if (v.includes('crit')) return 'critical';
  if (v.includes('serio')) return 'serious';
  if (v.includes('mod')) return 'moderate';
  if (v.includes('min')) return 'minor';
  return null;
}

const SYSTEM_PROMPT =
  'You are extracting accident victims from a news article. Return JSON only - no preamble, no code fences. ' +
  'ONLY include people directly involved in the accident as victim/driver/passenger/pedestrian/cyclist. ' +
  'NEVER include journalists (bylines), officers/officials/spokespersons, witnesses who only commented, ' +
  'family members merely quoted (UNLESS they were also in the vehicle), or attorneys/spokespeople. ' +
  'If unsure about a person, omit them.';

function userPromptFor({ articleText, city, state, incidentType, severity }) {
  return (
    'Article text:\n"""\n' + articleText + '\n"""\n\n' +
    'Known incident context: city=' + (city || 'unknown') + ', state=' + (state || 'unknown') +
    ', type=' + (incidentType || 'unknown') + ', severity=' + (severity || 'unknown') + '.\n\n' +
    'Return JSON of this exact shape:\n' +
    '{"victims": [{"full_name": "First Last", "role": "victim|driver|passenger|pedestrian|cyclist", ' +
    '"age": <int or null>, "city": "<string or null>", "state": "<2-letter or null>", ' +
    '"employer": "<string or null>", "suspected_severity": "fatal|critical|serious|moderate|minor", ' +
    '"family_mentioned": [{"name": "First Last", "relationship": "spouse|child|parent|sibling|grandparent|grandchild|other"}], ' +
    '"vehicle": {"make": "<string or null>", "model": "<string or null>", "color": "<string or null>", "year": <int or null>}, ' +
    '"license_plate": "<string or null>", "hospital": "<string or null>", "attorney_mentioned": "<string or null>"}]}\n\n' +
    'Use null (not empty string) for unknown values. Return up to 12 people. If no real victims found return {"victims": []}.'
  );
}

async function extractFromIncident(db, incidentId) {
  const { text, incident } = await buildArticleText(db, incidentId);
  if (!incident) return { ok: false, error: 'incident_not_found', incident_id: incidentId };
  if (!text || text.length < 80) {
    return { ok: false, error: 'no_article_text', incident_id: incidentId, text_len: text.length };
  }

  const parsed = await extractJson(db, {
    pipeline: 'enrich-ai-news-extractor',
    systemPrompt: SYSTEM_PROMPT,
    userPrompt: userPromptFor({
      articleText: text,
      city: incident.city,
      state: incident.state,
      incidentType: incident.incident_type,
      severity: incident.severity
    }),
    provider: 'claude',
    tier: 'auto',
    severityHint: incident.severity,
    timeoutMs: TIMEOUT_MS,
    temperature: 0
  });

  if (!parsed || !Array.isArray(parsed.victims)) {
    return { ok: false, error: 'ai_no_parse', incident_id: incidentId };
  }

  const inserted = [];
  const skipped = [];
  for (const v of parsed.victims) {
    const rawName = (v?.full_name || '').toString().trim();
    if (!rawName) { skipped.push({ reason: 'no_name' }); continue; }
    const safeName = applyDenyList(rawName, text);
    if (!safeName) { skipped.push({ reason: 'deny_list', name: rawName }); continue; }

    const exists = await db('persons')
      .where('incident_id', incidentId)
      .whereRaw('LOWER(full_name) = LOWER(?)', [safeName])
      .first();
    if (exists) { skipped.push({ reason: 'dup', name: safeName, id: exists.id }); continue; }

    const role = normalizeRole(v.role);
    const sev = normalizeSeverity(v.suspected_severity);
    const parts = safeName.split(/\s+/);
    const meta = {
      ai_news_extractor: {
        suspected_severity: v.suspected_severity || null,
        family_mentioned: Array.isArray(v.family_mentioned) ? v.family_mentioned.slice(0, 8) : [],
        vehicle: v.vehicle || null,
        license_plate: v.license_plate || null,
        hospital: v.hospital || null,
        attorney_mentioned: v.attorney_mentioned || null,
        extracted_at: new Date().toISOString()
      }
    };

    let newId;
    try {
      const row = {
        id: uuidv4(),
        incident_id: incidentId,
        role,
        is_injured: !(sev === 'fatal' || !sev),
        first_name: parts[0] || null,
        last_name: parts.length > 1 ? parts[parts.length - 1] : null,
        full_name: safeName,
        age: v.age && Number.isInteger(v.age) ? v.age : null,
        city: v.city || incident.city || null,
        state: (v.state && String(v.state).length === 2) ? String(v.state).toUpperCase() : (incident.state || null),
        employer: v.employer || null,
        injury_severity: sev,
        contact_status: 'not_contacted',
        confidence_score: 75,
        identity_confidence: 70,
        victim_verified: true,
        derived_from: 'ai-news-extractor',
        metadata: JSON.stringify(meta),
        created_at: new Date(),
        updated_at: new Date()
      };
      const ret = await db('persons').insert(row).returning(['id']);
      newId = ret?.[0]?.id || ret?.[0] || row.id;
    } catch (e1) {
      try {
        const row2 = {
          id: uuidv4(),
          incident_id: incidentId,
          role,
          is_injured: !(sev === 'fatal' || !sev),
          first_name: parts[0] || null,
          last_name: parts.length > 1 ? parts[parts.length - 1] : null,
          full_name: safeName,
          age: v.age && Number.isInteger(v.age) ? v.age : null,
          city: v.city || incident.city || null,
          state: (v.state && String(v.state).length === 2) ? String(v.state).toUpperCase() : (incident.state || null),
          injury_severity: sev,
          contact_status: 'not_contacted',
          confidence_score: 75,
          metadata: JSON.stringify(meta),
          created_at: new Date(),
          updated_at: new Date()
        };
        const ret2 = await db('persons').insert(row2).returning(['id']);
        newId = ret2?.[0]?.id || ret2?.[0] || row2.id;
      } catch (e2) {
        skipped.push({ reason: 'insert_error', name: safeName, error: e2.message?.slice(0, 120) });
        continue;
      }
    }

    inserted.push({ id: newId, name: safeName, role, severity: sev, age: v.age || null });

    try {
      await enqueueCascade(db, {
        person_id: newId,
        incident_id: incidentId,
        trigger_source: 'ai-news-extractor',
        trigger_field: 'victim_added',
        trigger_value: role,
        priority: sev === 'fatal' ? 1 : 4
      });
    } catch (_) {}

    if (Array.isArray(v.family_mentioned)) {
      for (const fam of v.family_mentioned.slice(0, 8)) {
        const famName = (fam?.name || '').toString().trim();
        if (!famName) continue;
        const safeFam = applyDenyList(famName, text);
        if (!safeFam) continue;
        const famExist = await db('persons')
          .where('incident_id', incidentId)
          .whereRaw('LOWER(full_name) = LOWER(?)', [safeFam])
          .first();
        if (famExist) continue;
        try {
          const famParts = safeFam.split(/\s+/);
          await db('persons').insert({
            id: uuidv4(),
            incident_id: incidentId,
            role: 'family_member',
            is_injured: false,
            first_name: famParts[0] || null,
            last_name: famParts.length > 1 ? famParts[famParts.length - 1] : null,
            full_name: safeFam,
            city: incident.city || null,
            state: incident.state || null,
            victim_id: newId,
            relationship_to_victim: String(fam.relationship || 'other').toLowerCase().slice(0, 30),
            derived_from: 'ai-news-extractor',
            victim_verified: false,
            confidence_score: 50,
            identity_confidence: 40,
            created_at: new Date(),
            updated_at: new Date()
          });
        } catch (_) {}
      }
    }
  }

  try {
    await db('enrichment_logs').insert({
      person_id: null,
      field_name: 'ai-news-extractor:incident',
      old_value: null,
      new_value: JSON.stringify({ inserted_count: inserted.length, names: inserted.map(i => i.name) }).slice(0, 4000),
      action: 'ai-news-extractor',
      confidence: 75,
      verified: true,
      meta: JSON.stringify({ incident_id: incidentId, inserted, skipped }).slice(0, 4000),
      created_at: new Date()
    });
  } catch (_) {}

  return {
    ok: true,
    incident_id: incidentId,
    article_chars: text.length,
    raw_count: parsed.victims.length,
    inserted_count: inserted.length,
    inserted,
    skipped_count: skipped.length
  };
}

async function batchExtract(db, limit = 10) {
  const rows = await db('incidents as i')
    .leftJoin('persons as p', function () {
      this.on('p.incident_id', 'i.id').andOn(db.raw("p.derived_from = 'ai-news-extractor'"));
    })
    .whereNull('p.id')
    .whereIn('i.incident_type', ['car_accident', 'motorcycle_accident', 'truck_accident', 'pedestrian', 'bicycle', 'other'])
    .where('i.created_at', '>', db.raw("NOW() - INTERVAL '21 days'"))
    .whereExists(function () {
      this.select('*').from('source_reports as sr').whereRaw('sr.incident_id = i.id');
    })
    .orderBy('i.severity', 'desc')
    .orderBy('i.created_at', 'desc')
    .limit(limit)
    .select('i.id');

  const out = {
    candidates: rows.length,
    processed: 0,
    incidents_with_extracts: 0,
    persons_inserted: 0,
    samples: [],
    errors: []
  };

  for (const r of rows) {
    try {
      const res = await extractFromIncident(db, r.id);
      out.processed++;
      if (res.ok) {
        if (res.inserted_count > 0) out.incidents_with_extracts++;
        out.persons_inserted += res.inserted_count;
        if (out.samples.length < 6 && res.inserted_count > 0) {
          out.samples.push({
            incident_id: r.id,
            inserted: res.inserted_count,
            names: res.inserted.map(i => i.name)
          });
        }
      } else {
        out.errors.push({ incident_id: r.id, error: res.error });
      }
    } catch (e) {
      out.errors.push({ incident_id: r.id, error: e.message?.slice(0, 200) });
      try { await reportError(db, 'enrich-ai-news-extractor', r.id, e.message); } catch (_) {}
    }
  }
  return out;
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const db = getDb();
  const action = (req.query?.action || 'health').toLowerCase();

  try {
    if (action === 'health') {
      const totalInserted = await db('persons').where('derived_from', 'ai-news-extractor').count('* as c').first().then(r => parseInt(r.c || 0));
      const last24h = await db('persons').where('derived_from', 'ai-news-extractor').where('created_at', '>', db.raw("NOW() - INTERVAL '24 hours'")).count('* as c').first().then(r => parseInt(r.c || 0));
      return res.json({
        success: true,
        action: 'health',
        ai_extracted_persons_total: totalInserted,
        ai_extracted_persons_24h: last24h,
        valid_actions: ['health', 'extract', 'batch'],
        timestamp: new Date().toISOString()
      });
    }
    if (action === 'extract') {
      const incidentId = req.query?.incident_id;
      if (!incidentId) return res.status(400).json({ error: 'incident_id required' });
      const r = await extractFromIncident(db, incidentId);
      return res.json({ success: !!r.ok, ...r, timestamp: new Date().toISOString() });
    }
    if (action === 'batch') {
      const limit = Math.min(30, parseInt(req.query?.limit || '10'));
      const out = await batchExtract(db, limit);
      return res.json({ success: true, action: 'batch', ...out, timestamp: new Date().toISOString() });
    }
    res.status(400).json({ error: 'unknown action', valid: ['health', 'extract', 'batch'] });
  } catch (e) {
    try { await reportError(db, 'enrich-ai-news-extractor', null, e.message); } catch (_) {}
    res.status(500).json({ success: false, error: e.message });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.extractFromIncident = extractFromIncident;
module.exports.batchExtract = batchExtract;
