/**
 * Backfill Nameless Incidents — Phase 20 #1
 *
 * Per CORE_INTENT.md: Person extraction recall stuck at 10% because TomTom/Waze/
 * OpenData are name-free sources. This engine recovers names for incidents that
 * have aged past the live-news window (>6h) and still have no person attached.
 *
 * Strategy: for every incident WHERE qualification_state='pending'
 * AND discovered_at < NOW() - INTERVAL '6 hours' AND no persons exist:
 *   1. Google News web search ("[city] [incident_type] [date]")
 *   2. Obituary search via legacy.com (fatal incidents only)
 *   3. CourtListener filing search ("[city] motor vehicle" within 30d)
 * Any name found → INSERT person + emit cascade.
 *
 * GET /api/v1/system/backfill-nameless?secret=ingest-now&limit=50&dry=false
 * Cron: folded into the 5-min qualify slot (no new vercel.json cron required)
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('./_errors');
const { dedupCache } = require('../../_cache');
const { enqueueCascade } = require('./_cascade');
const { normalizePerson } = require('../../_schema');
const { extractJson } = require('../enrich/_ai_router');
const { trackApiCall } = require('./cost');
const { logChange } = require('./changelog');

const GOOGLE_CSE_KEY = process.env.GOOGLE_CSE_KEY;
const GOOGLE_CSE_ID = process.env.GOOGLE_CSE_ID;
const COURTLISTENER_TOKEN = process.env.COURTLISTENER_TOKEN;

async function googleNewsSearch(db, incident) {
  if (!GOOGLE_CSE_KEY || !GOOGLE_CSE_ID) return null;
  const dateStr = incident.occurred_at
    ? new Date(incident.occurred_at).toISOString().split('T')[0]
    : new Date(incident.discovered_at).toISOString().split('T')[0];
  const q = `"${incident.city}" "${incident.state}" ${incident.incident_type || 'crash'} ${dateStr} victim identified name`;
  const url = `https://www.googleapis.com/customsearch/v1?key=${GOOGLE_CSE_KEY}&cx=${GOOGLE_CSE_ID}&q=${encodeURIComponent(q)}&num=5`;
  try {
    const r = await fetch(url, { signal: AbortSignal.timeout(10000) });
    if (!r.ok) return null;
    const data = await r.json();
    await trackApiCall(db, 'backfill-nameless', 'google_cse', 0, 0, true);
    return data.items || [];
  } catch (_) { return null; }
}

async function extractNameFromSnippets(db, incident, items) {
  if (!items || !items.length) return null;
  const snippets = items.slice(0, 5).map((it, i) =>
    `[${i+1}] ${it.title}\n${it.snippet || ''}\n${it.link}`).join('\n\n').substring(0, 4000);
  const prompt = `News snippets below mention a ${incident.incident_type || 'crash'} in ${incident.city}, ${incident.state} on ${incident.occurred_at || incident.discovered_at}.

Snippets:
"""
${snippets}
"""

Extract every named victim or person involved in THIS specific accident (not unrelated stories).
Return JSON only:
{
  "matches": [
    { "full_name": "string", "age": number|null, "role": "driver|passenger|pedestrian|cyclist",
      "is_injured": true|false, "injury_severity": "fatal|incapacitating|non_incapacitating|none|unknown",
      "match_confidence": 0-100, "source_url": "string|null", "match_reason": "brief" }
  ]
}
Empty matches:[] if nothing matches city+date. match_confidence>=70 only.`;
  return await extractJson(db, {
    pipeline: 'backfill-nameless',
    systemPrompt: 'You match news mentions to a specific accident by city+date+type. Be conservative — only return high-confidence matches.',
    userPrompt: prompt,
    tier: 'auto',
    severityHint: incident.severity,
    timeoutMs: 18000,
  });
}

async function obituarySearch(db, incident) {
  if (incident.severity !== 'fatal' && (incident.fatalities_count || 0) === 0) return null;
  if (!incident.city || !incident.state) return null;
  const url = `https://www.legacy.com/us/obituaries/search?firstName=&lastName=&keyword=&location=${encodeURIComponent(incident.city + ', ' + incident.state)}&limit=20`;
  try {
    const r = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; AIP-Backfill/1.0)' },
      signal: AbortSignal.timeout(10000)
    });
    if (!r.ok) return null;
    const html = (await r.text()).substring(0, 60000);
    const text = html.replace(/<script[\s\S]*?<\/script>/gi, '').replace(/<style[\s\S]*?<\/style>/gi, '')
      .replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').substring(0, 4500);
    const incDate = incident.occurred_at ? new Date(incident.occurred_at).toDateString() : 'recent';
    const prompt = `Match obituaries to this fatal accident:
City: ${incident.city}, ${incident.state}. Date: ${incDate}. Description: ${(incident.description || '').substring(0,200)}

Obituary listings:
"""
${text}
"""

JSON only:
{ "matches":[{ "full_name":"", "age":null, "city_residence":"", "death_date":"ISO|null",
  "match_confidence":0-100, "match_reason":"" }] }
Confidence>=60 only. Empty if no plausible match.`;
    return await extractJson(db, {
      pipeline: 'backfill-nameless',
      systemPrompt: 'Match obituaries to fatal crashes by city+date proximity.',
      userPrompt: prompt,
      tier: 'premium',
      severityHint: 'fatal',
      timeoutMs: 18000,
    });
  } catch (_) { return null; }
}

async function courtListenerSearch(db, incident) {
  if (!incident.city || !incident.state) return null;
  const headers = { 'Accept': 'application/json', 'User-Agent': 'AIP/1.0' };
  if (COURTLISTENER_TOKEN) headers['Authorization'] = `Token ${COURTLISTENER_TOKEN}`;
  const incDate = incident.occurred_at ? new Date(incident.occurred_at) : new Date(incident.discovered_at);
  const filed_after = new Date(incDate.getTime() - 7 * 86400000).toISOString().split('T')[0];
  const filed_before = new Date(incDate.getTime() + 90 * 86400000).toISOString().split('T')[0];
  const q = `(motor vehicle OR automobile OR pedestrian OR wrongful death) ${incident.city}`;
  try {
    const url = `https://www.courtlistener.com/api/rest/v3/search/?q=${encodeURIComponent(q)}&type=r&order_by=dateFiled+desc&filed_after=${filed_after}&filed_before=${filed_before}`;
    const r = await fetch(url, { headers, signal: AbortSignal.timeout(12000) });
    if (!r.ok) return null;
    const data = await r.json();
    await trackApiCall(db, 'backfill-nameless', 'courtlistener', 0, 0, true);
    const cases = (data.results || []).slice(0, 10);
    const plaintiffs = [];
    for (const c of cases) {
      const m = (c.caseName || '').match(/^([A-Z][A-Za-z'\-\.]+(?:\s+[A-Z][A-Za-z'\-\.]+){1,3})\s+v\.?\s/i);
      if (m) plaintiffs.push({
        full_name: m[1].trim(),
        case_name: c.caseName,
        court: c.court,
        date_filed: c.dateFiled,
        case_id: c.id,
        match_confidence: 65,
        match_reason: 'plaintiff in PI suit filed within 90d of accident'
      });
    }
    return { matches: plaintiffs };
  } catch (_) { return null; }
}

async function insertRecoveredPerson(db, incident, match, source, dsId, dryRun) {
  if (!match.full_name) return null;
  const exists = await db('persons').where('incident_id', incident.id)
    .whereRaw('LOWER(full_name) = LOWER(?)', [match.full_name.trim()]).first();
  if (exists) return { skipped: 'already_exists' };

  if (dryRun) return { dry: true, name: match.full_name, source };

  const person = normalizePerson({
    incident_id: incident.id,
    full_name: match.full_name,
    age: match.age || null,
    role: match.role || 'driver',
    is_injured: match.is_injured !== false,
    injury_severity: match.injury_severity || (incident.severity === 'fatal' ? 'fatal' : 'unknown'),
    city: match.city_residence || incident.city,
    state: incident.state,
    contact_status: 'not_contacted',
    has_attorney: source === 'courtlistener' ? true : null,
    confidence_score: match.match_confidence || 70,
    metadata: {
      backfill_source: source,
      match_reason: match.match_reason,
      source_url: match.source_url || match.case_name || null,
      recovered_at: new Date().toISOString(),
      backfill_run: true
    }
  });
  person.id = uuidv4();
  await db('persons').insert(person);

  await db('source_reports').insert({
    id: uuidv4(),
    incident_id: incident.id,
    data_source_id: dsId,
    source_type: source === 'courtlistener' ? 'court_records' : (source === 'obituary' ? 'obituary' : 'newsapi'),
    source_reference: `backfill:${source}:${incident.id}:${person.id}`,
    raw_data: JSON.stringify(match),
    parsed_data: JSON.stringify(match),
    contributed_fields: ['full_name', 'role'],
    confidence: match.match_confidence || 70,
    is_verified: false,
    fetched_at: new Date(), processed_at: new Date(), created_at: new Date()
  }).catch(() => {});

  await enqueueCascade(db, {
    person_id: person.id,
    incident_id: incident.id,
    trigger_source: `backfill_${source}`,
    trigger_field: 'full_name',
    trigger_value: match.full_name,
    priority: incident.severity === 'fatal' ? 9 : 6
  }).catch(() => {});

  return { inserted: true, person_id: person.id, name: match.full_name, source };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const dryRun = req.query?.dry === 'true' || req.query?.dry === '1';
  const limit = Math.min(parseInt(req.query?.limit) || 30, 100);
  const startTime = Date.now();
  const TIME_BUDGET = 50000;

  const results = {
    candidates: 0, processed: 0, names_recovered: 0, persons_added: 0,
    by_source: { news: 0, obituary: 0, courtlistener: 0 },
    samples: [], errors: []
  };

  try {
    let ds = await db('data_sources').where('name', 'Backfill Nameless').first();
    if (!ds) {
      const dsId = uuidv4();
      await db('data_sources').insert({
        id: dsId, name: 'Backfill Nameless', type: 'public_records',
        provider: 'multi-source', api_endpoint: 'google_cse + legacy + courtlistener',
        is_active: true, last_polled_at: new Date(),
        created_at: new Date(), updated_at: new Date()
      });
      ds = { id: dsId };
    }

    const { rows: candidates } = await db.raw(`
      SELECT i.id, i.city, i.state, i.severity, i.incident_type, i.fatalities_count,
             i.occurred_at, i.discovered_at, i.description
      FROM incidents i
      LEFT JOIN persons p ON p.incident_id = i.id
      WHERE i.qualification_state = 'pending'
        AND i.discovered_at < NOW() - INTERVAL '6 hours'
        AND i.discovered_at > NOW() - INTERVAL '30 days'
        AND i.city IS NOT NULL
        AND i.state IS NOT NULL
        AND p.id IS NULL
      ORDER BY
        (CASE i.severity WHEN 'fatal' THEN 0 WHEN 'critical' THEN 1 WHEN 'serious' THEN 2 ELSE 5 END),
        i.discovered_at DESC
      LIMIT ?
    `, [limit]);
    results.candidates = candidates.length;

    for (const inc of candidates) {
      if (Date.now() - startTime > TIME_BUDGET) break;
      const cacheKey = `bfn:${inc.id}`;
      if (dedupCache.has(cacheKey)) { results.processed++; continue; }
      dedupCache.set(cacheKey, 1);

      let recovered = false;
      try {
        const items = await googleNewsSearch(db, inc);
        if (items && items.length) {
          const parsed = await extractNameFromSnippets(db, inc, items);
          for (const m of (parsed?.matches || [])) {
            if ((m.match_confidence || 0) < 70) continue;
            const r = await insertRecoveredPerson(db, inc, m, 'news', ds.id, dryRun);
            if (r?.inserted || r?.dry) {
              results.persons_added += r.inserted ? 1 : 0;
              results.by_source.news++;
              recovered = true;
              if (results.samples.length < 10) results.samples.push({ incident_id: inc.id, name: m.full_name, source: 'news' });
            }
          }
        }

        if (!recovered && (inc.severity === 'fatal' || (inc.fatalities_count || 0) > 0)) {
          const obit = await obituarySearch(db, inc);
          for (const m of (obit?.matches || [])) {
            if ((m.match_confidence || 0) < 60) continue;
            const r = await insertRecoveredPerson(db, inc, { ...m, role: 'driver', injury_severity: 'fatal' }, 'obituary', ds.id, dryRun);
            if (r?.inserted || r?.dry) {
              results.persons_added += r.inserted ? 1 : 0;
              results.by_source.obituary++;
              recovered = true;
              if (results.samples.length < 10) results.samples.push({ incident_id: inc.id, name: m.full_name, source: 'obituary' });
            }
          }
        }

        if (!recovered) {
          const cl = await courtListenerSearch(db, inc);
          for (const m of (cl?.matches || [])) {
            const r = await insertRecoveredPerson(db, inc, m, 'courtlistener', ds.id, dryRun);
            if (r?.inserted || r?.dry) {
              results.persons_added += r.inserted ? 1 : 0;
              results.by_source.courtlistener++;
              recovered = true;
              if (results.samples.length < 10) results.samples.push({ incident_id: inc.id, name: m.full_name, source: 'courtlistener' });
            }
          }
        }

        if (recovered) results.names_recovered++;
        results.processed++;
      } catch (e) {
        results.errors.push(`${inc.id}: ${e.message}`);
        await reportError(db, 'backfill-nameless', inc.id, e.message);
      }
    }

    if (!dryRun) {
      await db('data_sources').where('id', ds.id).update({
        last_polled_at: new Date(), last_success_at: new Date(), updated_at: new Date()
      });
      await logChange(db, {
        kind: 'pipeline',
        title: `Backfill nameless: ${results.persons_added} names recovered`,
        summary: `Processed ${results.processed}/${results.candidates}, recovered ${results.names_recovered} (news=${results.by_source.news}, obit=${results.by_source.obituary}, courts=${results.by_source.courtlistener})`,
        meta: results
      }).catch(() => {});
    }

    res.json({
      success: true,
      message: `Backfill: ${results.persons_added} persons added across ${results.names_recovered} incidents${dryRun ? ' (DRY RUN)' : ''}`,
      dry_run: dryRun,
      ...results,
      duration_ms: Date.now() - startTime,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'backfill-nameless', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
