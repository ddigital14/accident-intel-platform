/**
 * Obituary Backfill — Phase 20 #5
 *
 * Refines obituaries.js for backfill mode. Targets fatal incidents WHERE
 * no person.full_name exists. Uses GPT-4o through _ai_router (premium tier
 * for fatal cases) to match obit names to incident details.
 *
 * Differs from /ingest/obituaries.js (FRESH fatals within 7 days). This
 * sweeps the full 60-day backlog of nameless fatals.
 *
 * GET /api/v1/enrich/obit-backfill?secret=ingest-now&limit=20&dry=false
 * Cron: every 30min (folded into existing slot — no new cron needed)
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('../system/_errors');
const { dedupCache } = require('../../_cache');
const { enqueueCascade } = require('../system/_cascade');
const { normalizePerson } = require('../../_schema');
const { extractJson } = require('./_ai_router');
const { trackApiCall } = require('../system/cost');
const { logChange } = require('../system/changelog');

const LEGACY_BASE = 'https://www.legacy.com/us/obituaries/search';

function buildLegacyUrl(city, state) {
  return `${LEGACY_BASE}?firstName=&lastName=&keyword=&location=${encodeURIComponent(city + ', ' + state)}&limit=30`;
}

async function fetchObitHtml(db, city, state) {
  try {
    const url = buildLegacyUrl(city, state);
    const r = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; AIP-Backfill/1.0)' },
      signal: AbortSignal.timeout(10000)
    });
    if (!r.ok) return null;
    await trackApiCall(db, 'obit-backfill', 'legacy_com', 0, 0, true);
    return (await r.text()).substring(0, 80000);
  } catch (_) { return null; }
}

async function matchObitsToIncident(db, html, incident) {
  if (!html) return null;
  const text = html
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .substring(0, 5000);

  const incDate = incident.occurred_at ? new Date(incident.occurred_at).toISOString().split('T')[0] : null;
  const incDay = incDate ? new Date(incDate).toDateString() : 'recent';
  const desc = (incident.description || '').substring(0, 250);

  const prompt = `Match obituaries from this listing to a SPECIFIC fatal accident:

ACCIDENT:
- Location: ${incident.city}, ${incident.state}
- Date: ${incDay}
- Description: ${desc}
- Fatalities: ${incident.fatalities_count || 1}

OBITUARY LISTING TEXT:
"""
${text}
"""

Score each candidate by:
1. Death date within 14 days of accident date (mandatory >=60% confidence)
2. City match (residence or death)
3. Keywords matching "crash", "accident", "auto", "motor vehicle", "killed", "died unexpectedly"
4. Age plausibility for an accident victim

Return JSON only:
{
  "matches": [
    {
      "full_name": "string",
      "age": number|null,
      "city_residence": "string|null",
      "death_date": "ISO|null",
      "obit_excerpt": "1-2 sentence quote from obit text|null",
      "match_confidence": 0-100,
      "match_reason": "brief"
    }
  ]
}
Empty matches:[] if nothing plausible. confidence>=60 only.`;

  return await extractJson(db, {
    pipeline: 'obit-backfill',
    systemPrompt: 'Match obituaries to fatal car/pedestrian/motorcycle crashes. Death date proximity (<=14d) is mandatory. Be conservative.',
    userPrompt: prompt,
    tier: 'premium',
    severityHint: 'fatal',
    timeoutMs: 22000,
  });
}

async function insertVictim(db, incident, match, dsId, dryRun) {
  if (!match.full_name) return null;
  const exists = await db('persons').where('incident_id', incident.id)
    .whereRaw('LOWER(full_name) = LOWER(?)', [match.full_name.trim()]).first();
  if (exists) return { skipped: 'exists' };

  if (dryRun) return { dry: true, name: match.full_name };

  const person = normalizePerson({
    incident_id: incident.id,
    full_name: match.full_name,
    age: match.age || null,
    role: 'driver',
    is_injured: true,
    injury_severity: 'fatal',
    city: match.city_residence || incident.city,
    state: incident.state,
    contact_status: 'not_contacted',
    confidence_score: match.match_confidence || 70,
    metadata: {
      backfill_source: 'obit-backfill',
      death_date: match.death_date,
      obit_excerpt: match.obit_excerpt,
      match_reason: match.match_reason,
      recovered_at: new Date().toISOString()
    }
  });
  person.id = uuidv4();
  await db('persons').insert(person);

  await db('source_reports').insert({
    id: uuidv4(),
    incident_id: incident.id,
    data_source_id: dsId,
    source_type: 'obituary',
    source_reference: `obit-backfill:${incident.id}:${person.id}`,
    raw_data: JSON.stringify(match),
    parsed_data: JSON.stringify(match),
    contributed_fields: ['full_name', 'fatal_confirmation', 'age'],
    confidence: match.match_confidence || 70,
    is_verified: false,
    fetched_at: new Date(), processed_at: new Date(), created_at: new Date()
  }).catch(() => {});

  await enqueueCascade(db, {
    person_id: person.id,
    incident_id: incident.id,
    trigger_source: 'obit_backfill',
    trigger_field: 'full_name',
    trigger_value: match.full_name,
    priority: 9
  }).catch(() => {});

  // Phase 21 Wire #3: chain to family-tree extraction on the obit text — when
  // we find "survived by [Name]" passages, enroll each relative + queue Trestle
  // Reverse Phone via cascade. The relative often holds the contact info.
  try {
    const ft = require('./family-tree');
    // Stitch: insert a synthetic obit source_report so family-tree.processDeceased can find the text
    const obitText = match.obit_excerpt || JSON.stringify(match);
    if (obitText && obitText.length > 80) {
      const r = await ft.extractRelatives(db, obitText, match.full_name);
      if (r?.ok && r.relatives?.length) {
        const { v4: uuidv4 } = require('uuid');
        const { normalizePerson } = require('../../_schema');
        for (const rel of r.relatives) {
          if (!rel.name) continue;
          const exists = await db('persons').where('incident_id', incident.id)
            .whereRaw('LOWER(full_name) = LOWER(?)', [rel.name]).first();
          if (exists) continue;
          const cleaned = normalizePerson({
            incident_id: incident.id,
            role: 'other',
            first_name: (rel.name || '').split(' ')[0],
            last_name: (rel.name || '').split(' ').slice(-1)[0],
            full_name: rel.name,
            age: rel.age || null,
            state: incident.state,
            city: (rel.city || '').split(',')[0]?.trim() || null,
            contact_status: 'not_contacted',
            confidence_score: 70,
            metadata: { source: 'obit_backfill_relative', relation: rel.relation, deceased_relative: !!rel.deceased }
          });
          cleaned.id = uuidv4();
          cleaned.related_to_person_id = person.id;
          cleaned.relation_type = rel.relation;
          try {
            await db('persons').insert(cleaned);
            await enqueueCascade(db, {
              person_id: cleaned.id,
              incident_id: incident.id,
              trigger_source: 'obit_backfill_relative',
              trigger_field: 'full_name',
              trigger_value: rel.name,
              priority: 7
            }).catch(()=>{});
          } catch (_) {}
        }
      }
    }
  } catch (_) { /* non-fatal */ }

  return { inserted: true, person_id: person.id, name: match.full_name };
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
  const limit = Math.min(parseInt(req.query?.limit) || 15, 40);
  const startTime = Date.now();
  const TIME_BUDGET = 50000;

  const results = {
    candidates: 0, searched: 0, matches_found: 0,
    persons_added: 0, samples: [], errors: []
  };

  try {
    let ds = await db('data_sources').where('name', 'Obituary Backfill').first();
    if (!ds) {
      const dsId = uuidv4();
      await db('data_sources').insert({
        id: dsId, name: 'Obituary Backfill', type: 'public_records',
        provider: 'legacy.com', api_endpoint: 'legacy.com search backfill',
        is_active: true, last_polled_at: new Date(),
        created_at: new Date(), updated_at: new Date()
      });
      ds = { id: dsId };
    }

    const { rows: candidates } = await db.raw(`
      SELECT i.id, i.city, i.state, i.severity, i.fatalities_count,
             i.occurred_at, i.discovered_at, i.description
      FROM incidents i
      LEFT JOIN persons p ON p.incident_id = i.id AND p.full_name IS NOT NULL
      WHERE (i.severity = 'fatal' OR i.fatalities_count > 0)
        AND i.city IS NOT NULL AND i.state IS NOT NULL
        AND i.discovered_at > NOW() - INTERVAL '60 days'
        AND p.id IS NULL
      ORDER BY i.discovered_at DESC
      LIMIT ?
    `, [limit]);
    results.candidates = candidates.length;

    for (const inc of candidates) {
      if (Date.now() - startTime > TIME_BUDGET) break;
      const cacheKey = `obitbf:${inc.id}`;
      if (dedupCache.has(cacheKey)) continue;
      dedupCache.set(cacheKey, 1);

      const prior = await db('source_reports')
        .where('incident_id', inc.id)
        .whereIn('source_type', ['obituary'])
        .whereRaw("source_reference LIKE 'obit-backfill:%'")
        .first();
      if (prior) continue;

      try {
        const html = await fetchObitHtml(db, inc.city, inc.state);
        if (!html) continue;
        results.searched++;

        const parsed = await matchObitsToIncident(db, html, inc);
        if (!parsed?.matches?.length) continue;

        for (const m of parsed.matches) {
          if ((m.match_confidence || 0) < 60) continue;
          if (m.death_date && inc.occurred_at) {
            const dd = new Date(m.death_date).getTime();
            const id = new Date(inc.occurred_at).getTime();
            const diffDays = Math.abs(dd - id) / 86400000;
            if (diffDays > 14) continue;
          }
          const r = await insertVictim(db, inc, m, ds.id, dryRun);
          if (r?.inserted || r?.dry) {
            results.matches_found++;
            results.persons_added += r.inserted ? 1 : 0;
            if (results.samples.length < 10) {
              results.samples.push({
                incident_id: inc.id, city: inc.city, state: inc.state,
                name: m.full_name, age: m.age,
                confidence: m.match_confidence
              });
            }
          }
        }
      } catch (e) {
        results.errors.push(`${inc.id}: ${e.message}`);
        await reportError(db, 'obit-backfill', inc.id, e.message);
      }
    }

    if (!dryRun) {
      await db('data_sources').where('id', ds.id).update({
        last_polled_at: new Date(), last_success_at: new Date(), updated_at: new Date()
      });
      if (results.persons_added > 0) {
        await logChange(db, {
          kind: 'pipeline',
          title: `Obit backfill: ${results.persons_added} fatal victims named`,
          summary: `Searched ${results.searched}/${results.candidates} fatal nameless incidents, matched ${results.matches_found} obituaries`,
          meta: results
        }).catch(() => {});
      }
    }

    res.json({
      success: true,
      message: `Obit backfill: ${results.persons_added} fatal victims named (${results.searched}/${results.candidates})${dryRun ? ' (DRY)' : ''}`,
      dry_run: dryRun,
      ...results,
      duration_ms: Date.now() - startTime,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'obit-backfill', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
