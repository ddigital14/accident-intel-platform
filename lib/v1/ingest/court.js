/**
 * Court Records / Attorney Check Pipeline
 * Cron: every 6 hours
 * GET /api/v1/ingest/court?secret=ingest-now
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('../system/_errors');
const { dedupCache } = require('../../_cache');
const { extractJson } = require('../enrich/_ai_router');

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

async function searchWebForAttorney(personName, city, state) {
  if (!personName) return null;
  const q = `${personName} ${state || ''} accident attorney represented`.replace(/\s+/g, ' ').trim();
  const url = `https://html.duckduckgo.com/html/?q=${encodeURIComponent(q)}`;
  try {
    const resp = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; AIP/1.0)' },
      signal: AbortSignal.timeout(15000)
    });
    if (!resp.ok) return null;
    return (await resp.text()).substring(0, 50000);
  } catch (_) { return null; }
}

async function classifyAttorneyMentions(db, html, personName) {
  if (!html) return null;
  const text = html
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .substring(0, 4000);

  const prompt = `Search results below mention "${personName}". Determine if this person currently has legal representation in any pending personal-injury / accident matter.

Search snippets:
"""
${text}
"""

Return JSON only:
{
  "has_attorney": true|false,
  "confidence": 0-100,
  "attorney_name": "string|null",
  "attorney_firm": "string|null",
  "evidence": "1-line evidence quote|null"
}`;

  // Attorney detection drives lead value — premium tier for accuracy
  return await extractJson(db, {
    pipeline: 'court',
    systemPrompt: 'You are a legal-records analyst. Answer in strict JSON. Only mark has_attorney=true with confidence>70 if there is direct evidence of representation, NOT just general attorney advertising.',
    userPrompt: prompt,
    tier: 'premium',
    timeoutMs: 22000,
  });
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const results = { checked: 0, has_attorney: 0, no_attorney: 0, indeterminate: 0, errors: [] };
  try {
    let courtDs = await db('data_sources').where('name', 'Court Records / Attorney Check').first();
    if (!courtDs) {
      const dsId = uuidv4();
      await db('data_sources').insert({
        id: dsId, name: 'Court Records / Attorney Check', type: 'public_records',
        api_endpoint: 'duckduckgo + GPT-4o classifier',
        is_active: true, last_polled_at: new Date(),
        created_at: new Date(), updated_at: new Date()
      });
      courtDs = { id: dsId };
    }

    const candidates = await db('persons')
      .whereNotNull('full_name')
      .whereNull('has_attorney')
      .where('created_at', '>', new Date(Date.now() - 7 * 86400000))
      .select('id', 'full_name', 'first_name', 'last_name', 'city', 'state', 'incident_id')
      .limit(20);

    for (const p of candidates) {
      try {
        const cacheKey = `court:${p.id}`;
        if (dedupCache.has(cacheKey)) continue;
        dedupCache.set(cacheKey, 1);

        const name = p.full_name || `${p.first_name || ''} ${p.last_name || ''}`.trim();
        if (!name || name.length < 4) continue;

        let st = p.state;
        if (!st) {
          const inc = await db('incidents').where('id', p.incident_id).first();
          st = inc?.state;
        }

        const html = await searchWebForAttorney(name, p.city, st);
        if (!html) { results.errors.push(`fetch failed: ${name}`); continue; }

        const verdict = await classifyAttorneyMentions(db, html, name);
        results.checked++;

        if (!verdict) { results.indeterminate++; continue; }

        const update = { updated_at: new Date() };
        if (verdict.has_attorney === true && verdict.confidence >= 70) {
          update.has_attorney = true;
          update.attorney_name = verdict.attorney_name || null;
          update.attorney_firm = verdict.attorney_firm || null;
          update.contact_status = 'has_attorney';
          results.has_attorney++;

          // Cross-engine wire: bubble attorney_firm up to the incident so
          // dashboards + other engines see it without joining persons.
          if (verdict.attorney_firm && p.incident_id) {
            try {
              await db('incidents').where('id', p.incident_id).update({
                tags: db.raw(`array_append(COALESCE(tags, ARRAY[]::text[]), 'has_attorney')`),
                updated_at: new Date()
              });
            } catch (_) {}
          }
        } else if (verdict.has_attorney === false && verdict.confidence >= 60) {
          update.has_attorney = false;
          results.no_attorney++;
        } else { results.indeterminate++; continue; }

        await db('persons').where('id', p.id).update(update);

        await db('activity_log').insert({
          id: uuidv4(),
          person_id: p.id,
          incident_id: p.incident_id,
          action: 'attorney_check',
          details: JSON.stringify({ verdict, source: 'court_pipeline' }),
          created_at: new Date()
        }).catch(() => {});
      } catch (e) {
        results.errors.push(`${p.full_name}: ${e.message}`);
        await reportError(db, 'court', p.id, e.message);
      }
    }

    await db('data_sources').where('id', courtDs.id).update({
      last_polled_at: new Date(), last_success_at: new Date(), updated_at: new Date()
    });

    res.json({
      success: true,
      message: `Court check: ${results.checked} checked, ${results.has_attorney} have attorney, ${results.no_attorney} clear`,
      ...results,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'court', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
