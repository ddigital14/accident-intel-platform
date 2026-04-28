/**
 * Obituaries Scraper for Fatal Crashes
 *
 * For each fatal incident in the last 7 days that doesn't have a victim name,
 * search Legacy.com / local funeral home obituaries for matches by city + date.
 * Extracts: full name, age, residence, family contact, funeral location.
 *
 * Critical for fatal-crash leads — families post obituaries within 24-72h
 * with detailed contact info that's otherwise impossible to obtain.
 *
 * GET /api/v1/ingest/obituaries?secret=ingest-now
 * Cron: every 4 hours
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('../system/_errors');
const { applyDenyList } = require('../enrich/_name_filter');
const { dedupCache } = require('../../_cache');
const { enqueueCascade } = require('../system/_cascade');
const { extractJson } = require('../enrich/_ai_router');

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;


// Phase 29: tributes.com dedicated DOM selector — lifts recall by skipping GPT for clean structured pages.
function extractTributesObitsFromHtml(html) {
  if (!html || !html.includes('tributes.com')) return [];
  const out = [];
  const cardRegex = /<a[^>]+href="(\/obituaries\/[^"]+)"[^>]*>([^<]+)<\/a>/gi;
  let m; while ((m = cardRegex.exec(html)) !== null) {
    const href = m[1]; const fullName = m[2].trim();
    if (fullName.split(' ').length >= 2 && fullName.length < 80) {
      out.push({ name: fullName, url: 'https://www.tributes.com' + href, source: 'tributes_dom' });
    }
  }
  // Age + city often embedded near the name
  return out.slice(0, 30);
}

// Phase 22: tributes.com primary, legacy.com fallback
function tributesSearchUrl(city, state) {
  return `https://www.tributes.com/obituaries/search?searchword=&city=${encodeURIComponent(city)}&state=${encodeURIComponent(state)}`;
}
function legacySearchUrl(city, state) {
  return `https://www.legacy.com/us/obituaries/search?firstName=&lastName=&keyword=&location=${encodeURIComponent(city + ', ' + state)}&limit=20`;
}

async function fetchObitsForCity(city, state) {
  // Phase 22: tributes.com primary
  try {
    const url = tributesSearchUrl(city, state);
    const resp = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; AIP/1.0; research)',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
      },
      signal: AbortSignal.timeout(12000)
    });
    if (resp.ok) return (await resp.text()).substring(0, 80000);
  } catch (_) {}
  // Fallback legacy.com
  try {
    const url = legacySearchUrl(city, state);
    const resp = await fetch(url, {
      headers: { 'User-Agent': 'AIP/1.0 (research)' },
      signal: AbortSignal.timeout(12000)
    });
    if (!resp.ok) return null;
    return (await resp.text()).substring(0, 80000);
  } catch (_) { return null; }
}

async function extractObitMatches(db, html, incident) {
  if (!html) return null;
  // Strip HTML
  const text = html
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .substring(0, 5000);

  const incDate = incident.occurred_at ? new Date(incident.occurred_at).toDateString() : 'recent';
  const prompt = `An obituary search page lists names. We're looking for matches to this fatal accident:
- City: ${incident.city}, ${incident.state}
- Date: ${incDate}
- Description: ${incident.description?.substring(0,200)}

Search results page text:
"""
${text}
"""

Find people who likely died in this accident (recent death matching city + date proximity). Return JSON only:
{
  "matches": [
    {
      "full_name": "string",
      "age": number|null,
      "city_residence": "string|null",
      "death_date": "ISO date|null",
      "funeral_home": "string|null",
      "match_confidence": 0-100,
      "match_reason": "brief reason"
    }
  ]
}`;

  // Obituaries are premium-tier (fatal crash victim = $1M+ case potential)
  return await extractJson(db, {
    pipeline: 'obituaries',
    systemPrompt: 'You match obituaries to fatal car crashes. Only return matches with confidence>=60. Empty array if no matches.',
    userPrompt: prompt,
    tier: 'premium',
    severityHint: 'fatal',
    timeoutMs: 45000,
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
  const results = { fatal_incidents: 0, searched: 0, matches: 0, persons_added: 0, errors: [] };
  const startTime = Date.now();

  try {
    let ds = await db('data_sources').where('name', 'Obituaries').first();
    if (!ds) {
      const dsId = uuidv4();
      await db('data_sources').insert({
        id: dsId, name: 'Obituaries', type: 'public_records',
        api_endpoint: 'legacy.com search', is_active: true,
        last_polled_at: new Date(), created_at: new Date(), updated_at: new Date()
      });
      ds = { id: dsId };
    }

    // Find fatal incidents in last 7d without confirmed victim names
    const fatalIncidents = await db('incidents')
      .where('severity', 'fatal')
      .orWhere('fatalities_count', '>', 0)
      .where('discovered_at', '>', new Date(Date.now() - 7 * 86400000))
      .whereNotNull('city')
      .whereNotNull('state')
      .select('id','city','state','occurred_at','description','fatalities_count')
      .limit(15);

    results.fatal_incidents = fatalIncidents.length;

    for (const inc of fatalIncidents) {
      if (Date.now() - startTime > 50000) break;
      try {
        const cacheKey = `obit:${inc.id}`;
        if (dedupCache.has(cacheKey)) continue;
        dedupCache.set(cacheKey, 1);

        // Check if we already extracted obit for this
        const exists = await db('source_reports')
          .where('incident_id', inc.id)
          .where('source_type', 'obituary')
          .first();
        if (exists) continue;

        const html = await fetchObitsForCity(inc.city, inc.state);
        if (!html) continue;
        results.searched++;

        const result = await extractObitMatches(db, html, inc);
        if (!result?.matches) continue;

        const newPersons = [];
        for (const m of result.matches) {
          if (!m.full_name || (m.match_confidence || 0) < 60) continue;
          const _src = (m.match_reason || '') + '\n' + (m.full_name || '') + '\n' + (m.funeral_home || '');
          const _safe = applyDenyList(m.full_name, _src);
          if (!_safe) { continue; }
          m.full_name = _safe;
          // Dedup by name
          const exists = await db('persons').where('incident_id', inc.id)
            .whereRaw('LOWER(full_name) = LOWER(?)', [m.full_name]).first();
          if (exists) continue;
          newPersons.push({
            id: uuidv4(), incident_id: inc.id,
            role: 'driver', is_injured: true,
            injury_severity: 'fatal',
            first_name: m.full_name.split(' ')[0],
            last_name: m.full_name.split(' ').slice(-1)[0],
            full_name: m.full_name,
            age: m.age || null,
            city: m.city_residence || inc.city,
            state: inc.state,
            contact_status: 'not_contacted',
            confidence_score: m.match_confidence || 75,
            metadata: JSON.stringify({
              source: 'obituary',
              death_date: m.death_date,
              funeral_home: m.funeral_home,
              match_reason: m.match_reason
            }),
            created_at: new Date(), updated_at: new Date()
          });
        }

        if (newPersons.length) {
          await db('persons').insert(newPersons);
          results.matches += newPersons.length;
          results.persons_added += newPersons.length;

          await db('source_reports').insert({
            id: uuidv4(), incident_id: inc.id, data_source_id: ds.id,
            source_type: 'obituary', source_reference: `legacy:${inc.id}:${Date.now()}`,
            raw_data: JSON.stringify({ search_url: legacySearchUrl(inc.city, inc.state) }),
            parsed_data: JSON.stringify(result),
            contributed_fields: ['victims', 'fatal_confirmation'],
            confidence: 75, is_verified: false,
            fetched_at: new Date(), processed_at: new Date(), created_at: new Date()
          });

          await db('incidents').where('id', inc.id).update({
            source_count: db.raw('COALESCE(source_count, 1) + 1'),
            confidence_score: db.raw('LEAST(99, COALESCE(confidence_score, 50) + 12)'),
            updated_at: new Date()
          });
        }
      } catch (e) {
        results.errors.push(`${inc.id}: ${e.message}`);
        await reportError(db, 'obituaries', inc.id, e.message);
      }
    }

    await db('data_sources').where('id', ds.id).update({
      last_polled_at: new Date(), last_success_at: new Date(), updated_at: new Date()
    });

    res.json({
      success: true,
      message: `Obituaries: ${results.searched}/${results.fatal_incidents} searched, ${results.matches} matches`,
      ...results,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'obituaries', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
