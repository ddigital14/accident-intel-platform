/**
 * Free People-Search Reverse Lookup
 *
 * Given a person's name + city/state, scrapes free people-finder sites
 * (TruePeopleSearch, FastPeopleSearch) for phone + address.
 *
 * This is our in-house alternative to Spokeo while we wait for B2B API access.
 * Sites scraped:
 *   - truepeoplesearch.com  (free, captcha-light, has age + relatives)
 *   - fastpeoplesearch.com  (free, has phone + address)
 *
 * GET /api/v1/enrich/people-search?secret=ingest-now
 * Cron: every 30 minutes
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { dedupCache } = require('../../_cache');

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

function tpsUrl(name, state) {
  const [first, ...rest] = name.split(' ');
  const last = rest[rest.length - 1] || '';
  return `https://www.truepeoplesearch.com/results?name=${encodeURIComponent(first + ' ' + last)}&citystatezip=${encodeURIComponent(state || '')}`;
}
function fpsUrl(name, state) {
  return `https://www.fastpeoplesearch.com/name/${encodeURIComponent(name.replace(/\s+/g, '-'))}_${encodeURIComponent(state || '')}`;
}

async function scrape(url) {
  try {
    const resp = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9'
      },
      signal: AbortSignal.timeout(15000)
    });
    if (!resp.ok) return null;
    return (await resp.text()).substring(0, 80000);
  } catch (_) { return null; }
}

async function extractContact(html, person) {
  if (!OPENAI_API_KEY || !html) return null;
  const text = html
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .substring(0, 5000);

  const prompt = `Search results for "${person.full_name}"${person.city ? ' in ' + person.city : ''}${person.state ? ', ' + person.state : ''}${person.age ? ' age ~' + person.age : ''}.

Page text:
"""
${text}
"""

Find the best matching record. Return JSON only:
{
  "best_match": {
    "full_name": "string|null",
    "age": number|null,
    "phones": ["string"],
    "emails": ["string"],
    "current_address": "string|null",
    "current_city": "string|null",
    "current_state": "string|null",
    "current_zip": "string|null",
    "previous_addresses": ["string"],
    "relatives": ["string"],
    "match_confidence": 0-100
  }
}
Only return match_confidence>=60 if name + (age OR location) clearly aligns. Else null best_match.`;

  try {
    const resp = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: [
          { role: 'system', content: 'Extract people-search records as JSON. Strict matching — never confuse different people with same name.' },
          { role: 'user', content: prompt }
        ],
        temperature: 0,
        response_format: { type: 'json_object' }
      }),
      signal: AbortSignal.timeout(20000)
    });
    if (!resp.ok) return null;
    const data = await resp.json();
    return JSON.parse(data.choices?.[0]?.message?.content || '{}');
  } catch (_) { return null; }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const results = { candidates: 0, scraped: 0, matches: 0, updated: 0, errors: [] };
  const startTime = Date.now();

  try {
    // Find persons with a name but missing contact info
    const candidates = await db('persons as p')
      .leftJoin('incidents as i', 'p.incident_id', 'i.id')
      .whereNotNull('p.full_name')
      .where(function() {
        this.whereNull('p.phone').orWhereNull('p.address');
      })
      .where('p.created_at', '>', new Date(Date.now() - 7 * 86400000))
      .select('p.id','p.full_name','p.first_name','p.last_name','p.age',
              'p.city','p.state','p.phone','p.email','p.address',
              'i.city as incident_city','i.state as incident_state')
      .limit(15);

    results.candidates = candidates.length;

    for (const p of candidates) {
      if (Date.now() - startTime > 50000) break;
      try {
        const cacheKey = `pps:${p.id}`;
        if (dedupCache.has(cacheKey)) continue;
        dedupCache.set(cacheKey, 1);

        const lookupState = p.state || p.incident_state;
        const lookupCity = p.city || p.incident_city;
        const personCtx = { full_name: p.full_name, city: lookupCity, state: lookupState, age: p.age };

        // Try TruePeopleSearch first
        let html = await scrape(tpsUrl(p.full_name, lookupState));
        let parsed = html ? await extractContact(html, personCtx) : null;
        let source = 'truepeoplesearch';

        // Fallback to FastPeopleSearch
        if (!parsed?.best_match || (parsed.best_match.match_confidence || 0) < 60) {
          html = await scrape(fpsUrl(p.full_name, lookupState));
          if (html) {
            parsed = await extractContact(html, personCtx);
            source = 'fastpeoplesearch';
          }
        }

        results.scraped++;

        if (!parsed?.best_match || (parsed.best_match.match_confidence || 0) < 60) continue;
        const m = parsed.best_match;
        results.matches++;

        // Build update — only fill empty fields
        const update = { updated_at: new Date() };
        if (!p.phone && m.phones?.length) update.phone = m.phones[0];
        if (!p.email && m.emails?.length) update.email = m.emails[0];
        if (!p.address && m.current_address) update.address = m.current_address;
        if (!p.city && m.current_city) update.city = m.current_city;
        if (!p.state && m.current_state) update.state = m.current_state;
        if (m.current_zip) update.zip = m.current_zip;
        if (m.age && !p.age) update.age = m.age;

        // Bump enrichment metadata
        const meta = {
          people_search_source: source,
          people_search_confidence: m.match_confidence,
          previous_addresses: m.previous_addresses || [],
          relatives: m.relatives || []
        };
        update.enrichment_data = JSON.stringify(meta);
        update.enrichment_score = db.raw('GREATEST(COALESCE(enrichment_score, 0), ?)', [m.match_confidence]);

        await db('persons').where('id', p.id).update(update);
        results.updated++;

        // Log enrichment
        await db('enrichment_logs').insert({
          person_id: p.id,
          field_name: 'people_search',
          old_value: null,
          new_value: JSON.stringify(update),
          source_url: source === 'truepeoplesearch' ? tpsUrl(p.full_name, lookupState) : fpsUrl(p.full_name, lookupState),
          source: source,
          confidence: m.match_confidence,
          verified: false,
          created_at: new Date()
        }).catch(() => {});
      } catch (e) {
        results.errors.push(`${p.full_name}: ${e.message}`);
        await reportError(db, 'people-search', p.id, e.message);
      }
    }

    res.json({
      success: true,
      message: `People-search: ${results.scraped} scraped, ${results.matches} matches, ${results.updated} updated`,
      ...results,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'people-search', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
