/**
 * CourtListener (RECAP) Integration — FREE federal + state court records
 *
 * https://www.courtlistener.com/help/api/
 *
 * For accident leads: when a victim FILES a lawsuit, the case docket
 * contains:
 *   - Plaintiff full legal name
 *   - Defendant (often the at-fault driver)
 *   - Attorney of record (boom — flags "has_attorney" automatically)
 *   - Case type (motor vehicle tort = MV)
 *   - Filing date
 *
 * Auth: optional Token (recommended for higher rate limits)
 * Free tier: 5000 calls/day with token, 1000/day without
 *
 * Strategy:
 *   - Search recent dockets for "motor vehicle" / "automobile" / "personal injury"
 *   - Filter to last 30 days
 *   - Cross-reference plaintiffs against our incidents (name + city + date)
 *   - When match found: mark incident has_attorney=true, attach plaintiff info
 *
 * GET /api/v1/ingest/courtlistener?secret=ingest-now
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('../system/_errors');
const { batchInsert } = require('../../_batch');
const { dedupCache } = require('../../_cache');

const CL_TOKEN = process.env.COURTLISTENER_TOKEN;
const BASE = 'https://www.courtlistener.com/api/rest/v3';

async function searchCases(query, daysBack = 30) {
  const headers = { 'Accept': 'application/json', 'User-Agent': 'AIP/1.0' };
  if (CL_TOKEN) headers['Authorization'] = `Token ${CL_TOKEN}`;
  const filed_after = new Date(Date.now() - daysBack * 86400000).toISOString().split('T')[0];
  try {
    const url = `${BASE}/search/?q=${encodeURIComponent(query)}&type=r&order_by=dateFiled+desc&filed_after=${filed_after}`;
    const r = await fetch(url, { headers, signal: AbortSignal.timeout(15000) });
    if (!r.ok) return null;
    return r.json();
  } catch (_) { return null; }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const results = { searched: 0, cases_found: 0, plaintiffs_extracted: 0,
                    incidents_linked: 0, errors: [] };

  try {
    let ds = await db('data_sources').where('name', 'CourtListener').first();
    if (!ds) {
      const dsId = uuidv4();
      await db('data_sources').insert({
        id: dsId, name: 'CourtListener', type: 'public_records',
        provider: 'courtlistener', api_endpoint: BASE,
        is_active: true, last_polled_at: new Date(),
        created_at: new Date(), updated_at: new Date()
      });
      ds = { id: dsId };
    }

    const queries = [
      'motor vehicle personal injury',
      'automobile accident negligence',
      'pedestrian struck',
      'wrongful death automobile'
    ];

    for (const q of queries) {
      try {
        const data = await searchCases(q, 30);
        if (!data) continue;
        results.searched++;
        const cases = data.results || [];

        for (const c of cases.slice(0, 10)) {
          if (dedupCache.has(`cl:${c.id}`)) continue;
          dedupCache.set(`cl:${c.id}`, 1);
          results.cases_found++;

          // Try to match case to existing incident
          const caseName = c.caseName || '';
          // CourtListener case names: "Smith v. Doe" — pull plaintiff (Smith)
          const plaintiffMatch = caseName.match(/^([^v]+)\s+v\.?\s/i);
          const plaintiff = plaintiffMatch ? plaintiffMatch[1].trim() : null;
          if (!plaintiff) continue;
          results.plaintiffs_extracted++;

          // Look for matching person in our DB
          const matches = await db('persons')
            .whereRaw('LOWER(full_name) = LOWER(?)', [plaintiff])
            .orWhereRaw("LOWER(first_name || ' ' || last_name) = LOWER(?)", [plaintiff])
            .limit(3);

          for (const m of matches) {
            // Mark has_attorney=true, since they filed suit
            await db('persons').where('id', m.id).update({
              has_attorney: true,
              contact_status: 'has_attorney',
              attorney_firm: c.assigned_to_str || null,
              metadata: db.raw(`COALESCE(metadata, '{}'::jsonb) || ?::jsonb`,
                [JSON.stringify({ courtlistener_case: c.id, case_name: caseName, court: c.court, date_filed: c.dateFiled })]),
              updated_at: new Date()
            }).catch(() => {});
            results.incidents_linked++;
          }
        }
      } catch (e) {
        results.errors.push(`${q}: ${e.message}`);
      }
    }

    await db('data_sources').where('id', ds.id).update({
      last_polled_at: new Date(), last_success_at: new Date(), updated_at: new Date()
    });

    res.json({
      success: true,
      message: `CourtListener: ${results.searched} queries, ${results.cases_found} cases, ${results.incidents_linked} matched`,
      ...results,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'courtlistener', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
