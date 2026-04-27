/**
 * Court Filing Reverse-Link — Phase 20 #4
 *
 * Reverse mapping of courtlistener.js: instead of "find filings for our victims",
 * this finds "victims for any nearby filing". For every nameless incident, scan
 * CourtListener for case captions matching "v.", "estate of", "wrongful death".
 * Filed within 7d before to 90d after the accident.
 *
 * GET /api/v1/enrich/court-reverse-link?secret=ingest-now&limit=30&dry=false
 * Cron: every 6h (folded into existing 'court' slot — no new cron)
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('../system/_errors');
const { dedupCache } = require('../../_cache');
const { enqueueCascade } = require('../system/_cascade');
const { normalizePerson } = require('../../_schema');
const { trackApiCall } = require('../system/cost');
const { logChange } = require('../system/changelog');

const COURTLISTENER_TOKEN = process.env.COURTLISTENER_TOKEN;
const BASE = 'https://www.courtlistener.com/api/rest/v3';

const CAPTION_PATTERNS = [
  /^([A-Z][A-Za-z'\-\.]+(?:\s+[A-Z][A-Za-z'\-\.]+){1,3})\s+v\.?s?\.?\s+/i,
  /Estate\s+of\s+([A-Z][A-Za-z'\-\.]+(?:\s+[A-Z][A-Za-z'\-\.]+){1,3})/i,
  /^([A-Z][A-Za-z'\-\.]+(?:\s+[A-Z][A-Za-z'\-\.]+){1,3})\s*,\s*deceased/i,
];

function extractPlaintiff(caption) {
  if (!caption) return null;
  for (const re of CAPTION_PATTERNS) {
    const m = caption.match(re);
    if (m) return m[1].trim();
  }
  return null;
}

function classifyCase(caption) {
  const c = (caption || '').toLowerCase();
  const wrongfulDeath = /wrongful death|estate of|deceased/.test(c);
  const personalInjury = /personal injury|negligence|motor vehicle|automobile|pedestrian|bodily injury/.test(c);
  return { wrongfulDeath, personalInjury, isPI: wrongfulDeath || personalInjury };
}

async function searchCases(db, city, state, dateFrom, dateTo) {
  const headers = { 'Accept': 'application/json', 'User-Agent': 'AIP/1.0' };
  if (COURTLISTENER_TOKEN) headers['Authorization'] = `Token ${COURTLISTENER_TOKEN}`;
  const queries = [
    `(motor vehicle OR automobile) ${city}`,
    `(wrongful death OR estate of) ${city} ${state}`,
    `(pedestrian struck OR bodily injury) ${city}`,
  ];
  const allCases = [];
  for (const q of queries) {
    try {
      const url = `${BASE}/search/?q=${encodeURIComponent(q)}&type=r&order_by=dateFiled+desc&filed_after=${dateFrom}&filed_before=${dateTo}`;
      const r = await fetch(url, { headers, signal: AbortSignal.timeout(12000) });
      if (!r.ok) continue;
      const data = await r.json();
      await trackApiCall(db, 'court-reverse-link', 'courtlistener', 0, 0, true);
      for (const c of (data.results || []).slice(0, 8)) {
        if (!allCases.find(x => x.id === c.id)) allCases.push(c);
      }
    } catch (_) {}
  }
  return allCases;
}

async function processIncident(db, inc, dsId, dryRun) {
  const incDate = inc.occurred_at ? new Date(inc.occurred_at) : new Date(inc.discovered_at);
  const dateFrom = new Date(incDate.getTime() - 7 * 86400000).toISOString().split('T')[0];
  const dateTo = new Date(incDate.getTime() + 90 * 86400000).toISOString().split('T')[0];

  const cases = await searchCases(db, inc.city, inc.state, dateFrom, dateTo);
  if (!cases.length) return { matches: 0 };

  let added = 0;
  const samples = [];
  for (const c of cases) {
    const caption = c.caseName || c.caseNameShort || '';
    const cls = classifyCase(caption);
    if (!cls.isPI) continue;
    const plaintiff = extractPlaintiff(caption);
    if (!plaintiff || plaintiff.length < 4) continue;

    const dedup = `crl:${inc.id}:${c.id}`;
    if (dedupCache.has(dedup)) continue;
    dedupCache.set(dedup, 1);

    const exists = await db('persons').where('incident_id', inc.id)
      .whereRaw('LOWER(full_name) = LOWER(?)', [plaintiff]).first();
    if (exists) continue;

    if (dryRun) { added++; samples.push({ name: plaintiff, case: caption }); continue; }

    const conf = cls.wrongfulDeath ? 75 : 65;
    const person = normalizePerson({
      incident_id: inc.id,
      full_name: plaintiff,
      role: 'driver',
      is_injured: true,
      injury_severity: cls.wrongfulDeath ? 'fatal' : 'unknown',
      city: inc.city,
      state: inc.state,
      contact_status: 'has_attorney',
      has_attorney: true,
      attorney_firm: c.assigned_to_str || null,
      confidence_score: conf,
      metadata: {
        backfill_source: 'court-reverse-link',
        case_name: caption,
        court: c.court,
        case_id: c.id,
        date_filed: c.dateFiled,
        wrongful_death: cls.wrongfulDeath,
        recovered_at: new Date().toISOString()
      }
    });
    person.id = uuidv4();
    await db('persons').insert(person);

    await db('source_reports').insert({
      id: uuidv4(),
      incident_id: inc.id,
      data_source_id: dsId,
      source_type: 'court_records',
      source_reference: `court-reverse:${c.id}:${inc.id}`,
      raw_data: JSON.stringify({ case: c, plaintiff }),
      parsed_data: JSON.stringify({ plaintiff, case_name: caption, classification: cls }),
      contributed_fields: ['full_name', 'has_attorney', 'attorney_firm'],
      confidence: conf, is_verified: true,
      fetched_at: new Date(), processed_at: new Date(), created_at: new Date()
    }).catch(() => {});

    await enqueueCascade(db, {
      person_id: person.id,
      incident_id: inc.id,
      trigger_source: 'court_reverse_link',
      trigger_field: 'has_attorney',
      trigger_value: 'true',
      priority: cls.wrongfulDeath ? 9 : 7
    }).catch(() => {});

    added++;
    samples.push({ name: plaintiff, case: caption, wrongful_death: cls.wrongfulDeath });
    if (added >= 3) break;
  }
  return { matches: added, samples };
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
  const limit = Math.min(parseInt(req.query?.limit) || 25, 60);
  const startTime = Date.now();
  const TIME_BUDGET = 50000;

  const results = {
    candidates: 0, processed: 0, persons_added: 0,
    incidents_with_match: 0, samples: [], errors: []
  };

  try {
    let ds = await db('data_sources').where('name', 'Court Reverse Link').first();
    if (!ds) {
      const dsId = uuidv4();
      await db('data_sources').insert({
        id: dsId, name: 'Court Reverse Link', type: 'public_records',
        provider: 'courtlistener', api_endpoint: BASE,
        is_active: true, last_polled_at: new Date(),
        created_at: new Date(), updated_at: new Date()
      });
      ds = { id: dsId };
    }

    const { rows: candidates } = await db.raw(`
      SELECT i.id, i.city, i.state, i.severity, i.fatalities_count,
             i.occurred_at, i.discovered_at, i.incident_type
      FROM incidents i
      LEFT JOIN persons p ON p.incident_id = i.id
      WHERE i.qualification_state IN ('pending','pending_named')
        AND i.city IS NOT NULL AND i.state IS NOT NULL
        AND i.discovered_at > NOW() - INTERVAL '60 days'
        AND p.id IS NULL
      ORDER BY
        (CASE i.severity WHEN 'fatal' THEN 0 WHEN 'critical' THEN 1 WHEN 'serious' THEN 2 ELSE 5 END),
        i.discovered_at DESC
      LIMIT ?
    `, [limit]);
    results.candidates = candidates.length;

    for (const inc of candidates) {
      if (Date.now() - startTime > TIME_BUDGET) break;
      try {
        const r = await processIncident(db, inc, ds.id, dryRun);
        results.processed++;
        if (r.matches > 0) {
          results.incidents_with_match++;
          results.persons_added += r.matches;
          if (results.samples.length < 10 && r.samples) {
            for (const s of r.samples) results.samples.push({ incident_id: inc.id, ...s });
          }
        }
      } catch (e) {
        results.errors.push(`${inc.id}: ${e.message}`);
        await reportError(db, 'court-reverse-link', inc.id, e.message);
      }
    }

    if (!dryRun) {
      await db('data_sources').where('id', ds.id).update({
        last_polled_at: new Date(), last_success_at: new Date(), updated_at: new Date()
      });
      if (results.persons_added > 0) {
        await logChange(db, {
          kind: 'pipeline',
          title: `Court reverse-link: ${results.persons_added} plaintiffs matched`,
          summary: `Processed ${results.processed}/${results.candidates} nameless incidents, matched ${results.incidents_with_match} via PI court filings`,
          meta: results
        }).catch(() => {});
      }
    }

    res.json({
      success: true,
      message: `Court reverse-link: ${results.persons_added} persons across ${results.incidents_with_match} incidents${dryRun ? ' (DRY)' : ''}`,
      dry_run: dryRun,
      ...results,
      duration_ms: Date.now() - startTime,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'court-reverse-link', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
