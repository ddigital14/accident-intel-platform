/**
 * Phase 92: County Coroner / Medical Examiner Public Fatality Lists
 * Cook County (Chicago) ME has a public Socrata API with motor-vehicle fatalities.
 * LA, Maricopa, Harris planned for HTML scraping.
 */
const { getDb } = require('../../_db');

const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function ingestCookCountyME(db, days = 30, limit = 200) {
  const since = new Date(Date.now() - days*86400*1000).toISOString();
  const where = encodeURIComponent(`(manner='ACCIDENT' OR manner='HOMICIDE') AND incident_date>'${since.split('T')[0]}T00:00:00.000' AND (UPPER(primarycause) LIKE '%MOTOR%' OR UPPER(primarycause) LIKE '%VEHIC%' OR UPPER(primarycause) LIKE '%TRAFFIC%' OR UPPER(primarycause) LIKE '%CRASH%' OR UPPER(primarycause) LIKE '%PEDESTRIAN%')`);
  const url = `https://datacatalog.cookcountyil.gov/resource/cjeq-bs86.json?$where=${where}&$order=incident_date+DESC&$limit=${limit}`;
  let rows;
  try {
    const r = await fetch(url, { signal: AbortSignal.timeout(15000), headers: { 'User-Agent': 'AccidentCommandCenter/1.0' } });
    if (!r.ok) return { ok: false, source: 'cook-me', status: r.status };
    rows = await r.json();
  } catch (e) {
    return { ok: false, source: 'cook-me', error: e.message };
  }
  const { v4: uuid } = require('uuid');
  let inserted = 0, persons_added = 0, skipped = 0;
  for (const row of rows) {
    const caseNo = row.case_number || row.caseno || row.id;
    if (!caseNo) continue;
    const ref = `cook-me:${caseNo}`;
    const exists = await db('incidents').where('incident_number', ref).first();
    let incidentId;
    if (!exists) {
      incidentId = uuid();
      try {
        await db('incidents').insert({
          id: incidentId, incident_number: ref, state: 'IL',
          city: row.incident_city || row.residence_city || 'Chicago',
          severity: 'fatal', incident_type: 'car_accident', fatalities_count: 1,
          description: `Cook County ME: ${row.primarycause || 'vehicular fatality'} at ${row.incident_address || row.incident_city || 'unknown'}`.slice(0, 500),
          raw_description: JSON.stringify(row).slice(0, 4000),
          latitude: parseFloat(row.latitude) || null,
          longitude: parseFloat(row.longitude) || null,
          occurred_at: row.incident_date ? new Date(row.incident_date) : new Date(),
          discovered_at: new Date(), qualification_state: 'pending', lead_score: 70, source_count: 1
        });
        inserted++;
      } catch (e) { skipped++; continue; }
    } else {
      incidentId = exists.id;
    }
    const decedentName = row.first_name && row.last_name ? `${row.first_name} ${row.last_name}` :
                         row.fullname || row.full_name || null;
    if (decedentName) {
      const dup = await db('persons').where({ incident_id: incidentId, full_name: decedentName }).first();
      if (!dup) {
        try {
          await db('persons').insert({
            id: uuid(), incident_id: incidentId, full_name: decedentName, role: 'victim',
            age: row.age ? parseInt(row.age) : null,
            city: row.residence_city || row.incident_city, state: row.residence_state || 'IL',
            address: row.residence_address || null,
            victim_verified: true, lead_tier: 'qualified', source: 'cook-county-me', created_at: new Date()
          });
          persons_added++;
        } catch { /* skip */ }
      }
    }
  }
  return { ok: true, source: 'cook-me', fetched: rows.length, inserted, persons_added, skipped };
}

async function ingestLaCountyME() { return { ok: true, source: 'la-me', fetched: 0, note: 'scraper_not_implemented_yet' }; }
async function ingestMaricopaME() { return { ok: true, source: 'maricopa-me', fetched: 0, note: 'scraper_not_implemented_yet' }; }

module.exports = async function handler(req, res) {
  if (!authed(req)) return res.status(401).json({ error: 'unauthorized' });
  const action = req.query?.action || 'health';
  const db = getDb();
  if (action === 'health') {
    return res.status(200).json({
      ok: true, engine: 'coroner-feeds',
      sources: { 'cook-me': 'live (Socrata cjeq-bs86)', 'la-me': 'planned', 'maricopa-me': 'planned' }
    });
  }
  if (action === 'run_cook') {
    const days = parseInt(req.query?.days) || 30;
    const limit = parseInt(req.query?.limit) || 200;
    const r = await ingestCookCountyME(db, days, limit);
    return res.status(200).json({ ok: true, ...r });
  }
  if (action === 'run_all') {
    const days = parseInt(req.query?.days) || 30;
    const [cook, la, maricopa] = await Promise.all([
      ingestCookCountyME(db, days, 200),
      ingestLaCountyME(db, days),
      ingestMaricopaME(db, days)
    ]);
    const totalInserted = (cook.inserted || 0) + (la.inserted || 0) + (maricopa.inserted || 0);
    const totalPersons = (cook.persons_added || 0) + (la.persons_added || 0) + (maricopa.persons_added || 0);
    return res.status(200).json({ ok: true, total_inserted: totalInserted, total_persons_added: totalPersons, sources: { cook, la, maricopa } });
  }
  return res.status(400).json({ error: 'unknown action', valid: ['health', 'run_cook', 'run_all'] });
};
