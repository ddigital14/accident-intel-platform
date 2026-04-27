/**
 * Police Department Press Release Scraper
 *
 * Direct scrape of each PD's news/press page (more reliable than Nitter).
 * Each PD posts crash details, sometimes with names, on their public news page
 * within hours of an incident.
 *
 * GET /api/v1/ingest/pd-press?secret=ingest-now
 * Cron: every 30 min
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('../system/_errors');
const { batchInsert } = require('../../_batch');
const { dedupCache } = require('../../_cache');
const { enqueueCascade } = require('../system/_cascade');
const { normalizeIncident, normalizePerson } = require('../../_schema');
const { extractJson } = require('../enrich/_ai_router');

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

// Police department news URLs (publicly accessible, no auth)
// Phase 20 #2 — expanded from 12 to 110 PDs covering top US metros.
// URLs verified or constructed from each city's official .gov/.org press path.
// Parser is shared (generic HTML→AI extract); the AI model is robust to varying layouts.
const PD_PAGES = [
  // Original 12 (kept)
  { name: 'NYPD News',         url: 'https://www.nyc.gov/site/nypd/news/news.page', metro: 'New York', state: 'NY' },
  { name: 'LAPD News',         url: 'https://www.lapdonline.org/newsroom/', metro: 'Los Angeles', state: 'CA' },
  { name: 'Chicago Police',    url: 'https://home.chicagopolice.org/news/', metro: 'Chicago', state: 'IL' },
  { name: 'Houston Police',    url: 'https://www.houstontx.gov/police/nr/index.htm', metro: 'Houston', state: 'TX' },
  { name: 'Atlanta PD',        url: 'https://www.atlantapd.org/Home/Components/News/News?', metro: 'Atlanta', state: 'GA' },
  { name: 'Dallas PD',         url: 'https://dallaspolice.net/News/Pages/default.aspx', metro: 'Dallas', state: 'TX' },
  { name: 'Seattle PD blotter',url: 'https://spdblotter.seattle.gov/', metro: 'Seattle', state: 'WA' },
  { name: 'SFPD News',         url: 'https://www.sanfranciscopolice.org/news', metro: 'San Francisco', state: 'CA' },
  { name: 'Cincinnati PD',     url: 'https://www.cincinnati-oh.gov/police/news/', metro: 'Cincinnati', state: 'OH' },
  { name: 'Boston PD News',    url: 'https://www.boston.gov/news?topic=43396', metro: 'Boston', state: 'MA' },
  { name: 'Miami PD',          url: 'https://www.miami-police.org/news.html', metro: 'Miami', state: 'FL' },
  { name: 'Philly PD',         url: 'https://www.phillypolice.com/news/', metro: 'Philadelphia', state: 'PA' },

  // Phase 20 expansion — top 100 US metro PDs
  { name: 'Albuquerque PD',    url: 'https://www.cabq.gov/police/news', metro: 'Albuquerque', state: 'NM' },
  { name: 'Anaheim PD',        url: 'https://www.anaheim.net/3935/Press-Releases', metro: 'Anaheim', state: 'CA' },
  { name: 'Anchorage PD',      url: 'https://www.muni.org/Departments/police/News/Pages/default.aspx', metro: 'Anchorage', state: 'AK' },
  { name: 'Arlington PD TX',   url: 'https://www.arlingtontx.gov/city_hall/departments/police/news_releases', metro: 'Arlington', state: 'TX' },
  { name: 'Aurora PD',         url: 'https://www.auroragov.org/news', metro: 'Aurora', state: 'CO' },
  { name: 'Austin PD',         url: 'https://www.austintexas.gov/news?keyword=&category=2391', metro: 'Austin', state: 'TX' },
  { name: 'Bakersfield PD',    url: 'https://www.bakersfieldpd.us/news-releases/', metro: 'Bakersfield', state: 'CA' },
  { name: 'Baltimore PD',      url: 'https://www.baltimorepolice.org/news', metro: 'Baltimore', state: 'MD' },
  { name: 'Baton Rouge PD',    url: 'https://www.brla.gov/582/Police-Department', metro: 'Baton Rouge', state: 'LA' },
  { name: 'Birmingham PD',     url: 'https://www.birminghamal.gov/police/news/', metro: 'Birmingham', state: 'AL' },
  { name: 'Boise PD',          url: 'https://www.cityofboise.org/departments/police/news-press/', metro: 'Boise', state: 'ID' },
  { name: 'Buffalo PD',        url: 'https://www.buffalony.gov/162/Police', metro: 'Buffalo', state: 'NY' },
  { name: 'Chandler PD',       url: 'https://www.chandlerpd.com/news/', metro: 'Chandler', state: 'AZ' },
  { name: 'Charlotte-Mecklenburg PD', url: 'https://www.charlottenc.gov/cmpdnews', metro: 'Charlotte', state: 'NC' },
  { name: 'Chesapeake PD',     url: 'https://www.cityofchesapeake.net/news/police-news/', metro: 'Chesapeake', state: 'VA' },
  { name: 'Chula Vista PD',    url: 'https://www.chulavistaca.gov/departments/police-department/news', metro: 'Chula Vista', state: 'CA' },
  { name: 'Cleveland Police',  url: 'https://www.clevelandohio.gov/CityofCleveland/Home/Government/CityAgencies/Police', metro: 'Cleveland', state: 'OH' },
  { name: 'Colorado Springs PD', url: 'https://coloradosprings.gov/news', metro: 'Colorado Springs', state: 'CO' },
  { name: 'Columbus PD',       url: 'https://www.columbus.gov/police/news/', metro: 'Columbus', state: 'OH' },
  { name: 'Corpus Christi PD', url: 'https://www.cctexas.com/departments/police-department/news', metro: 'Corpus Christi', state: 'TX' },
  { name: 'Denver PD',         url: 'https://www.denverpolice.org/news/', metro: 'Denver', state: 'CO' },
  { name: 'Des Moines PD',     url: 'https://www.dmgov.org/Departments/Police/Pages/News.aspx', metro: 'Des Moines', state: 'IA' },
  { name: 'Durham PD',         url: 'https://durhamnc.gov/CivicAlerts.aspx?CID=2', metro: 'Durham', state: 'NC' },
  { name: 'El Paso PD',        url: 'https://www.elpasotexas.gov/police-department/news', metro: 'El Paso', state: 'TX' },
  { name: 'Fontana PD',        url: 'https://www.fontana.org/2174/News-Releases', metro: 'Fontana', state: 'CA' },
  { name: 'Fort Wayne PD',     url: 'https://www.cityoffortwayne.org/news.html', metro: 'Fort Wayne', state: 'IN' },
  { name: 'Fort Worth PD',     url: 'https://police.fortworthtexas.gov/news/', metro: 'Fort Worth', state: 'TX' },
  { name: 'Fremont PD',        url: 'https://www.fremontpolice.gov/news', metro: 'Fremont', state: 'CA' },
  { name: 'Fresno PD',         url: 'https://www.fresno.gov/police/news/', metro: 'Fresno', state: 'CA' },
  { name: 'Garland PD',        url: 'https://www.garlandtx.gov/3247/News', metro: 'Garland', state: 'TX' },
  { name: 'Gilbert PD',        url: 'https://www.gilbertaz.gov/Home/Components/News/News/', metro: 'Gilbert', state: 'AZ' },
  { name: 'Glendale PD AZ',    url: 'https://www.glendaleaz.com/government/departments/police_department/news', metro: 'Glendale', state: 'AZ' },
  { name: 'Greensboro PD',     url: 'https://www.greensboro-nc.gov/departments/police/news', metro: 'Greensboro', state: 'NC' },
  { name: 'Henderson PD',      url: 'https://www.cityofhenderson.com/police/news', metro: 'Henderson', state: 'NV' },
  { name: 'Hialeah PD',        url: 'https://www.hialeahpd.org/news', metro: 'Hialeah', state: 'FL' },
  { name: 'Honolulu PD',       url: 'https://www.honolulupd.org/information/news-releases/', metro: 'Honolulu', state: 'HI' },
  { name: 'Indianapolis Metro PD', url: 'https://www.indy.gov/agency/indianapolis-metropolitan-police-department', metro: 'Indianapolis', state: 'IN' },
  { name: 'Irvine PD',         url: 'https://www.cityofirvine.org/police-department/news-events', metro: 'Irvine', state: 'CA' },
  { name: 'Irving PD',         url: 'https://www.cityofirving.org/3270/Police-News', metro: 'Irving', state: 'TX' },
  { name: 'Jacksonville Sheriff', url: 'https://www.jaxsheriff.org/News/', metro: 'Jacksonville', state: 'FL' },
  { name: 'Jersey City PD',    url: 'https://www.jerseycitynj.gov/cityhall/police', metro: 'Jersey City', state: 'NJ' },
  { name: 'Kansas City PD',    url: 'https://www.kcpd.org/news/', metro: 'Kansas City', state: 'MO' },
  { name: 'Laredo PD',         url: 'https://www.cityoflaredo.com/police/News.html', metro: 'Laredo', state: 'TX' },
  { name: 'Las Vegas Metro PD',url: 'https://www.lvmpd.com/en-us/Pages/News.aspx', metro: 'Las Vegas', state: 'NV' },
  { name: 'Lexington PD',      url: 'https://www.lexingtonky.gov/police-news', metro: 'Lexington', state: 'KY' },
  { name: 'Lincoln PD',        url: 'https://lincoln.ne.gov/city/police/news/', metro: 'Lincoln', state: 'NE' },
  { name: 'Long Beach PD',     url: 'https://www.longbeach.gov/police/news/', metro: 'Long Beach', state: 'CA' },
  { name: 'Louisville Metro PD', url: 'https://louisvilleky.gov/government/police/news', metro: 'Louisville', state: 'KY' },
  { name: 'Lubbock PD',        url: 'https://ci.lubbock.tx.us/departments/police-department/news-releases', metro: 'Lubbock', state: 'TX' },
  { name: 'Madison PD',        url: 'https://www.cityofmadison.com/police/newsroom/', metro: 'Madison', state: 'WI' },
  { name: 'Memphis PD',        url: 'https://www.memphispolice.org/news/', metro: 'Memphis', state: 'TN' },
  { name: 'Mesa PD',           url: 'https://www.mesaaz.gov/residents/police/news', metro: 'Mesa', state: 'AZ' },
  { name: 'Milwaukee PD',      url: 'https://city.milwaukee.gov/police/News-and-Events', metro: 'Milwaukee', state: 'WI' },
  { name: 'Minneapolis PD',    url: 'https://www.minneapolismn.gov/government/departments/police/news/', metro: 'Minneapolis', state: 'MN' },
  { name: 'Modesto PD',        url: 'https://www.modestogov.com/3134/Press-Releases', metro: 'Modesto', state: 'CA' },
  { name: 'Nashville Metro PD',url: 'https://www.nashville.gov/departments/police/news', metro: 'Nashville', state: 'TN' },
  { name: 'New Orleans PD',    url: 'https://nola.gov/nopd/news/', metro: 'New Orleans', state: 'LA' },
  { name: 'Newark PD',         url: 'https://www.newarkpd.org/news/', metro: 'Newark', state: 'NJ' },
  { name: 'Norfolk PD',        url: 'https://www.norfolk.gov/3437/Police-News', metro: 'Norfolk', state: 'VA' },
  { name: 'North Las Vegas PD',url: 'https://www.cityofnorthlasvegas.com/government/city_departments/police/news', metro: 'North Las Vegas', state: 'NV' },
  { name: 'Oakland PD',        url: 'https://www.oaklandca.gov/topics/police-news', metro: 'Oakland', state: 'CA' },
  { name: 'Oklahoma City PD',  url: 'https://www.okc.gov/departments/police/news', metro: 'Oklahoma City', state: 'OK' },
  { name: 'Omaha PD',          url: 'https://police.cityofomaha.org/news', metro: 'Omaha', state: 'NE' },
  { name: 'Orlando PD',        url: 'https://www.orlando.gov/Police-Department/News-Releases', metro: 'Orlando', state: 'FL' },
  { name: 'Oxnard PD',         url: 'https://www.oxnardpd.org/news/', metro: 'Oxnard', state: 'CA' },
  { name: 'Phoenix PD',        url: 'https://www.phoenix.gov/policesite/Pages/News.aspx', metro: 'Phoenix', state: 'AZ' },
  { name: 'Pittsburgh PD',     url: 'https://pittsburghpa.gov/publicsafety/police-news', metro: 'Pittsburgh', state: 'PA' },
  { name: 'Plano PD',          url: 'https://www.plano.gov/2153/News', metro: 'Plano', state: 'TX' },
  { name: 'Raleigh PD',        url: 'https://raleighnc.gov/police/news', metro: 'Raleigh', state: 'NC' },
  { name: 'Reno PD',           url: 'https://www.reno.gov/government/departments/police-department/news-releases', metro: 'Reno', state: 'NV' },
  { name: 'Richmond PD',       url: 'https://www.rva.gov/police/news', metro: 'Richmond', state: 'VA' },
  { name: 'Riverside PD',      url: 'https://riversideca.gov/rpd/news', metro: 'Riverside', state: 'CA' },
  { name: 'Rochester PD',      url: 'https://www.cityofrochester.gov/RPDNews/', metro: 'Rochester', state: 'NY' },
  { name: 'Sacramento PD',     url: 'https://www.cityofsacramento.org/Police/News', metro: 'Sacramento', state: 'CA' },
  { name: 'Saint Paul PD',     url: 'https://www.stpaul.gov/departments/police/news', metro: 'Saint Paul', state: 'MN' },
  { name: 'San Antonio PD',    url: 'https://www.sanantonio.gov/SAPD/News', metro: 'San Antonio', state: 'TX' },
  { name: 'San Bernardino PD', url: 'https://www.sbcity.org/police/news/', metro: 'San Bernardino', state: 'CA' },
  { name: 'San Diego PD',      url: 'https://www.sandiego.gov/police/news', metro: 'San Diego', state: 'CA' },
  { name: 'Santa Ana PD',      url: 'https://www.santa-ana.org/pd/news', metro: 'Santa Ana', state: 'CA' },
  { name: 'Santa Clarita Sheriff', url: 'https://www.santaclaritasheriff.org/News-And-Updates', metro: 'Santa Clarita', state: 'CA' },
  { name: 'Scottsdale PD',     url: 'https://www.scottsdaleaz.gov/police/news-and-blog', metro: 'Scottsdale', state: 'AZ' },
  { name: 'Spokane PD',        url: 'https://my.spokanecity.org/news/?categories=police', metro: 'Spokane', state: 'WA' },
  { name: 'St Louis Metro PD', url: 'https://www.slmpd.org/newsroom.shtml', metro: 'Saint Louis', state: 'MO' },
  { name: 'St Petersburg PD',  url: 'https://police.stpete.org/news/', metro: 'Saint Petersburg', state: 'FL' },
  { name: 'Stockton PD',       url: 'https://www.stocktonca.gov/government/departments/police/news.html', metro: 'Stockton', state: 'CA' },
  { name: 'Tacoma PD',         url: 'https://www.cityoftacoma.org/cms/One.aspx?portalId=169&pageId=4063', metro: 'Tacoma', state: 'WA' },
  { name: 'Tampa PD',          url: 'https://www.tampa.gov/police/news', metro: 'Tampa', state: 'FL' },
  { name: 'Toledo PD',         url: 'https://toledo.oh.gov/services/police/news', metro: 'Toledo', state: 'OH' },
  { name: 'Tucson PD',         url: 'https://www.tucsonaz.gov/Departments/Police/News', metro: 'Tucson', state: 'AZ' },
  { name: 'Tulsa PD',          url: 'https://www.cityoftulsa.org/government/departments/police/news/', metro: 'Tulsa', state: 'OK' },
  { name: 'Virginia Beach PD', url: 'https://www.vbgov.com/government/departments/police/news/', metro: 'Virginia Beach', state: 'VA' },
  { name: 'Washington Metro PD', url: 'https://mpdc.dc.gov/newsroom', metro: 'Washington', state: 'DC' },
  { name: 'Wichita PD',        url: 'https://www.wichita.gov/Police/News', metro: 'Wichita', state: 'KS' },
  { name: 'Winston-Salem PD',  url: 'https://www.cityofws.org/2015/Police-News', metro: 'Winston-Salem', state: 'NC' },
];

async function fetchPage(url) {
  try {
    const r = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml'
      },
      signal: AbortSignal.timeout(12000)
    });
    if (!r.ok) return null;
    return (await r.text()).substring(0, 100000);
  } catch (_) { return null; }
}

async function extractCrashes(db, html, pd) {
  if (!html) return null;
  const text = html
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .substring(0, 6000);

  const prompt = `Extract recent (last 7 days) crash/accident press releases from this police news page.
Department: ${pd.name}, ${pd.metro}, ${pd.state}.

"""
${text}
"""

JSON only:
{
  "crashes": [
    {
      "title": "press release headline",
      "summary": "1-2 sentence",
      "occurred_at": "ISO|null",
      "address": "string|null",
      "highway": "string|null",
      "lat": number|null, "lng": number|null,
      "incident_type": "car_accident|motorcycle_accident|truck_accident|pedestrian|bicycle|other",
      "severity": "fatal|serious|moderate|minor|unknown",
      "fatalities_count": number|null,
      "injuries_count": number|null,
      "police_report_number": "string|null",
      "victims": [
        { "full_name": "", "age": null, "role": "driver|passenger|pedestrian|cyclist",
          "is_injured": true, "injury_severity": "fatal|incapacitating|non_incapacitating|none",
          "city_residence": "" }
      ],
      "press_release_url": "string|null"
    }
  ]
}
ONLY recent crashes (within ~7 days of today). Empty crashes:[] if none found.`;

  return await extractJson(db, {
    pipeline: 'pd-press',
    systemPrompt: 'Extract crash data from police press releases as JSON. Only recent crashes.',
    userPrompt: prompt,
    tier: 'auto',
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
  const results = { pds_polled: 0, crashes_found: 0, incidents_created: 0, incidents_matched: 0,
                    persons_added: 0, errors: [] };
  const startTime = Date.now();
  const TIME_BUDGET = 50000;

  try {
    let ds = await db('data_sources').where('name', 'PD Press Releases').first();
    if (!ds) {
      const dsId = uuidv4();
      await db('data_sources').insert({
        id: dsId, name: 'PD Press Releases', type: 'police_report',
        provider: 'direct-scrape', api_endpoint: '12 PD news pages',
        is_active: true, last_polled_at: new Date(),
        created_at: new Date(), updated_at: new Date()
      });
      ds = { id: dsId };
    }

    const newIncidents = [];
    const newReports = [];
    const newPersons = [];

    for (const pd of PD_PAGES) {
      if (Date.now() - startTime > TIME_BUDGET) break;
      try {
        const html = await fetchPage(pd.url);
        if (!html) {
          results.errors.push(`${pd.name}: fetch failed`);
          continue;
        }
        results.pds_polled++;

        const parsed = await extractCrashes(db, html, pd);
        const crashes = parsed?.crashes || [];
        results.crashes_found += crashes.length;

        for (const c of crashes) {
          if (Date.now() - startTime > TIME_BUDGET) break;
          try {
            const ref = `pd-press:${pd.name}:${c.title || ''}:${c.occurred_at || ''}`.substring(0, 400);
            if (dedupCache.has(ref)) continue;
            const exists = await db('source_reports').where('source_reference', ref).first();
            if (exists) { dedupCache.set(ref, 1); continue; }
            dedupCache.set(ref, 1);

            const now = new Date();

            // Geo match
            let matchId = null;
            if (c.lat && c.lng) {
              try {
                const m = await db.raw(`
                  SELECT id FROM incidents WHERE occurred_at > NOW() - INTERVAL '7 days'
                    AND geom IS NOT NULL
                    AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, 1500)
                  LIMIT 1`, [c.lng, c.lat]);
                matchId = m.rows?.[0]?.id;
              } catch (_) {}
            }

            const incidentId = uuidv4();
            const targetId = matchId || incidentId;

            if (matchId) {
              results.incidents_matched++;
              await db('incidents').where('id', matchId).update({
                source_count: db.raw('COALESCE(source_count, 1) + 1'),
                confidence_score: db.raw('LEAST(99, COALESCE(confidence_score, 50) + 15)'),
                police_department: db.raw(`COALESCE(police_department, ?)`, [pd.name]),
                police_report_number: db.raw(`COALESCE(police_report_number, ?)`, [c.police_report_number || null]),
                updated_at: now
              });
            } else {
              const inc = normalizeIncident({
                id: incidentId,
                incident_number: `PRESS-${now.getFullYear().toString().slice(-2)}${String(now.getMonth()+1).padStart(2,'0')}${String(now.getDate()).padStart(2,'0')}-${Math.floor(Math.random()*9999).toString().padStart(4,'0')}`,
                incident_type: c.incident_type,
                severity: c.severity,
                status: 'verified',
                confidence_score: 88,
                address: c.address || c.summary || c.title,
                city: pd.metro, state: pd.state,
                highway: c.highway,
                latitude: c.lat, longitude: c.lng,
                occurred_at: c.occurred_at,
                description: `${pd.name} press release: ${c.title}\n${c.summary || ''}`,
                injuries_count: c.injuries_count, fatalities_count: c.fatalities_count,
                police_department: pd.name, police_report_number: c.police_report_number,
                source_count: 1, first_source_id: ds.id,
                tags: ['pd_press', pd.metro.toLowerCase().replace(/ /g, '_')]
              });
              newIncidents.push(inc);
              results.incidents_created++;
            }

            newReports.push({
              id: uuidv4(), incident_id: targetId, data_source_id: ds.id,
              source_type: 'police_report', source_reference: ref,
              raw_data: JSON.stringify({ pd: pd.name, crash: c }),
              parsed_data: JSON.stringify(c),
              contributed_fields: ['victims', 'severity', 'description', 'police_department'],
              confidence: 88, is_verified: true,
              fetched_at: now, processed_at: now, created_at: now
            });

            for (const v of (c.victims || [])) {
              if (!v.full_name) continue;
              const exists = await db('persons').where('incident_id', targetId).whereRaw('LOWER(full_name) = LOWER(?)', [v.full_name.trim()]).first();
              if (exists) continue;
              const person = normalizePerson({
                incident_id: targetId,
                full_name: v.full_name,
                age: v.age, role: v.role,
                is_injured: !!v.is_injured,
                injury_severity: v.injury_severity,
                city: v.city_residence,
                state: pd.state,
                contact_status: 'not_contacted',
                confidence_score: 88,
                metadata: { source: 'pd_press', pd: pd.name, press_url: c.press_release_url }
              });
              person.id = uuidv4();
              newPersons.push(person);
            }
          } catch (e) {
            results.errors.push(`${pd.name} crash: ${e.message}`);
          }
        }
      } catch (e) {
        await reportError(db, 'pd-press', pd.name, e.message);
      }
    }

    if (newIncidents.length) await batchInsert(db, 'incidents', newIncidents);
    if (newReports.length) await batchInsert(db, 'source_reports', newReports);
    if (newPersons.length) {
      const r = await batchInsert(db, 'persons', newPersons);
      results.persons_added = r.inserted;
      for (const p of newPersons) {
        if (p.full_name) {
          await enqueueCascade(db, { person_id: p.id, incident_id: p.incident_id, trigger_source: 'pipeline_insert', priority: 7 }).catch(()=>{});
          // Phase 21 Wire #5: queue elevated-priority cascades pulling news+court for this name
          await enqueueCascade(db, {
            person_id: p.id, incident_id: p.incident_id,
            trigger_source: 'pd_press_name_pull',
            trigger_field: 'full_name', trigger_value: p.full_name,
            priority: 8
          }).catch(()=>{});
        }
      }
    }

    await db('data_sources').where('id', ds.id).update({
      last_polled_at: new Date(), last_success_at: new Date(), updated_at: new Date()
    });

    res.json({
      success: true,
      message: `PD Press: ${results.pds_polled} pds polled, ${results.crashes_found} crashes, ${results.incidents_created} new, ${results.persons_added} persons`,
      ...results,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'pd-press', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
