/**
 * Phase 70: Combined ingest for 3 new sources — NYC OpenData, NTSB Aviation, Spanish News.
 * Single endpoint to keep router slot count tight. Each source is its own action.
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function ingestNYC(db, days = 3, limit = 100) {
  const since = new Date(Date.now() - days*86400*1000).toISOString().split('T')[0];
  const url = `https://data.cityofnewyork.us/resource/h9gi-nx95.json?$order=crash_date+DESC&$limit=${limit}`;
  try {
    const r = await fetch(url, { signal: AbortSignal.timeout(15000) });
    if (!r.ok) return { ok: false, source: 'nyc', status: r.status };
    const rows = await r.json();
    let inserted = 0, skipped = 0;
    for (const x of rows) {
      const id = x.collision_id;
      if (!id) continue;
      const ref = `nyc-opendata:${id}`;
      const exists = await db('incidents').where('incident_number', ref).first();
      if (exists) { skipped++; continue; }
      const killed = parseInt(x.number_of_persons_killed) || 0;
      const injured = parseInt(x.number_of_persons_injured) || 0;
      const severity = killed > 0 ? 'fatal' : (injured > 0 ? 'critical' : 'minor');
      const score = killed > 0 ? 60 : (injured > 0 ? 40 : 20);
      const { v4: uuid } = require('uuid');
      try {
        await db('incidents').insert({
          id: uuid(),
          incident_number: ref,
          state: 'NY',
          city: (x.borough || 'New York').toLowerCase().replace(/\b\w/g, c => c.toUpperCase()),
          severity,
          incident_type: 'car_accident',
          fatalities_count: killed,
          description: `${x.on_street_name || 'Unknown'} ${x.cross_street_name ? '& ' + x.cross_street_name : ''} · ${x.borough || 'NYC'} · ${killed} killed, ${injured} injured · ${x.contributing_factor_vehicle_1 || ''}`.slice(0, 500),
          raw_description: JSON.stringify(x).slice(0, 4000),
          latitude: parseFloat(x.latitude) || null,
          longitude: parseFloat(x.longitude) || null,
          occurred_at: x.crash_date ? new Date(x.crash_date) : new Date(),
          discovered_at: new Date(),
          qualification_state: 'pending',
          lead_score: score,
          source_count: 1
        });
        inserted++;
      } catch (e) { skipped++; }
    }
    return { ok: true, source: 'nyc', fetched: rows.length, inserted, skipped };
  } catch (e) { return { ok: false, source: 'nyc', error: e.message }; }
}

async function ingestNTSB(db, days = 30) {
  // NTSB CAROL has no public JSON API (carol.ntsb.gov is a SPA, data.ntsb.gov/avdata
  // is MDB zips). Closest stable public source is the Aviation Safety Network
  // (aviation-safety.net) year-listing HTML table — covers all NTSB-investigated
  // aviation accidents plus international, with fatalities counted. 25s budget,
  // graceful 404/HTML fallback. `days` retained for signature compat.
  const startedAt = Date.now();
  const BUDGET_MS = 25000;
  void days;
  const { v4: uuid } = require('uuid');
  const year = new Date().getUTCFullYear();
  const candidates = [
    `https://aviation-safety.net/database/year/${year}`,
    `https://aviation-safety.net/database/year/${year - 1}`
  ];
  let html = null, hitUrl = null;
  for (const url of candidates) {
    if (Date.now() - startedAt > BUDGET_MS - 4000) break;
    try {
      const r = await fetch(url, {
        signal: AbortSignal.timeout(12000),
        headers: { 'User-Agent': 'AccidentCommandCenter/1.0', 'Accept': 'text/html' }
      });
      if (!r.ok) continue;
      const text = await r.text();
      if (text && text.length > 2000 && /<tr class="list">/.test(text)) {
        html = text; hitUrl = url; break;
      }
    } catch { /* try next */ }
  }
  if (!html) return { ok: false, source: 'ntsb', error: 'endpoint_unreachable', fetched: 0, inserted: 0, skipped: 0 };

  // Parse list rows. Each row:
  // <tr class="list">
  //   <td class="list"><span class="nobr"><a href=/wikibase/569665>27 Apr 2026</a></span></td>
  //   <td class="list">Cessna 208B Grand Caravan</td>
  //   <td class="list">5Y-NOK</td>
  //   <td class="list">CityLink Africa Airways</td>
  //   <td class="list">14</td>
  //   <td class="list">about 20 km SW of Juba</td>
  //   <td class="list"><img src="/database/country/flags_15/STss.gif" /></td>
  //   <td class="list">w/o</td>...
  // </tr>
  const rowRe = /<tr class="list">([\s\S]*?)<\/tr>/g;
  const rows = [...html.matchAll(rowRe)].slice(0, 60);
  let inserted = 0, skipped = 0;
  for (const m of rows) {
    if (Date.now() - startedAt > BUDGET_MS - 1000) break;
    const block = m[1];
    const cells = [...block.matchAll(/<td class="list">([\s\S]*?)<\/td>/g)].map(c => c[1]);
    if (cells.length < 6) { skipped++; continue; }
    const linkM = cells[0].match(/<a href=([^>]+)>([^<]+)<\/a>/);
    const recPath = linkM ? linkM[1].replace(/^['"]|['"]$/g, '') : null;
    const dateStr = linkM ? linkM[2].trim() : '';
    const idM = recPath ? recPath.match(/\/(\d+)$/) : null;
    const id = idM ? idM[1] : null;
    if (!id) { skipped++; continue; }
    const ref = `ntsb:${id}`;
    const exists = await db('incidents').where('incident_number', ref).first();
    if (exists) { skipped++; continue; }
    const aircraftType = cells[1].replace(/<[^>]+>/g, '').trim();
    const reg = cells[2].replace(/<[^>]+>/g, '').trim();
    const operator = cells[3].replace(/<[^>]+>/g, '').trim();
    const fatals = parseInt(cells[4].replace(/<[^>]+>/g, '').trim()) || 0;
    const location = cells[5].replace(/<[^>]+>/g, '').trim();
    const flagM = cells[6] ? cells[6].match(/flags_15\/([A-Za-z0-9]+)\.gif/) : null;
    const countryCode = flagM ? flagM[1] : null;
    const dmg = cells[7] ? cells[7].replace(/<[^>]+>/g, '').trim() : '';
    const occurredAt = (() => {
      const d = new Date(dateStr);
      return isNaN(d.getTime()) ? new Date() : d;
    })();
    const severity = fatals > 0 ? 'fatal' : (dmg && /w\/o|destroyed/i.test(dmg) ? 'serious' : 'moderate');
    const score = fatals > 0 ? 50 : 30;
    // Best-effort US filter for state column — ASN uses 'N' for USA registrations
    const stateGuess = (countryCode === 'N' || /\b(USA|United States)\b/i.test(location)) ? 'US' : null;
    try {
      await db('incidents').insert({
        id: uuid(),
        incident_number: ref,
        state: stateGuess,
        city: location ? location.slice(0, 80) : null,
        severity,
        incident_type: 'aviation_accident',
        fatalities_count: fatals,
        description: `Aviation: ${aircraftType} ${reg ? '(' + reg + ')' : ''} ${operator ? '· ' + operator : ''} · ${location} · ${fatals} fatal · ${dmg}`.slice(0, 500),
        raw_description: block.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 4000),
        occurred_at: occurredAt,
        discovered_at: new Date(),
        qualification_state: 'pending',
        lead_score: score,
        source_count: 1
      });
      inserted++;
    } catch { skipped++; }
  }
  return { ok: true, source: 'ntsb', endpoint: hitUrl, fetched: rows.length, inserted, skipped };
}

const SPANISH_FEEDS = [
  'https://laopinion.com/feed/',
  'https://laopinion.com/category/noticias/feed/',
  'https://eldiariony.com/feed/',
  'https://www.diariolasamericas.com/rss/local'
];
const ES_KEYWORDS = /\b(accidente|choque|atropell|v[ií]ctimas?|fallec|colisi[oó]n|volcadura|veh[ií]culo|herid|muere|muri[oó]|muert|lesionad|impacto|tragedia|tiroteo|disparos?|asesin|homicidio|trágico|fatal|deceso|emergencia|ambulancia|incendio|explosi[oó]n)\b/i;
const ES_LOC_RE = /\ben\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)*),\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)/g;

async function ingestSpanish(db) {
  const { v4: uuid } = require('uuid');
  let total_inserted = 0, total_skipped = 0;
  const feed_results = [];
  for (const feedUrl of SPANISH_FEEDS) {
    try {
      const r = await fetch(feedUrl, { signal: AbortSignal.timeout(8000), headers: { 'User-Agent': 'AccidentCommandCenter/1.0' } });
      if (!r.ok) { feed_results.push({ feed: feedUrl, status: r.status }); continue; }
      const xml = await r.text();
      // Crude RSS parse — extract item blocks
      const items = [...xml.matchAll(/<item[^>]*>([\s\S]*?)<\/item>/g)].slice(0, 30);
      let feed_ins = 0;
      for (const item of items) {
        const block = item[1];
        const titleM = block.match(/<title[^>]*>(?:<!\[CDATA\[)?([^<\]]+)/);
        const descM = block.match(/<description[^>]*>(?:<!\[CDATA\[)?([^<\]]+)/);
        const linkM = block.match(/<link[^>]*>([^<]+)<\/link>/);
        const text = ((titleM?.[1] || '') + ' ' + (descM?.[1] || '')).slice(0, 2000);
        if (!ES_KEYWORDS.test(text)) continue;
        const url = linkM?.[1] || '';
        if (!url) continue;
        const ref = `spanish-news:${url.slice(-100)}`;
        const exists = await db('incidents').where('incident_number', ref).first();
        if (exists) { total_skipped++; continue; }
        // Detect fatal/severity from text
        const isFatal = /muert|fallec/.test(text.toLowerCase());
        const isCritical = /herid|grave|hospitalizad/.test(text.toLowerCase());
        const severity = isFatal ? 'fatal' : (isCritical ? 'critical' : 'moderate');
        const score = isFatal ? 50 : (isCritical ? 35 : 20);
        try {
          await db('incidents').insert({
            id: uuid(),
            incident_number: ref,
            severity,
            incident_type: 'car_accident',
            fatalities_count: isFatal ? 1 : 0,
            description: (titleM?.[1] || '').slice(0, 500),
            raw_description: text.slice(0, 4000),
            occurred_at: new Date(),
            discovered_at: new Date(),
            qualification_state: 'pending',
            lead_score: score,
            source_count: 1
          });
          feed_ins++;
          total_inserted++;
        } catch { total_skipped++; }
      }
      feed_results.push({ feed: feedUrl, fetched: items.length, inserted: feed_ins });
    } catch (e) { feed_results.push({ feed: feedUrl, error: e.message }); }
    await new Promise(r => setTimeout(r, 1000));
  }
  return { ok: true, source: 'spanish-news', total_inserted, total_skipped, feed_results };
}


async function ingestCaliforniaCHP(db) {
  // California Highway Patrol public incident feed (XML)
  // Free, no key. Returns active CA highway incidents in real time.
  const url = 'https://cad.chp.ca.gov/Traffic.aspx';
  const { v4: uuid } = require('uuid');
  let inserted = 0, skipped = 0;
  try {
    const r = await fetch(url, { signal: AbortSignal.timeout(15000), headers: { 'User-Agent': 'AccidentCommandCenter/1.0' } });
    if (!r.ok) return { ok: false, source: 'ca-chp', status: r.status };
    const html = await r.text();
    // Extract incident table rows
    const rowRe = /<tr[^>]*class=["']?(?:Trf_Inc[A-Za-z0-9_]*)?["']?[^>]*>([\s\S]*?)<\/tr>/g;
    const rows = [...html.matchAll(rowRe)].slice(0, 100);
    for (const m of rows) {
      const cells = [...m[1].matchAll(/<td[^>]*>(?:<[^>]+>)?([^<]*?)(?:<[^>]+>)?<\/td>/g)].map(c => c[1].trim());
      if (cells.length < 5) continue;
      const incNo = cells[0];
      const time = cells[1];
      const type = cells[2];
      const location = cells[3];
      const area = cells[4];
      if (!incNo || !type) continue;
      // Only crashes/collisions, not traffic alerts
      if (!/crash|collision|injur|fatal|trfc|hit|run/i.test(type)) continue;
      const ref = `ca-chp:${incNo}`;
      const exists = await db('incidents').where('incident_number', ref).first();
      if (exists) { skipped++; continue; }
      const isFatal = /fatal|fatality/i.test(type);
      const isInjury = /injury|injur/i.test(type);
      try {
        await db('incidents').insert({
          id: uuid(),
          incident_number: ref,
          state: 'CA',
          city: area || null,
          severity: isFatal ? 'fatal' : (isInjury ? 'critical' : 'moderate'),
          incident_type: 'car_accident',
          fatalities_count: isFatal ? 1 : 0,
          description: `${type} · ${location} · ${area} · CHP #${incNo}`.slice(0, 500),
          raw_description: m[1].slice(0, 4000),
          occurred_at: new Date(),
          discovered_at: new Date(),
          qualification_state: 'pending',
          lead_score: isFatal ? 50 : (isInjury ? 35 : 15),
          source_count: 1
        });
        inserted++;
      } catch { skipped++; }
    }
    return { ok: true, source: 'ca-chp', fetched: rows.length, inserted, skipped };
  } catch (e) { return { ok: false, source: 'ca-chp', error: e.message }; }
}


async function ingestChicago(db, days = 7, limit = 100) {
  const since = new Date(Date.now() - days * 86400 * 1000).toISOString();
  const url = `https://data.cityofchicago.org/resource/85ca-t3if.json?$where=crash_date>'${since.split('.')[0]}'&$order=crash_date+DESC&$limit=${limit}`;
  try {
    const r = await fetch(url, { signal: AbortSignal.timeout(15000) });
    if (!r.ok) return { ok: false, source: 'chicago', status: r.status };
    const rows = await r.json();
    let inserted = 0, skipped = 0;
    const { v4: uuid } = require('uuid');
    for (const x of rows) {
      const id = x.crash_record_id; if (!id) continue;
      const ref = `chicago:${id}`;
      const exists = await db('incidents').where('incident_number', ref).first();
      if (exists) { skipped++; continue; }
      const killed = parseInt(x.injuries_fatal) || 0;
      const injured = parseInt(x.injuries_total) || 0;
      const sev = killed > 0 ? 'fatal' : (injured > 0 ? 'critical' : 'minor');
      try {
        await db('incidents').insert({
          id: uuid(), incident_number: ref, state: 'IL', city: 'Chicago',
          severity: sev, incident_type: 'car_accident', fatalities_count: killed,
          description: `${x.first_crash_type || 'Crash'} · ${x.trafficway_type || ''} · ${killed} killed, ${injured} injured`.slice(0, 500),
          raw_description: JSON.stringify(x).slice(0, 4000),
          occurred_at: x.crash_date ? new Date(x.crash_date) : new Date(),
          discovered_at: new Date(), qualification_state: 'pending',
          lead_score: killed > 0 ? 60 : (injured > 0 ? 40 : 15), source_count: 1
        });
        inserted++;
      } catch { skipped++; }
    }
    return { ok: true, source: 'chicago', fetched: rows.length, inserted, skipped };
  } catch (e) { return { ok: false, source: 'chicago', error: e.message }; }
}

async function ingestLA(db, days = 30, limit = 100) {
  // LA Crime Data (d5tf-ez2w) is the only LA Socrata dataset that includes 'TRAFFIC COLLISION'
  // crime-code records. Dataset cadence is irregular (often months stale), so date-since
  // filtering returns 0 rows. Pull most-recent N TRAFFIC COLLISION rows ordered by date_occ DESC
  // and dedupe via incidents.incident_number — `days` arg retained for signature but unused.
  const startedAt = Date.now();
  const BUDGET_MS = 25000;
  void days; // signature kept for backward compat
  const where = encodeURIComponent("crm_cd_desc='TRAFFIC COLLISION'");
  const url = `https://data.lacity.org/resource/d5tf-ez2w.json?$where=${where}&$order=date_occ+DESC&$limit=${limit}`;
  try {
    const r = await fetch(url, { signal: AbortSignal.timeout(15000), headers: { 'Accept': 'application/json' } });
    if (!r.ok) return { ok: false, source: 'la', status: r.status, fetched: 0, inserted: 0, skipped: 0 };
    const rows = await r.json();
    if (!Array.isArray(rows)) return { ok: false, source: 'la', error: 'non_array_response', fetched: 0, inserted: 0, skipped: 0 };
    let inserted = 0, skipped = 0;
    const { v4: uuid } = require('uuid');
    for (const x of rows) {
      if (Date.now() - startedAt > BUDGET_MS - 1000) break;
      const id = x.dr_no; if (!id) { skipped++; continue; }
      const ref = `la:${id}`;
      const exists = await db('incidents').where('incident_number', ref).first();
      if (exists) { skipped++; continue; }
      // location_1 wrapper carries lat/lon for d5tf-ez2w
      const lat = parseFloat(x.location_1?.latitude || x.lat) || null;
      const lng = parseFloat(x.location_1?.longitude || x.lon) || null;
      const age = parseInt(x.vict_age) || 0;
      const severity = age > 0 ? 'moderate' : 'minor';
      try {
        await db('incidents').insert({
          id: uuid(), incident_number: ref, state: 'CA', city: 'Los Angeles',
          severity, incident_type: 'car_accident',
          description: `${x.crm_cd_desc || 'Traffic Collision'} · ${x.area_name || ''} · ${x.location || ''} ${x.cross_street ? '& ' + x.cross_street : ''} · victim age ${x.vict_age || '?'}`.slice(0, 500),
          raw_description: JSON.stringify(x).slice(0, 4000),
          latitude: lat,
          longitude: lng,
          occurred_at: x.date_occ ? new Date(x.date_occ) : new Date(),
          discovered_at: new Date(), qualification_state: 'pending',
          lead_score: 25, source_count: 1
        });
        inserted++;
      } catch { skipped++; }
    }
    return { ok: true, source: 'la', endpoint: url, fetched: rows.length, inserted, skipped };
  } catch (e) { return { ok: false, source: 'la', error: e.message, fetched: 0, inserted: 0, skipped: 0 }; }
}

async function ingestSF(db, limit = 100) {
  const url = `https://data.sfgov.org/resource/ubvf-ztfx.json?$order=collision_date+DESC&$limit=${limit}`;
  try {
    const r = await fetch(url, { signal: AbortSignal.timeout(15000) });
    if (!r.ok) return { ok: false, source: 'sf', status: r.status };
    const rows = await r.json();
    let inserted = 0, skipped = 0;
    const { v4: uuid } = require('uuid');
    for (const x of rows) {
      const id = x.unique_id || x.case_id_pkey; if (!id) continue;
      const ref = `sf:${id}`;
      const exists = await db('incidents').where('incident_number', ref).first();
      if (exists) { skipped++; continue; }
      try {
        await db('incidents').insert({
          id: uuid(), incident_number: ref, state: 'CA', city: 'San Francisco',
          severity: 'moderate', incident_type: 'car_accident',
          description: `SF collision ${x.juris || ''} · ${x.time_cat || ''}`.slice(0, 500),
          raw_description: JSON.stringify(x).slice(0, 4000),
          latitude: parseFloat(x.tb_latitude) || null,
          longitude: parseFloat(x.tb_longitude) || null,
          occurred_at: x.collision_date ? new Date(x.collision_date) : new Date(),
          discovered_at: new Date(), qualification_state: 'pending',
          lead_score: 25, source_count: 1
        });
        inserted++;
      } catch { skipped++; }
    }
    return { ok: true, source: 'sf', fetched: rows.length, inserted, skipped };
  } catch (e) { return { ok: false, source: 'sf', error: e.message }; }
}

async function ingestGoFundMe(db) {
  // Phase 89 rebuild: Brave Search HTML for discovery + per-campaign Apollo state parse.
  // GoFundMe blocks /s?q= and /discover/* search results (server-rendered without listings),
  // and exposes no public sitemap-fundraisers. Brave HTML site:gofundme.com/f reliably surfaces
  // accident-keyword campaigns. Each campaign page embeds __NEXT_DATA__ → __APOLLO_STATE__
  // with {title, currentAmount.amount, goalAmount.amount, location.{city,statePrefix}, createdAt, description}.
  const { v4: uuid } = require('uuid');
  const startedAt = Date.now();
  const BUDGET_MS = 25000;
  const FETCH_TIMEOUT_MS = 8000;
  const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';
  const terms = ['car accident', 'crash victim', 'hit and run', 'fatal accident', 'traffic accident', 'motorcycle accident'];
  const results = [];
  let total_inserted = 0, total_skipped = 0, total_seen = 0, total_errors = 0;
  const seenSlugs = new Set();
  const budgetLeft = () => BUDGET_MS - (Date.now() - startedAt);

  // 1. Discovery — Brave Search HTML site:-search per term (no API key, free, fast)
  const discoveredUrls = [];
  for (const term of terms) {
    if (budgetLeft() < 6000) break;
    try {
      const q = encodeURIComponent(`site:gofundme.com/f "${term}"`);
      const r = await fetch(`https://search.brave.com/search?q=${q}`, {
        headers: { 'User-Agent': UA, 'Accept': 'text/html' },
        signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
      });
      if (!r.ok) { results.push({ term, discovery_status: r.status }); continue; }
      const html = await r.text();
      const re = /https?:\/\/(?:www\.)?gofundme\.com\/f\/([a-zA-Z0-9_-]+)/g;
      const slugs = new Set();
      let m;
      while ((m = re.exec(html)) !== null) {
        const slug = m[1];
        if (slug && slug.length > 3 && !seenSlugs.has(slug)) { slugs.add(slug); seenSlugs.add(slug); }
      }
      for (const slug of slugs) discoveredUrls.push({ term, slug, url: `https://www.gofundme.com/f/${slug}` });
      results.push({ term, discovered: slugs.size });
    } catch (e) { total_errors++; results.push({ term, error: String(e.message || e).slice(0, 120) }); }
  }

  // 2. Pre-filter: skip slugs already in DB to save fetch budget
  const newCampaigns = [];
  for (const c of discoveredUrls) {
    const ref = `gofundme:${c.slug}`;
    const exists = await db('incidents').where('incident_number', ref).first();
    if (exists) { total_skipped++; continue; }
    newCampaigns.push(c);
  }

  // 3. Fetch each new campaign page → parse __NEXT_DATA__ → Apollo Fundraiser:* node
  for (const c of newCampaigns) {
    if (budgetLeft() < 3000) break;
    total_seen++;
    try {
      const r = await fetch(c.url, {
        headers: { 'User-Agent': UA, 'Accept': 'text/html' },
        signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
      });
      if (!r.ok) { total_errors++; continue; }
      const html = await r.text();
      const m = html.match(/<script id="__NEXT_DATA__" type="application\/json">([\s\S]+?)<\/script>/);
      if (!m) { total_errors++; continue; }
      let data;
      try { data = JSON.parse(m[1]); } catch { total_errors++; continue; }
      const apollo = data?.props?.pageProps?.__APOLLO_STATE__ || {};
      let fr = null;
      for (const k of Object.keys(apollo)) { if (k.startsWith('Fundraiser:')) { fr = apollo[k]; break; } }
      if (!fr) { total_errors++; continue; }

      const title = (fr.title || '').toString().slice(0, 200);
      const raised = fr.currentAmount?.amount;
      const goal = fr.goalAmount?.amount;
      const currency = fr.currentAmount?.currencyCode || 'USD';
      const loc = fr.location || {};
      const city = (loc.city || '').toString().slice(0, 80);
      const state = (loc.statePrefix || '').toString().slice(0, 8);
      const country = (loc.countryCode || '').toString().slice(0, 4);
      const createdAt = fr.createdAt ? new Date(fr.createdAt) : new Date();
      const occurredAt = isNaN(createdAt.getTime()) ? new Date() : createdAt;
      let descRaw = fr['description({"excerpt":false})'] || fr['description({"excerpt":true})'] || '';
      const descText = String(descRaw).replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();

      // Severity: fatal if death keywords in title+desc
      const blob = `${title} ${descText}`.toLowerCase();
      const fatal = /\b(fatal|killed|died|deceased|passed away|funeral|burial|laid to rest|in memory of|memorial fund)\b/.test(blob);
      const severity = fatal ? 'fatal' : 'serious';

      const locStr = [city, state].filter(Boolean).join(', ') || country || 'Unknown';
      const moneyStr = (raised != null && goal != null)
        ? `raised $${Math.round(raised).toLocaleString()} of $${Math.round(goal).toLocaleString()} ${currency}`
        : 'fundraiser active';
      const dateStr = occurredAt.toISOString().split('T')[0];
      const description = `GoFundMe campaign: ${title} · ${locStr} · ${moneyStr} · ${dateStr}`.slice(0, 800);

      const ref = `gofundme:${c.slug}`;
      try {
        const insertRow = {
          id: uuid(),
          incident_number: ref,
          severity,
          incident_type: 'car_accident',
          description,
          raw_description: descText.slice(0, 4000),
          source_url: c.url,
          city: city || null,
          state: state || null,
          occurred_at: occurredAt,
          discovered_at: new Date(),
          qualification_state: 'pending',
          lead_score: 50,
          source_count: 1,
        };
        // Drop columns that may not exist on this schema — knex will error on bad keys.
        // Check column existence cheaply on first insert by trying full row, fallback minimal.
        try {
          await db('incidents').insert(insertRow);
        } catch (colErr) {
          // Retry with minimal canonical columns only
          await db('incidents').insert({
            id: insertRow.id,
            incident_number: ref,
            severity,
            incident_type: 'car_accident',
            description,
            raw_description: insertRow.raw_description,
            occurred_at: occurredAt,
            discovered_at: new Date(),
            qualification_state: 'pending',
            lead_score: 50,
            source_count: 1,
          });
        }
        total_inserted++;
      } catch (insErr) {
        total_skipped++;
      }
      // Rate limit: 1s between campaign fetches
      await new Promise(res => setTimeout(res, 1000));
    } catch (e) {
      total_errors++;
    }
  }

  return {
    ok: true,
    source: 'gofundme',
    discovery: 'brave-search-html',
    terms_searched: terms.length,
    candidates_discovered: discoveredUrls.length,
    new_after_dedup: newCampaigns.length,
    total_inserted,
    total_skipped,
    total_errors,
    elapsed_ms: Date.now() - startedAt,
    results,
  };
}


// ────────────────────────────────────────────────────────────────────────────
// Phase 90: State DOT 511 traffic feeds — TX, GA, CA highways
// All three states publish 511 incident data, but endpoint stability varies.
// Each function tries multiple candidate URLs and degrades gracefully on 404/HTML.
// ────────────────────────────────────────────────────────────────────────────

async function _fetchJsonOrHtml(url, opts = {}) {
  // Helper: fetch + classify response. Returns { ok, status, body, kind }
  // kind: 'json' | 'html' | 'text' | 'error'
  try {
    const r = await fetch(url, {
      signal: AbortSignal.timeout(opts.timeout || 10000),
      headers: {
        'User-Agent': opts.ua || 'AccidentCommandCenter/1.0',
        'Accept': opts.accept || 'application/json, text/html;q=0.9'
      },
      redirect: 'follow'
    });
    if (!r.ok) return { ok: false, status: r.status, kind: 'error' };
    const ct = (r.headers.get('content-type') || '').toLowerCase();
    const text = await r.text();
    if (ct.includes('json') || (text.trim().startsWith('{') || text.trim().startsWith('['))) {
      try { return { ok: true, status: 200, body: JSON.parse(text), kind: 'json' }; }
      catch { return { ok: false, status: 200, kind: 'error', error: 'json_parse_failed' }; }
    }
    if (text.trim().startsWith('<')) return { ok: true, status: 200, body: text, kind: 'html' };
    return { ok: true, status: 200, body: text, kind: 'text' };
  } catch (e) { return { ok: false, kind: 'error', error: String(e.message || e).slice(0, 120) }; }
}

async function ingestTX511(db) {
  // Texas DOT — DriveTexas / TxDOT ITS. Endpoints rotate; try several.
  const { v4: uuid } = require('uuid');
  const startedAt = Date.now();
  const BUDGET_MS = 25000;
  const candidates = [
    'https://its.txdot.gov/ITS_WEB/FrontEnd/api/incident-list',
    'https://www.drivetexas.org/api/incidents/list',
    'https://drivetexas.org/api/v1/incidents'
  ];
  let resp = null, hitUrl = null;
  for (const url of candidates) {
    if (Date.now() - startedAt > BUDGET_MS - 4000) break;
    const r = await _fetchJsonOrHtml(url, { timeout: 10000 });
    if (r.ok && r.kind === 'json') { resp = r.body; hitUrl = url; break; }
  }
  if (!resp) return { ok: false, source: 'tx511', fetched: 0, inserted: 0, skipped: 0, error: 'endpoint_unreachable' };

  const rows = Array.isArray(resp) ? resp : (resp.incidents || resp.data || resp.results || resp.events || []);
  let inserted = 0, skipped = 0;
  for (const x of rows) {
    if (Date.now() - startedAt > BUDGET_MS - 1000) break;
    const id = x.incident_id || x.id || x.IncidentId || x.incidentId;
    if (!id) { skipped++; continue; }
    const cat = String(x.category || x.type || x.event_type || x.IncidentType || '').toLowerCase();
    if (!/crash|accident|collision/.test(cat)) { skipped++; continue; }
    const ref = `tx511:${id}`;
    const exists = await db('incidents').where('incident_number', ref).first();
    if (exists) { skipped++; continue; }
    const sevSrc = String(x.severity_level || x.severity || '').toLowerCase();
    let severity = 'serious';
    if (/fatal/.test(sevSrc)) severity = 'fatal';
    else if (/major|critical|severe/.test(sevSrc)) severity = 'critical';
    else if (/minor|low/.test(sevSrc)) severity = 'minor';
    const route = x.route || x.roadway || x.road || x.highway || '';
    const region = x.region || x.county || x.area || x.district || x.city || '';
    const details = x.description || x.summary || x.message || '';
    const eventType = x.category || x.type || 'Crash';
    const description = `${eventType} on ${route} in ${region} · ${details}`.slice(0, 500);
    const lat = parseFloat(x.latitude || x.lat || x.Latitude) || null;
    const lng = parseFloat(x.longitude || x.lng || x.lon || x.Longitude) || null;
    try {
      await db('incidents').insert({
        id: uuid(),
        incident_number: ref,
        state: 'TX',
        city: region || null,
        severity,
        incident_type: 'car_accident',
        fatalities_count: severity === 'fatal' ? 1 : 0,
        description,
        raw_description: JSON.stringify(x).slice(0, 4000),
        latitude: lat,
        longitude: lng,
        occurred_at: x.start_time ? new Date(x.start_time) : (x.created || x.timestamp ? new Date(x.created || x.timestamp) : new Date()),
        discovered_at: new Date(),
        qualification_state: 'pending',
        lead_score: severity === 'fatal' ? 50 : 35,
        source_count: 1
      });
      inserted++;
    } catch { skipped++; }
  }
  return { ok: true, source: 'tx511', endpoint: hitUrl, fetched: rows.length, inserted, skipped };
}

async function ingestGA511(db) {
  // Georgia DOT 511 — public event feed. May require API key (GDOT issues free keys).
  const { v4: uuid } = require('uuid');
  const startedAt = Date.now();
  const BUDGET_MS = 25000;
  const apiKey = process.env.GA511_API_KEY || '';
  const candidates = [
    apiKey ? `https://511ga.org/api/v2/events?key=${encodeURIComponent(apiKey)}&format=json` : null,
    'https://www.511ga.org/api/v1/event',
    'https://www.511ga.org/api/v2/event',
    'https://511ga.org/lg/services/getAllEvents?format=json'
  ].filter(Boolean);
  let resp = null, hitUrl = null;
  for (const url of candidates) {
    if (Date.now() - startedAt > BUDGET_MS - 4000) break;
    const r = await _fetchJsonOrHtml(url, { timeout: 10000 });
    if (r.ok && r.kind === 'json') { resp = r.body; hitUrl = url; break; }
  }
  if (!resp) return { ok: false, source: 'ga511', fetched: 0, inserted: 0, skipped: 0, error: 'endpoint_unreachable' };

  const rows = Array.isArray(resp) ? resp : (resp.events || resp.data || resp.results || resp.items || []);
  let inserted = 0, skipped = 0;
  for (const x of rows) {
    if (Date.now() - startedAt > BUDGET_MS - 1000) break;
    const id = x.id || x.event_id || x.eventId || x.EventID;
    if (!id) { skipped++; continue; }
    const eventType = String(x.event_type || x.EventType || x.type || x.eventCategory || '').toLowerCase();
    if (!/crash|accident|collision/.test(eventType) && !/crash|accident/.test(String(x.event_subtype || '').toLowerCase())) { skipped++; continue; }
    const ref = `ga511:${id}`;
    const exists = await db('incidents').where('incident_number', ref).first();
    if (exists) { skipped++; continue; }
    const sevSrc = String(x.severity || x.IncidentSeverity || '').toLowerCase();
    let severity = 'serious';
    if (/fatal/.test(sevSrc)) severity = 'fatal';
    else if (/major|critical|severe/.test(sevSrc)) severity = 'critical';
    const route = x.route || x.roadway_name || x.RoadwayName || x.road || '';
    const region = x.county || x.region || x.city || x.District || '';
    const details = x.description || x.headline || x.message || '';
    const description = `${x.event_type || 'Crash'} on ${route} in ${region} · ${details}`.slice(0, 500);
    const lat = parseFloat(x.latitude || x.Latitude || x.lat) || null;
    const lng = parseFloat(x.longitude || x.Longitude || x.lng || x.lon) || null;
    try {
      await db('incidents').insert({
        id: uuid(),
        incident_number: ref,
        state: 'GA',
        city: region || null,
        severity,
        incident_type: 'car_accident',
        fatalities_count: severity === 'fatal' ? 1 : 0,
        description,
        raw_description: JSON.stringify(x).slice(0, 4000),
        latitude: lat,
        longitude: lng,
        occurred_at: x.start_time ? new Date(x.start_time) : (x.LastUpdated || x.reported_time ? new Date(x.LastUpdated || x.reported_time) : new Date()),
        discovered_at: new Date(),
        qualification_state: 'pending',
        lead_score: severity === 'fatal' ? 50 : 35,
        source_count: 1
      });
      inserted++;
    } catch { skipped++; }
  }
  return { ok: true, source: 'ga511', endpoint: hitUrl, fetched: rows.length, inserted, skipped };
}

async function ingestCA511(db) {
  // California 511 / Caltrans / quickmap. api.511.org needs registered key (free signup).
  const { v4: uuid } = require('uuid');
  const startedAt = Date.now();
  const BUDGET_MS = 25000;
  const apiKey = process.env.CA511_API_KEY || '';
  const candidates = [
    apiKey ? `https://api.511.org/traffic/events?api_key=${encodeURIComponent(apiKey)}&format=json` : null,
    'https://quickmap.dot.ca.gov/data/chp-incidents.json',
    'https://quickmap.dot.ca.gov/data/lcs2way.json'
  ].filter(Boolean);
  let resp = null, hitUrl = null;
  for (const url of candidates) {
    if (Date.now() - startedAt > BUDGET_MS - 4000) break;
    const r = await _fetchJsonOrHtml(url, { timeout: 10000 });
    if (r.ok && r.kind === 'json') { resp = r.body; hitUrl = url; break; }
  }
  if (!resp) return { ok: false, source: 'ca511', fetched: 0, inserted: 0, skipped: 0, error: 'endpoint_unreachable' };

  const rows = Array.isArray(resp) ? resp : (resp.events || resp.data || resp.incidents || resp.results || []);
  let inserted = 0, skipped = 0;
  for (const x of rows) {
    if (Date.now() - startedAt > BUDGET_MS - 1000) break;
    const id = x.event_id || x.id || x.EventId || x.incident_id;
    if (!id) { skipped++; continue; }
    const eventType = String(x.event_type || x.type || x.headline || '').toLowerCase();
    if (eventType && !/crash|accident|collision|injur|fatal/.test(eventType)) { skipped++; continue; }
    const ref = `ca511:${id}`;
    const exists = await db('incidents').where('incident_number', ref).first();
    if (exists) { skipped++; continue; }
    const sevSrc = String(x.severity || '').toLowerCase();
    let severity = 'serious';
    if (/fatal/.test(sevSrc) || /fatal/.test(eventType)) severity = 'fatal';
    else if (/major|severe|critical/.test(sevSrc)) severity = 'critical';
    const roads = x.roads || x.road || x.route || x.highway || '';
    const route = Array.isArray(roads) ? (roads[0]?.name || roads[0]?.road || '') : roads;
    const region = x.areas?.[0]?.name || x.county || x.region || x.city || '';
    const details = x.description || x.headline || x.message || '';
    const description = `${x.event_type || 'Crash'} on ${route} in ${region} · ${details}`.slice(0, 500);
    const geo = x.geography || {};
    const coords = geo.coordinates || x.coordinates || [];
    const lat = parseFloat(coords[1] || x.latitude || x.lat) || null;
    const lng = parseFloat(coords[0] || x.longitude || x.lng || x.lon) || null;
    try {
      await db('incidents').insert({
        id: uuid(),
        incident_number: ref,
        state: 'CA',
        city: region || null,
        severity,
        incident_type: 'car_accident',
        fatalities_count: severity === 'fatal' ? 1 : 0,
        description,
        raw_description: JSON.stringify(x).slice(0, 4000),
        latitude: lat,
        longitude: lng,
        occurred_at: x.created ? new Date(x.created) : (x.updated ? new Date(x.updated) : new Date()),
        discovered_at: new Date(),
        qualification_state: 'pending',
        lead_score: severity === 'fatal' ? 50 : 35,
        source_count: 1
      });
      inserted++;
    } catch { skipped++; }
  }
  return { ok: true, source: 'ca511', endpoint: hitUrl, fetched: rows.length, inserted, skipped };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();

  if (action === 'health') return res.json({ success: true, service: 'new-sources',
    sources: ['nyc-opendata', 'chicago', 'la', 'sf', 'ntsb-aviation', 'spanish-news', 'ca-chp', 'gofundme', 'tx511', 'ga511', 'ca511'] });
  if (action === 'nyc') return res.json(await ingestNYC(db, parseInt(req.query?.days)||3, parseInt(req.query?.limit)||100));
  if (action === 'ntsb') return res.json(await ingestNTSB(db, parseInt(req.query?.days)||30));
  if (action === 'spanish') return res.json(await ingestSpanish(db));
  if (action === 'chp') return res.json(await ingestCaliforniaCHP(db));
  if (action === 'chicago') return res.json(await ingestChicago(db, parseInt(req.query?.days)||7, parseInt(req.query?.limit)||100));
  if (action === 'la') return res.json(await ingestLA(db, parseInt(req.query?.days)||30, parseInt(req.query?.limit)||100));
  if (action === 'sf') return res.json(await ingestSF(db, parseInt(req.query?.limit)||100));
  if (action === 'gofundme') return res.json(await ingestGoFundMe(db));
  if (action === 'tx511') return res.json(await ingestTX511(db));
  if (action === 'ga511') return res.json(await ingestGA511(db));
  if (action === 'ca511') return res.json(await ingestCA511(db));
  if (action === 'all') {
    const results = await Promise.all([ingestNYC(db, 3, 50), ingestChicago(db, 7, 50), ingestLA(db, 30, 50), ingestSF(db, 50), ingestNTSB(db, 14), ingestSpanish(db), ingestCaliforniaCHP(db), ingestGoFundMe(db), ingestTX511(db), ingestGA511(db), ingestCA511(db)]);
    const total = results.reduce((s, r) => s + (r.inserted || r.total_inserted || 0), 0);
    return res.json({ ok: true, total_inserted: total, results });
  }
  return res.status(400).json({ error: 'unknown action — use nyc, chicago, la, sf, ntsb, spanish, chp, gofundme, tx511, ga511, ca511, or all' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.ingestNYC = ingestNYC;
module.exports.ingestNTSB = ingestNTSB;
module.exports.ingestSpanish = ingestSpanish;
module.exports.ingestCaliforniaCHP = ingestCaliforniaCHP;
module.exports.ingestChicago = ingestChicago;
module.exports.ingestLA = ingestLA;
module.exports.ingestSF = ingestSF;
module.exports.ingestGoFundMe = ingestGoFundMe;
module.exports.ingestTX511 = ingestTX511;
module.exports.ingestGA511 = ingestGA511;
module.exports.ingestCA511 = ingestCA511;
