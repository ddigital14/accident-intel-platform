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
  // NTSB CAROL JSON endpoint
  const since = new Date(Date.now() - days*86400*1000).toISOString().split('T')[0];
  const url = `https://data.ntsb.gov/avdata/api/v2/aviation-data?start_date=${since}&format=json`;
  try {
    const r = await fetch(url, { signal: AbortSignal.timeout(15000), headers: { 'Accept': 'application/json' } });
    if (!r.ok) return { ok: false, source: 'ntsb', status: r.status };
    const text = await r.text();
    if (text.startsWith('<')) return { ok: false, source: 'ntsb', error: 'html_response' };
    let data;
    try { data = JSON.parse(text); } catch { return { ok: false, source: 'ntsb', error: 'json_parse' }; }
    const rows = Array.isArray(data) ? data : (data.events || data.data || []);
    let inserted = 0, skipped = 0;
    const { v4: uuid } = require('uuid');
    for (const x of rows.slice(0, 50)) {
      const id = x.ev_id || x.eventId || x.event_id;
      if (!id) continue;
      const ref = `ntsb:${id}`;
      const exists = await db('incidents').where('incident_number', ref).first();
      if (exists) { skipped++; continue; }
      const fatals = parseInt(x.inj_tot_f || x.injTotalFatal) || 0;
      const severity = fatals > 0 ? 'fatal' : 'serious';
      try {
        await db('incidents').insert({
          id: uuid(),
          incident_number: ref,
          state: x.ev_state || x.eventState || null,
          city: x.ev_city || x.eventCity || null,
          severity,
          incident_type: 'aviation_accident',
          fatalities_count: fatals,
          description: `Aviation incident: ${x.acft_make || ''} ${x.acft_model || ''} ${x.ev_state || ''}, ${x.ev_type || 'accident'}, ${fatals} fatal`.slice(0, 500),
          raw_description: JSON.stringify(x).slice(0, 4000),
          occurred_at: x.ev_date ? new Date(x.ev_date) : new Date(),
          discovered_at: new Date(),
          qualification_state: 'pending',
          lead_score: 30,
          source_count: 1
        });
        inserted++;
      } catch { skipped++; }
    }
    return { ok: true, source: 'ntsb', fetched: rows.length, inserted, skipped };
  } catch (e) { return { ok: false, source: 'ntsb', error: e.message }; }
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
  const since = new Date(Date.now() - days * 86400 * 1000).toISOString();
  const url = `https://data.lacity.org/resource/d5tf-ez2w.json?$where=date_occ>'${since.split('.')[0]}'+AND+crm_cd_desc='TRAFFIC COLLISION'&$order=date_occ+DESC&$limit=${limit}`;
  try {
    const r = await fetch(url, { signal: AbortSignal.timeout(15000) });
    if (!r.ok) return { ok: false, source: 'la', status: r.status };
    const rows = await r.json();
    let inserted = 0, skipped = 0;
    const { v4: uuid } = require('uuid');
    for (const x of rows) {
      const id = x.dr_no; if (!id) continue;
      const ref = `la:${id}`;
      const exists = await db('incidents').where('incident_number', ref).first();
      if (exists) { skipped++; continue; }
      try {
        await db('incidents').insert({
          id: uuid(), incident_number: ref, state: 'CA', city: 'Los Angeles',
          severity: 'moderate', incident_type: 'car_accident',
          description: `${x.crm_cd_desc || 'Traffic Collision'} · ${x.area_name || ''} · victim age ${x.vict_age || '?'}`.slice(0, 500),
          raw_description: JSON.stringify(x).slice(0, 4000),
          occurred_at: x.date_occ ? new Date(x.date_occ) : new Date(),
          discovered_at: new Date(), qualification_state: 'pending',
          lead_score: 25, source_count: 1
        });
        inserted++;
      } catch { skipped++; }
    }
    return { ok: true, source: 'la', fetched: rows.length, inserted, skipped };
  } catch (e) { return { ok: false, source: 'la', error: e.message }; }
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
  // GoFundMe Discover search for accident campaigns
  // Free public RSS-like search at gofundme.com/discover/<term>
  const terms = ['car-accident', 'crash-victim', 'hit-and-run', 'fatal-accident'];
  const { v4: uuid } = require('uuid');
  let total_inserted = 0, total_skipped = 0;
  const results = [];
  for (const term of terms) {
    try {
      const url = `https://www.gofundme.com/discover?term=${encodeURIComponent(term)}`;
      const r = await fetch(url, {
        headers: { 'User-Agent': 'AccidentCommandCenter/1.0' },
        signal: AbortSignal.timeout(10000)
      });
      if (!r.ok) { results.push({ term, status: r.status }); continue; }
      const html = await r.text();
      // Crude parse for campaign URLs
      const linkRe = /href="(https:\/\/www\.gofundme\.com\/f\/[a-z0-9-]+)"/g;
      const links = [...new Set([...html.matchAll(linkRe)].map(m => m[1]))].slice(0, 10);
      let term_inserted = 0;
      for (const link of links) {
        const ref = `gofundme:${link.split('/').pop()}`;
        const exists = await db('incidents').where('incident_number', ref).first();
        if (exists) { total_skipped++; continue; }
        try {
          await db('incidents').insert({
            id: uuid(), incident_number: ref,
            severity: 'serious', incident_type: 'car_accident',
            description: `GoFundMe campaign for ${term.replace(/-/g, ' ')}: ${link}`.slice(0, 500),
            raw_description: link,
            occurred_at: new Date(), discovered_at: new Date(),
            qualification_state: 'pending',
            lead_score: 40, source_count: 1
          });
          total_inserted++; term_inserted++;
        } catch { total_skipped++; }
      }
      results.push({ term, fetched: links.length, inserted: term_inserted });
      await new Promise(r => setTimeout(r, 1000));
    } catch (e) { results.push({ term, error: e.message }); }
  }
  return { ok: true, source: 'gofundme', total_inserted, total_skipped, results };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();

  if (action === 'health') return res.json({ success: true, service: 'new-sources',
    sources: ['nyc-opendata', 'chicago', 'la', 'sf', 'ntsb-aviation', 'spanish-news', 'ca-chp', 'gofundme'] });
  if (action === 'nyc') return res.json(await ingestNYC(db, parseInt(req.query?.days)||3, parseInt(req.query?.limit)||100));
  if (action === 'ntsb') return res.json(await ingestNTSB(db, parseInt(req.query?.days)||30));
  if (action === 'spanish') return res.json(await ingestSpanish(db));
  if (action === 'chp') return res.json(await ingestCaliforniaCHP(db));
  if (action === 'chicago') return res.json(await ingestChicago(db, parseInt(req.query?.days)||7, parseInt(req.query?.limit)||100));
  if (action === 'la') return res.json(await ingestLA(db, parseInt(req.query?.days)||30, parseInt(req.query?.limit)||100));
  if (action === 'sf') return res.json(await ingestSF(db, parseInt(req.query?.limit)||100));
  if (action === 'gofundme') return res.json(await ingestGoFundMe(db));
  if (action === 'all') {
    const results = await Promise.all([ingestNYC(db, 3, 50), ingestChicago(db, 7, 50), ingestLA(db, 30, 50), ingestSF(db, 50), ingestNTSB(db, 14), ingestSpanish(db), ingestCaliforniaCHP(db), ingestGoFundMe(db)]);
    const total = results.reduce((s, r) => s + (r.inserted || r.total_inserted || 0), 0);
    return res.json({ ok: true, total_inserted: total, results });
  }
  return res.status(400).json({ error: 'unknown action — use nyc, ntsb, spanish, or all' });
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
