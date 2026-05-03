/**
 * Phase 92: Crash<->News Bridge
 *
 * Open-data Socrata feeds give us location+date+severity but no names. News
 * articles have names but often miss exact street+time. This engine joins the
 * two: for every nameless qualifying crash (severity=fatal/critical), find news
 * incidents within +/-48h and <=5km, ask Claude to confirm the match and
 * extract the victim name, then insert a person row.
 */
const { getDb } = require('../../_db');

const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
const MODEL = 'claude-sonnet-4-5-20251022';

function distKm(lat1, lon1, lat2, lon2) {
  if (lat1 == null || lon1 == null || lat2 == null || lon2 == null) return Infinity;
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat/2) ** 2 + Math.cos(lat1*Math.PI/180) * Math.cos(lat2*Math.PI/180) * Math.sin(dLon/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

async function findCandidateArticles(db, incident) {
  const t = new Date(incident.occurred_at || incident.discovered_at).getTime();
  const lo = new Date(t - 48*3600*1000);
  const hi = new Date(t + 48*3600*1000);
  const rows = await db('incidents')
    .where('state', incident.state)
    .where('occurred_at', '>=', lo)
    .where('occurred_at', '<=', hi)
    .whereNot('id', incident.id)
    .whereNotNull('description')
    .whereRaw("incident_number NOT LIKE 'nyc-opendata:%' AND incident_number NOT LIKE 'sf-datasf:%' AND incident_number NOT LIKE 'chicago-socrata:%' AND incident_number NOT LIKE 'la-opendata:%'")
    .limit(30);
  const candidates = [];
  for (const r of rows) {
    let proximityKm = null;
    if (incident.latitude && incident.longitude && r.latitude && r.longitude) {
      proximityKm = distKm(incident.latitude, incident.longitude, r.latitude, r.longitude);
      if (proximityKm > 5) continue;
    }
    candidates.push({ ...r, _proximity_km: proximityKm });
  }
  candidates.sort((a, b) => {
    const ax = a._proximity_km == null ? 999 : a._proximity_km;
    const bx = b._proximity_km == null ? 999 : b._proximity_km;
    return ax - bx;
  });
  return candidates.slice(0, 6);
}

async function claudeMatch(incident, candidates) {
  if (!ANTHROPIC_KEY || candidates.length === 0) return null;
  const incidentSummary = `STRUCTURED CRASH RECORD\n- Source: ${incident.incident_number}\n- State: ${incident.state}, City: ${incident.city || '(unknown)'}\n- Occurred: ${incident.occurred_at}\n- Severity: ${incident.severity} (${incident.fatalities_count || 0} killed)\n- Location: ${incident.description || ''}\n- Lat/Lon: ${incident.latitude},${incident.longitude}`;
  const candList = candidates.map((c, i) => `[${i}] ${c.incident_number}\n  When: ${c.occurred_at}  Where: ${c.city}, ${c.state}  Proximity: ${c._proximity_km != null ? c._proximity_km.toFixed(2)+'km' : 'unknown'}\n  Description: ${(c.description || '').slice(0, 400)}`).join('\n');
  const prompt = `You are matching a structured open-data crash record to a candidate news article that may describe the SAME incident.\n\n${incidentSummary}\n\nCANDIDATE ARTICLES (in same state, +/-48h, <=5km):\n${candList}\n\nTask:\n1. Pick the SINGLE BEST match index, or null if no plausible match.\n2. From that article's description, extract the victim's full name(s) if mentioned.\n3. Provide a brief justification.\n\nRespond ONLY with JSON: {"match_index": <int|null>, "victim_names": ["First Last"], "justification": "..."}\nIf no plausible match: {"match_index": null, "victim_names": [], "justification": "no time/location overlap"}\nIf multi-victim: include all names.`;
  try {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'x-api-key': ANTHROPIC_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
      body: JSON.stringify({ model: MODEL, max_tokens: 600, messages: [{ role: 'user', content: prompt }] }),
      signal: AbortSignal.timeout(20000)
    });
    if (!r.ok) return null;
    const j = await r.json();
    const text = j.content?.[0]?.text || '';
    const m = text.match(/\{[\s\S]*\}/);
    if (!m) return null;
    return JSON.parse(m[0]);
  } catch { return null; }
}

async function processOne(db, incident) {
  const candidates = await findCandidateArticles(db, incident);
  if (candidates.length === 0) return { incident_id: incident.id, status: 'no_candidates', candidates: 0 };
  const result = await claudeMatch(incident, candidates);
  if (!result || result.match_index == null) return { incident_id: incident.id, status: 'no_match', candidates: candidates.length };
  const matched = candidates[result.match_index];
  if (!matched) return { incident_id: incident.id, status: 'invalid_index', candidates: candidates.length };
  const names = (result.victim_names || []).filter(n => n && n.length >= 5 && /\s/.test(n));
  if (names.length === 0) return { incident_id: incident.id, status: 'matched_no_names', matched_to: matched.incident_number };
  const { v4: uuid } = require('uuid');
  let personsInserted = 0;
  for (const name of names) {
    const exists = await db('persons')
      .where('incident_id', incident.id)
      .whereRaw('LOWER(full_name) = ?', [name.toLowerCase()])
      .first();
    if (exists) continue;
    try {
      await db('persons').insert({
        id: uuid(), incident_id: incident.id, full_name: name, role: 'victim',
        victim_verified: false, lead_tier: 'pending', source: 'crash-news-bridge', created_at: new Date()
      });
      personsInserted++;
      await db('enrichment_logs').insert({
        person_id: null, field_name: 'crash_news_bridge_match', old_value: null,
        new_value: JSON.stringify({
          source: 'crash-news-bridge', structured_incident: incident.incident_number,
          matched_news_incident: matched.incident_number, name_extracted: name,
          justification: result.justification, proximity_km: matched._proximity_km,
          time_delta_hours: Math.abs(new Date(incident.occurred_at) - new Date(matched.occurred_at)) / 3600000
        }).slice(0, 4000),
        created_at: new Date()
      }).catch(() => {});
    } catch { /* skip */ }
  }
  if (personsInserted > 0) {
    try {
      await db('incidents').where('id', incident.id).update({
        description: `${incident.description || ''}\n[BRIDGE: ${names.join(', ')} via ${matched.incident_number}]`.slice(0, 500),
        source_count: (incident.source_count || 1) + 1
      });
    } catch { /* ignore */ }
  }
  return { incident_id: incident.id, status: 'bridged', matched_to: matched.incident_number, names, persons_inserted: personsInserted, justification: result.justification };
}

async function findNamelessIncidents(db, limit) {
  return db.raw(`
    SELECT i.* FROM incidents i
    LEFT JOIN persons p ON p.incident_id = i.id
    WHERE p.id IS NULL
      AND i.severity IN ('fatal', 'critical')
      AND (i.incident_number LIKE 'nyc-opendata:%' OR i.incident_number LIKE 'sf-datasf:%'
           OR i.incident_number LIKE 'chicago-socrata:%' OR i.incident_number LIKE 'la-opendata:%')
      AND i.occurred_at > NOW() - INTERVAL '14 days'
    ORDER BY i.occurred_at DESC
    LIMIT ${parseInt(limit) || 20}
  `).then(r => r.rows || []);
}

module.exports = async function handler(req, res) {
  if (!authed(req)) return res.status(401).json({ error: 'unauthorized' });
  const action = req.query?.action || 'health';
  const db = getDb();
  if (action === 'health') {
    return res.status(200).json({ ok: true, engine: 'crash-news-bridge', anthropic_configured: !!ANTHROPIC_KEY, model: MODEL, strategy: '5km/48h spatial+temporal join, then Claude name extraction' });
  }
  if (action === 'match') {
    const id = req.query?.incident_id;
    if (!id) return res.status(400).json({ error: 'incident_id required' });
    const inc = await db('incidents').where('id', id).first();
    if (!inc) return res.status(404).json({ error: 'incident not found' });
    const result = await processOne(db, inc);
    return res.status(200).json({ ok: true, result });
  }
  if (action === 'run') {
    const limit = parseInt(req.query?.limit) || 20;
    const incidents = await findNamelessIncidents(db, limit);
    const results = [];
    let bridged = 0, persons_added = 0;
    for (const inc of incidents) {
      try {
        const r = await processOne(db, inc);
        results.push(r);
        if (r.status === 'bridged') { bridged++; persons_added += r.persons_inserted || 0; }
      } catch (e) {
        results.push({ incident_id: inc.id, status: 'error', error: e.message });
      }
    }
    return res.status(200).json({ ok: true, processed: incidents.length, bridged, persons_added, results: results.slice(0, 25) });
  }
  return res.status(400).json({ error: 'unknown action', valid: ['health', 'run', 'match'] });
};
