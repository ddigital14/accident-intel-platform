/**
 * PulsePoint Fire/EMS CAD ingest. Many fire departments stream their dispatch data
 * to the PulsePoint app. There's a public-facing JSON endpoint per agency.
 * Real-time accident dispatch = first to know after 911 itself.
 *
 * Each agency has a unique ID. We support a list of ID's per metro.
 * Endpoint: https://web.pulsepoint.org/DB/giba.php?agency_id=XXX
 * Returns active incidents with type, location, units assigned.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

// Sample agencies — Akron Fire (OH), Cleveland Fire, Houston FD, Atlanta FD.
// Real list curated via https://web.pulsepoint.org/?agencies=...
// Expanded to 50 major metros — pulled from Reddit r/PulsePoint community-curated list.
const AGENCIES = [
  // Verified-working IDs only — the 46 expansion IDs were placeholders that returned 404.
  // To add a real metro: visit pulsepoint.org, find the agency, copy its real ID from URL.
  { id: 'EMS1500', city: 'Akron', state: 'OH' },
  { id: 'EMS1501', city: 'Cleveland', state: 'OH' },
  { id: 'EMS1108', city: 'Houston', state: 'TX' },
  { id: 'EMS09110', city: 'Atlanta', state: 'GA' }
];

const ACCIDENT_KEYWORDS = /traffic|MVA|MVC|TC|vehicle accident|crash|rollover|pedestrian|motorcycle|fatal|injury collision/i;

async function fetchAgency(agencyId, db) {
  const url = `https://web.pulsepoint.org/DB/giba.php?agency_id=${encodeURIComponent(agencyId)}&both=true&inactive=false`;
  let body = null, ok = false;
  try { const r = await fetch(url, { timeout: 10000, headers: { 'User-Agent': 'AIP-AccidentIntel/1.0' } }); if (r.ok) { body = await r.json(); ok = true; } } catch (_) {}
  await trackApiCall(db, 'ingest-pulsepoint', `agency:${agencyId}`, 0, 0, ok).catch(() => {});
  if (!body) return [];
  const incidents = body?.incidents?.active || [];
  return incidents.filter(i => ACCIDENT_KEYWORDS.test(i.PulsePointIncidentCallType || ''));
}

async function run(db) {
  let totalFetched = 0, inserted = 0;
  for (const agency of AGENCIES) {
    const items = await fetchAgency(agency.id, db);
    totalFetched += items.length;
    for (const it of items) {
      try {
        const sourceId = `pulsepoint-${agency.id}-${it.ID || it.IncidentNumber}`;
        await db('incidents').insert({
          source: 'pulsepoint',
          source_id: sourceId,
          description: `${it.PulsePointIncidentCallType || 'Traffic incident'} — ${it.MedicalEmergencyDisplayName || ''}`,
          accident_type: 'vehicle',
          severity: /fatal/i.test(it.PulsePointIncidentCallType || '') ? 'fatal' : 'unknown',
          city: agency.city, state: agency.state,
          street: it.FullDisplayAddress || it.StreetName,
          latitude: it.Latitude, longitude: it.Longitude,
          occurred_at: it.CallReceivedDateTime ? new Date(it.CallReceivedDateTime) : new Date(),
          created_at: new Date(),
          raw_payload: JSON.stringify(it)
        }).onConflict('source_id').ignore();
        inserted++;
      } catch (_) {}
    }
  }
  return { fetched: totalFetched, inserted, agencies: AGENCIES.length };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { action, agency_id } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'pulsepoint', agencies: AGENCIES.map(a => `${a.city}, ${a.state}`), cost: 0 });
    if (agency_id) { const items = await fetchAgency(agency_id, db); return res.json({ count: items.length, items: items.slice(0, 5) }); }
    const out = await run(db);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'pulsepoint', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
