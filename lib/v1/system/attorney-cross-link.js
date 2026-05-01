/**
 * Phase 68 #9: Cross-incident attorney detector.
 * When a person has has_attorney=true with attorney_firm set, find OTHER persons
 * across the platform with the same firm and mark them as part of the same
 * "case ring" (often the same lawyer = same client cohort).
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function findRing(db, firm, excludePersonId) {
  if (!firm) return [];
  return db('persons')
    .whereRaw('LOWER(attorney_firm) = ?', [String(firm).toLowerCase()])
    .whereNot('id', excludePersonId || '')
    .select('id', 'full_name', 'incident_id', 'phone', 'email', 'state', 'attorney_firm', 'has_attorney')
    .limit(50);
}

async function scan(db, limit = 50) {
  const persons = await db('persons')
    .where('has_attorney', true)
    .whereNotNull('attorney_firm')
    .limit(limit)
    .select('id', 'attorney_firm');
  const rings = {};
  for (const p of persons) {
    const k = String(p.attorney_firm).toLowerCase();
    rings[k] = rings[k] || { firm: p.attorney_firm, person_ids: [] };
    rings[k].person_ids.push(p.id);
  }
  // Only return rings of size >= 2 (cross-link opportunities)
  const cross = Object.values(rings).filter(r => r.person_ids.length >= 2);
  return { ok: true, rings_total: Object.keys(rings).length, cross_incident_rings: cross.length, rings: cross };
}

async function getRingForPerson(db, personId) {
  const person = await db('persons').where('id', personId).first();
  if (!person?.attorney_firm) return { ok: true, person_id: personId, ring: [] };
  const ring = await findRing(db, person.attorney_firm, personId);
  return { ok: true, person_id: personId, firm: person.attorney_firm, ring_size: ring.length, ring };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();
  if (action === 'health') return res.json({ success: true, service: 'attorney-cross-link' });
  if (action === 'scan') {
    const limit = Math.min(200, parseInt(req.query?.limit) || 50);
    return res.json(await scan(db, limit));
  }
  if (action === 'get') {
    const pid = req.query?.person_id;
    if (!pid) return res.status(400).json({ error: 'person_id required' });
    return res.json(await getRingForPerson(db, pid));
  }
  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.scan = scan;
module.exports.getRingForPerson = getRingForPerson;
