/**
 * RELATIVES SEARCH engine — Family network builder via cross-references
 *
 * For every named person we have, find OTHER persons in our DB with:
 *   - Same last name
 *   - Within 30 miles (PostGIS ST_DWithin via incidents.geom)
 *   - Same state (cheap pre-filter)
 *
 * Strong "likely family" signal. Confirm with SearchBug (returns relatives
 * lists for a person). Confirmed pairs go into a `relationships` table, and
 * the related person gets a cascade so phone/email enrichers fire on them —
 * many fatal-accident leads convert through a brother/aunt/parent's confirmed
 * contact, not the deceased's.
 *
 * Per CORE_INTENT.md: every confirmed relationship emits enqueueCascade so the
 * cross-conversion graph extends along family edges.
 *
 * Endpoints:
 *   GET /api/v1/enrich/relatives-search?action=process&limit=20  (cron)
 *   GET /api/v1/enrich/relatives-search?action=for_person&person_id=<id>
 *   GET /api/v1/enrich/relatives-search?action=health
 *
 * Cost: ~$0.05/relative confirmation (SearchBug). Cross-exam weight: 70.
 */
const { v4: uuidv4 } = require('uuid');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { logChange } = require('../system/changelog');
const { enqueueCascade } = require('../system/_cascade');

const SEARCHBUG_KEY = process.env.SEARCHBUG_API_KEY;

let _ensured = false;
async function ensureTable(db) {
  if (_ensured) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS relationships (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        person_id UUID NOT NULL,
        related_person_id UUID NOT NULL,
        relation_type VARCHAR(40) DEFAULT 'family',
        confidence INTEGER DEFAULT 60,
        source VARCHAR(80) DEFAULT 'relatives_search',
        evidence JSONB DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(person_id, related_person_id)
      );
      CREATE INDEX IF NOT EXISTS idx_relationships_person ON relationships(person_id);
      CREATE INDEX IF NOT EXISTS idx_relationships_related ON relationships(related_person_id);
    `);
    _ensured = true;
  } catch (e) { /* non-fatal */ }
}

async function findCandidates(db, person, opts = {}) {
  if (!person.last_name || !person.state) return [];
  const radiusMiles = opts.radius || 30;

  let candidates = [];
  try {
    candidates = await db.raw(`
      SELECT p.*,
             ST_Distance(i_self.geom::geography, i_other.geom::geography) / 1609.34 AS distance_mi
      FROM persons p
      JOIN incidents i_other ON i_other.id = p.incident_id
      JOIN persons p_self ON p_self.id = ?
      JOIN incidents i_self ON i_self.id = p_self.incident_id
      WHERE p.id != p_self.id
        AND LOWER(p.last_name) = LOWER(?)
        AND p.state = ?
        AND i_other.geom IS NOT NULL AND i_self.geom IS NOT NULL
        AND ST_DWithin(i_self.geom::geography, i_other.geom::geography, ? * 1609.34)
      LIMIT 25
    `, [person.id, person.last_name, person.state, radiusMiles]).then(r => r.rows || []);
  } catch (_) { candidates = []; }

  if (!candidates.length) {
    try {
      const q = db('persons')
        .whereRaw('LOWER(last_name) = LOWER(?)', [person.last_name])
        .where('state', person.state)
        .where('id', '!=', person.id);
      if (person.city) q.whereRaw('LOWER(city) = LOWER(?)', [person.city]);
      candidates = await q.limit(25);
    } catch (_) {}
  }
  return candidates;
}

/**
 * Confirm via SearchBug person search — checks if candidate name appears
 * in the relatives list returned for the source person.
 */
async function confirmViaSearchBug(db, person, candidate) {
  if (!SEARCHBUG_KEY || !person.full_name) return { confirmed: false, source: 'searchbug', reason: 'no_key_or_name' };
  const url = `https://api.searchbug.com/api/people.aspx?TYPE=API1&FNAME=${encodeURIComponent(person.first_name || '')}&LNAME=${encodeURIComponent(person.last_name || '')}&STATE=${encodeURIComponent(person.state || '')}&CITY=${encodeURIComponent(person.city || '')}&CO_CODE=${SEARCHBUG_KEY}&FORMAT=JSON`;
  try {
    const resp = await fetch(url, { signal: AbortSignal.timeout(10000) });
    await trackApiCall(db, 'enrich-relatives-search', 'searchbug', 0, 0, resp.ok);
    if (!resp.ok) return { confirmed: false, source: 'searchbug', status: resp.status };
    const data = await resp.json().catch(() => null);
    const hits = data?.PEOPLE || data?.results || data?.records || [];
    const candFull = (candidate.full_name || '').toLowerCase().trim();
    const candFirst = (candidate.first_name || '').toLowerCase().trim();
    for (const h of hits) {
      const relatives = h.RELATIVES || h.relatives || h.Relatives || [];
      for (const r of relatives) {
        const rname = (typeof r === 'string' ? r : (r.name || r.NAME || '')).toLowerCase();
        if (!rname) continue;
        if (rname.includes(candFull) || (candFirst && rname.includes(candFirst))) {
          return { confirmed: true, source: 'searchbug', evidence: { matched: rname } };
        }
      }
    }
    return { confirmed: false, source: 'searchbug' };
  } catch (e) {
    await trackApiCall(db, 'enrich-relatives-search', 'searchbug', 0, 0, false);
    return { confirmed: false, source: 'searchbug', error: e.message };
  }
}

async function processPerson(db, person) {
  await ensureTable(db);
  const candidates = await findCandidates(db, person);
  const out = { person_id: person.id, candidates: candidates.length, confirmed: 0, links: [] };
  for (const c of candidates) {
    if (out.confirmed >= 5) break; // cost guardrail per person
    const exists = await db('relationships')
      .where(function () {
        this.where({ person_id: person.id, related_person_id: c.id })
          .orWhere({ person_id: c.id, related_person_id: person.id });
      }).first();
    if (exists) continue;

    let confidence = 55;
    let evidence = { same_last_name: true, same_state: true, distance_mi: c.distance_mi || null };
    let source = 'name_geo_match';

    const sb = await confirmViaSearchBug(db, person, c);
    if (sb.confirmed) {
      confidence = 80;
      source = 'searchbug';
      evidence = { ...evidence, ...sb.evidence };
    }

    if (c.address && person.address && c.address.toLowerCase() === person.address.toLowerCase()) {
      confidence = Math.max(confidence, 90);
      evidence.same_address = true;
    }

    if (confidence < 70) continue;

    try {
      await db('relationships').insert({
        id: uuidv4(),
        person_id: person.id,
        related_person_id: c.id,
        relation_type: 'family',
        confidence,
        source,
        evidence: JSON.stringify(evidence),
        created_at: new Date()
      });
      out.confirmed++;
      out.links.push({ related_person_id: c.id, name: c.full_name, confidence });

      // CASCADE — relative now part of cross-conversion graph
      await enqueueCascade(db, {
        person_id: c.id,
        incident_id: c.incident_id,
        trigger_source: 'relatives_search',
        trigger_field: 'related_to',
        trigger_value: person.id,
        priority: 5
      }).catch(() => {});
    } catch (e) {
      await reportError(db, 'enrich-relatives-search', person.id, e.message, { candidate: c.id });
    }
  }
  return out;
}

async function processBatch(db, limit = 20) {
  await ensureTable(db);
  const startTime = Date.now();
  const stats = { evaluated: 0, links_created: 0, errors: [] };

  const candidates = await db.raw(`
    SELECT p.* FROM persons p
    LEFT JOIN relationships r ON r.person_id = p.id
    WHERE p.full_name IS NOT NULL
      AND p.last_name IS NOT NULL
      AND p.state IS NOT NULL
      AND p.created_at > NOW() - INTERVAL '14 days'
      AND r.id IS NULL
    ORDER BY p.created_at DESC
    LIMIT ?
  `, [limit]).then(r => r.rows || []).catch(() => []);

  for (const p of candidates) {
    if (Date.now() - startTime > 45000) break;
    stats.evaluated++;
    try {
      const r = await processPerson(db, p);
      stats.links_created += r.confirmed;
    } catch (e) {
      stats.errors.push(`${p.id}: ${e.message}`);
      await reportError(db, 'enrich-relatives-search', p.id, e.message);
    }
  }
  return { ok: true, ...stats };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const db = getDb();
  const action = req.query.action || 'health';
  try {
    if (action === 'health') {
      return res.status(200).json({ ok: true, engine: 'relatives-search', configured: !!SEARCHBUG_KEY, weight: 70 });
    }
    if (action === 'for_person') {
      const personId = req.query.person_id;
      if (!personId) return res.status(400).json({ error: 'person_id required' });
      const person = await db('persons').where('id', personId).first();
      if (!person) return res.status(404).json({ error: 'person not found' });
      const result = await processPerson(db, person);
      return res.status(200).json({ success: true, ...result });
    }
    if (action === 'process') {
      const limit = parseInt(req.query.limit) || 20;
      const result = await processBatch(db, limit);
      if (result.links_created > 0) {
        try { await logChange(db, { kind: 'pipeline', title: `relatives-search: +${result.links_created} family links`, summary: `evaluated=${result.evaluated}`, ref: 'relatives-search' }); } catch (_) {}
      }
      return res.status(200).json({
        success: true,
        message: `relatives-search: ${result.links_created} links across ${result.evaluated} persons`,
        ...result,
        timestamp: new Date().toISOString()
      });
    }
    return res.status(400).json({ error: 'unknown action', valid: ['health', 'for_person', 'process'] });
  } catch (e) {
    await reportError(db, 'enrich-relatives-search', null, e.message);
    return res.status(500).json({ error: e.message });
  }
};

module.exports.processPerson = processPerson;
module.exports.processBatch = processBatch;
module.exports.findCandidates = findCandidates;
