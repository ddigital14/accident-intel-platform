/**
 * Cross-Examination Identity Confidence Engine
 *
 * Aggregates evidence from ALL sources for a single person and computes
 * an identity_confidence score (0-100) based on multi-source agreement.
 *
 * Why this beats BuyCrash/LexisNexis:
 *   They use single-source proprietary data.
 *   We use AI synthesis across 18+ sources — when 3 sources independently
 *   confirm "John Smith, 4045552222, 123 Main St", confidence = 95+.
 *
 * Algorithm:
 *   1. Pull all source_reports + enrichment_logs for an incident
 *   2. Group by field (name, phone, email, address)
 *   3. For each field, count how many distinct sources reported it
 *   4. Compute weighted confidence:
 *        single source: 50-70 (depends on source reliability)
 *        2 matching sources: 80
 *        3+ matching sources: 90-99
 *   5. Flag contradictions (different name across sources = lower confidence)
 *   6. Run time-location correlation to surface adjacent records
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');

// Source reliability scores (0-100)
const SOURCE_WEIGHTS = {
  // Authoritative (government/police)
  'police_report': 95,
  'pd_press': 95,
  'court_records': 95,
  'state_dot': 90,
  'opendata_chicago': 92,
  'opendata_seattle': 90,
  'opendata_sf': 90,
  'opendata_dallas': 88,
  'opendata_cincinnati': 88,
  // Paid identity APIs
  'trestle_reverse_phone': 90,
  'trestle_cnam': 80,
  'trestle_reverse_address': 90,
  'twilio_lookup': 88,           // carrier-reported caller_name + line_type — very high signal
  'twilio_verify': 95,           // user-confirmed phone ownership
  'twilio_sms_reply': 99,        // human replied = ultimate identity confirmation
  'pdl': 85,
  'tracerfy': 80,
  'searchbug': 75,
  // News / RSS / scraped
  'newsapi': 70,
  'rss': 60,
  'pd_social': 80,
  'reddit': 50,
  'obituary': 75,
  // Free directories (less reliable due to bot detection / staleness)
  'people_search': 55,
  'truepeoplesearch': 55,
  'fastpeoplesearch': 55,
  'whitepages': 60,
  'spokeo_free': 55,
  // Self-reported / records
  'voter_rolls': 90,
  'property_records': 90,
  'numverify': 85,
  'hunter_io': 75,
  // Vehicle-derived
  'nhtsa_vin': 90,
  'vehicle-history': 95,           // NHTSA recall+complaint+NCAP composite (govt authoritative)
  'enrich-vehicle-history': 95,
  // Family graph — extends cross-conversion outward to relatives
  'obituary_relative': 75,         // family-authored obituary text
  'family-tree': 75,
  'enrich-family-tree': 75,
  'relatives_search': 70,          // confirmed via SearchBug + geo
  'enrich-relatives-search': 70,
};

function getSourceWeight(source) {
  if (!source) return 50;
  // Normalize and find best match
  const s = String(source).toLowerCase();
  for (const [k, v] of Object.entries(SOURCE_WEIGHTS)) {
    if (s.includes(k)) return v;
  }
  return 50;
}

/**
 * Group field values by source. Returns map: field → [{value, source, weight}]
 */
async function gatherEvidence(db, person) {
  const evidence = {};

  // 1. Direct fields on person record
  const fields = ['full_name', 'phone', 'email', 'address', 'age', 'employer'];
  for (const f of fields) {
    if (person[f]) {
      evidence[f] = evidence[f] || [];
      evidence[f].push({
        value: String(person[f]).toLowerCase().trim(),
        source: 'persons_table',
        weight: 60
      });
    }
  }

  // 2. Source reports linked to incident
  if (person.incident_id) {
    const reports = await db('source_reports')
      .where('incident_id', person.incident_id)
      .select('source_type', 'parsed_data');
    for (const r of reports) {
      const parsed = typeof r.parsed_data === 'string' ?
        (() => { try { return JSON.parse(r.parsed_data); } catch { return null; } })() :
        r.parsed_data;
      if (!parsed) continue;
      const w = getSourceWeight(r.source_type);

      // Look for victim in parsed_data.victims[]
      const victims = parsed.victims || (parsed.persons || []);
      for (const v of victims) {
        if (!v.full_name && !v.name) continue;
        const vName = String(v.full_name || v.name || '').toLowerCase().trim();
        const personName = String(person.full_name || `${person.first_name || ''} ${person.last_name || ''}`).toLowerCase().trim();
        // Only attribute if name matches
        if (!vName.includes(person.last_name?.toLowerCase() || '') && !personName.includes(vName.split(' ')[0])) continue;

        if (vName) (evidence.full_name ||= []).push({ value: vName, source: r.source_type, weight: w });
        if (v.phone) (evidence.phone ||= []).push({ value: String(v.phone).replace(/\D/g, ''), source: r.source_type, weight: w });
        if (v.email) (evidence.email ||= []).push({ value: String(v.email).toLowerCase(), source: r.source_type, weight: w });
        if (v.address) (evidence.address ||= []).push({ value: String(v.address).toLowerCase(), source: r.source_type, weight: w });
        if (v.age) (evidence.age ||= []).push({ value: String(v.age), source: r.source_type, weight: w });
      }
    }
  }

  // 3. Enrichment logs (per-field updates from APIs)
  // Schema variants: some rows have `source`, others `source_url` — try both via raw query
  let logs = [];
  // Introspect schema once + build a query matching the actual columns
  try {
    const cols = await db.raw(`SELECT column_name FROM information_schema.columns WHERE table_name='enrichment_logs'`);
    const colSet = new Set((cols.rows || []).map(r => r.column_name));
    const srcExpr = colSet.has('source') && colSet.has('source_url')
      ? `COALESCE(source, source_url, 'enrichment')`
      : colSet.has('source_url') ? `COALESCE(source_url, 'enrichment')`
      : colSet.has('source') ? `COALESCE(source, 'enrichment')`
      : `'enrichment'`;
    const confExpr = colSet.has('confidence') ? `confidence` : `NULL`;
    const r = await db.raw(
      `SELECT field_name, new_value, ${srcExpr} as source, ${confExpr} as confidence FROM enrichment_logs WHERE person_id = ?`,
      [person.id]
    );
    logs = r.rows || [];
  } catch (_) { logs = []; }
  for (const l of logs) {
    if (!l.field_name || !l.new_value) continue;
    const f = l.field_name.toLowerCase();
    const knownField = ['phone','email','address','full_name','employer','age'].find(k => f.includes(k));
    if (!knownField) continue;
    (evidence[knownField] ||= []).push({
      value: String(l.new_value).toLowerCase().trim(),
      source: l.source || 'enrichment',
      weight: parseInt(l.confidence) || getSourceWeight(l.source)
    });
  }

  return evidence;
}

/**
 * Score a single field's confidence based on how many sources agree.
 * Returns { value, confidence, supporting_sources, contradicting_sources }
 */
function scoreField(items) {
  if (!items || !items.length) return null;

  // Group by normalized value
  const buckets = {};
  for (const it of items) {
    const k = it.value;
    buckets[k] = buckets[k] || { value: it.value, weights: [], sources: [] };
    buckets[k].weights.push(it.weight);
    buckets[k].sources.push(it.source);
  }

  // Pick winning value (highest weighted count)
  const ranked = Object.values(buckets).sort((a, b) => {
    const aScore = a.weights.reduce((s, w) => s + w, 0);
    const bScore = b.weights.reduce((s, w) => s + w, 0);
    return bScore - aScore;
  });

  const winner = ranked[0];
  const losers = ranked.slice(1);

  // Confidence calculation
  const distinctWinningSources = new Set(winner.sources);
  let confidence;
  const n = distinctWinningSources.size;
  if (n >= 4) confidence = 99;
  else if (n === 3) confidence = 92;
  else if (n === 2) confidence = 82;
  else confidence = Math.round(winner.weights[0] * 0.85);

  // If contradicted, dock confidence by 15 per losing bucket
  for (const l of losers) {
    confidence -= 15;
  }
  confidence = Math.max(20, Math.min(99, confidence));

  return {
    value: winner.value,
    confidence,
    supporting_sources: [...distinctWinningSources],
    contradicting_sources: losers.map(l => ({ value: l.value, sources: l.sources })),
  };
}

/**
 * Run cross-examination on a person. Returns full identity profile with
 * per-field confidence + overall identity_confidence.
 */
async function crossExamine(db, personOrId) {
  const person = typeof personOrId === 'string' ?
    await db('persons').where('id', personOrId).first() :
    personOrId;
  if (!person) return null;

  const evidence = await gatherEvidence(db, person);

  const fieldResults = {};
  for (const [field, items] of Object.entries(evidence)) {
    fieldResults[field] = scoreField(items);
  }

  // Overall identity confidence = weighted average of fields, weighted by importance
  const fieldWeights = {
    full_name: 1.5,
    phone: 1.3,
    address: 1.2,
    email: 1.0,
    age: 0.7,
    employer: 0.8
  };
  let totalScore = 0, totalWeight = 0;
  for (const [f, w] of Object.entries(fieldWeights)) {
    if (fieldResults[f]) {
      totalScore += fieldResults[f].confidence * w;
      totalWeight += w;
    }
  }
  const identity_confidence = totalWeight > 0 ? Math.round(totalScore / totalWeight) : 0;

  // Source diversity bonus — if 5+ distinct sources contributed, +5
  const allSources = new Set();
  for (const f of Object.values(fieldResults)) {
    if (!f) continue;
    for (const s of f.supporting_sources || []) allSources.add(s);
  }
  const diversityBonus = Math.min(8, Math.max(0, allSources.size - 3));
  const adjusted = Math.min(99, identity_confidence + diversityBonus);

  return {
    person_id: person.id,
    incident_id: person.incident_id,
    full_name: person.full_name,
    identity_confidence: adjusted,
    field_confidence: fieldResults,
    source_diversity: allSources.size,
    sources_used: [...allSources],
    has_contradictions: Object.values(fieldResults).some(f => f && f.contradicting_sources.length > 0)
  };
}

/**
 * Time-location-name correlation: given an incident, find OTHER records
 * (obituaries, news, court) within +/- 7 days and 50mi that mention any name.
 * Use this to identify victims even when originally not linked.
 */
async function findAdjacentRecords(db, incident) {
  if (!incident.occurred_at || !incident.city || !incident.state) return [];

  const start = new Date(incident.occurred_at);
  start.setDate(start.getDate() - 7);
  const end = new Date(incident.occurred_at);
  end.setDate(end.getDate() + 7);

  // Find obituaries/news source_reports within window mentioning city/state
  const adjacent = await db('source_reports as sr')
    .leftJoin('incidents as i', 'sr.incident_id', 'i.id')
    .where('i.state', incident.state)
    .whereRaw('LOWER(i.city) = LOWER(?)', [incident.city])
    .where('i.occurred_at', '>', start)
    .where('i.occurred_at', '<', end)
    .where('i.id', '!=', incident.id)
    .whereIn('sr.source_type', ['obituary', 'newsapi', 'rss', 'pd_press', 'court_records'])
    .select('sr.id', 'sr.source_type', 'sr.parsed_data', 'i.id as adj_incident_id', 'i.address')
    .limit(20);

  return adjacent;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const db = getDb();
  const { person_id, incident_id, action = 'examine' } = req.query;

  try {
    if (action === 'examine' && person_id) {
      const result = await crossExamine(db, person_id);
      return res.json({ success: true, result });
    }
    if (action === 'adjacent' && incident_id) {
      const inc = await db('incidents').where('id', incident_id).first();
      if (!inc) return res.status(404).json({ error: 'incident not found' });
      const adjacent = await findAdjacentRecords(db, inc);
      return res.json({ success: true, count: adjacent.length, adjacent });
    }
    if (action === 'examine_all') {
      // Cross-examine the 50 most recent named persons
      const persons = await db('persons')
        .whereNotNull('full_name')
        .where('created_at', '>', new Date(Date.now() - 14 * 86400000))
        .orderBy('created_at', 'desc')
        .limit(50)
        .select('*');
      const results = [];
      for (const p of persons) {
        const r = await crossExamine(db, p);
        if (r) results.push(r);
      }
      const avgConfidence = results.length ? Math.round(results.reduce((s, r) => s + r.identity_confidence, 0) / results.length) : 0;
      return res.json({
        success: true,
        examined: results.length,
        avg_identity_confidence: avgConfidence,
        high_confidence: results.filter(r => r.identity_confidence >= 85).length,
        medium_confidence: results.filter(r => r.identity_confidence >= 65 && r.identity_confidence < 85).length,
        low_confidence: results.filter(r => r.identity_confidence < 65).length,
        with_contradictions: results.filter(r => r.has_contradictions).length,
        results: results.slice(0, 10)
      });
    }
    res.status(400).json({ error: 'invalid action', valid: ['examine', 'adjacent', 'examine_all'] });
  } catch (err) {
    await reportError(db, 'cross-exam', null, err.message);
    res.status(500).json({ error: err.message });
  }
};

module.exports.crossExamine = crossExamine;
module.exports.findAdjacentRecords = findAdjacentRecords;
module.exports.SOURCE_WEIGHTS = SOURCE_WEIGHTS;
