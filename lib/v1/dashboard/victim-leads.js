/**
 * VICTIM LEADS DASHBOARD — Phase 39
 * Rep-facing view of every verified victim with contact-state segmentation.
 */
const { getDb } = require('../../_db');

const ALL_ENGINES = [
  'pdl_enrich', 'apollo_match', 'hunter', 'trestle_phone',
  'voter_rolls', 'maricopa_property', 'people_search_multi', 'google_cse',
  'obituary_search', 'court_records', 'gofundme', 'news_rescrape',
  'funeral_home', 'property_county'
];

function bucketOf(p) {
  const filled = ['phone', 'email', 'address'].filter(k => p[k] && String(p[k]).trim().length > 0).length;
  if (filled === 3) return 'complete_contacts';
  if (filled >= 1) return 'partial_contacts';
  return 'no_contacts';
}

function nextBestAction(p) {
  if (p.phone && p.email && p.address) return 'call';
  if (p.phone) return 'call';
  if (p.email) return 'email';
  if (p.address) return 'send-letter';
  return 'manual-research';
}

function recommendedEngines(triedSet, person) {
  const tried = new Set([...triedSet].map(s => String(s).toLowerCase()));
  const order = [];
  if (!tried.has('voter_rolls')) order.push('voter_rolls');
  if (String(person.state || '').toUpperCase() === 'AZ' && !tried.has('maricopa_property')) order.push('maricopa_property');
  if (!tried.has('news_rescrape')) order.push('news_rescrape');
  if (!tried.has('obituary_search')) order.push('obituary_search');
  if (!tried.has('gofundme')) order.push('gofundme');
  if (!tried.has('funeral_home')) order.push('funeral_home');
  if (!tried.has('court_records')) order.push('court_records');
  if (!tried.has('property_county')) order.push('property_county');
  if (!tried.has('pdl_enrich')) order.push('pdl_enrich');
  if (!tried.has('apollo_match')) order.push('apollo_match');
  if (!tried.has('people_search_multi')) order.push('people_search_multi');
  if (person.employer && !tried.has('hunter')) order.push('hunter');
  return order;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  const db = getDb();
  const limit = Math.max(1, Math.min(parseInt(req.query.limit) || 100, 500));
  const bucketFilter = (req.query.bucket || '').toLowerCase();
  const stateFilter = (req.query.state || '').toUpperCase();
  const minScore = parseInt(req.query.min_score || '0', 10) || 0;

  try {
    let q = db('persons as p')
      .leftJoin('incidents as i', 'i.id', 'p.incident_id')
      .where('p.victim_verified', true)
      .whereNotNull('p.full_name')
      .select(
        'p.id as person_id',
        'p.full_name',
        'p.victim_role',
        'p.victim_verifier_stage',
        'p.role',
        'p.phone',
        'p.email',
        'p.address',
        'p.city as person_city',
        'p.state as person_state',
        'p.employer',
        'p.age',
        'p.linkedin_url',
        'p.confidence_score',
        'p.created_at as person_created_at',
        'p.updated_at as person_updated_at',
        'i.id as incident_id',
        'i.city as incident_city',
        'i.state as incident_state',
        'i.incident_type',
        'i.severity',
        'i.lead_score',
        'i.qualification_state',
        'i.address as incident_headline',
        'i.discovered_at',
        'i.occurred_at'
      )
      .orderBy('i.lead_score', 'desc')
      .orderBy('p.updated_at', 'desc')
      .limit(limit);

    if (stateFilter) q = q.where('i.state', stateFilter);
    if (minScore > 0) q = q.where('i.lead_score', '>=', minScore);

    const rows = await q;

    const ids = rows.map(r => r.person_id);
    const logs = ids.length
      ? await db('enrichment_logs')
          .whereIn('person_id', ids)
          .select('person_id', 'field_name', 'created_at')
          .orderBy('created_at', 'desc')
          .limit(ids.length * 30)
      : [];
    const logsByPerson = {};
    for (const l of logs) {
      if (!logsByPerson[l.person_id]) logsByPerson[l.person_id] = new Set();
      const raw = String(l.field_name || '');
      const parts = raw.split(':');
      const tail = parts[parts.length - 1].trim();
      if (tail) logsByPerson[l.person_id].add(tail);
    }

    const buckets = { complete_contacts: [], partial_contacts: [], no_contacts: [] };

    for (const r of rows) {
      const bucket = bucketOf(r);
      if (bucketFilter && bucketFilter !== bucket) continue;
      const triedSet = logsByPerson[r.person_id] || new Set();
      const item = {
        person_id: r.person_id,
        name: r.full_name,
        role: r.victim_role || r.role || null,
        victim_verifier_stage: r.victim_verifier_stage || null,
        confidence_score: r.confidence_score || null,
        contact: {
          phone: r.phone || null,
          email: r.email || null,
          address: r.address || null,
          employer: r.employer || null,
          age: r.age || null,
          linkedin_url: r.linkedin_url || null
        },
        incident: {
          id: r.incident_id,
          city: r.incident_city || r.person_city,
          state: r.incident_state || r.person_state,
          type: r.incident_type,
          severity: r.severity,
          score: r.lead_score,
          qualification_state: r.qualification_state,
          headline: (r.incident_headline || '').slice(0, 200),
          occurred_at: r.occurred_at,
          discovered_at: r.discovered_at
        },
        sources_succeeded: Array.from(triedSet).filter(s => ALL_ENGINES.includes(s)),
        next_best_action: nextBestAction(r),
        recommended_engines: recommendedEngines(triedSet, r),
        last_updated: r.person_updated_at
      };
      buckets[bucket].push(item);
    }

    const summary = {
      complete_contacts: buckets.complete_contacts.length,
      partial_contacts: buckets.partial_contacts.length,
      no_contacts: buckets.no_contacts.length,
      total: buckets.complete_contacts.length + buckets.partial_contacts.length + buckets.no_contacts.length
    };

    return res.json({
      success: true,
      summary,
      buckets,
      filters: { bucket: bucketFilter || 'all', state: stateFilter || null, min_score: minScore },
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    return res.status(500).json({ success: false, error: err.message });
  }
};
module.exports.handler = module.exports;
