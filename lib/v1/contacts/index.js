/**
 * Contacts API - Enhanced person search with contact info filters
 * GET /api/v1/contacts - Search persons with full contact details
 */
const { getDb } = require('../../_db');
const { requireAuth } = require('../../_auth');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const user = requireAuth(req, res);
  if (!user) return;
  const db = getDb();

  if (req.method === 'GET') {
    try {
      const {
        page = 1, limit = 50, search,
        has_phone, has_email, has_address, has_attorney,
        is_injured, contact_status, injury_severity,
        phone_verified, email_verified,
        min_enrichment_score, min_confidence,
        city, state, zip,
        sortBy = 'created_at', sortDir = 'desc',
        incident_type, severity, metro_area_id,
        date_from, date_to,
        do_not_contact
      } = req.query;

      let query = db('persons as p')
        .leftJoin('incidents as i', 'p.incident_id', 'i.id')
        .leftJoin('metro_areas as ma', 'i.metro_area_id', 'ma.id')
        .select(
          'p.*',
          'i.incident_number', 'i.incident_type', 'i.severity as incident_severity',
          'i.address as incident_address', 'i.city as incident_city', 'i.state as incident_state',
          'i.occurred_at as incident_occurred_at', 'i.discovered_at as incident_discovered_at',
          'i.occurred_at', 'i.description as incident_description',
          'i.priority', 'i.confidence_score as incident_confidence',
          'ma.name as metro_area_name'
        );

      // Contact info filters
      if (has_phone === 'true') query = query.whereNotNull('p.phone').where('p.phone', '!=', '');
      if (has_phone === 'false') query = query.where(function() { this.whereNull('p.phone').orWhere('p.phone', ''); });
      if (has_email === 'true') query = query.whereNotNull('p.email').where('p.email', '!=', '');
      if (has_email === 'false') query = query.where(function() { this.whereNull('p.email').orWhere('p.email', ''); });
      if (has_address === 'true') query = query.whereNotNull('p.address').where('p.address', '!=', '');
      if (has_attorney === 'true') query = query.where('p.has_attorney', true);
      if (has_attorney === 'false') query = query.where(function() { this.where('p.has_attorney', false).orWhereNull('p.has_attorney'); });
      if (is_injured === 'true') query = query.where('p.is_injured', true);
      if (contact_status) query = query.where('p.contact_status', contact_status);
      if (injury_severity) query = query.where('p.injury_severity', injury_severity);
      if (phone_verified === 'true') query = query.where('p.phone_verified', true);
      if (email_verified === 'true') query = query.where('p.email_verified', true);
      if (min_enrichment_score) query = query.where('p.enrichment_score', '>=', parseFloat(min_enrichment_score));
      if (min_confidence) query = query.where('p.confidence_score', '>=', parseFloat(min_confidence));
      if (do_not_contact === 'true') query = query.where('p.do_not_contact', true);
      if (do_not_contact === 'false') query = query.where(function() { this.where('p.do_not_contact', false).orWhereNull('p.do_not_contact'); });

      // Location filters
      if (city) query = query.whereILike('p.city', `%${city}%`);
      if (state) query = query.where('p.state', state);
      if (zip) query = query.where('p.zip', zip);

      // Incident filters
      if (incident_type) query = query.where('i.incident_type', incident_type);
      if (severity) query = query.where('i.severity', severity);
      if (metro_area_id) query = query.where('i.metro_area_id', metro_area_id);
      if (date_from) query = query.where('i.occurred_at', '>=', date_from);
      if (date_to) query = query.where('i.occurred_at', '<=', date_to);

      // Search across name, phone, email
      if (search) {
        query = query.where(function() {
          this.whereILike('p.first_name', `%${search}%`)
            .orWhereILike('p.last_name', `%${search}%`)
            .orWhereILike('p.full_name', `%${search}%`)
            .orWhereILike('p.phone', `%${search}%`)
            .orWhereILike('p.email', `%${search}%`)
            .orWhereILike('p.employer', `%${search}%`);
        });
      }

      const countQuery = query.clone().clearSelect().clearOrder().count('* as total').first();

      const allowedSorts = ['created_at', 'first_name', 'last_name', 'enrichment_score', 'confidence_score', 'contact_status', 'occurred_at'];
      const sortField = allowedSorts.includes(sortBy) ? (sortBy === 'occurred_at' ? 'i.occurred_at' : `p.${sortBy}`) : 'p.created_at';
      query = query.orderBy(sortField, sortDir === 'asc' ? 'asc' : 'desc')
        .limit(Math.min(parseInt(limit), 200))
        .offset((parseInt(page) - 1) * parseInt(limit));

      const [contacts, countResult] = await Promise.all([query, countQuery]);
      const total = parseInt(countResult?.total || 0);

      // Get enrichment logs for these persons
      const personIds = contacts.map(c => c.id);
      const enrichments = personIds.length > 0
        ? await db('enrichment_logs').whereIn('person_id', personIds).orderBy('created_at', 'desc')
        : [];
      const enrichmentsByPerson = {};
      enrichments.forEach(e => {
        if (!enrichmentsByPerson[e.person_id]) enrichmentsByPerson[e.person_id] = [];
        enrichmentsByPerson[e.person_id].push(e);
      });

      const enrichedContacts = contacts.map(c => ({
        ...c,
        display_name: c.full_name || `${c.first_name || ''} ${c.last_name || ''}`.trim() || 'Unknown',
        enrichment_history: enrichmentsByPerson[c.id] || [],
        contact_quality: calculateContactQuality(c)
      }));

      // Summary stats
      const summary = {
        total,
        with_phone: contacts.filter(c => c.phone).length,
        with_email: contacts.filter(c => c.email).length,
        with_address: contacts.filter(c => c.address).length,
        no_attorney: contacts.filter(c => !c.has_attorney).length,
        injured: contacts.filter(c => c.is_injured).length,
        not_contacted: contacts.filter(c => c.contact_status === 'not_contacted').length,
        verified_phones: contacts.filter(c => c.phone_verified).length,
        avg_enrichment: contacts.length > 0 ? (contacts.reduce((s, c) => s + (parseFloat(c.enrichment_score) || 0), 0) / contacts.length).toFixed(1) : 0
      };

      res.json({
        data: enrichedContacts,
        summary,
        pagination: { page: parseInt(page), limit: parseInt(limit), total, totalPages: Math.ceil(total / parseInt(limit)) }
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  } else {
    res.status(405).json({ error: 'Method not allowed' });
  }
};

function calculateContactQuality(person) {
  let score = 0;
  if (person.phone) score += 20;
  if (person.phone_verified) score += 10;
  if (person.email) score += 20;
  if (person.email_verified) score += 10;
  if (person.address) score += 15;
  if (person.address_verified) score += 5;
  if (person.first_name && person.last_name) score += 10;
  if (person.age || person.date_of_birth) score += 5;
  if (person.employer) score += 5;
  return Math.min(score, 100);
}
