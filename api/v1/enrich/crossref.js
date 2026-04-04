/**
 * Cross-Reference Verification Engine
 * GET  /api/v1/enrich/crossref - Get cross-reference results
 * POST /api/v1/enrich/crossref - Resolve a cross-reference conflict
 */
const { getDb } = require('../../_db');
const { requireAuth } = require('../../_auth');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const user = requireAuth(req, res);
  if (!user) return;
  const db = getDb();

  if (req.method === 'GET') {
    try {
      const { person_id, incident_id, resolution, field_name, page = 1, limit = 50 } = req.query;

      let query = db('cross_references as cr')
        .leftJoin('persons as p', 'cr.person_id', 'p.id')
        .select('cr.*', db.raw("p.first_name || ' ' || p.last_name as person_name"));

      if (person_id) query = query.where('cr.person_id', person_id);
      if (incident_id) query = query.where('cr.incident_id', incident_id);
      if (resolution) query = query.where('cr.resolution', resolution);
      if (field_name) query = query.where('cr.field_name', field_name);

      const total = await query.clone().clearSelect().clearOrder().count('* as c').first();
      const refs = await query.orderBy('cr.created_at', 'desc')
        .limit(Math.min(parseInt(limit), 200))
        .offset((parseInt(page) - 1) * parseInt(limit));

      const stats = {
        total: parseInt(total?.c || 0),
        pending: await db('cross_references').where('resolution', 'pending').count('* as c').first().then(r => parseInt(r.c)),
        auto_resolved: await db('cross_references').where('resolution', 'auto_resolved').count('* as c').first().then(r => parseInt(r.c)),
        manual_resolved: await db('cross_references').where('resolution', 'manual_resolved').count('* as c').first().then(r => parseInt(r.c)),
        conflicted: await db('cross_references').where('resolution', 'conflicted').count('* as c').first().then(r => parseInt(r.c)),
      };

      res.json({ data: refs, stats, pagination: { page: parseInt(page), limit: parseInt(limit), total: parseInt(total?.c || 0) } });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  }

  else if (req.method === 'POST') {
    try {
      const { id, resolved_value, resolution = 'manual_resolved' } = req.body;
      if (!id) return res.status(400).json({ error: 'Cross-reference ID required' });

      const ref = await db('cross_references').where('id', id).first();
      if (!ref) return res.status(404).json({ error: 'Not found' });

      // Update the cross-reference
      await db('cross_references').where('id', id).update({
        resolved_value,
        resolution,
        resolved_by: user.id,
        resolved_at: new Date()
      });

      // Apply the resolved value to the person
      if (resolved_value && ref.person_id && ref.field_name) {
        await db('persons').where('id', ref.person_id).update({
          [ref.field_name]: resolved_value,
          updated_at: new Date()
        });
      }

      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  }
  else {
    res.status(405).json({ error: 'Method not allowed' });
  }
};
