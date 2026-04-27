/**
 * SMART ROUTER — Phase 21
 *
 * Given a (person, incident) pair, decide the OPTIMAL next-best-action by
 * analyzing what we know vs what we're missing. Saves cycles by NOT calling
 * APIs that won't add value.
 *
 * Decision tree (priority order — first match wins):
 *   !full_name                       → backfill-nameless
 *   !phone && full_name              → enrich-pdl-by-name
 *   phone && !carrier                → twilio-lookup
 *   phone && !email && employer      → hunter-find
 *   !email && !address               → searchbug-and-voter
 *   full_name && phone && !facebook  → social-search
 *   fatal && !relatives              → family-tree
 *   has_attorney unknown             → court-reverse-link
 *   identity_confidence < 70         → claude-cross-reasoner
 *   else                             → ready-for-rep
 */
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

function pickNextAction(person, incident, identityConfidence = null) {
  if (!person) return { action: 'noop', reason: 'no_person', cost_estimate_usd: 0 };
  const sev = (incident?.severity || '').toLowerCase();
  const fatal = sev === 'fatal' || (incident?.fatalities_count || 0) > 0;

  if (!person.full_name && !person.last_name) {
    return { action: 'backfill-nameless', reason: 'no_name_yet', params: { incident_id: incident?.id, limit: 1 }, cost_estimate_usd: 0.01, ready_for_rep: false };
  }
  if (!person.phone && (person.full_name || person.last_name)) {
    return { action: 'enrich-pdl-by-name', reason: 'name_but_no_phone', params: { person_id: person.id }, cost_estimate_usd: 0.02, ready_for_rep: false };
  }
  if (person.phone && (!person.phone_carrier || !person.phone_line_type)) {
    return { action: 'twilio-lookup', reason: 'phone_unverified', params: { person_id: person.id, phone: person.phone }, cost_estimate_usd: 0.008, ready_for_rep: false };
  }
  if (person.phone && !person.email && person.employer) {
    return { action: 'hunter-find', reason: 'no_email_have_employer', params: { person_id: person.id, employer: person.employer }, cost_estimate_usd: 0.04, ready_for_rep: false };
  }
  if (!person.email && !person.address) {
    return { action: 'searchbug-and-voter', reason: 'no_email_no_address', params: { person_id: person.id }, cost_estimate_usd: 0.05, ready_for_rep: false };
  }
  if (person.full_name && person.phone && !person.facebook_url) {
    return { action: 'social-search', reason: 'no_social_url', params: { person_id: person.id }, cost_estimate_usd: 0.005, ready_for_rep: false };
  }
  if (fatal && !person.has_relatives_searched) {
    return { action: 'family-tree', reason: 'fatal_no_relatives', params: { person_id: person.id }, cost_estimate_usd: 0.0002, ready_for_rep: false };
  }
  if (person.has_attorney === null || person.has_attorney === undefined) {
    return { action: 'court-reverse-link', reason: 'attorney_unknown', params: { incident_id: incident?.id, limit: 1 }, cost_estimate_usd: 0.0, ready_for_rep: false };
  }
  const conf = identityConfidence != null ? identityConfidence : (person.identity_confidence || person.confidence_score || 0);
  if (conf < 70) {
    return { action: 'claude-cross-reasoner', reason: `confidence_${conf}_below_70`, params: { person_id: person.id }, cost_estimate_usd: 0.005, ready_for_rep: false };
  }
  return { action: 'ready-for-rep', reason: 'all_signals_present', params: { person_id: person.id }, cost_estimate_usd: 0, ready_for_rep: true };
}

async function routeAndExecute(db, person, incident, opts = {}) {
  let identityConfidence = null;
  if (opts.computeConfidence !== false) {
    try {
      const { crossExamine } = require('./cross-exam');
      const r = await crossExamine(db, person);
      identityConfidence = r?.identity_confidence ?? null;
    } catch (_) {}
  }
  const picked = pickNextAction(person, incident, identityConfidence);
  if (picked.ready_for_rep || picked.action === 'noop') {
    return { picked, executed: false, identity_confidence: identityConfidence };
  }
  if (opts.dryRun) return { picked, executed: false, dry: true, identity_confidence: identityConfidence };

  let result = null;
  try {
    switch (picked.action) {
      case 'twilio-lookup': {
        const tw = require('./twilio');
        const lu = await tw.lookupPhone(db, person.phone);
        if (lu?.ok && tw.applyLookupToPerson) await tw.applyLookupToPerson(db, person.id, lu);
        result = { ok: !!lu?.ok, line_type: lu?.line_type, carrier: lu?.carrier_name, caller_name: lu?.caller_name };
        break;
      }
      case 'family-tree': {
        const ft = require('./family-tree');
        result = await ft.processDeceased(db, person);
        // Mark searched even on no-op so we don't loop
        try { await db('persons').where('id', person.id).update({ has_relatives_searched: true, updated_at: new Date() }); } catch(_) {}
        break;
      }
      case 'social-search': {
        const ss = require('./social-search');
        if (typeof ss.processPerson === 'function') result = await ss.processPerson(db, person);
        else result = { skipped: 'no_processPerson_export' };
        break;
      }
      case 'claude-cross-reasoner': {
        const cr = require('./claude-cross-reasoner');
        if (typeof cr.reasonAboutPerson === 'function') result = await cr.reasonAboutPerson(db, person);
        else result = { skipped: 'no_reasonAboutPerson_export' };
        break;
      }
      case 'court-reverse-link':
      case 'backfill-nameless':
      case 'searchbug-and-voter':
      case 'enrich-pdl-by-name':
      case 'hunter-find':
        result = { deferred_to_cron: picked.action };
        break;
      default:
        result = { skipped: picked.action };
    }
  } catch (e) {
    await reportError(db, 'smart-router', person.id, `${picked.action}: ${e.message}`).catch(()=>{});
    return { picked, executed: false, error: e.message };
  }

  try {
    await enqueueCascade(db, {
      person_id: person.id,
      incident_id: incident?.id || person.incident_id,
      trigger_source: `smart_router:${picked.action}`,
      trigger_field: 'next_action',
      trigger_value: picked.action,
      priority: 5,
    });
  } catch (_) {}

  return { picked, executed: true, result, identity_confidence: identityConfidence };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const { getDb } = require('../../_db');
  const db = getDb();
  const action = req.query.action || 'pick';

  try {
    if (action === 'pick' && req.query.person_id) {
      const p = await db('persons').where('id', req.query.person_id).first();
      if (!p) return res.status(404).json({ error: 'person_not_found' });
      const inc = p.incident_id ? await db('incidents').where('id', p.incident_id).first() : null;
      const dry = req.query.dry === 'true' || req.query.dry === '1';
      const result = await routeAndExecute(db, p, inc, { dryRun: dry });
      return res.json({ success: true, ...result });
    }
    if (action === 'batch') {
      const limit = Math.min(parseInt(req.query.limit) || 20, 50);
      const persons = await db.raw(`
        SELECT p.*, i.severity, i.fatalities_count
        FROM persons p
        JOIN incidents i ON i.id = p.incident_id
        WHERE p.full_name IS NOT NULL
          AND i.discovered_at > NOW() - INTERVAL '14 days'
        ORDER BY COALESCE(i.lead_score, 0) DESC, i.discovered_at DESC
        LIMIT ?
      `, [limit]).then(r => r.rows || []).catch(() => []);
      const out = { evaluated: 0, executed: 0, ready_for_rep: 0, by_action: {}, samples: [] };
      const startTime = Date.now();
      for (const p of persons) {
        if (Date.now() - startTime > 45000) break;
        out.evaluated++;
        const inc = { id: p.incident_id, severity: p.severity, fatalities_count: p.fatalities_count };
        const r = await routeAndExecute(db, p, inc, { dryRun: req.query.dry === 'true' });
        const a = r.picked?.action || 'noop';
        out.by_action[a] = (out.by_action[a] || 0) + 1;
        if (r.executed) out.executed++;
        if (r.picked?.ready_for_rep) out.ready_for_rep++;
        if (out.samples.length < 8) out.samples.push({ person_id: p.id, action: a, reason: r.picked?.reason, ic: r.identity_confidence });
      }
      return res.json({ success: true, message: `Smart router: ${out.executed}/${out.evaluated} executed, ${out.ready_for_rep} ready_for_rep`, ...out });
    }
    return res.status(400).json({ error: 'invalid action', valid: ['pick', 'batch'] });
  } catch (err) {
    await reportError(db, 'smart-router', null, err.message).catch(()=>{});
    res.status(500).json({ error: err.message });
  }
};

module.exports.pickNextAction = pickNextAction;
module.exports.routeAndExecute = routeAndExecute;
