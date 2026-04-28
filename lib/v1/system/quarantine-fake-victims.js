/**
 * GET /api/v1/system/quarantine-fake-victims?secret=ingest-now
 *
 * One-shot manual cleanup: scan every person attached to a currently-qualified
 * incident, run Stage-A of the victim-verifier (regex hard rules only — fast),
 * and if the person fails, demote the incident to qualification_state =
 * 'pending_unverified' and log the rejection reason on enrichment_log.
 *
 * Returns: { checked, kept, quarantined, samples: [...] }
 *
 * Wired into the master router (NOT cron) — Phase 38 Wave A.
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { trackApiCall } = require('./cost');
const { quickClassify } = require('../enrich/_name_filter');

const SECRET = 'ingest-now';

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function _ensureLogTable(db) {
  try {
    await db.raw(
      'CREATE TABLE IF NOT EXISTS enrichment_log (' +
      '  id BIGSERIAL PRIMARY KEY,' +
      '  person_id UUID,' +
      '  incident_id UUID,' +
      '  pipeline VARCHAR(80),' +
      '  action VARCHAR(60),' +
      '  reason TEXT,' +
      '  context JSONB DEFAULT \'{}\'::jsonb,' +
      '  created_at TIMESTAMPTZ DEFAULT NOW()' +
      ');' +
      'CREATE INDEX IF NOT EXISTS idx_enrichment_log_person ON enrichment_log(person_id);' +
      'CREATE INDEX IF NOT EXISTS idx_enrichment_log_incident ON enrichment_log(incident_id);'
    );
  } catch (e) {
    console.error('enrichment_log ensure failed:', e.message);
  }
}

async function _representativeText(db, incidentId) {
  try {
    const rep = await db('source_reports')
      .where('incident_id', incidentId)
      .orderBy('fetched_at', 'desc')
      .first('raw_data', 'parsed_data');
    if (!rep) return '';
    const raw = typeof rep.raw_data === 'string'
      ? (() => { try { return JSON.parse(rep.raw_data); } catch (_) { return null; } })()
      : rep.raw_data;
    if (raw && typeof raw === 'object') {
      return [
        raw?.item?.title, raw?.item?.description,
        raw?.title, raw?.description, raw?.body, raw?.text
      ].filter(Boolean).join('\n').slice(0, 6000);
    }
    return String(rep.raw_data || '').slice(0, 6000);
  } catch (_) { return ''; }
}

async function quarantine(db) {
  await _ensureLogTable(db);
  const t0 = Date.now();
  // Pull every person on a currently-qualified incident
  const rows = await db('persons')
    .innerJoin('incidents', 'incidents.id', 'persons.incident_id')
    .where('incidents.qualification_state', 'qualified')
    .whereNotNull('persons.full_name')
    .select(
      'persons.id as pid',
      'persons.full_name as name',
      'persons.incident_id',
      'incidents.incident_type as inc_type',
      'incidents.city as city',
      'incidents.state as state'
    );

  const results = {
    checked: 0,
    kept: 0,
    quarantined: 0,
    quarantined_incidents: 0,
    samples_kept: [],
    samples_rejected: [],
    elapsed_ms: 0
  };

  // Group persons by incident
  const byIncident = {};
  for (const r of rows) {
    (byIncident[r.incident_id] ||= []).push(r);
  }

  for (const [incidentId, people] of Object.entries(byIncident)) {
    const text = await _representativeText(db, incidentId);
    let anyVictim = false;
    const rejectedHere = [];

    for (const p of people) {
      results.checked++;
      const cls = quickClassify(p.name, text);
      const denied = cls.decision === 'deny' && cls.confidence >= 70;
      const accepted = cls.decision === 'accept' && cls.confidence >= 80;
      if (denied) {
        results.quarantined++;
        rejectedHere.push({ pid: p.pid, name: p.name, reason: cls.reason });
        // Mark person as failing verification
        try {
          await db('persons').where('id', p.pid).update({
            victim_verified: false,
            victim_role: _roleFromReason(cls.reason),
            victim_verifier_reason: 'stage_a:' + cls.reason,
            victim_verifier_stage: 'A',
            updated_at: new Date()
          });
        } catch (_) {}
        try {
          await db('enrichment_log').insert({
            person_id: p.pid,
            incident_id: incidentId,
            pipeline: 'quarantine-fake-victims',
            action: 'reject',
            reason: cls.reason,
            context: JSON.stringify({ classifier: cls, name: p.name })
          });
        } catch (_) {}
        if (results.samples_rejected.length < 12) {
          results.samples_rejected.push({ name: p.name, reason: cls.reason, incident_id: incidentId });
        }
      } else if (accepted) {
        anyVictim = true;
        results.kept++;
        try {
          await db('persons').where('id', p.pid).update({
            victim_verified: true,
            victim_role: 'victim',
            victim_verifier_reason: 'stage_a:' + cls.reason,
            victim_verifier_stage: 'A',
            updated_at: new Date()
          });
        } catch (_) {}
        if (results.samples_kept.length < 8) {
          results.samples_kept.push({ name: p.name, reason: cls.reason, incident_id: incidentId });
        }
      } else {
        // unsure — leave alone (Stage B / batch will handle)
        results.kept++;
      }
    }

    // If NO confirmed victim survived on this incident, quarantine the incident
    if (!anyVictim && rejectedHere.length > 0) {
      try {
        await db('incidents').where('id', incidentId).update({
          qualification_state: 'pending_unverified',
          updated_at: new Date()
        });
        results.quarantined_incidents++;
        try {
          await db('enrichment_log').insert({
            incident_id: incidentId,
            pipeline: 'quarantine-fake-victims',
            action: 'demote_incident',
            reason: 'all_persons_failed_stage_a',
            context: JSON.stringify({ rejected: rejectedHere })
          });
        } catch (_) {}
      } catch (e) {
        await reportError(db, 'quarantine-fake-victims', incidentId,
          'demote_failed:' + e.message, { severity: 'warning' });
      }
    }
  }

  results.elapsed_ms = Date.now() - t0;
  return results;
}

function _roleFromReason(reason) {
  if (!reason) return 'unknown';
  if (reason.startsWith('byline_match') || reason.startsWith('outlet_tag')) return 'author';
  if (reason.startsWith('official_title')) return 'officer';
  if (reason.startsWith('attribution_only')) return 'witness';
  return 'unknown';
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const db = getDb();
  try {
    const r = await quarantine(db);
    await trackApiCall(db, 'quarantine-fake-victims', 'rules', 0, 0, true).catch(() => {});
    return res.status(200).json({ success: true, ...r });
  } catch (e) {
    await reportError(db, 'quarantine-fake-victims', null, e.message, { severity: 'error' });
    return res.status(500).json({ error: e.message, success: false });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.quarantine = quarantine;
