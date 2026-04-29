/**
 * AI CROSS-SOURCE MERGE — Phase 41 Module 3
 *
 * When 2+ source_reports describe the same incident (correlate.js groups them),
 * synthesize all intelligence into one canonical record using Claude Opus 4.6.
 *
 * Resolves conflicts by majority signal, flags genuine disagreements. Never
 * overwrites higher-confidence existing data — only fills nulls or upgrades
 * lower-confidence values. Emits cascade so downstream engines re-fire.
 *
 * Uses Opus because conflict resolution + multi-source reasoning is the case
 * Sonnet starts to drop nuance. ~3-4x more expensive per call but only fires
 * for genuinely multi-sourced incidents (~15% of feed).
 */
const { v4: uuidv4 } = require('uuid');
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { extractJson } = require('../enrich/_ai_router');
const { applyDenyList } = require('../enrich/_name_filter');
const { enqueueCascade } = require('./_cascade');

const SECRET = 'ingest-now';
const AI_TIMEOUT_MS = 45000;

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

function safeParseJson(v) {
  if (!v) return null;
  if (typeof v === 'object') return v;
  try { return JSON.parse(v); } catch (_) { return null; }
}

function buildSourcesPayload(reports) {
  const sources = [];
  for (const r of reports.slice(0, 8)) {
    const raw = safeParseJson(r.raw_data) || {};
    const it = raw.item || raw;
    const lines = [];
    if (it.title) lines.push('TITLE: ' + String(it.title));
    if (it.description) lines.push('DESC: ' + String(it.description));
    if (it.content) lines.push('CONTENT: ' + String(it.content).slice(0, 1500));
    if (it.body) lines.push('BODY: ' + String(it.body).slice(0, 1500));
    sources.push({
      source_type: r.source_type,
      url: r.source_reference,
      text: lines.join('\n').slice(0, 3000)
    });
  }
  return sources;
}

const SYSTEM_PROMPT =
  'Multiple news/data sources cover the same accident. Synthesize ALL information ' +
  'into one canonical incident record. Resolve conflicts by majority signal across sources, ' +
  'flag genuine disagreements. Return JSON only. Be conservative - if a field is not ' +
  'consistently supported, leave it null. Never invent data.';

function userPromptFor(sources) {
  const blocks = sources.map((s, i) =>
    'SOURCE ' + (i + 1) + ' (' + s.source_type + ', ' + (s.url || 'no-url') + '):\n' + s.text
  ).join('\n\n---\n\n');
  return (
    blocks + '\n\n---\n\n' +
    'Return JSON of this exact shape:\n' +
    '{"canonical": {"accident_summary": "<one paragraph>", ' +
    '"victims": [{"full_name": "...", "role": "victim|driver|passenger|pedestrian|cyclist", ' +
    '"age": <int or null>, "city": "<string or null>", "state": "<2-letter or null>", ' +
    '"injury_severity": "fatal|critical|serious|moderate|minor|unknown", ' +
    '"hospital": "<string or null>", "employer": "<string or null>"}], ' +
    '"date": "<YYYY-MM-DD or null>", "time": "<HH:MM 24h or null>", ' +
    '"location": {"address": "<string or null>", "intersection": "<string or null>", "city": "<string or null>", "state": "<2-letter or null>"}, ' +
    '"vehicles": [{"make": "<string or null>", "model": "<string or null>", "color": "<string or null>", "year": <int or null>}], ' +
    '"cause": "<string or null>", "fatalities": <int>, "injuries": <int>, ' +
    '"agencies_involved": ["..."], "attorneys_attached": ["..."]},\n' +
    '"conflicts": [{"field": "...", "source_a_value": "...", "source_b_value": "...", "severity": "minor|major"}],\n' +
    '"confidence_per_field": {"full_name": <0-100>, "date": <0-100>, "location": <0-100>, "cause": <0-100>, "fatalities": <0-100>, "victims_total": <0-100>}}\n\n' +
    'Use null/empty arrays where data is genuinely missing.'
  );
}

function pickBetter(existing, candidate, fieldConfidence) {
  // Only fill nulls or upgrade if our new confidence is high enough
  if (existing === null || existing === undefined || existing === '') return candidate;
  if (typeof fieldConfidence === 'number' && fieldConfidence >= 80) return candidate;
  return existing;
}

async function mergeOneIncident(db, incidentId) {
  const incident = await db('incidents').where('id', incidentId).first();
  if (!incident) return { ok: false, error: 'incident_not_found' };

  const reports = await db('source_reports')
    .where('incident_id', incidentId)
    .orderBy('created_at', 'desc')
    .limit(8)
    .select('raw_data', 'source_type', 'source_reference', 'created_at');

  if (reports.length < 2) {
    return { ok: false, error: 'insufficient_sources', sources: reports.length };
  }

  const sourcesPayload = buildSourcesPayload(reports);
  if (!sourcesPayload.length) return { ok: false, error: 'no_text_in_sources' };

  // Force Claude Opus for premium reasoning
  const parsed = await extractJson(db, {
    pipeline: 'system-ai-cross-source-merge',
    systemPrompt: SYSTEM_PROMPT,
    userPrompt: userPromptFor(sourcesPayload),
    provider: 'claude',
    tier: 'premium',
    severityHint: 'fatal',
    timeoutMs: AI_TIMEOUT_MS,
    temperature: 0
  });

  if (!parsed || !parsed.canonical) {
    return { ok: false, error: 'ai_no_parse', incident_id: incidentId };
  }

  const can = parsed.canonical;
  const conf = parsed.confidence_per_field || {};
  const conflicts = Array.isArray(parsed.conflicts) ? parsed.conflicts : [];

  // Update incident — only fill nulls or upgrade with high confidence
  const update = { updated_at: new Date() };
  if (can.accident_summary) update.description = pickBetter(incident.description, String(can.accident_summary).slice(0, 2000), 100);
  if (can.cause) update.metadata = JSON.stringify(Object.assign({},
    safeParseJson(incident.metadata) || {},
    { ai_merged_cause: can.cause, ai_merged_at: new Date().toISOString(), conflicts_count: conflicts.length }
  ));
  if (can.location) {
    if (can.location.address) update.address = pickBetter(incident.address, String(can.location.address).slice(0, 500), conf.location);
    if (can.location.intersection) update.intersection = pickBetter(incident.intersection, String(can.location.intersection).slice(0, 250), conf.location);
    if (can.location.city) update.city = pickBetter(incident.city, String(can.location.city).slice(0, 100), conf.location);
    if (can.location.state && String(can.location.state).length === 2) {
      update.state = pickBetter(incident.state, String(can.location.state).toUpperCase(), conf.location);
    }
  }
  if (Number.isInteger(can.fatalities) && (incident.fatalities_count == null || conf.fatalities >= 80)) update.fatalities_count = can.fatalities;
  if (Number.isInteger(can.injuries) && (incident.injuries_count == null || conf.victims_total >= 80)) update.injuries_count = can.injuries;
  if (can.date) {
    try {
      const isoStart = can.time ? can.date + 'T' + can.time + ':00Z' : can.date + 'T00:00:00Z';
      const dt = new Date(isoStart);
      if (!isNaN(dt.getTime()) && (incident.occurred_at == null || conf.date >= 80)) update.occurred_at = dt;
    } catch (_) {}
  }

  await db('incidents').where('id', incidentId).update(update);

  // Victims — augment / link
  let victimsAdded = 0;
  const victimsList = Array.isArray(can.victims) ? can.victims : [];
  for (const v of victimsList.slice(0, 15)) {
    const rawName = (v?.full_name || '').toString().trim();
    if (!rawName) continue;
    const safeName = applyDenyList(rawName, sourcesPayload.map(s => s.text).join('\n'));
    if (!safeName) continue;
    const existing = await db('persons')
      .where('incident_id', incidentId)
      .whereRaw('LOWER(full_name) = LOWER(?)', [safeName])
      .first();

    const role = (function () {
      const x = String(v.role || 'victim').toLowerCase();
      if (x.includes('driv')) return 'driver';
      if (x.includes('pass')) return 'passenger';
      if (x.includes('pedes')) return 'pedestrian';
      if (x.includes('cyc')) return 'cyclist';
      return 'victim';
    })();

    if (existing) {
      // Fill nulls only
      const personUpd = { updated_at: new Date() };
      if (!existing.age && Number.isInteger(v.age)) personUpd.age = v.age;
      if (!existing.injury_severity && v.injury_severity) personUpd.injury_severity = String(v.injury_severity).toLowerCase();
      if (!existing.transported_to && v.hospital) personUpd.transported_to = String(v.hospital).slice(0, 200);
      if (!existing.employer && v.employer) personUpd.employer = String(v.employer).slice(0, 200);
      if (Object.keys(personUpd).length > 1) {
        await db('persons').where('id', existing.id).update(personUpd);
        try {
          await enqueueCascade(db, {
            person_id: existing.id, incident_id: incidentId,
            trigger_source: 'ai-cross-source-merge', trigger_field: 'augmented'
          });
        } catch (_) {}
      }
    } else {
      // Insert new
      try {
        const parts = safeName.split(/\s+/);
        const id = uuidv4();
        await db('persons').insert({
          id,
          incident_id: incidentId,
          role,
          first_name: parts[0] || null,
          last_name: parts.length > 1 ? parts[parts.length - 1] : null,
          full_name: safeName,
          age: Number.isInteger(v.age) ? v.age : null,
          city: v.city || incident.city || null,
          state: (v.state && String(v.state).length === 2) ? String(v.state).toUpperCase() : (incident.state || null),
          employer: v.employer || null,
          injury_severity: v.injury_severity || null,
          transported_to: v.hospital || null,
          contact_status: 'not_contacted',
          confidence_score: 80,
          identity_confidence: 75,
          victim_verified: true,
          derived_from: 'ai-cross-source-merge',
          metadata: JSON.stringify({ source: 'ai-cross-source-merge' }),
          created_at: new Date(),
          updated_at: new Date()
        });
        victimsAdded++;
        try {
          await enqueueCascade(db, {
            person_id: id, incident_id: incidentId,
            trigger_source: 'ai-cross-source-merge', trigger_field: 'victim_added',
            priority: v.injury_severity === 'fatal' ? 1 : 4
          });
        } catch (_) {}
      } catch (_) {}
    }
  }

  // Log conflicts to enrichment_logs.data
  try {
    await db('enrichment_logs').insert({
      person_id: null,
      field_name: 'ai-cross-source-merge:incident',
      old_value: null,
      new_value: JSON.stringify({ victims_added: victimsAdded, conflicts: conflicts.length, sources: reports.length }).slice(0, 4000),
      action: 'ai-cross-source-merge',
      confidence: 90,
      verified: true,
      meta: JSON.stringify({
        incident_id: incidentId,
        cross_source_conflicts: conflicts,
        confidence_per_field: conf,
        sources_count: reports.length
      }).slice(0, 4000),
      created_at: new Date()
    });
  } catch (_) {}

  return {
    ok: true,
    incident_id: incidentId,
    sources_merged: reports.length,
    victims_added: victimsAdded,
    conflicts_count: conflicts.length,
    fields_updated: Object.keys(update).filter(k => k !== 'updated_at')
  };
}

async function batchMerge(db, limit = 5) {
  // Find incidents that have 2+ source_reports and haven't been merged yet
  const rows = await db.raw(
    'SELECT i.id ' +
    'FROM incidents i ' +
    'JOIN ( ' +
    '  SELECT incident_id, COUNT(*) AS c ' +
    '  FROM source_reports ' +
    '  WHERE incident_id IS NOT NULL ' +
    '  GROUP BY incident_id ' +
    '  HAVING COUNT(*) >= 2 ' +
    ') sr ON sr.incident_id = i.id ' +
    'WHERE i.created_at > NOW() - INTERVAL \'21 days\' ' +
    'AND NOT EXISTS ( ' +
    '  SELECT 1 FROM enrichment_logs el ' +
    '  WHERE el.action = ? ' +
    '  AND (el.data->>\'incident_id\') = i.id::text ' +
    ') ' +
    'ORDER BY sr.c DESC, i.created_at DESC ' +
    'LIMIT ?',
    ['ai-cross-source-merge', limit]
  );
  const ids = rows.rows.map(r => r.id);

  const out = { candidates: ids.length, processed: 0, merged: 0, victims_added: 0, conflicts: 0, samples: [], errors: [] };
  for (const id of ids) {
    try {
      const r = await mergeOneIncident(db, id);
      out.processed++;
      if (r.ok) {
        out.merged++;
        out.victims_added += r.victims_added || 0;
        out.conflicts += r.conflicts_count || 0;
        if (out.samples.length < 5) out.samples.push({
          incident_id: id, sources: r.sources_merged, victims_added: r.victims_added, conflicts: r.conflicts_count
        });
      } else {
        out.errors.push({ incident_id: id, error: r.error });
      }
    } catch (e) {
      out.errors.push({ incident_id: id, error: e.message?.slice(0, 200) });
      try { await reportError(db, 'system-ai-cross-source-merge', id, e.message); } catch (_) {}
    }
  }
  return out;
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const db = getDb();
  const action = (req.query?.action || 'health').toLowerCase();

  try {
    if (action === 'health') {
      const merged = await db('enrichment_logs').where('action', 'ai-cross-source-merge').count('* as c').first().then(r => parseInt(r.c || 0));
      return res.json({
        success: true,
        action: 'health',
        merges_total: merged,
        valid_actions: ['health', 'merge', 'batch'],
        timestamp: new Date().toISOString()
      });
    }
    if (action === 'merge') {
      const incidentId = req.query?.incident_id;
      if (!incidentId) return res.status(400).json({ error: 'incident_id required' });
      const r = await mergeOneIncident(db, incidentId);
      return res.json({ success: !!r.ok, ...r, timestamp: new Date().toISOString() });
    }
    if (action === 'batch') {
      const limit = Math.min(10, parseInt(req.query?.limit || '5'));
      const out = await batchMerge(db, limit);
      return res.json({ success: true, action: 'batch', ...out, timestamp: new Date().toISOString() });
    }
    res.status(400).json({ error: 'unknown action', valid: ['health', 'merge', 'batch'] });
  } catch (e) {
    try { await reportError(db, 'system-ai-cross-source-merge', null, e.message); } catch (_) {}
    res.status(500).json({ success: false, error: e.message });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.mergeOneIncident = mergeOneIncident;
module.exports.batchMerge = batchMerge;
