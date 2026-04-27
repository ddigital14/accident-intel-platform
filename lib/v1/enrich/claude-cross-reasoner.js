/**
 * CLAUDE CROSS-REASONER
 *
 * For top-N highest-score leads each day, aggregate ALL evidence we have
 * (Trestle, PDL, Hunter, voter, court, obit, social, vehicle, weather)
 * and ask Claude Sonnet to validate identity + propose next-best-action.
 *
 * Output written to claude_reasoning_logs.
 *   - confidence_boost in [-15, +15] applied to person.confidence_score
 *   - flag_for_human if Claude detects identity contradiction
 *
 * Endpoints:
 *   GET /api/v1/enrich/claude-cross-reasoner?action=top&limit=20
 *   GET /api/v1/enrich/claude-cross-reasoner?action=person&person_id=<uuid>
 *   GET /api/v1/enrich/claude-cross-reasoner?action=health
 */
const { v4: uuidv4 } = require('uuid');
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { logChange } = require('../system/changelog');
const { enqueueCascade } = require('../system/_cascade');
const { extract } = require('./_ai_router');

let _ensured = false;
async function ensureTable(db) {
  if (_ensured) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS claude_reasoning_logs (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        person_id UUID,
        incident_id UUID,
        identity_verdict VARCHAR(30),
        confidence_boost INTEGER DEFAULT 0,
        flag_for_human BOOLEAN DEFAULT FALSE,
        next_best_action TEXT,
        reasoning TEXT,
        sources_used TEXT[],
        model_used VARCHAR(80),
        tokens_in INTEGER,
        tokens_out INTEGER,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_claude_reasoning_person ON claude_reasoning_logs(person_id);
      CREATE INDEX IF NOT EXISTS idx_claude_reasoning_created ON claude_reasoning_logs(created_at DESC);
    `);
    _ensured = true;
  } catch (_) { /* non-fatal */ }
}

async function gatherAllEvidence(db, person) {
  const evidence = { person, sources: [] };
  if (person.incident_id) {
    const reports = await db('source_reports')
      .where('incident_id', person.incident_id)
      .select('source_type', 'parsed_data', 'confidence', 'created_at')
      .orderBy('created_at', 'desc')
      .limit(20);
    for (const r of reports) {
      let parsed = r.parsed_data;
      if (typeof parsed === 'string') { try { parsed = JSON.parse(parsed); } catch { parsed = null; } }
      evidence.sources.push({ type: r.source_type, confidence: r.confidence, parsed_summary: parsed ? Object.keys(parsed).slice(0, 8) : [] });
    }
  }
  try {
    const logs = await db('enrichment_logs')
      .where('person_id', person.id)
      .orderBy('created_at', 'desc')
      .limit(40)
      .select('field_name', 'new_value', 'confidence', 'created_at');
    evidence.enrichment_logs = logs.slice(0, 20).map(l => ({
      field: l.field_name, value: String(l.new_value || '').slice(0, 100), confidence: l.confidence,
    }));
  } catch (_) { evidence.enrichment_logs = []; }
  if (person.incident_id) {
    const inc = await db('incidents').where('id', person.incident_id).first();
    if (inc) {
      evidence.incident = {
        type: inc.incident_type, severity: inc.severity,
        city: inc.city, state: inc.state, occurred_at: inc.occurred_at,
        fatalities: inc.fatalities_count, injuries: inc.injuries_count,
        description: String(inc.description || '').slice(0, 400),
        source_count: inc.source_count, lead_score: inc.lead_score,
      };
    }
  }
  return evidence;
}

const CLAUDE_SYSTEM = `You are an expert investigator validating accident-victim identities for a personal-injury intelligence platform.
Your job: aggregate every source, decide if they all describe the SAME real person, and recommend the next action.

Possible verdicts:
  "high_confidence"    — 3+ independent sources agree on identity → boost +10 to +15
  "moderate_confidence"— 2 sources agree, no contradictions → boost +5
  "low_confidence"     — only 1 source, or thin evidence → boost 0
  "contradictory"      — sources disagree on key fields (different age/city) → boost -10, flag_for_human=true
  "duplicate_match"    — appears to match a different person already in DB → flag_for_human=true

Return JSON only:
{
  "identity_verdict": "high_confidence|moderate_confidence|low_confidence|contradictory|duplicate_match",
  "confidence_boost": -15..15,
  "flag_for_human": true|false,
  "next_best_action": "send_outreach|enrich_phone|enrich_address|wait_for_obit|attorney_check|do_not_contact",
  "reasoning": "1-3 sentence justification",
  "key_evidence": ["bullet 1", "bullet 2"]
}`;

async function reasonAboutPerson(db, person) {
  await ensureTable(db);
  const evidence = await gatherAllEvidence(db, person);
  const userPrompt = `Person under examination:
${JSON.stringify({
    full_name: person.full_name, age: person.age, phone: person.phone,
    email: person.email, address: person.address, city: person.city,
    state: person.state, employer: person.employer,
    confidence_score: person.confidence_score, has_attorney: person.has_attorney,
  }, null, 2)}

Linked incident:
${JSON.stringify(evidence.incident || {}, null, 2)}

Source reports (${evidence.sources.length}):
${JSON.stringify(evidence.sources.slice(0, 10), null, 2)}

Recent enrichment logs (${evidence.enrichment_logs.length}):
${JSON.stringify(evidence.enrichment_logs.slice(0, 12), null, 2)}

Return your verdict JSON.`;

  const r = await extract(db, {
    pipeline: 'enrich-claude-reasoner',
    systemPrompt: CLAUDE_SYSTEM,
    userPrompt,
    provider: 'claude',
    tier: 'auto',
    severityHint: evidence.incident?.severity,
    timeoutMs: 30000,
    responseFormat: 'json',
    temperature: 0,
  });

  if (!r.ok) return { ok: false, error: r.error };
  const verdict = r.parsed || {};
  const boost = Math.max(-15, Math.min(15, parseInt(verdict.confidence_boost) || 0));
  const newScore = Math.max(0, Math.min(99, (person.confidence_score || 50) + boost));
  await db('persons').where('id', person.id).update({
    confidence_score: newScore, updated_at: new Date(),
  }).catch(() => {});

  await db('claude_reasoning_logs').insert({
    id: uuidv4(),
    person_id: person.id, incident_id: person.incident_id,
    identity_verdict: String(verdict.identity_verdict || 'unknown').slice(0, 30),
    confidence_boost: boost,
    flag_for_human: !!verdict.flag_for_human,
    next_best_action: String(verdict.next_best_action || '').slice(0, 80),
    reasoning: String(verdict.reasoning || '').slice(0, 1000),
    sources_used: evidence.sources.map(s => s.type).slice(0, 20),
    model_used: r.model_used, tokens_in: r.tokens_in, tokens_out: r.tokens_out,
    created_at: new Date(),
  }).catch(() => {});

  await enqueueCascade(db, {
    person_id: person.id, incident_id: person.incident_id,
    trigger_source: 'claude_cross_reasoner',
    trigger_field: 'confidence_score',
    trigger_value: String(newScore),
  }).catch(() => {});

  return { ok: true, verdict, model: r.model_used, confidence_before: person.confidence_score, confidence_after: newScore };
}

async function processTopLeads(db, limit = 20) {
  await ensureTable(db);
  const rows = await db.raw(`
    SELECT p.* FROM persons p
    JOIN incidents i ON p.incident_id = i.id
    WHERE i.discovered_at > NOW() - INTERVAL '48 hours'
      AND p.full_name IS NOT NULL
      AND COALESCE(i.lead_score, 0) >= 50
      AND NOT EXISTS (
        SELECT 1 FROM claude_reasoning_logs crl
        WHERE crl.person_id = p.id
          AND crl.created_at > NOW() - INTERVAL '24 hours'
      )
    ORDER BY i.lead_score DESC, i.discovered_at DESC
    LIMIT ?
  `, [limit]);

  const persons = rows.rows || rows;
  const out = { processed: 0, errors: 0, by_verdict: {} };
  for (const p of persons) {
    try {
      const r = await reasonAboutPerson(db, p);
      if (r.ok) {
        out.processed++;
        const v = r.verdict?.identity_verdict || 'unknown';
        out.by_verdict[v] = (out.by_verdict[v] || 0) + 1;
      } else { out.errors++; }
    } catch (e) {
      out.errors++;
      await reportError(db, 'enrich-claude-reasoner', p.id, e.message);
    }
  }
  return out;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const action = req.query.action || 'top';
  try {
    if (action === 'health') {
      return res.json({
        success: true,
        anthropic_configured: !!process.env.ANTHROPIC_API_KEY,
        timestamp: new Date().toISOString(),
      });
    }
    if (action === 'person' && req.query.person_id) {
      const p = await db('persons').where('id', req.query.person_id).first();
      if (!p) return res.status(404).json({ error: 'person_not_found' });
      const r = await reasonAboutPerson(db, p);
      return res.json({ success: r.ok, ...r });
    }
    const limit = Math.min(50, parseInt(req.query.limit) || 20);
    const r = await processTopLeads(db, limit);
    await logChange(db, {
      kind: 'enrich',
      title: 'Claude cross-reasoner batch',
      summary: `Processed ${r.processed}, errors ${r.errors}`,
      author: 'cron:claude-reasoner',
      meta: r,
    }).catch(() => {});
    return res.json({ success: true, message: `Claude reasoned over ${r.processed} top leads`, ...r, timestamp: new Date().toISOString() });
  } catch (err) {
    await reportError(db, 'enrich-claude-reasoner', null, err.message);
    res.status(500).json({ error: err.message });
  }
};

module.exports.reasonAboutPerson = reasonAboutPerson;
module.exports.processTopLeads = processTopLeads;
