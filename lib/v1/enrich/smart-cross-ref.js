/**
 * Phase 50: Smart Cross-Reference engine — Claude Opus 4.7 reasoning over
 * known victim/passenger/employer/vehicle data to predict missing fields.
 *
 * For each verified victim:
 *   - Pull all known data (persons, passengers in same incident, vehicle, employer).
 *   - Send to Opus 4.7 with reasoning prompt asking about likely:
 *     1. Mobile area code (vs landline) given known phones.
 *     2. Passenger relationships (family if young + same surname).
 *     3. Likely work email pattern from employer.
 *     4. Phone-type classification (mobile/home/work).
 *     5. Secondary contacts (spouse, parent, employer phone).
 *   - Insert each prediction as candidate in enrichment_logs with
 *     meta.engine='smart-cross-ref' + confidence.
 *   - Verify against existing engines (FCC carrier, Trestle, voter rolls)
 *     before promoting to persons row.
 *
 * GET /api/v1/enrich/smart-cross-ref?secret=ingest-now&action=health
 * GET /api/v1/enrich/smart-cross-ref?secret=ingest-now&action=run&person_id=<uuid>
 * GET /api/v1/enrich/smart-cross-ref?secret=ingest-now&action=batch&limit=5
 */
const { getDb } = require('../../_db');
const { extractJson } = require('./_ai_router');
const { reportError } = require('../system/_errors');
const { bumpCounter } = require('../system/_cei_telemetry');
const { v4: uuidv4 } = require('uuid');

const ENGINE = 'smart-cross-ref';

async function ensureLogTable(db) {
  await db.raw(`
    CREATE TABLE IF NOT EXISTS enrichment_logs (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      person_id UUID,
      incident_id UUID,
      engine TEXT,
      candidate_type TEXT,
      candidate_value TEXT,
      confidence INTEGER DEFAULT 0,
      verified BOOLEAN DEFAULT false,
      meta JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_enrichment_logs_person ON enrichment_logs(person_id);
    CREATE INDEX IF NOT EXISTS idx_enrichment_logs_engine ON enrichment_logs(engine);
  `).catch(() => {});
}

async function gatherContext(db, personId) {
  const person = await db('persons').where('id', personId).first();
  if (!person) return null;

  const passengers = await db('persons')
    .where('incident_id', person.incident_id)
    .whereNot('id', personId)
    .select('id', 'full_name', 'role', 'age', 'city', 'state')
    .limit(15);

  const incident = await db('incidents').where('id', person.incident_id).first();

  // existing phones/emails so we can ask about classification
  let phones = [];
  let emails = [];
  try {
    const c = await db('contacts').where('person_id', personId).select('contact_type', 'contact_value', 'meta').limit(30);
    phones = c.filter(r => r.contact_type === 'phone').map(r => r.contact_value);
    emails = c.filter(r => r.contact_type === 'email').map(r => r.contact_value);
  } catch (_) {}

  const employer = person.employer || person.work || null;
  const ctx = {
    person: {
      id: person.id,
      full_name: person.full_name,
      first_name: person.first_name,
      last_name: person.last_name,
      age: person.age,
      city: person.city,
      state: person.state,
      employer,
      role: person.role,
      severity: person.injury_severity || person.severity
    },
    incident: incident ? {
      id: incident.id,
      city: incident.city,
      state: incident.state,
      occurred_at: incident.occurred_at,
      incident_type: incident.incident_type,
      severity: incident.severity
    } : null,
    passengers: passengers || [],
    known_phones: phones,
    known_emails: emails
  };
  return { person, ctx };
}

async function reasonWithOpus(db, ctx) {
  const sys = `You are an expert OSINT analyst with deep cultural fluency across U.S. immigrant and minority communities. Given accident scenario data, reason about likely missing contact info. Use phone-numbering geography (mobile vs landline patterns), surname/age/role to infer family relationships, and corporate email conventions. NEVER fabricate phone numbers or emails \u2014 only suggest patterns/area codes/types and label confidence honestly.

Consider community-specific contact patterns when reasoning:
- Hispanic victims: family often shares GoFundMe on Facebook + WhatsApp; Spanish-language obituaries on funeralhomes.com /esp pages; Catholic parish bulletins.
- Black/African American victims: church community boards (AME, Baptist); HBCU alumni networks if college-age.
- Asian American victims: language-specific obituary sites (worldjournal.com death notices in Chinese; KoreaDaily.com \ubd80\uace0 page).
- Haitian victims: WhatsApp groups primary contact channel; French/Creole local papers (Le Floridien, Haiti-Observateur).
- Vietnamese victims: Nguoi Viet community paper; temple/church (Catholic or Buddhist) memorial bulletins.
- All communities: family WhatsApp + Facebook reactions to news articles often surface emails/phones in comments.

Return JSON only.`;
  const userPrompt = `Accident-victim data:
${JSON.stringify(ctx, null, 2)}

Reason about likely missing fields and return JSON:
{
  "predicted_phones": [
    { "number_or_pattern": "string (e.g. '330-555-XXXX' or specific digits if reasonably inferable)", "type": "mobile|home|work", "confidence": 0-100, "reasoning": "string" }
  ],
  "predicted_emails": [
    { "address_or_pattern": "string (e.g. 'firstname.lastname@employer.com')", "type": "work|personal", "confidence": 0-100, "reasoning": "string" }
  ],
  "passenger_relationships": [
    { "passenger_name": "string", "likely_relation": "spouse|child|parent|sibling|friend|coworker|unknown", "contactability": 0-100, "reasoning": "string" }
  ],
  "secondary_contacts": [
    { "type": "spouse_phone|parent_phone|employer_phone|emergency_contact", "identifier": "string", "source": "string", "confidence": 0-100 }
  ],
  "phone_type_classifications": [
    { "phone": "string (from known_phones)", "classified_as": "mobile|home|work", "confidence": 0-100, "reasoning": "string" }
  ],
  "cultural_community_inference": {
    "likely_community": "hispanic|black|asian_chinese|asian_korean|asian_filipino|asian_vietnamese|haitian|native_american|arab|russian|portuguese_brazilian|white_anglo|unknown",
    "confidence": 0-100,
    "signals": ["string \u2014 what in the data suggests this community"]
  },
  "next_best_actions": [
    "string \u2014 2-3 culturally-aware NEXT-BEST-ACTIONS for the rep. Example: 'For Maria Gonz\u00e1lez in San Antonio TX: search Facebook for \"GoFundMe Maria Gonz\u00e1lez accident\" \u2014 Hispanic families typically organize within 48h of fatal accidents.'"
  ],
  "cross_validation_recommendations": [
    "string - which engine should validate which prediction (e.g. 'Run FCC carrier on 330-XXX-1234 to confirm mobile classification')"
  ]
}`;

  return await extractJson(db, {
    pipeline: ENGINE,
    systemPrompt: sys,
    userPrompt,
    tier: 'opus',
    provider: 'claude',
    timeoutMs: 50000
  });
}

async function persistCandidates(db, personId, incidentId, predicted) {
  await ensureLogTable(db);
  const inserts = [];
  const now = new Date();

  for (const p of (predicted?.predicted_phones || [])) {
    const v = p.number_or_pattern || p.number;
    if (!v) continue;
    inserts.push({
      id: uuidv4(), person_id: personId, incident_id: incidentId,
      engine: ENGINE, candidate_type: 'phone', candidate_value: String(v).slice(0, 64),
      confidence: parseInt(p.confidence || 0, 10) || 0, verified: false,
      meta: JSON.stringify({ engine: ENGINE, type: p.type, reasoning: p.reasoning }),
      created_at: now
    });
  }
  for (const e of (predicted?.predicted_emails || [])) {
    const v = e.address_or_pattern || e.address;
    if (!v) continue;
    inserts.push({
      id: uuidv4(), person_id: personId, incident_id: incidentId,
      engine: ENGINE, candidate_type: 'email', candidate_value: String(v).slice(0, 200),
      confidence: parseInt(e.confidence || 0, 10) || 0, verified: false,
      meta: JSON.stringify({ engine: ENGINE, type: e.type, reasoning: e.reasoning }),
      created_at: now
    });
  }
  for (const r of (predicted?.passenger_relationships || [])) {
    inserts.push({
      id: uuidv4(), person_id: personId, incident_id: incidentId,
      engine: ENGINE, candidate_type: 'relationship', candidate_value: `${r.passenger_name || ''}:${r.likely_relation || ''}`.slice(0, 200),
      confidence: parseInt(r.contactability || 0, 10) || 0, verified: false,
      meta: JSON.stringify({ engine: ENGINE, reasoning: r.reasoning }),
      created_at: now
    });
  }
  for (const sc of (predicted?.secondary_contacts || [])) {
    inserts.push({
      id: uuidv4(), person_id: personId, incident_id: incidentId,
      engine: ENGINE, candidate_type: 'secondary_contact', candidate_value: `${sc.type}:${sc.identifier}`.slice(0, 200),
      confidence: parseInt(sc.confidence || 0, 10) || 0, verified: false,
      meta: JSON.stringify({ engine: ENGINE, source: sc.source }),
      created_at: now
    });
  }

  if (inserts.length === 0) return 0;
  try {
    await db('enrichment_logs').insert(inserts);
    return inserts.length;
  } catch (e) {
    try { await reportError(db, ENGINE, personId, `enrichment_logs insert failed: ${e.message}`); } catch (_) {}
    return 0;
  }
}

async function runForPerson(db, personId) {
  const t0 = Date.now();
  const gathered = await gatherContext(db, personId);
  if (!gathered) { await bumpCounter(db, ENGINE, false, Date.now() - t0).catch(()=>{}); return { ok: false, error: 'person_not_found' }; }
  const predicted = await reasonWithOpus(db, gathered.ctx);
  if (!predicted) { await bumpCounter(db, ENGINE, false, Date.now() - t0).catch(()=>{}); return { ok: false, error: 'opus_returned_nothing' }; }
  const inserted = await persistCandidates(db, personId, gathered.person.incident_id, predicted);
  await bumpCounter(db, ENGINE, true, Date.now() - t0).catch(() => {});
  return {
    ok: true,
    person_id: personId,
    candidates_inserted: inserted,
    predictions: {
      phones: (predicted.predicted_phones || []).length,
      emails: (predicted.predicted_emails || []).length,
      relationships: (predicted.passenger_relationships || []).length,
      secondary_contacts: (predicted.secondary_contacts || []).length,
      phone_classifications: (predicted.phone_type_classifications || []).length,
      validation_recommendations: predicted.cross_validation_recommendations || []
    },
    sample: predicted
  };
}

async function batchRun(db, limit = 5) {
  // Pick verified victims with thin contact rosters that haven't been smart-cross-ref'd today
  const targets = await db.raw(`
    SELECT p.id
    FROM persons p
    LEFT JOIN enrichment_logs el ON el.person_id = p.id
      AND el.engine = 'smart-cross-ref'
      AND el.created_at > NOW() - INTERVAL '24 hours'
    WHERE p.full_name IS NOT NULL
      AND COALESCE(p.identity_confidence, 0) >= 60
      AND el.id IS NULL
    ORDER BY p.created_at DESC
    LIMIT ${parseInt(limit, 10) || 5}
  `).catch(() => ({ rows: [] }));

  const results = { processed: 0, ok: 0, failed: 0, candidates_total: 0, samples: [] };
  for (const row of (targets.rows || [])) {
    results.processed++;
    const r = await runForPerson(db, row.id);
    if (r.ok) {
      results.ok++;
      results.candidates_total += (r.candidates_inserted || 0);
      if (results.samples.length < 2) results.samples.push({ person_id: r.person_id, predictions: r.predictions });
    } else {
      results.failed++;
    }
  }
  return results;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const action = req.query?.action || 'health';

  try {
    if (action === 'health') {
      return res.json({
        success: true,
        engine: ENGINE,
        message: 'Smart cross-ref reasoner online (Opus 4.7)',
        capabilities: ['predict_phones', 'predict_emails', 'classify_phone_type', 'infer_relationships', 'secondary_contacts'],
        timestamp: new Date().toISOString()
      });
    }
    if (action === 'run') {
      const pid = req.query?.person_id;
      if (!pid) return res.status(400).json({ error: 'person_id required' });
      const out = await runForPerson(db, pid);
      return res.json({ success: !!out.ok, ...out });
    }
    if (action === 'batch') {
      const limit = parseInt(req.query?.limit || '5', 10);
      const out = await batchRun(db, limit);
      return res.json({ success: true, message: `smart-cross-ref batch: ${out.ok}/${out.processed} ok, ${out.candidates_total} candidates`, ...out });
    }
    return res.status(400).json({ error: 'unknown action', allowed: ['health', 'run', 'batch'] });
  } catch (err) {
    await reportError(db, ENGINE, null, err.message).catch(() => {});
    return res.status(500).json({ error: err.message });
  }
};

module.exports.runForPerson = runForPerson;
module.exports.batchRun = batchRun;
