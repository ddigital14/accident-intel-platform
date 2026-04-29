/**
 * FAMILY TREE engine — Obituary NER → relatives extractor
 *
 * For every fatal-incident obituary we already scraped (source_type='obituary'),
 * extract "survived by", "preceded in death by", "leaves behind" passages with
 * GPT-4o-mini and emit a structured list of relatives. Each relative becomes a
 * persons row linked to the deceased via `related_to_person_id`, then a cascade
 * fires so phone/email/address enrichers run on the relative — extending the
 * cross-conversion graph from the deceased outward to family contacts (the
 * actual people who hire attorneys after a fatal crash).
 *
 * Per CORE_INTENT.md: every relative INSERT triggers enqueueCascade so the
 * full multi-source identity chain runs against family members too.
 *
 * Endpoints:
 *   GET /api/v1/enrich/family-tree?action=process&limit=20  (cron)
 *   GET /api/v1/enrich/family-tree?action=extract&person_id=<id>
 *   GET /api/v1/enrich/family-tree?action=health
 *
 * Cost: GPT-4o-mini ~$0.0002 per obituary
 * Cross-exam weight: 75 (obituary text is family-authored, authoritative)
 */
const { getModelForTask } = require('../system/model-registry');
const { v4: uuidv4 } = require('uuid');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { logChange } = require('../system/changelog');
const { enqueueCascade } = require('../system/_cascade');
const { normalizePerson } = require('../../_schema');

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const RELATIONS = ['spouse', 'child', 'sibling', 'parent', 'grandchild', 'grandparent', 'in_law', 'other'];

let _ensured = false;
async function ensureColumns(db) {
  if (_ensured) return;
  try {
    await db.raw(`ALTER TABLE persons ADD COLUMN IF NOT EXISTS related_to_person_id UUID`);
    await db.raw(`ALTER TABLE persons ADD COLUMN IF NOT EXISTS relation_type VARCHAR(40)`);
    await db.raw(`CREATE INDEX IF NOT EXISTS idx_persons_related_to ON persons(related_to_person_id)`);
    _ensured = true;
  } catch (e) { /* non-fatal */ }
}

/**
 * Extract relatives from ANY source text (news article, obituary, GoFundMe, court filing,
 * social post) using GPT-4o-mini. Works for both fatal AND non-fatal injury cases —
 * relatives can be mentioned in any source.
 */
async function extractRelatives(db, sourceText, victimName) {
  if (!OPENAI_API_KEY || !sourceText) return { ok: false, error: 'no_openai_or_text' };
  const txt = String(sourceText).substring(0, 4000);
  const prompt = `Source text mentioning ${victimName || 'an accident victim'}:
"""
${txt}
"""

Extract every NAMED RELATIVE of the victim from the text. Look for patterns like:
- "survived by" / "leaves behind" / "preceded in death by" (fatal cases)
- "his/her wife/husband/son/daughter/mother/father [Name]" (fatal OR injury cases)
- "[Name]'s family" / "[Name]'s mother [SubName]" / "the victim's brother [Name]"
- "GoFundMe organized by [Name], the victim's [relation]"
- "[Name] told reporters her brother was the driver"
- Court filings: "[Name] et al. as next of kin"

Works for ALL accident cases, not just fatalities. A car accident with a husband-passenger
or a slip-and-fall where the news mentions "his daughter said" both count.

Return JSON only:
{
  "relatives": [
    {
      "name": "Full Name",
      "relation": "spouse|child|sibling|parent|grandchild|grandparent|in_law|other",
      "age": number|null,
      "city": "City, ST or null",
      "deceased": true|false
    }
  ]
}
Skip generic phrases like "many friends" or "extended family". Only named people.`;

  try {
    const resp = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: await getModelForTask('obit_ner', 'gpt-4o-mini'),
        messages: [
          { role: 'system', content: 'Extract named relatives from obituaries. Return JSON only. Empty list if none found.' },
          { role: 'user', content: prompt }
        ],
        temperature: 0,
        response_format: { type: 'json_object' }
      }),
      signal: AbortSignal.timeout(15000)
    });
    const tin = Math.ceil(prompt.length / 4);
    const tout = 400;
    await trackApiCall(db, 'enrich-family-tree', 'gpt-4o-mini', tin, tout, resp.ok);
    if (!resp.ok) return { ok: false, status: resp.status };
    const data = await resp.json();
    const parsed = JSON.parse(data.choices?.[0]?.message?.content || '{}');
    const relatives = (parsed.relatives || []).filter(r => r && r.name && RELATIONS.includes(r.relation));
    return { ok: true, relatives };
  } catch (e) {
    await trackApiCall(db, 'enrich-family-tree', 'gpt-4o-mini', 0, 0, false);
    return { ok: false, error: e.message };
  }
}

/**
 * For one deceased person: pull obit source_report → extract relatives →
 * INSERT persons (canonical normalized) → emit cascade for each relative.
 */
async function processDeceased(db, person) {
  await ensureColumns(db);
  if (!person || !person.id) return { ok: false, error: 'no_person' };

  // Phase 43: family info can come from ANY source — news articles ("survived by"),
  // GoFundMe ("organized by his sister"), court records, social posts, scanner reports.
  // Pull all available source_reports for the incident and concatenate text.
  // Previously gated on source_type='obituary' only, which excluded all non-fatal injury cases.
  const sources = await db('source_reports')
    .where('incident_id', person.incident_id)
    .orderBy('created_at', 'desc')
    .limit(8);
  if (!sources.length) return { ok: true, skipped: 'no_source_reports' };

  // Concatenate text from up to 8 sources
  let obitText = '';
  for (const src of sources) {
    let txt = '';
    try {
      const raw = typeof src.raw_data === 'string' ? JSON.parse(src.raw_data) : src.raw_data;
      txt = raw?.html_excerpt || raw?.text || raw?.body || '';
      if (!txt) {
        const parsed = typeof src.parsed_data === 'string' ? JSON.parse(src.parsed_data) : src.parsed_data;
        txt = parsed?.full_text || JSON.stringify(parsed || {}).substring(0, 1500);
      }
    } catch (_) { txt = String(src.raw_data || src.parsed_data || ''); }
    if (txt) obitText += '\n\n[' + (src.source_type || 'source') + ']\n' + txt.slice(0, 2000);
    if (obitText.length > 8000) break;
  }

  if (!obitText || obitText.length < 80) return { ok: true, skipped: 'thin_source_text' };

  const result = await extractRelatives(db, obitText, person.full_name);  // var name kept for compat
  if (!result.ok) return result;

  const inserted = [];
  for (const r of (result.relatives || [])) {
    const cleaned = normalizePerson({
      incident_id: person.incident_id,
      role: 'other',
      first_name: (r.name || '').split(' ')[0],
      last_name: (r.name || '').split(' ').slice(-1)[0],
      full_name: r.name,
      age: r.age || null,
      city: (r.city || '').split(',')[0]?.trim() || null,
      state: (r.city || '').split(',')[1]?.trim()?.substring(0, 2) || person.state,
      contact_status: 'not_contacted',
      confidence_score: 75,
      metadata: { source: 'obituary_relative', relation: r.relation, deceased_relative: !!r.deceased }
    });

    // Dedup by name on this incident
    const exists = await db('persons').where('incident_id', person.incident_id)
      .whereRaw('LOWER(full_name) = LOWER(?)', [r.name]).first();
    if (exists) continue;

    cleaned.id = uuidv4();
    cleaned.related_to_person_id = person.id;
    cleaned.relation_type = r.relation;
    try {
      await db('persons').insert(cleaned);
      inserted.push({ id: cleaned.id, name: r.name, relation: r.relation });
      // CASCADE — every relative INSERT triggers cross-conversion (CORE_INTENT.md rule)
      await enqueueCascade(db, {
        person_id: cleaned.id,
        incident_id: person.incident_id,
        trigger_source: 'obituary_relative',
        trigger_field: 'full_name',
        trigger_value: r.name,
        priority: 6
      }).catch(() => {});
    } catch (e) {
      await reportError(db, 'enrich-family-tree', person.id, e.message, { relative: r.name });
    }
  }

  return { ok: true, person_id: person.id, relatives_inserted: inserted.length, relatives: inserted };
}

/**
 * Cron: scan recent fatal incidents with obit source_reports and extract relatives.
 */
async function processBatch(db, limit = 20) {
  await ensureColumns(db);
  const startTime = Date.now();
  const stats = { evaluated: 0, persons_added: 0, errors: [] };

  // Find deceased persons (fatal injury) on incidents that have obituary source_reports
  // and don't yet have any related_to_person_id child rows.
  const candidates = await db.raw(`
    SELECT DISTINCT p.* FROM persons p
    JOIN source_reports sr ON sr.incident_id = p.incident_id AND sr.source_type = 'obituary'
    LEFT JOIN persons rel ON rel.related_to_person_id = p.id
    WHERE p.injury_severity = 'fatal'
      AND p.full_name IS NOT NULL
      AND rel.id IS NULL
      AND p.created_at > NOW() - INTERVAL '14 days'
    ORDER BY p.created_at DESC
    LIMIT ?
  `, [limit]).then(r => r.rows || []).catch(() => []);

  for (const p of candidates) {
    if (Date.now() - startTime > 50000) break;
    stats.evaluated++;
    try {
      const r = await processDeceased(db, p);
      if (r.ok) stats.persons_added += (r.relatives_inserted || 0);
      else if (r.error) stats.errors.push(`${p.id}: ${r.error}`);
    } catch (e) {
      stats.errors.push(`${p.id}: ${e.message}`);
      await reportError(db, 'enrich-family-tree', p.id, e.message);
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
      return res.status(200).json({
        ok: true, engine: 'family-tree',
        configured: !!OPENAI_API_KEY,
        relations: RELATIONS,
        weight: 75
      });
    }
    if (action === 'extract') {
      const personId = req.query.person_id;
      if (!personId) return res.status(400).json({ error: 'person_id required' });
      const person = await db('persons').where('id', personId).first();
      if (!person) return res.status(404).json({ error: 'person not found' });
      const result = await processDeceased(db, person);
      return res.status(200).json({ success: result.ok, ...result });
    }
    if (action === 'process') {
      const limit = parseInt(req.query.limit) || 20;
      const result = await processBatch(db, limit);
      // Log to changelog when meaningful work happened
      if (result.persons_added > 0) {
        try { await logChange(db, { kind: 'pipeline', title: `family-tree: +${result.persons_added} relatives`, summary: `evaluated=${result.evaluated}`, ref: 'family-tree' }); } catch (_) {}
      }
      return res.status(200).json({
        success: true,
        message: `family-tree: ${result.persons_added} relatives across ${result.evaluated} obits`,
        ...result,
        timestamp: new Date().toISOString()
      });
    }
    return res.status(400).json({ error: 'unknown action', valid: ['health', 'extract', 'process'] });
  } catch (e) {
    await reportError(db, 'enrich-family-tree', null, e.message);
    return res.status(500).json({ error: e.message });
  }
};

module.exports.extractRelatives = extractRelatives;
module.exports.processDeceased = processDeceased;
module.exports.processBatch = processBatch;
