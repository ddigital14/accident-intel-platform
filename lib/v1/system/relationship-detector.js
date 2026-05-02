/**
 * Phase 75: Relationship Detector.
 *
 * Mason's directive (2026-04-30):
 *   "It's okay if phone registered comes back different name as long as you see
 *    they are somehow connected by relation, family, same address, etc.
 *    It should still be a lead — not pushed away."
 *
 * Takes two identity claims (stored victim vs phone-resolved owner) and detects
 * whether they're plausibly the same household/family. If yes, the lead stays
 * workable — the resolved person becomes a household_contact, not a conflict.
 */
const { getDb } = require('../../_db');
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

function lastName(fullName) {
  if (!fullName) return '';
  const parts = String(fullName).trim().split(/\s+/);
  return parts[parts.length - 1].toLowerCase().replace(/[^a-z]/g, '');
}

function firstName(fullName) {
  if (!fullName) return '';
  return String(fullName).trim().split(/\s+/)[0].toLowerCase().replace(/[^a-z]/g, '');
}

function normalizeAddress(s) {
  return String(s || '').toLowerCase()
    .replace(/[^a-z0-9 ]+/g, ' ')
    .replace(/\b(street|st|avenue|ave|road|rd|drive|dr|lane|ln|boulevard|blvd|highway|hwy|court|ct|place|pl|way|parkway|pkwy)\b/g, '')
    .replace(/\s+/g, ' ').trim();
}

function addressOverlapScore(a, b) {
  const na = normalizeAddress(a);
  const nb = normalizeAddress(b);
  if (!na || !nb) return 0;
  const ta = new Set(na.split(' ').filter(s => s.length > 2));
  const tb = new Set(nb.split(' ').filter(s => s.length > 2));
  if (!ta.size || !tb.size) return 0;
  let overlap = 0;
  for (const t of ta) if (tb.has(t)) overlap++;
  return overlap / Math.min(ta.size, tb.size);
}

/**
 * detectRelationship({ stored, resolved })
 *   stored:   { name, address, city, state }     — victim row in our DB
 *   resolved: { name, address, city, state, relatives? }   — what the resolver returned
 *
 * Returns:
 *   {
 *     related: boolean,
 *     relationship_type: 'same_person' | 'spouse_or_family' | 'household_member' |
 *                        'same_address_unrelated' | 'unrelated' | 'insufficient_data',
 *     confidence: 0-100,
 *     reason: string explaining why
 *   }
 */
function detectRelationship({ stored, resolved }) {
  if (!stored || !resolved) return { related: false, relationship_type: 'insufficient_data', confidence: 0, reason: 'missing_input' };

  const sLast = lastName(stored.name);
  const rLast = lastName(resolved.name);
  const sFirst = firstName(stored.name);
  const rFirst = firstName(resolved.name);

  const sName = (stored.name || '').toLowerCase().trim();
  const rName = (resolved.name || '').toLowerCase().trim();

  // 1. Same person — exact name match
  if (sName && sName === rName) {
    return { related: true, relationship_type: 'same_person', confidence: 100, reason: 'exact_name_match' };
  }

  // 2. Address overlap — strongest household signal
  let addrScore = 0;
  if (stored.address && resolved.address) {
    addrScore = addressOverlapScore(stored.address, resolved.address);
  }

  // 3. Same surname (likely family)
  const sameSurname = sLast && rLast && sLast === rLast;

  // 4. In Trestle relatives list (resolved.relatives is array of {name})
  let inRelativesList = false;
  if (Array.isArray(resolved.relatives)) {
    for (const rel of resolved.relatives) {
      const relName = (rel.name || rel).toString().toLowerCase();
      if (relName.includes(sFirst) || relName.includes(sLast)) {
        inRelativesList = true;
        break;
      }
    }
  }

  // Decision tree
  if (addrScore >= 0.7 && sameSurname) {
    return { related: true, relationship_type: 'spouse_or_family', confidence: 95,
      reason: `same_address (${(addrScore*100).toFixed(0)}% match) + same_surname (${sLast})` };
  }

  if (addrScore >= 0.7) {
    return { related: true, relationship_type: 'household_member', confidence: 80,
      reason: `same_address (${(addrScore*100).toFixed(0)}% match), different surname (${sLast} vs ${rLast})` };
  }

  if (sameSurname && stored.state === resolved.state) {
    return { related: true, relationship_type: 'spouse_or_family', confidence: 65,
      reason: `same_surname (${sLast}) + same_state (${stored.state})` };
  }

  if (inRelativesList) {
    return { related: true, relationship_type: 'spouse_or_family', confidence: 75,
      reason: `appears_in_relatives_list of resolved person` };
  }

  if (sameSurname) {
    return { related: true, relationship_type: 'spouse_or_family', confidence: 50,
      reason: `same_surname (${sLast}) — different state/no address shared` };
  }

  if (addrScore >= 0.4) {
    return { related: true, relationship_type: 'same_address_unrelated', confidence: 40,
      reason: `partial_address_match (${(addrScore*100).toFixed(0)}%) — could be neighbor or roommate` };
  }

  return { related: false, relationship_type: 'unrelated', confidence: 20,
    reason: `no surname/address/relatives connection found` };
}

/**
 * Wraps universal-resolver: after resolving a phone/email, if the returned
 * name differs from a stored victim, run relationship detection and decide
 * whether to KEEP the lead (with household_contact label) or flag conflict.
 */
async function checkLeadIntegrity(db, personId) {
  await ensureSchema(db);
  const person = await db('persons').where('id', personId).first();
  if (!person) return { ok: false, error: 'not_found' };

  const ur = require('./universal-resolver');
  const input = {};
  if (person.phone) input.phone = person.phone;
  else if (person.email) input.email = person.email;
  else return { ok: true, skipped: 'no_contact_to_check' };

  const r = await ur.resolve(db, input);
  const stored = {
    name: person.full_name,
    address: person.address,
    city: person.city,
    state: person.state
  };
  const resolved = {
    name: r.identity?.name?.value,
    address: r.identity?.address?.value && (typeof r.identity.address.value === 'string'
      ? r.identity.address.value
      : (r.identity.address.value.street_line_1 || JSON.stringify(r.identity.address.value).slice(0, 100))),
    city: r.identity?.city?.value,
    state: r.identity?.state?.value,
    relatives: []
  };

  const rel = detectRelationship({ stored, resolved });

  // Build a human-readable discrepancy note for the rep
  // Mason directive: ALWAYS keep the lead, just annotate the discrepancy
  const sName = stored.name || 'Unknown';
  const rName = resolved.name || null;
  let discrepancyNote = null;
  if (rName && rName.toLowerCase() !== sName.toLowerCase()) {
    if (rel.relationship_type === 'spouse_or_family' || rel.relationship_type === 'household_member') {
      discrepancyNote = `${sName} (phone/contact registered to ${rName} — likely ${rel.relationship_type.replace(/_/g, ' ')}, ${rel.confidence}% confidence: ${rel.reason})`;
    } else if (rel.relationship_type === 'same_address_unrelated') {
      discrepancyNote = `${sName} (phone/contact registered to ${rName} — possibly roommate or neighbor, ${rel.confidence}% confidence: ${rel.reason}). Recommend research before call.`;
    } else if (rel.relationship_type === 'unrelated') {
      discrepancyNote = `${sName} (phone/contact registered to ${rName} — no household connection found, but lead retained for manual research. Possible friend, extended family, or wrong number).`;
    } else {
      discrepancyNote = `${sName} (phone/contact registered to ${rName} — ${rel.relationship_type}, ${rel.reason})`;
    }
  } else if (rName && rName.toLowerCase() === sName.toLowerCase()) {
    discrepancyNote = `${sName} — phone confirmed registered to victim (exact match)`;
  }

  // Always write the integrity log (audit trail)
  try {
    await db('enrichment_logs').insert({
      person_id: personId,
      field_name: 'lead_integrity_check',
      old_value: null,
      new_value: JSON.stringify({
        stored, resolved, relationship: rel,
        discrepancy_note: discrepancyNote,
        decision: 'KEEP_LEAD',
        source: 'relationship-detector'
      }).slice(0, 4000),
      created_at: new Date()
    });
  } catch (_) {}

  // Phase 75c: 3-TIER LEAD CLASSIFICATION
  //   normal   → confidence ≥ 60 OR same_person OR strong household match (top of list)
  //   review   → confidence 30-59 OR same_address_unrelated (separate column, bottom)
  //   demoted  → confidence < 30 AND state mismatch AND no surname overlap (major inconsistency)
  let leadTier = 'normal';
  let demoteReason = null;

  if (resolved.name && rel.confidence < 30) {
    // Detect MAJOR inconsistency: state mismatch + no surname overlap + no address overlap
    const sLast = lastName(stored.name);
    const rLast = lastName(resolved.name);
    const stateMismatch = stored.state && resolved.state && stored.state !== resolved.state;
    const surnameDiffer = sLast && rLast && sLast !== rLast;
    const noAddressOverlap = stored.address && resolved.address &&
      addressOverlapScore(stored.address, resolved.address) < 0.2;
    if (stateMismatch && surnameDiffer && noAddressOverlap) {
      leadTier = 'demoted';
      demoteReason = `MAJOR: state mismatch (${stored.state} vs ${resolved.state}) + different surname (${sLast} vs ${rLast}) + no address overlap`;
    } else {
      leadTier = 'review';
    }
  } else if (rel.confidence >= 30 && rel.confidence < 60) {
    leadTier = 'review';
  }
  // else: normal (≥60 confidence or exact match)

  if (discrepancyNote) {
    try {
      await db('enrichment_logs').insert({
        person_id: personId,
        field_name: 'discrepancy_note',
        old_value: null,
        new_value: JSON.stringify({
          note: discrepancyNote,
          severity: leadTier === 'demoted' ? 'high' : (leadTier === 'review' ? 'medium' : 'low'),
          lead_tier: leadTier,
          demote_reason: demoteReason,
          source: 'relationship-detector'
        }).slice(0, 4000),
        created_at: new Date()
      });
      // Write tier to persons.lead_tier column (and notes for display)
      const upd = { updated_at: new Date() };
      try { upd.notes = discrepancyNote; } catch (_) {}
      try { upd.lead_tier = leadTier; } catch (_) {}
      await db('persons').where('id', personId).update(upd).catch(() => {});

      // PHASE 75c: actually demote the parent incident if major inconsistency
      if (leadTier === 'demoted' && person.incident_id) {
        try {
          await db('incidents')
            .where('id', person.incident_id)
            .where('qualification_state', 'qualified')
            .update({ qualification_state: 'pending_review', updated_at: new Date() });
        } catch (_) {}
      }
    } catch (_) {}
  }

  // If related, optionally insert the resolved person as a related-person row
  if (rel.related && rel.relationship_type !== 'same_person' && resolved.name) {
    try {
      const { v4: uuid } = require('uuid');
      const exists = await db('persons')
        .where('victim_id', personId)
        .whereRaw('LOWER(full_name) = ?', [resolved.name.toLowerCase()])
        .first();
      if (!exists) {
        await db('persons').insert({
          id: uuid(),
          incident_id: person.incident_id,
          role: 'related',
          relationship_to_victim: rel.relationship_type,
          victim_id: personId,
          full_name: resolved.name,
          address: typeof resolved.address === 'string' ? resolved.address : null,
          city: resolved.city,
          state: resolved.state,
          phone: person.phone,
          // Phase 87 fix: household members must NOT auto-qualify as their own leads.
          // They're contacts, not separate cases. The qualifier checks victim_verified.
          victim_verified: false,
          victim_verifier_reason: 'auto_inserted_household_member',
          lead_tier: 'related',
          created_at: new Date(),
          updated_at: new Date()
        });
      }
    } catch (_) {}
  }

  return { ok: true, person_id: personId, stored, resolved, relationship: rel,
    discrepancy_note: discrepancyNote,
    lead_tier: leadTier,
    demote_reason: demoteReason,
    decision: leadTier === 'demoted' ? 'DEMOTED_TO_PENDING_REVIEW' :
              (leadTier === 'review' ? 'KEEP_BUT_FLAG_FOR_REVIEW' : 'KEEP_LEAD_NORMAL')
  };
}

async function batchCheck(db, limit = 30) {
  const persons = await db('persons')
    .whereNotNull('phone')
    .where('victim_verified', true)
    .limit(limit)
    .select('id');
  const results = [];
  for (const p of persons) {
    try { results.push(await checkLeadIntegrity(db, p.id)); }
    catch (e) { results.push({ id: p.id, error: e.message }); }
  }
  return { ok: true, scanned: persons.length, results: results.slice(0, 20) };
}

let _migrationApplied = false;
async function ensureSchema(db) {
  if (_migrationApplied) return;
  try {
    await db.raw(`ALTER TABLE persons ADD COLUMN IF NOT EXISTS lead_tier VARCHAR(20)`);
    await db.raw(`ALTER TABLE persons ADD COLUMN IF NOT EXISTS notes TEXT`);
    await db.raw(`CREATE INDEX IF NOT EXISTS idx_persons_lead_tier ON persons(lead_tier)`);
    _migrationApplied = true;
  } catch (e) { console.error('[rel-detector] migration:', e.message); }
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();

  if (action === 'health') return res.json({ success: true, service: 'relationship-detector' });

  if (action === 'detect') {
    let body = req.body;
    if (!body || typeof body !== 'object') {
      body = await new Promise(r => {
        let d=''; req.on('data', c=>d+=c);
        req.on('end', () => { try { r(JSON.parse(d || '{}')); } catch { r({}); } });
      });
    }
    return res.json(detectRelationship(body));
  }

  if (action === 'check_lead') {
    const pid = req.query?.person_id;
    if (!pid) return res.status(400).json({ error: 'person_id required' });
    return res.json(await checkLeadIntegrity(db, pid));
  }

  if (action === 'batch') {
    const limit = Math.min(50, parseInt(req.query?.limit) || 20);
    return res.json(await batchCheck(db, limit));
  }

  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.detectRelationship = detectRelationship;
module.exports.checkLeadIntegrity = checkLeadIntegrity;
module.exports.batchCheck = batchCheck;
