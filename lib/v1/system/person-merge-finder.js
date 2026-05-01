/**
 * Person Merge Finder — Phase 61 (embedding-based fuzzy person linker)
 *
 * Finds duplicate / near-duplicate person rows across incidents using
 * VoyageAI voyage-3 (1024-dim) embeddings + cosine similarity. Real example:
 *   "Ray Hartmann" appears twice in current qualified leads (Unknown GA +
 *   St Louis MO). This engine proposes the merge so a rep can confirm.
 *
 * NEVER auto-merges. A merge collapses two persons into one and copies all
 * enrichment_logs / cascade_queue / strategist_decisions references — bad
 * merges are catastrophic and effectively un-doable. Human-in-the-loop only.
 *
 * Pipeline:
 *   1. For up to N persons, build a canonical text signature:
 *        `${full_name} | ${city||?}, ${state||?} | ${age||?} | ${employer||?} | role:${role||?}`
 *   2. embed() each signature (cached in vector_cache by sha256(model::text))
 *   3. Pull all candidate persons (same scan window) and cosineSim each pair
 *   4. For each person, keep top-3 neighbors. If best ≥ min_score (0.92) AND
 *      role compatible AND not already proposed/merged/rejected → record a
 *      proposal in person_merge_proposals.
 *   5. Rep hits ?action=confirm with proposal_id + keep_person_id → merge
 *      runs in a transaction.
 *
 * Hard rules (skip pair if any fails):
 *   - role differs (driver vs passenger vs witness) — accidents have multiple
 *     people, similarity in name/city is NOT enough.
 *   - already linked (existing proposal in any status, or same id)
 *   - persons identical id (self)
 *
 * HTTP:
 *   GET  ?action=health
 *   POST ?action=scan      body:{limit?:20, min_score?:0.92}
 *   GET  ?action=list&status=proposed
 *   POST ?action=confirm   body:{proposal_id, keep_person_id}
 *   POST ?action=reject    body:{proposal_id, reason?}
 *   GET  ?action=stats
 *
 * 45s budget per scan. Bails out early if budget exceeded.
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { trackApiCall } = require('./cost');
const voyage = require('../enrich/_voyage_router');

const SECRET = 'ingest-now';
const DEFAULT_LIMIT = 20;
const DEFAULT_MIN_SCORE = 0.85;
const TOP_K = 3;
const SCAN_BUDGET_MS = 45_000;
const EMBED_MODEL = 'voyage-3';

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

let _tableEnsured = false;
async function ensureProposalTable(db) {
  if (_tableEnsured) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS person_merge_proposals (
        id BIGSERIAL PRIMARY KEY,
        person_a_id UUID NOT NULL,
        person_b_id UUID NOT NULL,
        similarity_score NUMERIC(6,5) NOT NULL,
        proposed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        status VARCHAR(20) NOT NULL DEFAULT 'proposed',
        reviewed_by VARCHAR(120),
        reviewed_at TIMESTAMPTZ,
        merge_evidence JSONB,
        CONSTRAINT pmp_status_chk CHECK (status IN ('proposed','confirmed','rejected','merged')),
        CONSTRAINT pmp_distinct_chk CHECK (person_a_id <> person_b_id)
      );
      CREATE INDEX IF NOT EXISTS idx_pmp_status ON person_merge_proposals(status);
      CREATE INDEX IF NOT EXISTS idx_pmp_proposed_at ON person_merge_proposals(proposed_at DESC);
      CREATE UNIQUE INDEX IF NOT EXISTS idx_pmp_canonical_pair ON person_merge_proposals(
        LEAST(person_a_id, person_b_id),
        GREATEST(person_a_id, person_b_id)
      );
    `);
    _tableEnsured = true;
  } catch (e) {
    console.error('person_merge_proposals table:', e.message);
  }
}

function buildSignature(p) {
  const name = (p.full_name || '').trim() || '?';
  const city = (p.city || '?').trim() || '?';
  const state = (p.state || '?').trim() || '?';
  const age = (p.age != null ? String(p.age) : '?');
  const employer = (p.employer || '?').trim() || '?';
  const role = (p.role || '?').trim() || '?';
  return `${name} | ${city}, ${state} | ${age} | ${employer} | role:${role}`;
}

function rolesCompatible(a, b) {
  const ra = (a || '').toLowerCase().trim();
  const rb = (b || '').toLowerCase().trim();
  if (!ra || !rb) return true;
  if (ra === rb) return true;
  if (['unknown', 'pending', 'pending_named', '?'].includes(ra)) return true;
  if (['unknown', 'pending', 'pending_named', '?'].includes(rb)) return true;
  return false;
}

async function pairAlreadyKnown(db, a, b) {
  try {
    const row = await db.raw(
      `SELECT id, status FROM person_merge_proposals
        WHERE LEAST(person_a_id, person_b_id) = LEAST(?::uuid, ?::uuid)
          AND GREATEST(person_a_id, person_b_id) = GREATEST(?::uuid, ?::uuid)
        LIMIT 1`,
      [a, b, a, b]
    );
    const r = row?.rows?.[0] || row?.[0];
    return r || null;
  } catch (_) { return null; }
}

async function scan(db, opts = {}) {
  const startedAt = Date.now();
  const limit = Math.max(1, Math.min(500, Number(opts.limit) || DEFAULT_LIMIT));
  const minScore = Math.max(0.5, Math.min(1, Number(opts.min_score) || DEFAULT_MIN_SCORE));
  await ensureProposalTable(db);
  await voyage.ensureCacheTable(db);

  let people = [];
  try {
    people = await db('persons')
      .select('id', 'full_name', 'city', 'state', 'age', 'role', 'incident_id', 'updated_at')
      .whereNotNull('full_name')
      .andWhereRaw("LENGTH(TRIM(full_name)) > 1")
      .orderBy('updated_at', 'desc')
      .limit(limit);
  } catch (e) {
    await reportError(db, 'person-merge-finder', null, `select persons: ${e.message}`).catch(() => {});
    return { success: false, error: 'persons select failed', proposals: [] };
  }

  if (people.length < 2) {
    return { success: true, scanned: people.length, proposals: [], note: 'need >= 2 persons' };
  }

  const sigs = people.map(buildSignature);
  const vectors = new Array(people.length);
  for (let i = 0; i < people.length; i += 50) {
    if (Date.now() - startedAt > SCAN_BUDGET_MS) break;
    const slice = sigs.slice(i, i + 50);
    const vecs = await voyage.embedBatch(slice, EMBED_MODEL, db);
    for (let j = 0; j < vecs.length; j++) vectors[i + j] = vecs[j];
  }

  const proposals = [];
  const seenPairs = new Set();
  for (let i = 0; i < people.length; i++) {
    if (Date.now() - startedAt > SCAN_BUDGET_MS) break;
    const va = vectors[i];
    if (!va) continue;
    const scored = [];
    for (let j = 0; j < people.length; j++) {
      if (i === j) continue;
      const vb = vectors[j];
      if (!vb) continue;
      const score = voyage.cosineSim(va, vb);
      if (score >= minScore) scored.push({ j, score });
    }
    scored.sort((a, b) => b.score - a.score);
    const top = scored.slice(0, TOP_K);

    for (const cand of top) {
      const A = people[i];
      const B = people[cand.j];
      // Phase 66: same-incident role mismatch is a hard reject (driver+passenger of one crash = different people).
      // Cross-incident is allowed — same person can be driver in crash A, passenger in crash B.
      if (A.incident_id === B.incident_id && !rolesCompatible(A.role, B.role)) continue;

      const pairKey = [A.id, B.id].sort().join('::');
      if (seenPairs.has(pairKey)) continue;
      seenPairs.add(pairKey);

      const known = await pairAlreadyKnown(db, A.id, B.id);
      if (known) continue;

      const evidence = {
        sig_a: sigs[i],
        sig_b: sigs[cand.j],
        score: cand.score,
        model: EMBED_MODEL,
        scanned_at: new Date().toISOString()
      };
      try {
        const ins = await db('person_merge_proposals')
          .insert({
            person_a_id: A.id,
            person_b_id: B.id,
            similarity_score: cand.score,
            status: 'proposed',
            merge_evidence: JSON.stringify(evidence)
          })
          .returning(['id', 'person_a_id', 'person_b_id', 'similarity_score', 'status']);
        const row = Array.isArray(ins) ? ins[0] : ins;
        if (row) proposals.push({ ...row, evidence });
      } catch (e) {
        // unique-pair race or other
      }
    }
  }

  await trackApiCall(db, 'person-merge-finder', 'scan', people.length, 0, true).catch(() => {});
  return {
    success: true,
    scanned: people.length,
    elapsed_ms: Date.now() - startedAt,
    min_score: minScore,
    proposals
  };
}

async function listProposals(db, status = 'proposed', limit = 100) {
  await ensureProposalTable(db);
  const rows = await db('person_merge_proposals')
    .where({ status })
    .orderBy('similarity_score', 'desc')
    .limit(Math.max(1, Math.min(500, Number(limit) || 100)));

  const ids = new Set();
  for (const r of rows) { ids.add(r.person_a_id); ids.add(r.person_b_id); }
  const persons = ids.size === 0 ? [] : await db('persons')
    .select('id', 'full_name', 'city', 'state', 'age', 'role', 'phone', 'email', 'updated_at')
    .whereIn('id', Array.from(ids));
  const byId = new Map(persons.map(p => [p.id, p]));

  return rows.map(r => ({
    id: r.id,
    similarity_score: Number(r.similarity_score),
    status: r.status,
    proposed_at: r.proposed_at,
    reviewed_by: r.reviewed_by,
    reviewed_at: r.reviewed_at,
    person_a: byId.get(r.person_a_id) || { id: r.person_a_id, missing: true },
    person_b: byId.get(r.person_b_id) || { id: r.person_b_id, missing: true },
    merge_evidence: typeof r.merge_evidence === 'string'
      ? (() => { try { return JSON.parse(r.merge_evidence); } catch (_) { return null; } })()
      : r.merge_evidence
  }));
}

const COPYABLE_FIELDS = [
  'full_name', 'first_name', 'last_name', 'age', 'city', 'state', 'address',
  'phone', 'phone_alt', 'email', 'email_alt', 'employer', 'job_title',
  'role', 'gender', 'dob', 'next_of_kin', 'attorney_name', 'attorney_firm',
  'lat', 'lng', 'identity_confidence', 'victim_verified',
  'pdl_id', 'apollo_id', 'linkedin_url', 'facebook_url'
];

async function confirmMerge(db, proposalId, keepPersonId, reviewer = 'rep') {
  await ensureProposalTable(db);
  const proposal = await db('person_merge_proposals').where({ id: proposalId }).first();
  if (!proposal) return { success: false, error: 'proposal not found' };
  if (proposal.status !== 'proposed' && proposal.status !== 'confirmed') {
    return { success: false, error: `proposal is ${proposal.status}, not mergeable` };
  }
  const a = proposal.person_a_id;
  const b = proposal.person_b_id;
  if (keepPersonId !== a && keepPersonId !== b) {
    return { success: false, error: 'keep_person_id must match person_a_id or person_b_id' };
  }
  const keepId = keepPersonId;
  const dropId = (keepId === a) ? b : a;

  let mergedEnrichmentRows = 0;
  let mergedFields = [];
  let cascadeUpdated = 0;
  let strategistUpdated = 0;

  try {
    await db.transaction(async trx => {
      const keep = await trx('persons').where({ id: keepId }).first();
      const drop = await trx('persons').where({ id: dropId }).first();
      if (!keep || !drop) throw new Error('one or both persons missing');

      const upd = await trx('enrichment_logs').where({ person_id: dropId }).update({ person_id: keepId });
      mergedEnrichmentRows = Number(upd) || 0;

      const patch = {};
      for (const f of COPYABLE_FIELDS) {
        const ka = keep[f];
        const kb = drop[f];
        const aEmpty = (ka === null || ka === undefined || ka === '');
        const bHas = (kb !== null && kb !== undefined && kb !== '');
        if (aEmpty && bHas) {
          patch[f] = kb;
          mergedFields.push(f);
        }
      }
      if (Object.keys(patch).length > 0) {
        patch.updated_at = new Date();
        await trx('persons').where({ id: keepId }).update(patch);
      }

      try {
        const c = await trx('cascade_queue').where({ person_id: dropId }).update({ person_id: keepId });
        cascadeUpdated = Number(c) || 0;
      } catch (_) {}
      try {
        const s = await trx('strategist_decisions').where({ person_id: dropId }).update({ person_id: keepId });
        strategistUpdated = Number(s) || 0;
      } catch (_) {}

      for (const { table, col } of [
        { table: 'incident_persons', col: 'person_id' },
        { table: 'person_relations', col: 'person_id' },
        { table: 'person_relations', col: 'related_person_id' }
      ]) {
        try { await trx(table).where({ [col]: dropId }).update({ [col]: keepId }); } catch (_) {}
      }

      await trx('persons').where({ id: dropId }).delete();

      const ev = (typeof proposal.merge_evidence === 'string')
        ? (() => { try { return JSON.parse(proposal.merge_evidence); } catch (_) { return {}; } })()
        : (proposal.merge_evidence || {});
      ev.merged = {
        kept_person_id: keepId,
        dropped_person_id: dropId,
        merged_at: new Date().toISOString(),
        reviewer,
        enrichment_rows_repointed: mergedEnrichmentRows,
        fields_filled: mergedFields,
        cascade_repointed: cascadeUpdated,
        strategist_repointed: strategistUpdated
      };
      await trx('person_merge_proposals').where({ id: proposalId }).update({
        status: 'merged',
        reviewed_by: reviewer,
        reviewed_at: new Date(),
        merge_evidence: JSON.stringify(ev)
      });
    });
  } catch (e) {
    await reportError(db, 'person-merge-finder', null, `confirmMerge: ${e.message}`).catch(() => {});
    return { success: false, error: e.message };
  }

  try {
    await db('enrichment_logs').insert({
      person_id: keepId,
      field_name: 'person_merge',
      old_value: dropId,
      new_value: keepId,
      created_at: new Date()
    });
  } catch (_) {}

  return {
    success: true,
    proposal_id: proposalId,
    kept_person_id: keepId,
    dropped_person_id: dropId,
    enrichment_rows_repointed: mergedEnrichmentRows,
    fields_filled: mergedFields,
    cascade_repointed: cascadeUpdated,
    strategist_repointed: strategistUpdated
  };
}

async function rejectProposal(db, proposalId, reason = '', reviewer = 'rep') {
  await ensureProposalTable(db);
  const p = await db('person_merge_proposals').where({ id: proposalId }).first();
  if (!p) return { success: false, error: 'proposal not found' };
  if (p.status === 'merged') return { success: false, error: 'already merged - cannot reject' };
  const ev = (typeof p.merge_evidence === 'string')
    ? (() => { try { return JSON.parse(p.merge_evidence); } catch (_) { return {}; } })()
    : (p.merge_evidence || {});
  ev.rejected = { reason, reviewer, rejected_at: new Date().toISOString() };
  await db('person_merge_proposals').where({ id: proposalId }).update({
    status: 'rejected',
    reviewed_by: reviewer,
    reviewed_at: new Date(),
    merge_evidence: JSON.stringify(ev)
  });
  return { success: true, proposal_id: proposalId, status: 'rejected' };
}

async function stats(db) {
  await ensureProposalTable(db);
  const rows = await db('person_merge_proposals').select('status').count('* as n').groupBy('status');
  const out = { proposed: 0, confirmed: 0, rejected: 0, merged: 0 };
  for (const r of rows) out[r.status] = Number(r.n) || 0;
  out.total = out.proposed + out.confirmed + out.rejected + out.merged;
  return out;
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  const db = getDb();
  const action = String(req.query?.action || 'health').toLowerCase();
  const body = (req.body && typeof req.body === 'object') ? req.body : {};

  try {
    if (action === 'health') {
      await ensureProposalTable(db);
      const s = await stats(db);
      return res.json({
        success: true,
        pipeline: 'person-merge-finder',
        embed_model: EMBED_MODEL,
        threshold: DEFAULT_MIN_SCORE,
        scan_budget_ms: SCAN_BUDGET_MS,
        stats: s,
        timestamp: new Date().toISOString()
      });
    }

    if (action === 'scan') {
      const r = await scan(db, {
        limit: body.limit ?? req.query?.limit,
        min_score: body.min_score ?? req.query?.min_score
      });
      return res.json({ action: 'scan', ...r, timestamp: new Date().toISOString() });
    }

    if (action === 'list') {
      const status = String(req.query?.status || 'proposed');
      const limit = req.query?.limit;
      const rows = await listProposals(db, status, limit);
      return res.json({ success: true, action: 'list', status, count: rows.length, proposals: rows });
    }

    if (action === 'confirm') {
      const proposalId = body.proposal_id ?? req.query?.proposal_id;
      const keepPersonId = body.keep_person_id ?? req.query?.keep_person_id;
      const reviewer = body.reviewer || req.query?.reviewer || 'rep';
      if (!proposalId || !keepPersonId) {
        return res.status(400).json({ error: 'proposal_id and keep_person_id required' });
      }
      const r = await confirmMerge(db, Number(proposalId), keepPersonId, reviewer);
      return res.status(r.success ? 200 : 400).json({ action: 'confirm', ...r });
    }

    if (action === 'reject') {
      const proposalId = body.proposal_id ?? req.query?.proposal_id;
      const reason = body.reason || req.query?.reason || '';
      const reviewer = body.reviewer || req.query?.reviewer || 'rep';
      if (!proposalId) return res.status(400).json({ error: 'proposal_id required' });
      const r = await rejectProposal(db, Number(proposalId), reason, reviewer);
      return res.status(r.success ? 200 : 400).json({ action: 'reject', ...r });
    }

    if (action === 'stats') {
      const s = await stats(db);
      return res.json({ success: true, action: 'stats', ...s });
    }

    return res.status(400).json({
      error: 'unknown action',
      supported: ['health', 'scan', 'list', 'confirm', 'reject', 'stats']
    });
  } catch (e) {
    await reportError(db, 'person-merge-finder', null, `handler: ${e.message}`).catch(() => {});
    return res.status(500).json({ error: e.message });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.default = handler;
module.exports.scan = scan;
module.exports.listProposals = listProposals;
module.exports.confirmMerge = confirmMerge;
module.exports.rejectProposal = rejectProposal;
module.exports.stats = stats;
module.exports.ensureProposalTable = ensureProposalTable;
