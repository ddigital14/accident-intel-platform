/**
 * Family Graph — Phase 67 (cross-incident family bridge engine)
 *
 * Mason's directive: when victim X (incident A) has a family member Y mentioned
 * in their obituary (Y already stored as a person row, role IN
 * ('family','spouse','child','parent','sibling','next_of_kin') OR
 * relationship_to_victim NOT NULL), AND a NEW incident B has someone named Y
 * as a victim/witness/passenger, AUTO-PROPOSE the link as the same person
 * across incidents. Builds a family graph reps use to triangulate contacts.
 *
 * Pipeline:
 *   1. Pull up to N "family-role" persons (role family|spouse|child|parent|
 *      sibling|next_of_kin OR relationship_to_victim NOT NULL).
 *   2. Pull broad candidate counterparts (persons with names, most-recent first).
 *   3. Embed each side via VoyageAI voyage-3 on `${full_name}|${state}|${age}`
 *      summary. Cosine-sim every cross-incident pair.
 *   4. If cosine ≥ 0.92 AND Levenshtein(name_a, name_b) ≤ 3 → propose a
 *      family bridge. bridge_type='same_person' for tight match,
 *      bridge_type='related' for surname-only / different first name (e.g.
 *      cousin, sibling appearing in a different incident under same surname).
 *   5. Rep confirms via ?action=confirm. On confirm, target person inherits
 *      relationship_to_victim + victim_id from source (does NOT merge person
 *      rows — see person-merge-finder for that).
 *
 * NEVER auto-confirms. Only proposes.
 *
 * HTTP:
 *   GET  ?action=health
 *   GET  ?action=scan&limit=N
 *   GET  ?action=list&status=proposed
 *   POST ?action=confirm           body:{bridge_id}
 *   GET  ?action=neighborhood&person_id=<uuid>   (2-hop graph walk)
 *   GET  ?action=stats
 *
 * Wired into router only. NOT in ENGINE_MATRIX (one-shot scanner, runs as
 * cron, not per-person enrichment). IS in ENGINE_CATALOGUE so the strategist
 * can invoke it.
 *
 * 45s budget per scan. Skips pairs in the same incident (we want CROSS-incident
 * bridges only — same-incident dedup is person-merge-finder's job).
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
let trackApiCall = async () => {};
try { trackApiCall = require('./cost-tracker').trackApiCall || trackApiCall; } catch (_) {
  try { trackApiCall = require('./cost').trackApiCall || trackApiCall; } catch (_) {}
}
const voyage = require('../enrich/_voyage_router');

const SECRET = 'ingest-now';
const DEFAULT_LIMIT = 30;
const MIN_SCORE = 0.92;
const MAX_LEV = 3;
const SCAN_BUDGET_MS = 45_000;
const EMBED_MODEL = 'voyage-3';
const NEIGHBORHOOD_HOPS = 2;

const FAMILY_ROLES = ['family', 'spouse', 'child', 'parent', 'sibling', 'next_of_kin'];

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

let _tableEnsured = false;
async function ensureBridgeTable(db) {
  if (_tableEnsured) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS family_bridges (
        id BIGSERIAL PRIMARY KEY,
        source_person_id UUID NOT NULL,
        target_person_id UUID NOT NULL,
        similarity_score NUMERIC(6,5) NOT NULL,
        bridge_type VARCHAR(20) NOT NULL DEFAULT 'same_person',
        proposed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        status VARCHAR(20) NOT NULL DEFAULT 'proposed',
        reviewed_at TIMESTAMPTZ,
        reviewed_by VARCHAR(120),
        evidence JSONB,
        CONSTRAINT fb_status_chk CHECK (status IN ('proposed','confirmed','rejected')),
        CONSTRAINT fb_type_chk CHECK (bridge_type IN ('same_person','related')),
        CONSTRAINT fb_distinct_chk CHECK (source_person_id <> target_person_id)
      );
      CREATE INDEX IF NOT EXISTS idx_fb_status ON family_bridges(status);
      CREATE INDEX IF NOT EXISTS idx_fb_source ON family_bridges(source_person_id);
      CREATE INDEX IF NOT EXISTS idx_fb_target ON family_bridges(target_person_id);
      CREATE INDEX IF NOT EXISTS idx_fb_proposed_at ON family_bridges(proposed_at DESC);
      CREATE UNIQUE INDEX IF NOT EXISTS idx_fb_canonical_pair ON family_bridges(
        LEAST(source_person_id, target_person_id),
        GREATEST(source_person_id, target_person_id)
      );
    `);
    _tableEnsured = true;
  } catch (e) {
    console.error('family_bridges table:', e.message);
  }
}

// ── Levenshtein (small strings only) ────────────────────────────
function levenshtein(a, b) {
  a = String(a || '').toLowerCase().trim();
  b = String(b || '').toLowerCase().trim();
  if (a === b) return 0;
  if (!a.length) return b.length;
  if (!b.length) return a.length;
  const m = a.length, n = b.length;
  const dp = new Array(n + 1);
  for (let j = 0; j <= n; j++) dp[j] = j;
  for (let i = 1; i <= m; i++) {
    let prev = dp[0];
    dp[0] = i;
    for (let j = 1; j <= n; j++) {
      const tmp = dp[j];
      const cost = a.charCodeAt(i - 1) === b.charCodeAt(j - 1) ? 0 : 1;
      dp[j] = Math.min(dp[j] + 1, dp[j - 1] + 1, prev + cost);
      prev = tmp;
    }
  }
  return dp[n];
}

function buildSig(p) {
  const name = (p.full_name || '').trim() || '?';
  const state = (p.state || '?').toString().trim() || '?';
  const age = (p.age != null ? String(p.age) : '?');
  return `${name}|${state}|${age}`;
}

function classifyBridge(nameA, nameB, score) {
  const a = String(nameA || '').toLowerCase().trim().split(/\s+/);
  const b = String(nameB || '').toLowerCase().trim().split(/\s+/);
  const lastA = a[a.length - 1] || '';
  const lastB = b[b.length - 1] || '';
  const firstA = a[0] || '';
  const firstB = b[0] || '';
  // Same surname + different first name → related sibling/cousin/parent, not same person
  if (lastA && lastB && lastA === lastB && firstA && firstB && firstA !== firstB) {
    return 'related';
  }
  // Default: tight name match across incidents → same person
  return 'same_person';
}

async function pairAlreadyKnown(db, a, b) {
  try {
    const row = await db.raw(
      `SELECT id, status FROM family_bridges
        WHERE LEAST(source_person_id, target_person_id) = LEAST(?::uuid, ?::uuid)
          AND GREATEST(source_person_id, target_person_id) = GREATEST(?::uuid, ?::uuid)
        LIMIT 1`,
      [a, b, a, b]
    );
    const r = row?.rows?.[0] || row?.[0];
    return r || null;
  } catch (_) { return null; }
}

// ── core scan ────────────────────────────────────────────────────
async function scan(db, opts = {}) {
  const startedAt = Date.now();
  const limit = Math.max(1, Math.min(500, Number(opts.limit) || DEFAULT_LIMIT));
  await ensureBridgeTable(db);
  await voyage.ensureCacheTable(db);

  // 1. Pull family-role source persons (role = family/* OR relationship_to_victim NOT NULL)
  let family = [];
  try {
    family = await db('persons')
      .select('id', 'incident_id', 'full_name', 'role', 'relationship_to_victim',
              'victim_id', 'city', 'state', 'age', 'updated_at')
      .whereNotNull('full_name')
      .andWhereRaw("LENGTH(TRIM(full_name)) > 1")
      .andWhere(function () {
        this.whereIn('role', FAMILY_ROLES).orWhereNotNull('relationship_to_victim');
      })
      .orderBy('updated_at', 'desc')
      .limit(limit);
  } catch (e) {
    await reportError(db, 'family-graph', null, `select family persons: ${e.message}`).catch(() => {});
    return { success: false, error: 'family persons select failed', bridges: [] };
  }

  if (family.length === 0) {
    return { success: true, scanned: 0, bridges: [], note: 'no family-role persons found' };
  }

  // 2. Pull candidate counterparts — any persons with a name (most-recent 2k).
  //    We only score CROSS-incident pairs (filtered below) so this pool is wide on purpose.
  let candidates = [];
  try {
    candidates = await db('persons')
      .select('id', 'incident_id', 'full_name', 'role', 'victim_id',
              'city', 'state', 'age', 'updated_at')
      .whereNotNull('full_name')
      .andWhereRaw("LENGTH(TRIM(full_name)) > 1")
      .orderBy('updated_at', 'desc')
      .limit(2000);
  } catch (e) {
    await reportError(db, 'family-graph', null, `select candidates: ${e.message}`).catch(() => {});
    return { success: false, error: 'candidates select failed', bridges: [] };
  }

  // Embed both sides in a single batch run
  const allPeople = [...family, ...candidates];
  const sigs = allPeople.map(buildSig);
  const vectors = new Array(allPeople.length);
  for (let i = 0; i < allPeople.length; i += 50) {
    if (Date.now() - startedAt > SCAN_BUDGET_MS) break;
    const slice = sigs.slice(i, i + 50);
    const vecs = await voyage.embedBatch(slice, EMBED_MODEL, db);
    for (let j = 0; j < vecs.length; j++) vectors[i + j] = vecs[j];
  }

  const familyVecs = vectors.slice(0, family.length);
  const candVecs = vectors.slice(family.length);

  // 3. Score family[i] vs candidates[j] across incidents
  const bridges = [];
  const seenPairs = new Set();
  for (let i = 0; i < family.length; i++) {
    if (Date.now() - startedAt > SCAN_BUDGET_MS) break;
    const A = family[i];
    const va = familyVecs[i];
    if (!va) continue;

    const scored = [];
    for (let j = 0; j < candidates.length; j++) {
      const B = candidates[j];
      if (B.id === A.id) continue;
      // Same incident → not a cross-incident bridge (skip)
      if (A.incident_id && B.incident_id && A.incident_id === B.incident_id) continue;
      // Same victim chain → already linked via victim_id
      if (A.victim_id && B.id === A.victim_id) continue;
      if (B.victim_id && A.id === B.victim_id) continue;
      // State mismatch (when both known) → still allow but use as evidence
      const vb = candVecs[j];
      if (!vb) continue;
      const score = voyage.cosineSim(va, vb);
      if (score < MIN_SCORE) continue;
      const lev = levenshtein(A.full_name, B.full_name);
      if (lev > MAX_LEV) continue;
      scored.push({ j, score, lev });
    }
    scored.sort((a, b) => b.score - a.score);

    for (const cand of scored.slice(0, 5)) {
      if (Date.now() - startedAt > SCAN_BUDGET_MS) break;
      const B = candidates[cand.j];
      const pairKey = [A.id, B.id].sort().join('::');
      if (seenPairs.has(pairKey)) continue;
      seenPairs.add(pairKey);

      const known = await pairAlreadyKnown(db, A.id, B.id);
      if (known) continue;

      const bridge_type = classifyBridge(A.full_name, B.full_name, cand.score);
      const evidence = {
        sig_a: sigs[i],
        sig_b: sigs[family.length + cand.j],
        score: cand.score,
        levenshtein: cand.lev,
        model: EMBED_MODEL,
        a_role: A.role,
        a_relationship: A.relationship_to_victim,
        a_incident_id: A.incident_id,
        b_role: B.role,
        b_incident_id: B.incident_id,
        a_state: A.state, b_state: B.state,
        scanned_at: new Date().toISOString()
      };
      try {
        const ins = await db('family_bridges')
          .insert({
            source_person_id: A.id,
            target_person_id: B.id,
            similarity_score: cand.score,
            bridge_type,
            status: 'proposed',
            evidence: JSON.stringify(evidence)
          })
          .returning(['id', 'source_person_id', 'target_person_id',
                      'similarity_score', 'bridge_type', 'status']);
        const row = Array.isArray(ins) ? ins[0] : ins;
        if (row) bridges.push({ ...row, evidence });
      } catch (_) {
        // unique-pair race or other → ignore
      }
    }
  }

  await trackApiCall(db, 'family-graph', 'scan', allPeople.length, 0, true).catch(() => {});
  return {
    success: true,
    scanned_family: family.length,
    scanned_candidates: candidates.length,
    elapsed_ms: Date.now() - startedAt,
    min_score: MIN_SCORE,
    max_levenshtein: MAX_LEV,
    bridges
  };
}

// ── list ─────────────────────────────────────────────────────────
async function listBridges(db, status = 'proposed', limit = 100) {
  await ensureBridgeTable(db);
  const rows = await db('family_bridges')
    .where({ status })
    .orderBy('similarity_score', 'desc')
    .limit(Math.max(1, Math.min(500, Number(limit) || 100)));

  const ids = new Set();
  for (const r of rows) { ids.add(r.source_person_id); ids.add(r.target_person_id); }
  const persons = ids.size === 0 ? [] : await db('persons')
    .select('id', 'incident_id', 'full_name', 'role', 'relationship_to_victim',
            'victim_id', 'city', 'state', 'age', 'phone', 'email', 'updated_at')
    .whereIn('id', Array.from(ids));
  const byId = new Map(persons.map(p => [p.id, p]));

  return rows.map(r => ({
    id: r.id,
    similarity_score: Number(r.similarity_score),
    bridge_type: r.bridge_type,
    status: r.status,
    proposed_at: r.proposed_at,
    reviewed_at: r.reviewed_at,
    reviewed_by: r.reviewed_by,
    source: byId.get(r.source_person_id) || { id: r.source_person_id, missing: true },
    target: byId.get(r.target_person_id) || { id: r.target_person_id, missing: true },
    evidence: typeof r.evidence === 'string'
      ? (() => { try { return JSON.parse(r.evidence); } catch (_) { return null; } })()
      : r.evidence
  }));
}

// ── confirm: records family link, propagates relationship metadata ──
async function confirmBridge(db, bridgeId, reviewer = 'rep') {
  await ensureBridgeTable(db);
  const b = await db('family_bridges').where({ id: bridgeId }).first();
  if (!b) return { success: false, error: 'bridge not found' };
  if (b.status === 'confirmed') {
    return { success: true, bridge_id: bridgeId, status: 'confirmed', already: true };
  }
  if (b.status === 'rejected') {
    return { success: false, error: 'bridge is rejected, cannot confirm' };
  }

  let inheritedFields = [];
  try {
    await db.transaction(async trx => {
      const source = await trx('persons').where({ id: b.source_person_id }).first();
      const target = await trx('persons').where({ id: b.target_person_id }).first();
      if (!source || !target) throw new Error('one or both persons missing');

      // Inherit relationship metadata from source → target (only when target is null/empty)
      const patch = {};
      if (!target.relationship_to_victim && source.relationship_to_victim) {
        patch.relationship_to_victim = source.relationship_to_victim;
        inheritedFields.push('relationship_to_victim');
      }
      if (!target.victim_id && source.victim_id) {
        patch.victim_id = source.victim_id;
        inheritedFields.push('victim_id');
      }
      // If source has a clean family role and target role is generic, lift it
      const tRole = (target.role || '').toLowerCase();
      if (FAMILY_ROLES.includes((source.role || '').toLowerCase())
          && !FAMILY_ROLES.includes(tRole)
          && (tRole === '' || ['unknown', 'pending', 'pending_named', '?'].includes(tRole))) {
        patch.role = source.role;
        inheritedFields.push('role');
      }

      if (Object.keys(patch).length > 0) {
        patch.updated_at = new Date();
        await trx('persons').where({ id: target.id }).update(patch);

        // Log inheritance in enrichment_logs (minimal schema: person_id, field_name, old_value, new_value, created_at)
        for (const f of Object.keys(patch)) {
          if (f === 'updated_at') continue;
          try {
            await trx('enrichment_logs').insert({
              person_id: target.id,
              field_name: f,
              old_value: target[f] == null ? null : String(target[f]),
              new_value: patch[f] == null ? null : String(patch[f]),
              created_at: new Date()
            });
          } catch (_) {}
        }
      }

      const ev = (typeof b.evidence === 'string')
        ? (() => { try { return JSON.parse(b.evidence); } catch (_) { return {}; } })()
        : (b.evidence || {});
      ev.confirmed = {
        reviewer,
        confirmed_at: new Date().toISOString(),
        inherited_fields: inheritedFields
      };
      await trx('family_bridges').where({ id: bridgeId }).update({
        status: 'confirmed',
        reviewed_by: reviewer,
        reviewed_at: new Date(),
        evidence: JSON.stringify(ev)
      });

      // Marker enrichment row tying the two persons together
      try {
        await trx('enrichment_logs').insert({
          person_id: target.id,
          field_name: 'family_bridge',
          old_value: null,
          new_value: source.id,
          created_at: new Date()
        });
      } catch (_) {}
    });
  } catch (e) {
    await reportError(db, 'family-graph', null, `confirmBridge: ${e.message}`).catch(() => {});
    return { success: false, error: e.message };
  }

  return {
    success: true,
    bridge_id: bridgeId,
    status: 'confirmed',
    inherited_fields: inheritedFields
  };
}

// ── neighborhood: BFS up to NEIGHBORHOOD_HOPS hops from a root person ──
async function neighborhood(db, personId, hops = NEIGHBORHOOD_HOPS) {
  await ensureBridgeTable(db);
  const visited = new Set([personId]);
  const frontier = new Set([personId]);
  const edges = [];
  const allowedStatuses = ['proposed', 'confirmed'];

  for (let h = 0; h < hops; h++) {
    if (frontier.size === 0) break;
    const ids = Array.from(frontier);
    frontier.clear();

    let rows = [];
    try {
      rows = await db('family_bridges')
        .whereIn('status', allowedStatuses)
        .andWhere(function () {
          this.whereIn('source_person_id', ids).orWhereIn('target_person_id', ids);
        });
    } catch (_) { rows = []; }

    for (const r of rows) {
      const a = r.source_person_id;
      const c = r.target_person_id;
      edges.push({
        bridge_id: r.id,
        source_person_id: a,
        target_person_id: c,
        bridge_type: r.bridge_type,
        similarity_score: Number(r.similarity_score),
        status: r.status,
        hop: h + 1
      });
      if (!visited.has(a)) { visited.add(a); frontier.add(a); }
      if (!visited.has(c)) { visited.add(c); frontier.add(c); }
    }
  }

  // Hydrate persons (cap output)
  const ids = Array.from(visited);
  let persons = [];
  if (ids.length > 0) {
    try {
      persons = await db('persons')
        .select('id', 'incident_id', 'full_name', 'role',
                'relationship_to_victim', 'victim_id',
                'city', 'state', 'age', 'phone', 'email')
        .whereIn('id', ids);
    } catch (_) { persons = []; }
  }
  return {
    root_person_id: personId,
    hops,
    node_count: persons.length,
    edge_count: edges.length,
    persons,
    edges
  };
}

async function stats(db) {
  await ensureBridgeTable(db);
  const rows = await db('family_bridges').select('status').count('* as n').groupBy('status');
  const out = { proposed: 0, confirmed: 0, rejected: 0 };
  for (const r of rows) out[r.status] = Number(r.n) || 0;
  out.total = out.proposed + out.confirmed + out.rejected;
  return out;
}

// ── HTTP handler ────────────────────────────────────────────────
async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  const db = getDb();
  const action = String(req.query?.action || 'health').toLowerCase();
  const body = (req.body && typeof req.body === 'object') ? req.body : {};

  try {
    if (action === 'health') {
      await ensureBridgeTable(db);
      const s = await stats(db);
      return res.json({
        success: true,
        pipeline: 'family-graph',
        embed_model: EMBED_MODEL,
        min_score: MIN_SCORE,
        max_levenshtein: MAX_LEV,
        family_roles: FAMILY_ROLES,
        scan_budget_ms: SCAN_BUDGET_MS,
        stats: s,
        timestamp: new Date().toISOString()
      });
    }

    if (action === 'scan') {
      const r = await scan(db, {
        limit: body.limit ?? req.query?.limit
      });
      return res.json({ action: 'scan', ...r, timestamp: new Date().toISOString() });
    }

    if (action === 'list') {
      const status = String(req.query?.status || 'proposed');
      const limit = req.query?.limit;
      const rows = await listBridges(db, status, limit);
      return res.json({ success: true, action: 'list', status, count: rows.length, bridges: rows });
    }

    if (action === 'confirm') {
      const bridgeId = body.bridge_id ?? req.query?.bridge_id;
      const reviewer = body.reviewer || req.query?.reviewer || 'rep';
      if (!bridgeId) return res.status(400).json({ error: 'bridge_id required' });
      const r = await confirmBridge(db, Number(bridgeId), reviewer);
      return res.status(r.success ? 200 : 400).json({ action: 'confirm', ...r });
    }

    if (action === 'neighborhood') {
      const personId = req.query?.person_id || body.person_id;
      const hops = Number(req.query?.hops || body.hops) || NEIGHBORHOOD_HOPS;
      if (!personId) return res.status(400).json({ error: 'person_id required' });
      const r = await neighborhood(db, String(personId), Math.max(1, Math.min(4, hops)));
      return res.json({ success: true, action: 'neighborhood', ...r });
    }

    if (action === 'stats') {
      const s = await stats(db);
      return res.json({ success: true, action: 'stats', ...s });
    }

    return res.status(400).json({
      error: 'unknown action',
      supported: ['health', 'scan', 'list', 'confirm', 'neighborhood', 'stats']
    });
  } catch (e) {
    await reportError(db, 'family-graph', null, `handler: ${e.message}`).catch(() => {});
    return res.status(500).json({ error: e.message });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.default = handler;
module.exports.scan = scan;
module.exports.listBridges = listBridges;
module.exports.confirmBridge = confirmBridge;
module.exports.neighborhood = neighborhood;
module.exports.stats = stats;
module.exports.ensureBridgeTable = ensureBridgeTable;
module.exports.levenshtein = levenshtein;
