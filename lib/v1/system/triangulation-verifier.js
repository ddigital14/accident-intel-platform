/**
 * Phase 76: Triangulation Verifier — when discrepancy detected, fire all engines
 * on BOTH names and pick the winner by total cross-source agreement.
 *
 * Mason: "if inconsistencies, cross verify consistencies with other homegrown
 * solutions or connection integrations to check"
 *
 * Logic:
 *   1. Take {storedName, resolvedName, sharedPhone, sharedAddress?}
 *   2. Run universal-resolver on both names + state independently
 *   3. Score each by: # of engines that returned data + agreement on phone/address/employer
 *   4. Whoever has more cross-source agreement is the verified primary
 *   5. The other becomes a household_contact
 */
const { getDb } = require('../../_db');
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function verify(db, { person_id, stored_name, resolved_name, state, city, phone }) {
  const ur = require('./universal-resolver');
  const t0 = Date.now();

  // Run universal-resolver on both names in parallel
  const [storedR, resolvedR] = await Promise.all([
    ur.resolve(db, { name: stored_name, state, city, phone }).catch(e => ({ error: e.message })),
    ur.resolve(db, { name: resolved_name, state, city, phone }).catch(e => ({ error: e.message }))
  ]);

  // Score function: count engines that returned data + cross-source agreement
  const score = r => {
    if (!r || r.error || !r.identity) return { score: 0, evidence_count: 0, sources: [] };
    const ev = Object.values(r.identity);
    const totalVotes = ev.reduce((s, e) => s + (e.votes || 0), 0);
    const sources = new Set();
    for (const e of ev) for (const s of e.sources || []) sources.add(s);
    return {
      score: totalVotes + (sources.size * 2),  // weight unique sources higher
      evidence_count: ev.length,
      sources: [...sources],
      identity: r.identity
    };
  };

  const storedScore = score(storedR);
  const resolvedScore = score(resolvedR);

  let winner, reason;
  if (storedScore.score > resolvedScore.score * 1.5) {
    winner = 'stored';
    reason = `stored name (${stored_name}) has ${storedScore.score} cross-source agreement vs ${resolvedScore.score} for resolved name`;
  } else if (resolvedScore.score > storedScore.score * 1.5) {
    winner = 'resolved';
    reason = `resolved name (${resolved_name}) has ${resolvedScore.score} cross-source agreement vs ${storedScore.score} for stored name`;
  } else {
    winner = 'ambiguous';
    reason = `cross-source evidence is similar (${storedScore.score} vs ${resolvedScore.score}) — manual research required`;
  }

  // Log triangulation result
  if (person_id) {
    try {
      await db('enrichment_logs').insert({
        person_id,
        field_name: 'triangulation_verification',
        old_value: null,
        new_value: JSON.stringify({
          stored_name, resolved_name, winner, reason,
          stored_score: storedScore.score, resolved_score: resolvedScore.score,
          stored_sources: storedScore.sources, resolved_sources: resolvedScore.sources,
          source: 'triangulation-verifier'
        }).slice(0, 4000),
        created_at: new Date()
      });
    } catch (_) {}
  }

  return {
    ok: true,
    winner,
    reason,
    duration_ms: Date.now() - t0,
    stored: { name: stored_name, ...storedScore },
    resolved: { name: resolved_name, ...resolvedScore },
    recommendation: winner === 'stored' ? 'KEEP_STORED_NAME' :
                    winner === 'resolved' ? 'CONSIDER_SWAP_OR_LABEL_BOTH' :
                    'FLAG_FOR_MANUAL_REVIEW'
  };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();

  if (action === 'health') return res.json({ success: true, service: 'triangulation-verifier' });

  if (action === 'verify') {
    let body = req.body;
    if (!body || typeof body !== 'object') {
      body = await new Promise(r => {
        let d=''; req.on('data', c=>d+=c);
        req.on('end', () => { try { r(JSON.parse(d || '{}')); } catch { r({}); } });
      });
    }
    if (!body.stored_name || !body.resolved_name) {
      return res.status(400).json({ error: 'stored_name and resolved_name required' });
    }
    return res.json(await verify(db, body));
  }

  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.verify = verify;
