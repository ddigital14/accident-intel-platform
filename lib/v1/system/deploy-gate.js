/**
 * Phase 83: Deploy CI Gate.
 *
 * From CaseFlow: "The 4-pillar framework only matters if a CI gate enforces it."
 *
 * This endpoint runs the 4-pillar gate on all surfaces and returns:
 *   - HTTP 200 + ok:true     → deploy allowed (all surfaces 4/4 green)
 *   - HTTP 200 + ok:partial  → deploy allowed with warnings (≥80% pillars pass)
 *   - HTTP 500 + ok:false    → DEPLOY BLOCKED (critical failures)
 *
 * Wire into pre-deploy hook (vercel build script or GitHub Action):
 *   curl -fsSL "/api/v1/system/deploy-gate?secret=ingest-now&action=check" || exit 1
 *
 * Configurable via ?strict=true (require ALL pillars green) or default
 * (allow up to 1 pillar fail per surface as long as overall ≥80% pass).
 */
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function check(db, { strict = false } = {}) {
  const gate = require('./four-pillar-gate');
  const result = await gate.runAll(db);

  // Total cells = surfaces × 4 pillars
  let totalCells = 0, passedCells = 0;
  const surfaceFailures = [];
  for (const r of result.results || []) {
    let surfaceFailed = 0;
    for (const k of ['schema', 'interface', 'behavioral', 'system_map']) {
      totalCells++;
      if (r.pillars?.[k]?.ok) passedCells++;
      else surfaceFailed++;
    }
    if (surfaceFailed > 0) {
      surfaceFailures.push({ surface: r.surface, failed_pillars: surfaceFailed });
    }
  }
  const cellPassPct = passedCells / Math.max(1, totalCells);
  const allGreen = result.all_passed === true;

  let verdict, status;
  if (allGreen) {
    verdict = 'ALL_GREEN';
    status = 'deploy_allowed';
  } else if (strict) {
    verdict = 'BLOCKED';
    status = 'deploy_blocked';
  } else if (cellPassPct >= 0.8) {
    verdict = 'WARNINGS';
    status = 'deploy_allowed_with_warnings';
  } else {
    verdict = 'BLOCKED';
    status = 'deploy_blocked';
  }

  return {
    ok: verdict === 'ALL_GREEN' || verdict === 'WARNINGS',
    verdict,
    status,
    surfaces_passed: result.surfaces_passed,
    surfaces_total: result.surfaces_total,
    cells_passed: passedCells,
    cells_total: totalCells,
    cell_pass_pct: Number((cellPassPct * 100).toFixed(1)),
    surface_failures: surfaceFailures,
    detail: result.results
  };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = require('../../_db').getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'check').toLowerCase();
  if (action === 'health') return res.json({ success: true, service: 'deploy-gate' });
  if (action === 'check') {
    const strict = req.query?.strict === 'true';
    const r = await check(db, { strict });
    if (r.verdict === 'BLOCKED') return res.status(500).json(r);
    return res.json(r);
  }
  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.check = check;
