/**
 * Phase 100: Bulk Research Runner
 *
 * Runs the research-agent over all named-but-not-qualified persons in batches,
 * tracking what was written and which still need work. Returns a leaderboard.
 *
 * Endpoints:
 *   GET ?action=health
 *   POST ?action=run&limit=10&max_steps=4&scope=named-pending
 *     scope: 'named-pending' (default) — name+no contact, not qualified
 *            'all-non-qualified' — anyone not in qualified, even if has some contact
 *            'fresh' — created last 7 days
 */
const { getDb } = require('../../_db');

const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

const NAME_JUNK_RE = /(^(unknown|unnamed|not provided|n\/a|brother|sister|cousin|son|daughter|wife|husband|kearny\s+(woman|man)|teen\s+(girl|boy))|federal\s+agent|security guard|^a\s|sgt\.|staff\s+sgt|'s\s+(daughter|son|brother|sister|wife|husband)|\bU\.?S\.?\s+(federal|navy|army|marine))/i;

function isResearchableName(name) {
  if (!name || name.length < 5) return false;
  if (NAME_JUNK_RE.test(name)) return false;
  const tokens = name.trim().split(/\s+/).filter(Boolean);
  if (tokens.length < 2) return false;
  return true;
}

async function findTargets(db, scope, limit) {
  let where = '';
  if (scope === 'all-non-qualified') {
    where = `AND (i.qualification_state IS NULL OR i.qualification_state != 'qualified')`;
  } else if (scope === 'fresh') {
    where = `AND p.created_at > NOW() - INTERVAL '7 days'
             AND (i.qualification_state IS NULL OR i.qualification_state != 'qualified')`;
  } else {
    // named-pending: has name + no contact + not qualified
    where = `AND (p.phone IS NULL AND p.email IS NULL AND p.address IS NULL)
             AND (i.qualification_state IS NULL OR i.qualification_state != 'qualified')`;
  }
  const rows = (await db.raw(`
    SELECT p.id, p.full_name, p.role, p.lead_tier, p.age, p.phone, p.email, p.address,
           i.state, i.city, i.severity, i.lead_score, i.occurred_at,
           i.qualification_state, i.id as incident_id
    FROM persons p JOIN incidents i ON i.id = p.incident_id
    WHERE p.full_name IS NOT NULL AND length(p.full_name) >= 5
      AND (p.full_name ~ ' ')
      ${where}
    ORDER BY
      CASE i.severity WHEN 'fatal' THEN 1 WHEN 'critical' THEN 2 WHEN 'serious' THEN 3 ELSE 4 END,
      i.lead_score DESC NULLS LAST,
      i.occurred_at DESC NULLS LAST
    LIMIT ${parseInt(limit) || 10}
  `)).rows;
  return rows.filter(p => isResearchableName(p.full_name));
}

async function runOne(person, host, secret, maxSteps) {
  const url = `${host}/api/v1/system/research-agent?secret=${secret}&action=research&person_id=${person.id}&max_steps=${maxSteps}`;
  try {
    const r = await fetch(url, { method: 'POST', signal: AbortSignal.timeout(45000) });
    if (!r.ok) return { person_id: person.id, name: person.full_name, error: `http_${r.status}` };
    const j = await r.json();
    return {
      person_id: person.id,
      name: person.full_name,
      city_before: person.city,
      state_before: person.state,
      tool_calls: j.tool_calls,
      fields_written: j.fields_written || [],
      family_added: j.family_added || [],
      summary: (j.final_summary || '').slice(0, 200)
    };
  } catch (e) {
    return { person_id: person.id, name: person.full_name, error: e.message };
  }
}

module.exports = async function handler(req, res) {
  if (!authed(req)) return res.status(401).json({ error: 'unauthorized' });
  const action = req.query?.action || 'health';
  const db = getDb();

  if (action === 'health') {
    return res.status(200).json({ ok: true, engine: 'bulk-research', scopes: ['named-pending','all-non-qualified','fresh'] });
  }

  if (action === 'list') {
    const scope = req.query?.scope || 'named-pending';
    const limit = parseInt(req.query?.limit) || 30;
    const targets = await findTargets(db, scope, limit);
    return res.status(200).json({ ok: true, scope, count: targets.length, targets: targets.map(p => ({
      id: p.id, name: p.full_name, city: p.city, state: p.state, severity: p.severity, score: p.lead_score
    })) });
  }

  if (action === 'run') {
    const scope = req.query?.scope || 'named-pending';
    const limit = parseInt(req.query?.limit) || 5;
    const maxSteps = parseInt(req.query?.max_steps) || 3;
    const targets = await findTargets(db, scope, limit);
    if (targets.length === 0) return res.status(200).json({ ok: true, scope, processed: 0, results: [] });

    // Note: each runOne call hits the same Vercel function with 45s budget.
    // We can't run in parallel (would 429) — sequential.
    const host = req.headers['x-forwarded-proto'] && req.headers['x-forwarded-host']
      ? `${req.headers['x-forwarded-proto']}://${req.headers['x-forwarded-host']}`
      : 'https://accident-intel-platform.vercel.app';
    const recurseOnFamily = req.query?.recurse_on_family === 'true';
    const results = [];
    let total_fields = 0, total_family = 0, recursed_runs = 0;
    for (const p of targets.slice(0, Math.min(targets.length, 4))) {
      const r = await runOne(p, host, SECRET, maxSteps);
      results.push(r);
      total_fields += (r.fields_written || []).length;
      total_family += (r.family_added || []).length;
      // Recurse: research each new family member (cap at 2 per victim to avoid budget blowout)
      if (recurseOnFamily && Array.isArray(r.family_added) && r.family_added.length > 0) {
        const familyPersons = await db('persons')
          .where('victim_id', p.id)
          .where('source', 'research-agent')
          .whereIn('full_name', r.family_added.map(f => f.name))
          .limit(2);
        for (const fp of familyPersons) {
          const rr = await runOne(fp, host, SECRET, Math.min(maxSteps, 3));
          recursed_runs++;
          results.push({ ...rr, _recursed_from: p.id });
          total_fields += (rr.fields_written || []).length;
        }
      }
    }
    return res.status(200).json({
      ok: true, scope,
      candidates_in_pool: targets.length,
      processed: results.length,
      recursed_runs,
      total_fields_written: total_fields,
      total_family_added: total_family,
      results
    });
  }

  return res.status(400).json({ error: 'unknown action', valid: ['health','list','run'] });
};
