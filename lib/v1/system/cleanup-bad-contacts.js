/**
 * Phase 38: one-shot DB cleanup
 *  - NULL out scraper-website-own emails (support@thatsthem.com etc)
 *  - Re-run Stage-A verifier on all victim_verified=true persons (catches historical/WWII cases now that temporal check exists)
 *  - Demote orphan qualified incidents (no remaining verified victims)
 */
const { getDb } = require('../../_db');
const { quickClassify } = require('../enrich/_name_filter');

const SCRAPER_EMAIL_RE = /@(thatsthem|fastpeoplesearch|truepeoplesearch|radaris|whitepages|spokeo|beenverified|peoplefinder|usatoday|cnn|nytimes|washingtonpost|reuters|apnews|bbc|cbsnews|abcnews|nbcnews|foxnews|huffpost)\.com$/i;
const ROLE_PREFIX_RE = /^(support|info|contact|hello|admin|noreply|no-reply|webmaster|press|editor|abuse|legal|privacy|sales|marketing|hr|jobs|careers|feedback|help|service)@/i;

const HISTORICAL_PATTERNS = [
  /\b\d+\s*years?\s*(ago|later|after|since)\b/i,
  /\bworld war\s*(i{1,3}|1|2|two|three)\b/i,
  /\bvietnam war\b/i,
  /\bkorean war\b/i,
  /\bremains (returned|recovered|identified|found)\b/i,
  /\bidentified\s+\d+\s+years?\s+(after|later)\b/i,
  /\banniversary of\b/i,
  /\b(renamed|named after|in (honor|memory) of|memorial(ize)?)\b/i,
  /\bWWII\b|\bWW2\b/i
];
function isHistorical(text) {
  if (!text) return false;
  return HISTORICAL_PATTERNS.some(rx => rx.test(text));
}

module.exports = async function handler(req, res) {
  if (req.query?.secret !== 'ingest-now') return res.status(401).json({ error: 'unauthorized' });
  const db = getDb();
  const out = { emails_cleared: 0, persons_re_unverified: 0, incidents_demoted: 0, samples: [] };

  try {
    // Step 1: NULL out scraper/role-prefix emails on persons
    const persons = await db('persons').select('id', 'email').whereNotNull('email');
    for (const p of persons) {
      if (SCRAPER_EMAIL_RE.test(p.email) || ROLE_PREFIX_RE.test(p.email)) {
        await db('persons').where({ id: p.id }).update({ email: null, updated_at: new Date() });
        out.emails_cleared++;
        if (out.samples.length < 5) out.samples.push({ id: p.id, bad_email: p.email });
      }
    }

    // Step 2: re-run Stage-A on currently verified persons + check temporal (only fetch persons attached to qualified incidents)
    const verifiedPersons = await db.raw(`
      SELECT p.id, p.full_name, i.id as incident_id, i.qualification_state,
             COALESCE(i.address, '') || ' ' || COALESCE(i.description, '') AS context
      FROM persons p
      JOIN incidents i ON i.id = p.incident_id
      WHERE COALESCE(p.victim_verified, false) = true
    `).then(r => r.rows || r);

    for (const vp of verifiedPersons) {
      const text = vp.context || '';
      // temporal check
      if (isHistorical(text)) {
        await db('persons').where({ id: vp.id }).update({
          victim_verified: false,
          victim_role: 'historical',
          victim_verifier_reason: 'historical_or_anniversary_case_retro_check',
          updated_at: new Date()
        });
        out.persons_re_unverified++;
        if (out.samples.length < 8) out.samples.push({ name: vp.full_name, reason: 'historical' });
        continue;
      }
      // Stage A re-check
      const r = quickClassify(vp.full_name, text);
      if (r.decision === 'deny' && r.confidence >= 70) {
        await db('persons').where({ id: vp.id }).update({
          victim_verified: false,
          victim_role: r.reason,
          victim_verifier_reason: 'stage_a:' + r.reason,
          updated_at: new Date()
        });
        out.persons_re_unverified++;
      }
    }

    // Step 3: demote orphan qualified incidents
    const orphans = await db.raw(`
      SELECT i.id FROM incidents i
      LEFT JOIN persons p ON p.incident_id = i.id AND COALESCE(p.victim_verified, false) = true
      WHERE i.qualification_state = 'qualified'
      GROUP BY i.id
      HAVING COUNT(p.id) = 0
    `).then(r => r.rows || r);
    for (const o of orphans) {
      await db('incidents').where({ id: o.id }).update({
        qualification_state: 'pending_unverified',
        qualified_at: null,
        updated_at: new Date()
      });
      out.incidents_demoted++;
    }

    return res.json({ success: true, ...out, timestamp: new Date().toISOString() });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
};
