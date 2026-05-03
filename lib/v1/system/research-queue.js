/**
 * Phase 95: Research Queue
 *
 * Mason's insight (correct): we have 53 named persons in DB beyond the 11
 * qualified leads. Apollo/PDL/Trestle don't have them. But each name + state
 * is a strong starting point for a rep to manually research in <5 min.
 *
 * This endpoint produces a research-ready list with curated 1-click URLs
 * that open the right search in Brave / Google / Obituary aggregators / county
 * property / facebook / linkedin / whitepages. Rep clicks through 5-10 tabs,
 * finds contact info, manually inserts.
 *
 * Endpoints:
 *   GET ?action=list  — JSON list of research-ready persons with one-click URLs
 *   GET ?action=html  — HTML report (rep-friendly clickable view)
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
  // Need at least 2 word tokens (first + last)
  const tokens = name.trim().split(/\s+/).filter(Boolean);
  if (tokens.length < 2) return false;
  // Reject if any token is a possessive descriptor
  if (/(\b(of|the|a|an)\b)/i.test(name) && tokens.length < 3) return false;
  return true;
}

function buildResearchUrls(p) {
  const name = encodeURIComponent(p.full_name);
  const nameQ = encodeURIComponent(`"${p.full_name}"`);
  const state = p.state || '';
  const city = p.city || '';
  const cityState = encodeURIComponent(`${city ? city+',' : ''} ${state}`.trim());
  const obitQ = encodeURIComponent(`"${p.full_name}" obituary ${state}`);
  const accidentQ = encodeURIComponent(`"${p.full_name}" accident OR crash ${state}`);
  return {
    google_obit: `https://www.google.com/search?q=${obitQ}`,
    brave_obit: `https://search.brave.com/search?q=${obitQ}`,
    google_accident: `https://www.google.com/search?q=${accidentQ}`,
    legacy_com: `https://www.legacy.com/search?firstName=${encodeURIComponent(p.full_name.split(' ')[0])}&lastName=${encodeURIComponent(p.full_name.split(' ').slice(-1)[0])}&state=${state}`,
    facebook: `https://www.facebook.com/search/people/?q=${name}+${cityState}`,
    linkedin: `https://www.linkedin.com/search/results/people/?keywords=${name}&location=${cityState}`,
    truepeoplesearch: `https://www.truepeoplesearch.com/results?name=${name}&citystatezip=${cityState}`,
    whitepages: `https://www.whitepages.com/name/${encodeURIComponent(p.full_name.replace(/\s+/g,'-'))}/${state}`,
    findagrave: `https://www.findagrave.com/memorial/search?firstname=${encodeURIComponent(p.full_name.split(' ')[0])}&lastname=${encodeURIComponent(p.full_name.split(' ').slice(-1)[0])}&location=${state}`,
    gofundme: `https://www.gofundme.com/s?q=${name}`,
    caringbridge: `https://www.caringbridge.org/search?q=${name}`,
    courtlistener: `https://www.courtlistener.com/?q=${nameQ}&type=r`,
    voter_search: state === 'FL' ? `https://registration.elections.myflorida.com/CheckVoterStatus` :
                  state === 'GA' ? `https://www.mvp.sos.ga.gov/s/voter-information` :
                  state === 'TX' ? `https://teamrv-mvp.sos.texas.gov/MVP/mvp.do` : null,
    radaris: `https://radaris.com/p/${encodeURIComponent(p.full_name.replace(/\s+/g,'-'))}/${state}`
  };
}

async function findResearchablePersons(db, limit) {
  const rows = (await db.raw(`
    SELECT p.id, p.full_name, p.role, p.lead_tier, p.age,
           i.state, i.city, i.severity, i.lead_score,
           i.qualification_state, i.occurred_at, i.id as incident_id,
           i.incident_number, i.description
    FROM persons p JOIN incidents i ON i.id = p.incident_id
    WHERE p.full_name IS NOT NULL AND length(p.full_name) >= 5
      AND (i.qualification_state IS NULL OR i.qualification_state != 'qualified')
      AND (p.phone IS NULL AND p.email IS NULL AND p.address IS NULL)
      AND (i.occurred_at > NOW() - INTERVAL '90 days' OR i.discovered_at > NOW() - INTERVAL '30 days')
    ORDER BY
      CASE i.severity
        WHEN 'fatal' THEN 1
        WHEN 'critical' THEN 2
        WHEN 'serious' THEN 3
        ELSE 4
      END,
      i.lead_score DESC NULLS LAST,
      i.occurred_at DESC NULLS LAST
    LIMIT ${parseInt(limit) || 80}
  `)).rows;
  return rows.filter(p => isResearchableName(p.full_name)).map(p => ({
    ...p,
    research_urls: buildResearchUrls(p)
  }));
}

function renderHtml(persons, total) {
  const rows = persons.map(p => {
    const u = p.research_urls;
    const links = [
      u.brave_obit ? `<a href="${u.brave_obit}" target="_blank">Brave Obit</a>` : '',
      u.google_obit ? `<a href="${u.google_obit}" target="_blank">Google Obit</a>` : '',
      u.legacy_com ? `<a href="${u.legacy_com}" target="_blank">Legacy.com</a>` : '',
      u.findagrave ? `<a href="${u.findagrave}" target="_blank">FindAGrave</a>` : '',
      u.facebook ? `<a href="${u.facebook}" target="_blank">FB</a>` : '',
      u.linkedin ? `<a href="${u.linkedin}" target="_blank">LinkedIn</a>` : '',
      u.truepeoplesearch ? `<a href="${u.truepeoplesearch}" target="_blank">TPS</a>` : '',
      u.whitepages ? `<a href="${u.whitepages}" target="_blank">WP</a>` : '',
      u.gofundme ? `<a href="${u.gofundme}" target="_blank">GoFundMe</a>` : '',
      u.caringbridge ? `<a href="${u.caringbridge}" target="_blank">Caringbridge</a>` : '',
      u.courtlistener ? `<a href="${u.courtlistener}" target="_blank">CourtListener</a>` : '',
      u.radaris ? `<a href="${u.radaris}" target="_blank">Radaris</a>` : '',
      u.voter_search ? `<a href="${u.voter_search}" target="_blank">Voter</a>` : ''
    ].filter(Boolean).join(' · ');
    const sev = p.severity === 'fatal' ? '<span style="color:#dc2626;font-weight:600">FATAL</span>' :
                p.severity === 'critical' ? '<span style="color:#ea580c;font-weight:600">CRITICAL</span>' :
                p.severity === 'serious' ? '<span style="color:#ca8a04;font-weight:500">SERIOUS</span>' :
                p.severity || '-';
    const occurred = p.occurred_at ? new Date(p.occurred_at).toISOString().split('T')[0] : '-';
    return `<tr style="border-bottom:1px solid #E2E8F0;">
      <td style="padding:10px;font-weight:600;font-family:'Space Grotesk',sans-serif;">${p.full_name}</td>
      <td style="padding:10px;">${p.role || '-'}${p.age ? ' · age '+p.age : ''}</td>
      <td style="padding:10px;">${p.city || '-'}, ${p.state || '?'}</td>
      <td style="padding:10px;">${sev}</td>
      <td style="padding:10px;font-family:'JetBrains Mono',monospace;">${p.lead_score || '-'}</td>
      <td style="padding:10px;color:#64748B;">${occurred}</td>
      <td style="padding:10px;font-size:12px;">${links}</td>
    </tr>
    <tr><td colspan="7" style="padding:4px 14px 12px;color:#64748B;font-size:12px;border-bottom:2px solid #F1F5F9;">${(p.description || '').slice(0, 200)}</td></tr>`;
  }).join('\n');

  return `<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Research Queue · ${persons.length} named-pending</title>
<style>
body { font-family: 'Inter',-apple-system,sans-serif; max-width:1400px; margin:24px auto; padding:0 24px; color:#1F2937; }
h1 { font-family:'Space Grotesk',sans-serif; color:#1F2937; }
h1 span { color:#EF4444; }
.tag { display:inline-block; background:#F1F5F9; border-radius:6px; padding:2px 10px; font-size:12px; color:#475569; margin-right:6px; }
table { width:100%; border-collapse:collapse; background:#fff; box-shadow:0 1px 3px rgba(0,0,0,0.08); border-radius:8px; overflow:hidden; }
th { padding:12px 10px; text-align:left; background:#F8FAFC; font-size:11px; text-transform:uppercase; letter-spacing:1px; color:#64748B; border-bottom:2px solid #E2E8F0; }
a { color:#1F2937; text-decoration:none; padding:2px 6px; background:#F1F5F9; border-radius:4px; }
a:hover { background:#FF6600; color:#fff; }
.intro { background:#FEF3C7; border:1px solid #FCD34D; padding:14px 18px; border-radius:8px; margin:16px 0; font-size:14px; }
</style></head><body>
<h1><span>R</span>esearch <span>Q</span>ueue · 53 named-pending persons</h1>
<div class="intro">
  <strong>Mason's insight (Phase 95):</strong> we have ${persons.length} named persons our auto-enrichment couldn't fill. Each row below has 12 one-click research links pre-populated with name + state + city. Most contact info can be found in 3-5 minutes per person via the <em>Brave Obit · Legacy.com · FindAGrave · TruePeopleSearch</em> chain.
  <span class="tag">Total in DB: ${total}</span>
  <span class="tag">Showing: ${persons.length}</span>
</div>
<table>
<thead><tr>
<th>Name</th><th>Role/Age</th><th>Location</th><th>Severity</th><th>Score</th><th>Occurred</th><th>Research links</th>
</tr></thead>
<tbody>${rows}</tbody>
</table>
<p style="text-align:center;color:#94A3B8;margin-top:32px;font-size:12px;">Generated by AIP research-queue endpoint · ACC by Donovan Digital Solutions</p>
</body></html>`;
}

module.exports = async function handler(req, res) {
  if (!authed(req)) return res.status(401).json({ error: 'unauthorized' });
  const action = req.query?.action || 'list';
  const db = getDb();

  if (action === 'health') {
    const total = (await db.raw(`
      SELECT COUNT(*) as c FROM persons p JOIN incidents i ON i.id = p.incident_id
      WHERE p.full_name IS NOT NULL AND length(p.full_name) >= 5
        AND (i.qualification_state IS NULL OR i.qualification_state != 'qualified')
        AND (p.phone IS NULL AND p.email IS NULL AND p.address IS NULL)
    `)).rows[0].c;
    return res.status(200).json({ ok: true, engine: 'research-queue', total_named_no_contact: parseInt(total) });
  }

  const limit = parseInt(req.query?.limit) || 80;
  const persons = await findResearchablePersons(db, limit);
  const total = persons.length;

  if (action === 'html') {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    return res.status(200).send(renderHtml(persons, total));
  }

  // default: JSON list
  return res.status(200).json({ ok: true, count: persons.length, total, persons });
};
