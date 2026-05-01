/**
 * MASTER LEAD LIST — Phase 48
 *
 * Compiles every qualified lead across the platform into a single
 * tightly-formatted HTML email digest a PI lawyer can forward to their
 * intake team without editing.
 *
 * HTTP shapes:
 *   GET /api/v1/system/master-lead-list?secret=ingest-now&format=html
 *   GET /api/v1/system/master-lead-list?secret=ingest-now&format=json
 *   GET /api/v1/system/master-lead-list?secret=ingest-now&format=html&admin=1
 *
 * Cron entrypoint:
 *   GET /api/v1/system/master-lead-list?secret=ingest-now&action=cron-digest
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { trackApiCall } = require('./cost');

const SECRET = 'ingest-now';

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

function safeJson(v) {
  if (!v) return null;
  if (typeof v === 'object') return v;
  try { return JSON.parse(v); } catch (_) { return null; }
}

function escHtml(s) {
  if (s === null || s === undefined) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function fmtDateTime(d) {
  if (!d) return '—';
  try {
    const dt = new Date(d);
    if (isNaN(dt.getTime())) return '—';
    return dt.toLocaleString('en-US', { timeZone: 'America/New_York', dateStyle: 'medium', timeStyle: 'short' });
  } catch (_) { return '—'; }
}

async function gatherLeads(db) {
  const incidents = await db('incidents')
    .where('qualification_state', 'qualified')
    .orderBy('lead_score', 'desc')
    .orderBy('discovered_at', 'desc')
    .limit(200);

  const leads = [];

  for (const inc of incidents) {
    let persons = [];
    try {
      persons = await db('persons').where('incident_id', inc.id).orderBy('created_at', 'asc');
    } catch (_) {}

    let primary = persons.find(p => p.victim_verified) ||
                  persons.find(p => p.phone || p.email) ||
                  persons[0] || null;

    let family = [];
    if (primary?.id) {
      try {
        family = await db('persons')
          .where('victim_id', primary.id)
          .select('full_name', 'relationship_to_victim', 'phone', 'email', 'derived_from')
          .limit(8);
      } catch (_) {}
    }

    let sources = [];
    try {
      sources = await db('incident_sources')
        .where('incident_id', inc.id)
        .select('source_type', 'source_url', 'title', 'published_at')
        .orderBy('published_at', 'desc')
        .limit(6);
    } catch (_) {}
    if ((!sources || !sources.length) && inc.source_url) {
      sources = [{ source_type: inc.source_type || 'news', source_url: inc.source_url, title: inc.source_title || inc.summary || null, published_at: inc.discovered_at }];
    }

    let insuranceCarrier = null;
    const incData = safeJson(inc.incident_data) || {};
    if (incData.insurance_carrier) insuranceCarrier = incData.insurance_carrier;
    if (!insuranceCarrier && primary) {
      const pd = safeJson(primary.enrichment_data) || {};
      if (pd.insurance_doi?.carrier) insuranceCarrier = pd.insurance_doi.carrier;
      if (pd.insurance_carrier) insuranceCarrier = pd.insurance_carrier;
    }

    let repBrief = null;
    if (primary?.id) {
      try {
        const br = await db('enrichment_logs')
          .where('person_id', primary.id)
          .where('source', 'rep-call-brief')
          .where('created_at', '>', new Date(Date.now() - 3 * 86400000))
          .orderBy('created_at', 'desc')
          .first();
        if (br) {
          const d = safeJson(br.new_value) || safeJson(br.data) || {};
          repBrief = d.next_action || d.opening_talking_point || d.brief || null;
        }
      } catch (_) {}
    }

    // Phase 50b: pull cultural context (language + smart-cross-ref next-best-actions)
    let primaryLang = null;
    let smartCrossNba = null;
    if (primary?.id) {
      try {
        const pp = await db('persons').where('id', primary.id).select('preferred_language', 'enrichment_data').first();
        primaryLang = pp?.preferred_language || null;
        const ed = safeJson(pp?.enrichment_data) || {};
        if (ed.preferred_language && !primaryLang) primaryLang = ed.preferred_language;
        if (ed.original_language && !primaryLang) primaryLang = ed.original_language;
      } catch (_) {}
      try {
        const sx = await db('enrichment_logs')
          .where('person_id', primary.id)
          .where('engine', 'smart-cross-ref')
          .where('candidate_type', 'next_best_action')
          .orderBy('created_at', 'desc').limit(2);
        if (sx?.length) smartCrossNba = sx.map(r => r.candidate_value);
      } catch (_) {}
    }

    let crossCheck = null;
    if (primary?.id) {
      try {
        const cc = await db('enrichment_logs')
          .where('person_id', primary.id)
          .where('field_name', 'evidence_cross_check_summary')
          .orderBy('created_at', 'desc')
          .first();
        if (cc) {
          const d = safeJson(cc.new_value) || {};
          const meta = safeJson(cc.data) || {};
          crossCheck = {
            matches: d.matches || 0,
            conflicts: d.conflicts || 0,
            detail: Array.isArray(d.detail) ? d.detail : [],
            cross_engine_conflict: !!meta.cross_engine_conflict
          };
        }
      } catch (_) {}
    }

    const fieldSources = { phone: null, email: null, address: null };
    if (primary?.id) {
      try {
        const recentLogs = await db('enrichment_logs')
          .where('person_id', primary.id)
          .whereIn('field_name', ['phone', 'email', 'address'])
          .orderBy('created_at', 'desc')
          .limit(40);
        for (const l of recentLogs) {
          if (!fieldSources[l.field_name]) {
            fieldSources[l.field_name] = { source: l.source || 'unknown', confidence: l.confidence || null };
          }
        }
      } catch (_) {}
    }

    leads.push({
      incident: inc,
      primary,
      persons,
      family,
      sources,
      insuranceCarrier,
      repBrief,
      crossCheck,
      fieldSources,
      primaryLang,
      smartCrossNba
    });
  }

  return leads;
}

function vehiclesString(inc) {
  const d = safeJson(inc.incident_data) || {};
  const list = [];
  if (Array.isArray(d.vehicles)) {
    for (const v of d.vehicles) {
      const parts = [v.year, v.make, v.model].filter(Boolean).join(' ');
      if (parts) list.push(parts + (v.plate ? ` (${v.plate})` : ''));
    }
  }
  if (inc.vehicle_count) list.push(`${inc.vehicle_count} vehicles`);
  return list.length ? list.join(', ') : '—';
}

function attorneyStatus(primary) {
  if (!primary) return '—';
  if (primary.has_attorney === true) {
    return `Retained${primary.attorney_firm ? ` — ${primary.attorney_firm}` : ''}`;
  }
  if (primary.has_attorney === false) return 'No attorney on file';
  return 'Unknown';
}

const LANG_FLAGS = {
  es: '\uD83C\uDDF2\uD83C\uDDFD',  // MX
  fr: '\uD83C\uDDEB\uD83C\uDDF7',  // FR
  ht: '\uD83C\uDDED\uD83C\uDDF9',  // HT
  vi: '\uD83C\uDDFB\uD83C\uDDF3',  // VN
  tl: '\uD83C\uDDF5\uD83C\uDDED',  // PH
  ko: '\uD83C\uDDF0\uD83C\uDDF7',  // KR
  zh: '\uD83C\uDDE8\uD83C\uDDF3',  // CN
  ru: '\uD83C\uDDF7\uD83C\uDDFA',  // RU
  pt: '\uD83C\uDDE7\uD83C\uDDF7',  // BR
  ar: '\uD83C\uDDF8\uD83C\uDDE6',  // SA (proxy)
};
const LANG_NAMES = {
  es: 'Spanish', fr: 'French', ht: 'Haitian Creole', vi: 'Vietnamese',
  tl: 'Tagalog', ko: 'Korean', zh: 'Chinese', ru: 'Russian',
  pt: 'Portuguese', ar: 'Arabic'
};

function buildLeadCard(L, opts) {
  const { admin = false } = opts || {};
  const inc = L.incident;
  const p = L.primary || {};
  const score = inc.lead_score ?? '—';

  // Phase 50b: cultural context row when victim language != English
  const lang = L.primaryLang && L.primaryLang !== 'en' ? L.primaryLang : null;
  const culturalContextRow = lang ? `
          <tr><td colspan="2" style="border-top:1px dashed #eee;height:6px;"></td></tr>
          <tr><td style="color:#555;vertical-align:top;">Cultural context</td><td>
            <span style="font-size:14px;">${LANG_FLAGS[lang] || ''}</span>
            <strong>${escHtml(LANG_NAMES[lang] || lang)}</strong>-speaking lead.
            ${L.smartCrossNba?.length ? '<ul style="margin:4px 0 0 18px;padding:0;color:#444;">' + L.smartCrossNba.map(a => `<li>${escHtml(a)}</li>`).join('') + '</ul>' : '<span style="color:#888;font-size:12px;">Run smart-cross-ref for community-aware next-best-actions.</span>'}
          </td></tr>` : '';

  const phoneCell = p.phone ? `${escHtml(p.phone)}<span style="color:#888;font-size:11px;"> ${L.fieldSources.phone ? `(${escHtml(L.fieldSources.phone.source)}, conf ${L.fieldSources.phone.confidence ?? '?'})` : ''}</span>` : '<span style="color:#999;">—</span>';
  const emailCell = p.email ? `${escHtml(p.email)}<span style="color:#888;font-size:11px;"> ${L.fieldSources.email ? `(${escHtml(L.fieldSources.email.source)}, conf ${L.fieldSources.email.confidence ?? '?'})` : ''}</span>` : '<span style="color:#999;">—</span>';
  const addrParts = [p.address, p.city, p.state, p.zip].filter(Boolean).join(', ');
  const addrCell = addrParts ? `${escHtml(addrParts)}<span style="color:#888;font-size:11px;"> ${L.fieldSources.address ? `(${escHtml(L.fieldSources.address.source)}, conf ${L.fieldSources.address.confidence ?? '?'})` : ''}</span>` : '<span style="color:#999;">—</span>';

  const familyHtml = (L.family || []).length
    ? '<ul style="margin:4px 0 0 18px;padding:0;">' +
      L.family.map(f => `<li>${escHtml(f.full_name || '—')}${f.relationship_to_victim ? ` <span style="color:#666;">(${escHtml(f.relationship_to_victim)})</span>` : ''}${f.phone ? ` — ${escHtml(f.phone)}` : ''}${f.email ? ` — ${escHtml(f.email)}` : ''}</li>`).join('') +
      '</ul>'
    : '<span style="color:#999;">—</span>';

  const sourcesHtml = (L.sources || []).length
    ? '<ul style="margin:4px 0 0 18px;padding:0;">' +
      L.sources.map(s => `<li><a href="${escHtml(s.source_url || '#')}" style="color:#1a73e8;">${escHtml(s.title || s.source_url || '(article)')}</a>${s.source_type ? ` <span style="color:#888;">[${escHtml(s.source_type)}]</span>` : ''}</li>`).join('') +
      '</ul>'
    : '<span style="color:#999;">—</span>';

  const ccLine = L.crossCheck
    ? `${L.crossCheck.matches} match${L.crossCheck.matches === 1 ? '' : 'es'} / ${L.crossCheck.conflicts} conflict${L.crossCheck.conflicts === 1 ? '' : 's'}${L.crossCheck.cross_engine_conflict ? ' <strong style="color:#d93025;">(CONFLICT FLAGGED)</strong>' : ''}${L.crossCheck.detail?.length ? ` — ${escHtml(L.crossCheck.detail.join(', '))}` : ''}`
    : '<span style="color:#999;">not yet checked</span>';

  const adminFooter = admin
    ? `<tr><td colspan="2" style="background:#fafafa;padding:8px 12px;border-top:1px solid #eee;color:#555;font-size:12px;">
        <strong>Admin:</strong>
        cost-per-lead ${inc.cost_estimate_usd ? '$' + Number(inc.cost_estimate_usd).toFixed(2) : 'n/a'} ·
        primary source ${escHtml(inc.source_type || inc.discovery_source || '—')} ·
        confidence ${p.confidence ?? '—'} ·
        verified ${p.victim_verified ? 'YES' : 'no'} ·
        person_id <code>${escHtml(p.id || '—')}</code>
       </td></tr>`
    : '';

  return `
  <table cellpadding="0" cellspacing="0" border="0" width="100%" style="border:1px solid #ddd;border-radius:6px;margin-bottom:18px;background:#fff;">
    <tr>
      <td style="background:#1a73e8;color:#fff;padding:10px 14px;border-top-left-radius:6px;border-top-right-radius:6px;">
        <span style="font-size:18px;font-weight:bold;">${escHtml(p.full_name || 'Unknown victim')}</span>
        ${p.age ? `<span style="font-size:13px;font-weight:normal;opacity:0.9;"> · age ${escHtml(p.age)}</span>` : ''}
        ${p.role ? `<span style="font-size:13px;font-weight:normal;opacity:0.9;"> · ${escHtml(p.role)}</span>` : ''}
        <span style="float:right;font-size:13px;">Score <strong>${escHtml(score)}</strong></span>
      </td>
    </tr>
    <tr>
      <td style="padding:10px 14px;border-bottom:1px solid #eee;">
        <table cellpadding="4" cellspacing="0" border="0" width="100%" style="font-size:13px;">
          <tr><td style="width:140px;color:#555;vertical-align:top;">Phone</td><td>${phoneCell}</td></tr>
          <tr><td style="color:#555;vertical-align:top;">Email</td><td>${emailCell}</td></tr>
          <tr><td style="color:#555;vertical-align:top;">Address</td><td>${addrCell}</td></tr>
          <tr><td style="color:#555;vertical-align:top;">Family / NoK</td><td>${familyHtml}</td></tr>
          <tr><td colspan="2" style="border-top:1px dashed #eee;height:6px;"></td></tr>
          <tr><td style="color:#555;vertical-align:top;">Incident</td><td>${escHtml(inc.incident_type || '—')} · severity <strong>${escHtml(inc.severity || '—')}</strong></td></tr>
          <tr><td style="color:#555;vertical-align:top;">When</td><td>${escHtml(fmtDateTime(inc.occurred_at))}</td></tr>
          <tr><td style="color:#555;vertical-align:top;">Where</td><td>${escHtml([inc.address || inc.intersection, inc.city, inc.state].filter(Boolean).join(', ') || '—')}</td></tr>
          <tr><td style="color:#555;vertical-align:top;">Vehicles</td><td>${escHtml(vehiclesString(inc))}</td></tr>
          <tr><td style="color:#555;vertical-align:top;">Attorney</td><td>${escHtml(attorneyStatus(p))}</td></tr>
          <tr><td style="color:#555;vertical-align:top;">Insurance</td><td>${L.insuranceCarrier ? escHtml(L.insuranceCarrier) : '<span style="color:#999;">unknown</span>'}</td></tr>
          ${culturalContextRow}
          <tr><td colspan="2" style="border-top:1px dashed #eee;height:6px;"></td></tr>
          <tr><td style="color:#555;vertical-align:top;">Sources</td><td>${sourcesHtml}</td></tr>
          <tr><td style="color:#555;vertical-align:top;">Cross-check</td><td>${ccLine}</td></tr>
          <tr><td style="color:#555;vertical-align:top;">Next action</td><td>${L.repBrief ? escHtml(L.repBrief) : '<span style="color:#999;">Run rep-call-brief from dashboard.</span>'}</td></tr>
        </table>
      </td>
    </tr>
    ${adminFooter}
  </table>`;
}

function buildHtml(leads, opts) {
  const { admin = false } = opts || {};
  const date = new Date().toLocaleDateString('en-US', { timeZone: 'America/New_York', dateStyle: 'long' });
  const subjectLine = `${leads.length} active qualified lead${leads.length === 1 ? '' : 's'} · ${date}`;

  // Phase 52 v5b: ACC impact-starburst logomark inlined for email
  const accLogoInline = `<svg width="42" height="42" viewBox="0 0 240 240" xmlns="http://www.w3.org/2000/svg" style="vertical-align:middle;display:block;"><defs><radialGradient id="emE" cx="50%" cy="50%" r="50%"><stop offset="0%" stop-color="#FCA5A5"/><stop offset="55%" stop-color="#EF4444"/><stop offset="100%" stop-color="#B91C1C"/></radialGradient></defs><g transform="translate(120,120)"><circle r="106" fill="none" stroke="#1F2937" stroke-width="0.6" opacity="0.4"/><circle r="92" fill="none" stroke="#1F2937" stroke-width="0.5" opacity="0.25"/><g fill="none" stroke="#1F2937" stroke-width="1.5" stroke-linecap="round"><line x1="0" y1="0" x2="0" y2="-82" opacity="0.95"/><line x1="0" y1="0" x2="56" y2="-62" opacity="0.85"/><line x1="0" y1="0" x2="80" y2="-18" opacity="0.9"/><line x1="0" y1="0" x2="68" y2="42" opacity="0.7"/><line x1="0" y1="0" x2="-40" y2="64" opacity="0.6"/><line x1="0" y1="0" x2="-76" y2="28" opacity="0.85"/><line x1="0" y1="0" x2="-62" y2="-50" opacity="0.78"/></g><g fill="#EF4444" opacity="0.85"><circle cx="54" cy="-50" r="2.1"/><circle cx="-68" cy="20" r="1.6"/><circle cx="38" cy="58" r="1.2"/></g><circle r="20" fill="#FFFFFF"/><circle r="13" fill="url(#emE)"/><circle r="5.5" fill="#FCA5A5"/><circle r="2.2" fill="#FFFFFF"/></g></svg>`;

  // ACC v5b header — strict white, charcoal text, hairline border, red ember
  const summary = `
  <table cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-bottom:20px;background:#FFFFFF;border:1px solid #E2E8F0;border-radius:12px;box-shadow:0 1px 3px rgba(31,41,55,0.06);">
    <tr><td style="padding:20px 22px;">
      <table cellpadding="0" cellspacing="0" border="0">
        <tr>
          <td style="vertical-align:middle;padding-right:14px;">${accLogoInline}</td>
          <td style="vertical-align:middle;">
            <div style="font-family:\'Space Grotesk\',-apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif;font-size:20px;font-weight:500;color:#1F2937;letter-spacing:-0.2px;line-height:1.1;"><span style="color:#EF4444;">A</span>ccident <span style="color:#EF4444;">C</span>ommand <span style="color:#EF4444;">C</span>enter</div>
            <div style="font-family:\'Inter\',-apple-system,Helvetica,Arial,sans-serif;font-size:10px;font-weight:500;color:#94A3B8;letter-spacing:1.4px;text-transform:uppercase;margin-top:4px;">Daily Lead List</div>
          </td>
        </tr>
      </table>
      <div style="font-family:\'Inter\',-apple-system,Helvetica,Arial,sans-serif;font-size:13px;color:#475569;margin-top:14px;border-top:1px solid #E2E8F0;padding-top:12px;">${escHtml(subjectLine)}${admin ? ' <span style="color:#B91C1C;font-weight:500;">(ADMIN VIEW — includes raw cost / source / confidence)</span>' : ''}</div>
    </td></tr>
  </table>`;

  const cards = leads.map(L => buildLeadCard(L, { admin })).join('\n');

  const ctaButton = `
  <div style="text-align:center;margin:24px 0 12px 0;">
    <a href="https://accidentcommandcenter.com" style="display:inline-block;padding:12px 24px;background:#1F2937;color:#FFFFFF;text-decoration:none;font-family:\'Inter\',-apple-system,Helvetica,Arial,sans-serif;font-size:14px;font-weight:500;border-radius:8px;letter-spacing:0.2px;">Open Command Center →</a>
  </div>`;

  const footer = `
  <div style="margin-top:24px;padding:16px 18px;background:#F8FAFC;border:1px solid #E2E8F0;border-radius:8px;font-family:\'Inter\',-apple-system,Helvetica,Arial,sans-serif;font-size:12px;color:#475569;line-height:1.5;">
    Generated ${escHtml(new Date().toISOString())} by AIP master-lead-list endpoint.
    Click any victim name to call up the rep-call-brief in the dashboard.
    Conflicts flagged by the evidence-cross-checker are marked in red — QA before contact.
    <div style="margin-top:10px;padding-top:10px;border-top:1px solid #E2E8F0;text-align:center;color:#94A3B8;font-size:11px;letter-spacing:1.4px;text-transform:uppercase;">
      by <span style="color:#FF6600;font-weight:600;">D</span>onovan <span style="color:#FF6600;font-weight:600;">D</span>igital <span style="color:#FF6600;font-weight:600;">S</span>olutions · <a href="https://accidentcommandcenter.com" style="color:#1F2937;text-decoration:none;font-weight:500;">accidentcommandcenter.com</a>
    </div>
  </div>`;

  return `<div style="font-family:\'Inter\',-apple-system,BlinkMacSystemFont,\'Segoe UI\',Helvetica,Arial,sans-serif;max-width:600px;margin:0 auto;padding:16px;color:#1F2937;background:#FFFFFF;">
${summary}
${cards}
${ctaButton}
${footer}
</div>`;
}

async function renderHtml(db, opts) {
  const leads = await gatherLeads(db);
  return { html: buildHtml(leads, opts || {}), count: leads.length, leads };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  let db;
  try { db = getDb(); } catch (e) {
    return res.status(500).json({ error: 'db_unavailable: ' + e.message });
  }

  const action = String(req.query?.action || '').toLowerCase();
  const format = String(req.query?.format || 'html').toLowerCase();
  const admin = req.query?.admin === '1' || req.query?.admin === 'true';

  try {
    if (action === 'cron-digest') {
      const r = await renderHtml(db, { admin: true });
      try {
        await db('enrichment_logs').insert({
          person_id: null,
          field_name: 'cron_digest',
          old_value: null,
          new_value: JSON.stringify({ count: r.count, generated_at: new Date().toISOString() }).slice(0, 4000),
          source_url: null,
          source: 'master-lead-list',
          confidence: 100,
          verified: true,
          created_at: new Date()
        });
      } catch (_) {}
      await trackApiCall(db, 'master-lead-list', 'cron_digest', 0, 0, true).catch(() => {});
      return res.status(200).json({ success: true, count: r.count, message: 'cron digest rendered (draft creation handled out-of-band).', timestamp: new Date().toISOString() });
    }

    const r = await renderHtml(db, { admin });
    await trackApiCall(db, 'master-lead-list', 'render', 0, 0, true).catch(() => {});
    if (format === 'json') {
      return res.status(200).json({
        success: true,
        count: r.count,
        leads: r.leads.map(L => ({
          incident_id: L.incident.id,
          person_id: L.primary?.id || null,
          score: L.incident.lead_score,
          victim: L.primary?.full_name,
          age: L.primary?.age,
          role: L.primary?.role,
          phone: L.primary?.phone || null,
          email: L.primary?.email || null,
          city: L.primary?.city,
          state: L.primary?.state,
          attorney: L.primary?.has_attorney ? (L.primary.attorney_firm || true) : false,
          insurance: L.insuranceCarrier,
          family_count: L.family.length,
          sources_count: L.sources.length,
          cross_check: L.crossCheck,
          next_action: L.repBrief
        })),
        timestamp: new Date().toISOString()
      });
    }
    // Phase 49: ?send=true auto-emails the master list via Resend to all active reps + admins
    if (req.query?.send === 'true' || req.query?.send === '1') {
      try {
        const { sendEmail } = require('./resend');
        // Phase 49: ?to=email allows single-recipient (works in Resend sandbox until domain verified)
        let all;
        if (req.query?.to) {
          all = String(req.query.to).split(',').map(e => e.trim()).filter(Boolean);
        } else {
          const recipients = await db('users')
            .whereIn('role', ['rep','admin','manager'])
            .where('is_active', true)
            .whereNotNull('email')
            .select('email','first_name','last_name','role');
          if (!recipients.length) return res.json({ success: false, error: 'no_active_recipients' });
          all = [...new Set(recipients.map(u => u.email))];
        }
        const dateStr = new Date().toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' });
        const subject = `AIP Master Lead List — ${r.count} Qualified Leads (${dateStr})`;
        const sendRes = await sendEmail({
          to: all,
          subject,
          html: r.html,
          text: 'See HTML version for full lead details. Open https://accident-intel-platform.vercel.app for live dashboard.'
        });
        return res.json({ success: !!sendRes.ok, sent_to: all, lead_count: r.count, resend_id: sendRes.id, error: sendRes.error });
      } catch (sendErr) {
        return res.status(500).json({ error: 'send_failed:' + sendErr.message });
      }
    }
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    return res.status(200).send(r.html);
  } catch (e) {
    await reportError(db, 'master-lead-list', null, e.message, { severity: 'error' }).catch(() => {});
    return res.status(500).json({ error: e.message });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.renderHtml = renderHtml;
module.exports.gatherLeads = gatherLeads;
module.exports.buildHtml = buildHtml;
