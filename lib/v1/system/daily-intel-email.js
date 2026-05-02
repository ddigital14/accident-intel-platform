/**
 * Phase 73: Platform Intelligence Daily Email
 *
 * Branded daily digest. Runs measurement.snapshot, formats it as branded HTML,
 * sends to admin/manager users (Mason + Chris + Fritzi).
 *
 * Endpoints:
 *   GET  /api/v1/system/daily-intel-email?secret=ingest-now&action=health
 *   POST /api/v1/system/daily-intel-email?secret=ingest-now&action=preview  → returns HTML body without sending
 *   POST /api/v1/system/daily-intel-email?secret=ingest-now&action=send     → sends to admin+manager users
 */
const { getDb } = require('../../_db');
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

function formatHTML(snap) {
  const inc = snap.incidents || {};
  const q = snap.qualified || {};
  const p = snap.persons || {};
  const e = snap.enrichment || {};
  const conv = (q.conversion_rate || 0) * 100;
  const top_engines = (snap.top_engines || []).slice(0, 5);
  const top_signals = (snap.top_signals || []).slice(0, 5);

  const fmt = n => Number(n || 0).toLocaleString('en-US');

  return `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1.0"/></head>
<body style="margin:0;padding:0;background:#F8FAFC;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#1F2937;line-height:1.55;">
<div style="max-width:680px;margin:0 auto;background:#FFFFFF;">

<div style="background:#1F2937;padding:28px 32px;">
  <div style="font-family:'Space Grotesk',sans-serif;font-weight:700;font-size:30px;letter-spacing:-0.02em;line-height:1;">
    <span style="color:#EF4444;">A</span><span style="color:#EF4444;">C</span><span style="color:#EF4444;">C</span>
    <span style="display:inline-block;width:14px;"></span>
    <span style="color:#FF6600;font-size:16px;font-weight:600;vertical-align:middle;">DDS</span>
  </div>
  <div style="margin-top:6px;color:#94A3B8;font-size:12px;font-weight:500;letter-spacing:0.04em;text-transform:uppercase;">Daily Platform Intelligence · ${new Date().toLocaleDateString('en-US',{weekday:'long',month:'short',day:'numeric',year:'numeric'})}</div>
</div>

<div style="padding:32px 32px 8px 32px;">
  <h1 style="margin:0 0 18px 0;font-family:'Space Grotesk',sans-serif;font-size:24px;line-height:1.2;letter-spacing:-0.02em;color:#0F172A;">Platform snapshot</h1>

  <table cellpadding="0" cellspacing="0" border="0" style="width:100%;border-collapse:collapse;background:#F8FAFC;border:1px solid #E2E8F0;border-radius:10px;overflow:hidden;margin-bottom:18px;">
    <tr>
      <td style="padding:16px 18px;font-size:12px;color:#64748B;text-transform:uppercase;letter-spacing:0.05em;font-weight:600;width:50%;border-right:1px solid #E2E8F0;">Incidents · 24h / 7d / total</td>
      <td style="padding:16px 18px;font-size:18px;color:#0F172A;font-weight:700;">${fmt(inc.last_24h)} / ${fmt(inc.last_7d)} / ${fmt(inc.total)}</td>
    </tr>
    <tr>
      <td style="padding:16px 18px;font-size:12px;color:#64748B;text-transform:uppercase;letter-spacing:0.05em;font-weight:600;border-right:1px solid #E2E8F0;border-top:1px solid #E2E8F0;">Qualified leads · conversion</td>
      <td style="padding:16px 18px;font-size:18px;color:#EF4444;font-weight:700;border-top:1px solid #E2E8F0;">${fmt(q.total)} · ${conv.toFixed(2)}%</td>
    </tr>
    <tr>
      <td style="padding:16px 18px;font-size:12px;color:#64748B;text-transform:uppercase;letter-spacing:0.05em;font-weight:600;border-right:1px solid #E2E8F0;border-top:1px solid #E2E8F0;">Persons · with phone / email / address</td>
      <td style="padding:16px 18px;font-size:18px;color:#0F172A;font-weight:700;border-top:1px solid #E2E8F0;">${fmt(p.total)} · ${fmt(p.with_phone)} / ${fmt(p.with_email)} / ${fmt(p.with_address)}</td>
    </tr>
    <tr>
      <td style="padding:16px 18px;font-size:12px;color:#64748B;text-transform:uppercase;letter-spacing:0.05em;font-weight:600;border-right:1px solid #E2E8F0;border-top:1px solid #E2E8F0;">Enrichment activity · 24h</td>
      <td style="padding:16px 18px;font-size:18px;color:#6366F1;font-weight:700;border-top:1px solid #E2E8F0;">${fmt(e.logs_last_24h)} log writes · ${fmt(e.total_cross_checked)} cross-checked</td>
    </tr>
  </table>

  <h2 style="margin:24px 0 8px 0;font-family:'Space Grotesk',sans-serif;font-size:16px;letter-spacing:-0.01em;color:#0F172A;">Top engines (last-fired)</h2>
  <table cellpadding="0" cellspacing="0" border="0" style="width:100%;border-collapse:collapse;font-size:13px;">
    <thead><tr><th style="text-align:left;padding:6px 8px;color:#64748B;font-weight:600;border-bottom:2px solid #E2E8F0;">Engine</th><th style="text-align:left;padding:6px 8px;color:#64748B;font-weight:600;border-bottom:2px solid #E2E8F0;">Input shape</th><th style="text-align:right;padding:6px 8px;color:#64748B;font-weight:600;border-bottom:2px solid #E2E8F0;">Hit rate</th></tr></thead>
    <tbody>
      ${top_engines.map(eng => {
        const r = (eng.attempts || 0) > 0 ? ((eng.successes / eng.attempts) * 100).toFixed(0) + '%' : '—';
        return `<tr><td style="padding:6px 8px;border-bottom:1px solid #F1F5F9;font-weight:600;color:#1F2937;">${eng.engine_id}</td><td style="padding:6px 8px;border-bottom:1px solid #F1F5F9;color:#475569;font-family:JetBrains Mono,Menlo,monospace;font-size:11px;">${(eng.input_shape || '?').slice(0, 30)}</td><td style="padding:6px 8px;border-bottom:1px solid #F1F5F9;text-align:right;color:#0F172A;font-weight:600;">${eng.successes}/${eng.attempts} · ${r}</td></tr>`;
      }).join('')}
    </tbody>
  </table>

  ${top_signals.length ? `
  <h2 style="margin:24px 0 8px 0;font-family:'Space Grotesk',sans-serif;font-size:16px;letter-spacing:-0.01em;color:#0F172A;">Top conversion signals (mined)</h2>
  <table cellpadding="0" cellspacing="0" border="0" style="width:100%;border-collapse:collapse;font-size:13px;">
    <thead><tr><th style="text-align:left;padding:6px 8px;color:#64748B;font-weight:600;border-bottom:2px solid #E2E8F0;">Signal</th><th style="text-align:right;padding:6px 8px;color:#64748B;font-weight:600;border-bottom:2px solid #E2E8F0;">Δ score</th><th style="text-align:right;padding:6px 8px;color:#64748B;font-weight:600;border-bottom:2px solid #E2E8F0;">Conversion</th></tr></thead>
    <tbody>
      ${top_signals.map(s => {
        const d = s.suggested_score_delta || 0;
        const dColor = d > 0 ? '#047857' : (d < 0 ? '#B91C1C' : '#64748B');
        return `<tr><td style="padding:6px 8px;border-bottom:1px solid #F1F5F9;font-weight:600;color:#1F2937;">${s.signal_type}::${s.pattern}</td><td style="padding:6px 8px;border-bottom:1px solid #F1F5F9;text-align:right;color:${dColor};font-weight:700;">${d > 0 ? '+' : ''}${d}</td><td style="padding:6px 8px;border-bottom:1px solid #F1F5F9;text-align:right;color:#0F172A;">${((s.conversion_rate || 0) * 100).toFixed(1)}% (n=${s.sample_size || 0})</td></tr>`;
      }).join('')}
    </tbody>
  </table>` : ''}

  <div style="margin-top:24px;padding:14px 18px;background:#FFFBEB;border-left:4px solid #F59E0B;border-radius:6px;font-size:13px;color:#78350F;">
    <strong>What changed today:</strong> ${fmt(inc.last_24h)} new incidents ingested · ${fmt(e.logs_last_24h)} enrichment writes · ${fmt(q.total)} qualified leads currently active
  </div>

  <div style="margin-top:18px;padding:14px 18px;background:#EEF2FF;border-left:4px solid #6366F1;border-radius:6px;font-size:13px;color:#312E81;">
    <strong>What's running in the background:</strong> auto-sweep every 15min · text-extractors every 2h · pattern-miner daily 4am · merge-finder daily 5am · family-graph daily 6am
  </div>
</div>

<div style="padding:24px 32px 32px 32px;border-top:1px solid #E2E8F0;margin-top:24px;">
  <a href="https://accidentcommandcenter.com/login" style="display:inline-block;background:#EF4444;color:#FFFFFF;text-decoration:none;font-weight:600;padding:12px 22px;border-radius:8px;font-size:14px;">Open dashboard →</a>
  <span style="display:inline-block;width:8px;"></span>
  <a href="https://accident-intel-platform.vercel.app/api/v1/system/measurement?secret=ingest-now&action=snapshot" style="color:#6366F1;text-decoration:none;font-weight:600;font-size:13px;">Live JSON snapshot</a>
  <div style="margin-top:18px;font-size:12px;color:#94A3B8;">Built by Donovan Digital Solutions · accidentcommandcenter.com</div>
</div>

</div>
</body></html>`;
}

async function buildAndSend(db, opts = {}) {
  const measurement = require('./measurement');
  const snap = await measurement.snapshot(db);
  const html = formatHTML(snap);

  if (opts.preview) return { ok: true, html };

  // Send to admin + manager users
  const recipients = await db('users')
    .whereIn('role', ['admin', 'manager'])
    .where('is_active', true)
    .select('email');
  const to = recipients.map(r => r.email).filter(Boolean);
  if (!to.length) return { ok: false, error: 'no_recipients' };

  const resend = require('./resend');
  const r = await resend.sendEmail({
    to,
    subject: `ACC Daily Intelligence — ${new Date().toLocaleDateString('en-US')}`,
    html,
    from: 'Accident Command Center <alerts@caseflowplatform.com>'
  });
  return { ok: r.ok, resend_id: r.id, recipients: to.length, error: r.error };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();

  if (action === 'health') return res.json({ success: true, service: 'daily-intel-email' });
  if (action === 'preview') {
    const r = await buildAndSend(db, { preview: true });
    res.setHeader('Content-Type', 'text/html');
    return res.send(r.html);
  }
  if (action === 'send') {
    return res.json(await buildAndSend(db, { preview: false }));
  }
  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.buildAndSend = buildAndSend;
