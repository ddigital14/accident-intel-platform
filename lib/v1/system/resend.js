/**
 * Phase 49: Resend email sender — replaces Gmail draft workflow with API auto-send.
 * Phase 50: Added send-html action for HTML emails with multi-recipient (PRD distribution).
 * Used for: master-list daily digest, qualified-lead alerts, rep credentials, PRD email.
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');

const RESEND_API_BASE = 'https://api.resend.com/emails';

async function getResendKey(db) {
  if (process.env.RESEND_API_KEY) return process.env.RESEND_API_KEY;
  try {
    const row = await db('system_config').where({ key: 'resend_api_key' }).first();
    if (row?.value) return typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
  } catch (_) {}
  return null;
}

async function getFromEmail(db) {
  if (process.env.RESEND_FROM_EMAIL) return process.env.RESEND_FROM_EMAIL;
  try {
    const row = await db('system_config').where({ key: 'resend_from_email' }).first();
    if (row?.value) return typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
  } catch (_) {}
  return 'AIP <onboarding@resend.dev>';
}

async function sendEmail(opts) {
  const db = getDb();
  const key = await getResendKey(db);
  if (!key) return { ok: false, error: 'no_resend_key' };

  const fromEmail = opts.from || await getFromEmail(db);
  const body = {
    from: fromEmail,
    to: Array.isArray(opts.to) ? opts.to : [opts.to],
    subject: opts.subject || '(no subject)',
  };
  if (opts.cc?.length) body.cc = Array.isArray(opts.cc) ? opts.cc : [opts.cc];
  if (opts.bcc?.length) body.bcc = Array.isArray(opts.bcc) ? opts.bcc : [opts.bcc];
  if (opts.html) body.html = opts.html;
  if (opts.text) body.text = opts.text;
  if (opts.replyTo) body.reply_to = opts.replyTo;

  try {
    const r = await fetch(RESEND_API_BASE, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${key}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(15000)
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) {
      await reportError(db, 'resend-send', null, `${r.status}:${d.message || JSON.stringify(d)}`).catch(() => {});
      return { ok: false, status: r.status, error: d.message || `http_${r.status}`, raw: d };
    }
    return { ok: true, id: d.id, ...d };
  } catch (e) {
    await reportError(db, 'resend-send', null, e.message).catch(() => {});
    return { ok: false, error: e.message };
  }
}

async function health(db) {
  const key = await getResendKey(db);
  return { ok: !!key, configured: !!key };
}

/**
 * Read raw POST body if not already parsed by router.
 */
async function readBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  if (typeof req.body === 'string') {
    try { return JSON.parse(req.body); } catch { return {}; }
  }
  return new Promise((resolve) => {
    let data = '';
    req.on('data', (chunk) => { data += chunk; });
    req.on('end', () => {
      try { resolve(JSON.parse(data || '{}')); } catch { resolve({}); }
    });
    req.on('error', () => resolve({}));
  });
}

module.exports = { sendEmail, health };
module.exports.handler = async function (req, res) {
  const db = getDb();
  const action = req.query?.action;

  if (action === 'health') return res.json(await health(db));

  if (action === 'test' && req.query?.to) {
    const r = await sendEmail({
      to: req.query.to,
      subject: req.query.subject || 'AIP Resend Test',
      text: req.query.body || 'Resend integration test from AIP.',
      html: `<p>${req.query.body || 'Resend integration test from AIP.'}</p>`
    });
    return res.json(r);
  }

  // send-html: fetches HTML from a URL and sends to multi-recipient list
  // Usage: GET /api/v1/system/resend?action=send-html&to=a@x.com,b@y.com&subject=...&html_url=https://...
  // OR:    POST /api/v1/system/resend?action=send-html with body {to:[], subject, html, from?}
  if (action === 'send-html') {
    let payload = {};
    if (req.method === 'POST') {
      payload = await readBody(req);
    } else {
      payload = {
        to: (req.query?.to || '').split(',').map(s => s.trim()).filter(Boolean),
        subject: req.query?.subject || '(no subject)',
        from: req.query?.from,
        html_url: req.query?.html_url,
        text: req.query?.text
      };
    }
    if (!payload.to || (Array.isArray(payload.to) && !payload.to.length)) {
      return res.status(400).json({ error: 'missing_to' });
    }
    let html = payload.html;
    if (!html && payload.html_url) {
      try {
        const r = await fetch(payload.html_url, { signal: AbortSignal.timeout(20000) });
        if (r.ok) html = await r.text();
        else return res.status(502).json({ error: 'html_url_fetch_failed', status: r.status });
      } catch (e) {
        return res.status(502).json({ error: 'html_url_fetch_error', message: e.message });
      }
    }
    if (!html) return res.status(400).json({ error: 'missing_html_or_html_url' });
    const result = await sendEmail({
      to: payload.to,
      subject: payload.subject || '(no subject)',
      from: payload.from,
      html,
      text: payload.text || undefined,
      replyTo: payload.reply_to || payload.replyTo
    });
    return res.json(result);
  }

  return res.status(400).json({ error: 'use action=health, action=test&to=..., or action=send-html (POST or html_url)' });
};
