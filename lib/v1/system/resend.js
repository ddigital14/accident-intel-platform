/**
 * Phase 49: Resend email sender — replaces Gmail draft workflow with API auto-send.
 * Used for: master-list daily digest, qualified-lead alerts, rep credentials.
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
  // Default: use Resend's onboarding sender until user adds their own domain
  if (process.env.RESEND_FROM_EMAIL) return process.env.RESEND_FROM_EMAIL;
  try {
    const row = await db('system_config').where({ key: 'resend_from_email' }).first();
    if (row?.value) return typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
  } catch (_) {}
  return 'AIP <onboarding@resend.dev>';  // Resend default sandbox sender
}

/**
 * Send an email via Resend.
 * @param {Object} opts - {to: [], cc: [], bcc: [], subject, html, text, from}
 * @returns {Object} {ok, id, error}
 */
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

/**
 * Health check.
 */
async function health(db) {
  const key = await getResendKey(db);
  return { ok: !!key, configured: !!key };
}

module.exports = { sendEmail, health };
module.exports.handler = async function (req, res) {
  const db = getDb();
  if (req.query?.action === 'health') return res.json(await health(db));
  if (req.query?.action === 'test' && req.query?.to) {
    const r = await sendEmail({
      to: req.query.to,
      subject: req.query.subject || 'AIP Resend Test',
      text: req.query.body || 'Resend integration test from AIP.',
      html: `<p>${req.query.body || 'Resend integration test from AIP.'}</p>`
    });
    return res.json(r);
  }
  return res.status(400).json({ error: 'use action=health or action=test&to=...' });
};
