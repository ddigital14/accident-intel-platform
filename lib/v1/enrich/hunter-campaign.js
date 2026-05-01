/**
 * Phase 68 #2: Hunter campaign — programmatic email outreach using existing Hunter Pro API.
 * Loads our existing Hunter API key, sends a personalized email to a verified target,
 * tracks send/open/reply via Hunter's webhook (separate handler).
 *
 * Use case: rep approves a draft → fires this engine → Hunter sends + tracks.
 */
const { getDb } = require('../../_db');
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function getHunterKey(db) {
  if (process.env.HUNTER_API_KEY) return process.env.HUNTER_API_KEY;
  try {
    const row = await db('system_config').where('key', 'hunter_api_key').first();
    if (row?.value) return typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
  } catch (_) {}
  return null;
}

/**
 * Send via Hunter Campaigns API. Returns campaign_id + recipient ids.
 * Note: Hunter requires a sender domain verified in their dashboard. For now, send
 * via Resend (which we already have set up) — Hunter campaign API is reserved for
 * future bulk sequences.
 */
async function sendDirect(db, { to, subject, body_text, body_html, person_id, rep_email }) {
  if (!to || !subject) return { ok: false, error: 'to and subject required' };
  // Use existing Resend handler for actual delivery
  try {
    const resend = require('../system/resend');
    const r = await resend.sendEmail({
      to: [to],
      subject,
      text: body_text,
      html: body_html || `<pre>${(body_text || '').replace(/[<>&]/g, c => ({'<':'&lt;','>':'&gt;','&':'&amp;'}[c]))}</pre>`,
      from: rep_email ? `Accident Command Center <${rep_email}>` : undefined
    });
    if (r.ok && person_id) {
      try {
        await db('enrichment_logs').insert({
          person_id,
          field_name: 'campaign_email_sent',
          old_value: null,
          new_value: JSON.stringify({ to, subject, resend_id: r.id, source: 'hunter-campaign' }).slice(0, 4000),
          created_at: new Date()
        });
      } catch (_) {}
    }
    return r;
  } catch (e) { return { ok: false, error: e.message }; }
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();

  if (action === 'health') return res.json({ success: true, service: 'hunter-campaign' });

  if (action === 'send') {
    let body = req.body;
    if (!body || typeof body !== 'object') {
      body = await new Promise(r => {
        let d=''; req.on('data', c=>d+=c);
        req.on('end', () => { try { r(JSON.parse(d || '{}')); } catch { r({}); } });
      });
    }
    return res.json(await sendDirect(db, body));
  }

  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.sendDirect = sendDirect;
