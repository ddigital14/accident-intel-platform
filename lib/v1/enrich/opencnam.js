/**
 * Phase 68 #1: OpenCNAM free reverse-phone (caller-ID) lookup.
 * Returns the name people store the number under — useful for spam/business detection.
 * Free tier: anonymous endpoint, ~120 req/hour.
 */
const { getDb } = require('../../_db');
const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function lookupPhone(phone) {
  if (!phone) return { ok: false, error: 'no_phone' };
  const digits = String(phone).replace(/\D+/g, '');
  if (digits.length < 10) return { ok: false, error: 'invalid_phone' };
  const e164 = digits.length === 10 ? '+1' + digits : '+' + digits;
  // OpenCNAM free endpoint
  const url = `https://api.opencnam.com/v3/phone/${encodeURIComponent(e164)}`;
  try {
    const r = await fetch(url, { signal: AbortSignal.timeout(8000) });
    const text = await r.text();
    if (!r.ok) return { ok: false, status: r.status, raw: text.slice(0, 200) };
    return { ok: true, phone: e164, name: text.trim(), source: 'opencnam' };
  } catch (e) { return { ok: false, error: e.message }; }
}

async function lookupOne(db, personId) {
  const p = await db('persons').where('id', personId).first();
  if (!p?.phone) return { ok: true, skipped: 'no_phone' };
  const r = await lookupPhone(p.phone);
  if (r.ok && r.name) {
    try {
      await db('enrichment_logs').insert({
        person_id: personId,
        field_name: 'opencnam_caller_id',
        old_value: null,
        new_value: JSON.stringify({ name: r.name, phone: r.phone, source: 'opencnam' }).slice(0, 4000),
        created_at: new Date()
      });
    } catch (_) {}
  }
  return r;
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();
  if (action === 'health') return res.json({ success: true, service: 'opencnam' });
  if (action === 'lookup') {
    const phone = req.query?.phone;
    if (!phone) return res.status(400).json({ error: 'phone required' });
    return res.json(await lookupPhone(phone));
  }
  if (action === 'lookup_person') {
    const pid = req.query?.person_id;
    if (!pid) return res.status(400).json({ error: 'person_id required' });
    return res.json(await lookupOne(db, pid));
  }
  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.lookupPhone = lookupPhone;
module.exports.lookupOne = lookupOne;
