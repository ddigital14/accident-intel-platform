/**
 * Free phone-carrier lookup — FCC numlookup + FreeCarrierLookup.com scrape.
 * Two free votes on carrier replaces NumVerify free-tier ceiling.
 * +5 cross-exam confidence at $0 marginal cost when both agree.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

function clean10(p) { return String(p || '').replace(/\D/g, '').slice(-10); }

async function fccLookup(phone, db) {
  const ten = clean10(phone); if (ten.length !== 10) return null;
  const url = `https://opendata.fcc.gov/resource/n4w7-tncj.json?npa=${ten.slice(0, 3)}&nxx=${ten.slice(3, 6)}&$limit=1`;
  let body = null, ok = false;
  try { const r = await fetch(url, { timeout: 8000 }); if (r.ok) { body = await r.json(); ok = true; } } catch (_) {}
  await trackApiCall(db, 'enrich-fcc-carrier', 'fcc', 0, 0, ok).catch(() => {});
  if (!body || !body[0]) return null;
  const row = body[0];
  return { source: 'fcc', carrier: row.ocn_name || row.lata_name || null, line_type: row.svc_type || null, raw: row };
}

async function freeCarrierLookup(phone, db) {
  const ten = clean10(phone); if (ten.length !== 10) return null;
  const url = `https://freecarrierlookup.com/getcarrier.php?phonenum=${ten}&cc=US`;
  let body = null, ok = false;
  try { const r = await fetch(url, { timeout: 8000, headers: { 'X-Requested-With': 'XMLHttpRequest' } }); if (r.ok) { body = await r.json(); ok = true; } } catch (_) {}
  await trackApiCall(db, 'enrich-fcc-carrier', 'freecarrierlookup', 0, 0, ok).catch(() => {});
  if (!body) return null;
  return { source: 'freecarrierlookup', carrier: body.Carrier, line_type: body.Type, raw: body };
}

async function lookup(phone, db) {
  const [fcc, fcl] = await Promise.all([fccLookup(phone, db), freeCarrierLookup(phone, db)]);
  const consensus = fcc && fcl && fcc.carrier && fcl.carrier &&
    fcc.carrier.toLowerCase().includes((fcl.carrier || '').toLowerCase().split(' ')[0]);
  return { fcc, freecarrierlookup: fcl, consensus, weight: consensus ? 75 : 50 };
}

async function batch(db, limit = 25) {
  let rows = []; try {
    rows = await db('persons').whereNotNull('phone').where('phone', '!=', '')
      .where(function () { this.whereNull('carrier').orWhere('carrier', ''); })
      .limit(limit);
  } catch (_) {}
  let updated = 0;
  for (const p of rows) {
    const r = await lookup(p.phone, db);
    const carrier = r.fcc?.carrier || r.freecarrierlookup?.carrier;
    const line_type = r.fcc?.line_type || r.freecarrierlookup?.line_type;
    if (carrier) {
      try { await db('persons').where({ id: p.id }).update({ carrier, line_type, updated_at: new Date() }); updated++; await enqueueCascade(db, 'person', p.id, 'fcc-carrier', { weight: r.weight }); } catch (_) {}
    }
  }
  return { rows: rows.length, updated };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { phone, action } = req.query || {};
    if (action === 'health') return res.json({ ok: true, engine: 'fcc-carrier', tiers: ['fcc', 'freecarrierlookup'], cost: 0, weight: 75 });
    if (action === 'batch') { const out = await batch(db, parseInt(req.query.limit) || 25); return res.json({ success: true, ...out }); }
    if (phone) { const r = await lookup(phone, db); return res.json({ success: true, ...r }); }
    return res.status(400).json({ error: 'need phone or action=batch|health' });
  } catch (err) { await reportError(db, 'fcc-carrier', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.lookup = lookup;
