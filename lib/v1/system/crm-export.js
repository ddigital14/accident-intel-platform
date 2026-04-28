/**
 * CRM export hook. When a lead becomes qualified, push to configured CRM via webhook.
 * Generic webhook target — works with Salesforce Web-to-Lead, HubSpot Forms, Zapier, n8n, Make.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('./cost');
const { reportError } = require('./_errors');

async function getConfig(db) {
  try {
    const row = await db('system_config').where({ key: 'crm_export' }).first();
    if (row?.value) return typeof row.value === 'string' ? JSON.parse(row.value) : row.value;
  } catch (_) {}
  return null;
}

async function push(db, person, incident) {
  const cfg = await getConfig(db);
  if (!cfg?.webhook_url) return { ok: false, reason: 'no_webhook_configured' };
  const payload = {
    first_name: (person.full_name || '').split(' ')[0] || '',
    last_name: (person.full_name || '').split(' ').slice(-1)[0] || '',
    email: person.email,
    phone: person.phone,
    address: person.location_street_address,
    city: person.location_locality,
    state: person.location_region,
    zip: person.location_postal_code,
    incident_type: incident?.accident_type,
    incident_severity: incident?.severity,
    incident_date: incident?.occurred_at,
    incident_description: incident?.description,
    lead_score: incident?.lead_score,
    identity_confidence: person.identity_confidence,
    aip_person_id: person.id,
    aip_incident_id: incident?.id,
    has_attorney: person.has_attorney,
    policy_limits_max: person.policy_limits_max,
    source: 'AIP'
  };
  let body = null, ok = false;
  try {
    const r = await fetch(cfg.webhook_url, { method: 'POST', headers: { 'Content-Type': 'application/json', ...(cfg.headers || {}) }, body: JSON.stringify(payload), timeout: 12000 });
    ok = r.ok; if (r.ok) { try { body = await r.json(); } catch (_) { body = await r.text(); } }
  } catch (e) { return { ok: false, error: e.message }; }
  await trackApiCall(db, 'system-crm-export', cfg.target || 'webhook', 0, 0, ok).catch(() => {});
  await db('persons').where({ id: person.id }).update({ crm_exported_at: new Date() }).catch(() => {});
  return { ok, response: body };
}

async function batch(db, limit = 20) {
  const rows = await db('persons')
    .leftJoin('incidents', 'incidents.id', 'persons.incident_id')
    .where('persons.qualification_state', 'qualified')
    .whereNotNull('persons.full_name').where('persons.full_name', '!=', '')
    .where(function () { this.whereNull('persons.crm_exported_at'); })
    .select('persons.*', 'incidents.id as incident_pk', 'incidents.accident_type', 'incidents.severity', 'incidents.occurred_at', 'incidents.description', 'incidents.lead_score')
    .limit(limit).catch(() => []);
  let pushed = 0;
  for (const r of rows) {
    const result = await push(db, r, { id: r.incident_pk, accident_type: r.accident_type, severity: r.severity, occurred_at: r.occurred_at, description: r.description, lead_score: r.lead_score });
    if (result.ok) pushed++;
  }
  return { rows: rows.length, pushed };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { action } = req.query || {};
    if (action === 'health') { const cfg = await getConfig(db); return res.json({ ok: true, engine: 'crm-export', configured: !!cfg?.webhook_url, target: cfg?.target }); }
    if (action === 'test' && req.method === 'POST') {
      const body = await new Promise(r => { let d = ''; req.on('data', c => d += c); req.on('end', () => { try { r(JSON.parse(d)); } catch (_) { r({}); } }); });
      const out = await push(db, body.person || {}, body.incident || {}); return res.json(out);
    }
    if (action === 'batch') { const out = await batch(db, parseInt(req.query.limit) || 20); return res.json({ success: true, ...out }); }
    return res.status(400).json({ error: 'unknown action' });
  } catch (err) { await reportError(db, 'crm-export', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.push = push;
module.exports.batch = batch;
