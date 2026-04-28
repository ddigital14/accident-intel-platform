/**
 * MCP plugin export hooks — Apollo, Common Room, GoHighLevel.
 * Generic webhook target with per-plugin payload shaping.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('./cost');
const { reportError } = require('./_errors');

async function getCfg(db, plugin) {
  try {
    const row = await db('system_config').where({ key: `plugin_${plugin}` }).first();
    if (row?.value) return typeof row.value === 'string' ? JSON.parse(row.value) : row.value;
  } catch (_) {}
  return null;
}

function shapeForApollo(person, incident) {
  return {
    first_name: (person.full_name || '').split(' ')[0],
    last_name: (person.full_name || '').split(' ').slice(-1)[0],
    email: person.email,
    title: 'Accident victim - PI lead',
    phone: person.phone,
    address: person.location_street_address,
    city: person.location_locality,
    state: person.location_region,
    zip: person.location_postal_code,
    organization_name: 'AIP - ' + (incident?.accident_type || 'unknown'),
    custom_fields: {
      lead_score: incident?.lead_score,
      severity: incident?.severity,
      occurred_at: incident?.occurred_at,
      identity_confidence: person.identity_confidence,
      predicted_value: incident?.predicted_value_likely
    }
  };
}

function shapeForCommonRoom(person, incident) {
  return {
    fullName: person.full_name,
    email: person.email,
    phone: person.phone,
    location: { city: person.location_locality, state: person.location_region },
    signals: [
      { name: 'aip_lead', value: 'true' },
      { name: 'severity', value: incident?.severity },
      { name: 'lead_score', value: String(incident?.lead_score || 0) },
      { name: 'predicted_value', value: String(incident?.predicted_value_likely || 0) }
    ]
  };
}

function shapeForGHL(person, incident) {
  return {
    firstName: (person.full_name || '').split(' ')[0],
    lastName: (person.full_name || '').split(' ').slice(-1)[0],
    email: person.email,
    phone: person.phone,
    address1: person.location_street_address,
    city: person.location_locality,
    state: person.location_region,
    postalCode: person.location_postal_code,
    tags: ['AIP', incident?.severity || 'unknown', incident?.accident_type || 'unknown'].filter(Boolean),
    customFields: [
      { key: 'aip_lead_score', value: String(incident?.lead_score || 0) },
      { key: 'aip_predicted_value', value: String(incident?.predicted_value_likely || 0) },
      { key: 'aip_incident_id', value: String(incident?.id || '') }
    ]
  };
}

async function push(db, plugin, person, incident) {
  const cfg = await getCfg(db, plugin);
  if (!cfg?.webhook_url && !cfg?.api_key) return { ok: false, error: 'plugin_not_configured', plugin };
  const shaper = { apollo: shapeForApollo, common_room: shapeForCommonRoom, ghl: shapeForGHL }[plugin];
  if (!shaper) return { ok: false, error: 'unknown_plugin', plugin };
  const payload = shaper(person, incident);
  const url = cfg.webhook_url || ({
    apollo: 'https://api.apollo.io/v1/contacts',
    common_room: 'https://api.commonroom.io/community/v1/members',
    ghl: cfg.location_id ? `https://services.leadconnectorhq.com/contacts/?locationId=${cfg.location_id}` : 'https://rest.gohighlevel.com/v1/contacts/'
  }[plugin]);
  const headers = { 'Content-Type': 'application/json', ...(cfg.headers || {}) };
  if (cfg.api_key) headers.Authorization = plugin === 'ghl' ? `Bearer ${cfg.api_key}` : (plugin === 'apollo' ? `Cache-Control: no-cache` : `Bearer ${cfg.api_key}`);
  if (plugin === 'apollo' && cfg.api_key) headers['X-Api-Key'] = cfg.api_key;
  let body = null, ok = false;
  try {
    const r = await fetch(url, { method: 'POST', headers, body: JSON.stringify(payload), timeout: 12000 });
    ok = r.ok; body = await (r.json().catch(() => r.text()));
  } catch (e) { return { ok: false, error: e.message, plugin }; }
  await trackApiCall(db, 'system-plugin-export', plugin, 0, 0, ok).catch(() => {});
  return { ok, plugin, response: typeof body === 'object' ? body : { text: body } };
}

async function batch(db, plugin, limit = 15) {
  const rows = await db('persons')
    .leftJoin('incidents', 'incidents.id', 'persons.incident_id')
    .where('persons.qualification_state', 'qualified')
    .whereNotNull('persons.full_name').where('persons.full_name', '!=', '')
    .where(function () { this.whereNull(`persons.${plugin}_exported_at`); })
    .select('persons.*', 'incidents.id as incident_pk', 'incidents.accident_type', 'incidents.severity', 'incidents.occurred_at', 'incidents.description', 'incidents.lead_score', 'incidents.predicted_value_likely')
    .limit(limit).catch(() => []);
  let pushed = 0;
  for (const r of rows) {
    const result = await push(db, plugin, r, { id: r.incident_pk, accident_type: r.accident_type, severity: r.severity, occurred_at: r.occurred_at, description: r.description, lead_score: r.lead_score, predicted_value_likely: r.predicted_value_likely });
    if (result.ok) {
      pushed++;
      try { await db('persons').where({ id: r.id }).update({ [`${plugin}_exported_at`]: new Date() }); } catch (_) {}
    }
  }
  return { plugin, rows: rows.length, pushed };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { action, plugin } = req.query || {};
    if (action === 'health') {
      const status = {};
      for (const p of ['apollo', 'common_room', 'ghl']) { const c = await getCfg(db, p); status[p] = !!(c?.webhook_url || c?.api_key); }
      return res.json({ ok: true, engine: 'plugin-export', plugins: status });
    }
    if (action === 'batch' && plugin) { const out = await batch(db, plugin, parseInt(req.query.limit) || 15); return res.json({ success: true, ...out }); }
    return res.status(400).json({ error: 'need plugin + action=batch|health' });
  } catch (err) { await reportError(db, 'plugin-export', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.push = push;
module.exports.batch = batch;
