/**
 * Phase 50b: Resend domain setup for accidentcommandcenter.com.
 *
 * Endpoints (all require ?secret=ingest-now):
 *   action=add        -> POST /domains, returns DNS records to paste in Vercel
 *   action=status     -> GET /domains/<id>, polls verification state
 *   action=verify     -> POST /domains/<id>/verify, force re-check
 *   action=list       -> GET /domains
 *   action=health     -> key/config status
 *
 * On a successful verify, system_config.resend_from_email is updated to
 * 'Accident Command Center <alerts@accidentcommandcenter.com>'.
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { trackApiCall } = require('./cost');
const { bumpCounter } = require('./_cei_telemetry');

const ENGINE = 'resend-domain-setup';
const TARGET_DOMAIN = 'accidentcommandcenter.com';
const PRODUCTION_FROM = 'Accident Command Center <alerts@accidentcommandcenter.com>';

async function getResendKey(db) {
  if (process.env.RESEND_API_KEY) return process.env.RESEND_API_KEY;
  try {
    const row = await db('system_config').where({ key: 'resend_api_key' }).first();
    if (row?.value) return typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
  } catch (_) {}
  return null;
}

async function getStoredDomainId(db) {
  try {
    const row = await db('system_config').where({ key: 'resend_domain_id' }).first();
    if (row?.value) return typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
  } catch (_) {}
  return null;
}

async function setStored(db, key, value) {
  try {
    await db('system_config')
      .insert({ key, value: String(value), updated_at: new Date() })
      .onConflict('key').merge({ value: String(value), updated_at: new Date() });
  } catch (_) {}
}

async function resendApi(key, method, path, body) {
  const t0 = Date.now();
  const headers = {
    'Authorization': `Bearer ${key}`,
    'Content-Type': 'application/json'
  };
  const opts = { method, headers, signal: AbortSignal.timeout(15000) };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(`https://api.resend.com${path}`, opts);
  const text = await r.text();
  let data = null;
  try { data = JSON.parse(text); } catch (_) { data = { raw: text }; }
  return { ok: r.ok, status: r.status, data, latency_ms: Date.now() - t0 };
}

function shapeRecords(records) {
  if (!Array.isArray(records)) return [];
  return records.map(r => ({
    type: r.type || r.record || '?',
    name: r.name || r.host || '@',
    value: r.value || r.data || r.content || '',
    priority: r.priority || null,
    ttl: r.ttl || 'auto',
    purpose: r.purpose || (
      /spf|v=spf1/i.test(JSON.stringify(r)) ? 'SPF' :
      /dmarc/i.test(JSON.stringify(r)) ? 'DMARC' :
      /dkim|domainkey/i.test(JSON.stringify(r)) ? 'DKIM' : 'unknown'
    )
  }));
}

async function actionAdd(db, key) {
  const existingId = await getStoredDomainId(db);
  if (existingId) {
    const r = await resendApi(key, 'GET', `/domains/${existingId}`);
    if (r.ok) {
      return {
        ok: true,
        domain_id: existingId,
        domain: r.data?.name,
        status: r.data?.status,
        region: r.data?.region,
        dns_records: shapeRecords(r.data?.records),
        note: 'Domain already registered with Resend. Add these DNS records at Vercel DNS, then call action=verify.',
        raw: r.data
      };
    }
  }

  const r = await resendApi(key, 'POST', '/domains', { name: TARGET_DOMAIN, region: 'us-east-1' });
  if (!r.ok) return { ok: false, status: r.status, error: r.data?.message || `http_${r.status}`, raw: r.data };
  if (r.data?.id) await setStored(db, 'resend_domain_id', r.data.id);
  return {
    ok: true,
    domain_id: r.data?.id,
    domain: r.data?.name,
    status: r.data?.status,
    region: r.data?.region,
    dns_records: shapeRecords(r.data?.records),
    note: 'Add the DNS records below at Vercel -> Domains -> accidentcommandcenter.com -> DNS, then call action=verify.',
    raw: r.data
  };
}

async function actionStatus(db, key) {
  const id = await getStoredDomainId(db);
  if (!id) return { ok: false, error: 'no_stored_domain_id_call_action_add_first' };
  const r = await resendApi(key, 'GET', `/domains/${id}`);
  if (!r.ok) return { ok: false, status: r.status, error: r.data?.message || `http_${r.status}`, raw: r.data };
  const status = r.data?.status;
  if (status === 'verified') {
    await setStored(db, 'resend_from_email', PRODUCTION_FROM);
  }
  return {
    ok: true,
    domain_id: id,
    domain: r.data?.name,
    status,
    verified: status === 'verified',
    region: r.data?.region,
    dns_records: shapeRecords(r.data?.records),
    from_email: status === 'verified' ? PRODUCTION_FROM : null,
    raw: r.data
  };
}

async function actionVerify(db, key) {
  const id = await getStoredDomainId(db);
  if (!id) return { ok: false, error: 'no_stored_domain_id_call_action_add_first' };
  const r = await resendApi(key, 'POST', `/domains/${id}/verify`, {});
  const post = await resendApi(key, 'GET', `/domains/${id}`);
  const status = post.data?.status;
  if (status === 'verified') await setStored(db, 'resend_from_email', PRODUCTION_FROM);
  return {
    ok: r.ok,
    domain_id: id,
    triggered: r.status,
    status,
    verified: status === 'verified',
    from_email: status === 'verified' ? PRODUCTION_FROM : null,
    dns_records: shapeRecords(post.data?.records),
    raw: { trigger: r.data, current: post.data }
  };
}

async function actionList(db, key) {
  const r = await resendApi(key, 'GET', '/domains');
  if (!r.ok) return { ok: false, status: r.status, error: r.data?.message || `http_${r.status}`, raw: r.data };
  return { ok: true, domains: r.data?.data || r.data, raw: r.data };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const action = req.query?.action || 'health';
  const t0 = Date.now();

  try {
    const key = await getResendKey(db);
    if (action === 'health') {
      const storedId = await getStoredDomainId(db);
      return res.json({
        success: true,
        engine: ENGINE,
        target_domain: TARGET_DOMAIN,
        production_from: PRODUCTION_FROM,
        resend_key_configured: !!key,
        domain_id_stored: !!storedId,
        timestamp: new Date().toISOString()
      });
    }

    if (!key) return res.status(400).json({ error: 'no_resend_api_key (set system_config.resend_api_key or RESEND_API_KEY env)' });

    let out;
    if (action === 'add')          out = await actionAdd(db, key);
    else if (action === 'status')  out = await actionStatus(db, key);
    else if (action === 'verify')  out = await actionVerify(db, key);
    else if (action === 'list')    out = await actionList(db, key);
    else return res.status(400).json({ error: 'unknown action', allowed: ['health', 'add', 'status', 'verify', 'list'] });

    const latency = Date.now() - t0;
    await trackApiCall(db, ENGINE, `resend:${action}`, !!out.ok, latency).catch(() => {});
    await bumpCounter(db, ENGINE, !!out.ok, latency).catch(() => {});

    return res.json({ success: !!out.ok, action, ...out, latency_ms: latency });
  } catch (err) {
    await reportError(db, ENGINE, null, err.message).catch(() => {});
    return res.status(500).json({ error: err.message });
  }
};

module.exports.TARGET_DOMAIN = TARGET_DOMAIN;
module.exports.PRODUCTION_FROM = PRODUCTION_FROM;
