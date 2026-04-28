/**
 * Trestle Reverse Address auto-activation probe.
 *
 * GET /api/v1/system/trestle-probe?secret=ingest-now
 *
 * Pings Trestle Reverse Address with a benign known address. If the response
 * is 200, the account has been granted access — flip system_config so all
 * downstream callers immediately use it.
 *
 * If the response is 403 (still pending), record retry_after.
 *
 * When access first flips ON, queue a backfill on persons that have address
 * but no full_name (premises liability use case).
 *
 * Wired into the existing 6h cron dispatch slot — no new vercel.json cron.
 */
const { getDb } = require('../../_db');
const { reverseAddress } = require('../enrich/trestle');
const { reportError } = require('./_errors');
const { logChange } = require('./changelog');
const { enqueueCascade } = require('./_cascade');

// Benign probe address — a public well-known address. Used only for the access check.
// We pick a county courthouse so even if Trestle returns owner data, it's a public-record entity.
const PROBE = { street: '100 N Calvert St', city: 'Baltimore', state: 'MD', postal_code: '21202' };

async function getAccessStatus(db) {
  try {
    const row = await db('system_config').where('key', 'trestle_access').first();
    if (!row) return { reverse_address: 'unknown' };
    return typeof row.value === 'string' ? JSON.parse(row.value) : (row.value || {});
  } catch (_) { return { reverse_address: 'unknown' }; }
}

async function setAccessStatus(db, status) {
  try {
    const existing = await db('system_config').where('key', 'trestle_access').first();
    const value = JSON.stringify(status);
    if (existing) {
      await db('system_config').where('key', 'trestle_access').update({ value, updated_at: new Date() });
    } else {
      await db('system_config').insert({ key: 'trestle_access', value, created_at: new Date(), updated_at: new Date() });
    }
  } catch (_) {}
}

async function probeReverseAddress(db) {
  const r = await reverseAddress(PROBE, db);
  if (r && !r.error) return { ok: true, status: 200 };
  if (r?.status === 403) return { ok: false, status: 403, message: r.error };
  if (r?.status === 401) return { ok: false, status: 401, message: 'auth issue (check TRESTLE_API_KEY)' };
  return { ok: false, status: r?.status || 0, message: r?.error || 'unknown' };
}

async function backfillAddressOnly(db, limit = 25) {
  // Persons with address known but no full_name — premises-liability use case
  let candidates = [];
  try {
    candidates = await db('persons')
      .whereNotNull('address').where('address', '!=', '')
      .where(function () { this.whereNull('full_name').orWhere('full_name', ''); })
      .orderBy('updated_at', 'desc')
      .limit(limit);
  } catch (_) {}
  let enriched = 0;
  for (const p of candidates) {
    try {
      const parts = (p.address || '').split(',').map(s => s.trim());
      const street = parts[0]; const city = p.city || parts[1]; const state = p.state || (parts[2] || '').slice(0, 2);
      if (!street || !city || !state) continue;
      const r = await reverseAddress({ street, city, state, postal_code: p.zip || null }, db);
      if (r && !r.error && Array.isArray(r.current_residents) && r.current_residents.length) {
        const owner = r.current_residents[0];
        const updates = {};
        if (owner.full_name && !p.full_name) updates.full_name = owner.full_name;
        if (owner.firstname && !p.first_name) updates.first_name = owner.firstname;
        if (owner.lastname && !p.last_name) updates.last_name = owner.lastname;
        if (Array.isArray(owner.phones) && owner.phones[0] && !p.phone) updates.phone = owner.phones[0].phone_number || owner.phones[0];
        if (Object.keys(updates).length) {
          updates.updated_at = new Date();
          try { await db('persons').where('id', p.id).update(updates); } catch (_) {}
          await enqueueCascade(db, p.id, 'trestle_reverse_address').catch(() => {});
          enriched++;
        }
      }
    } catch (_) {}
  }
  return { candidates: candidates.length, enriched };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  try {
    const before = await getAccessStatus(db);
    const probe = await probeReverseAddress(db);
    const newStatus = {
      ...before,
      reverse_address: probe.ok ? 'enabled' : 'pending',
      reverse_address_last_probed: new Date().toISOString(),
      reverse_address_last_status: probe.status,
      reverse_address_last_message: probe.message || null
    };
    await setAccessStatus(db, newStatus);

    let backfill = null;
    const justEnabled = before.reverse_address !== 'enabled' && probe.ok;
    if (justEnabled) {
      // Mark in changelog so future Claude sessions / dashboards see this
      try {
        await logChange(db, {
          kind: 'config',
          title: 'Trestle Reverse Address — access activated',
          description: 'Probe returned 200; reverse_address auto-enabled. Backfilling address-only persons.',
          metadata: { last_probed: newStatus.reverse_address_last_probed }
        });
      } catch (_) {}
      backfill = await backfillAddressOnly(db, parseInt(req.query.backfill_limit) || 25);
    }

    return res.status(200).json({
      success: true,
      reverse_address: newStatus.reverse_address,
      probe,
      newly_enabled: justEnabled,
      backfill,
      previous_status: before.reverse_address || 'unknown',
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    await reportError(db, 'trestle-probe', null, e.message).catch(() => {});
    return res.status(500).json({ error: e.message });
  }
};
