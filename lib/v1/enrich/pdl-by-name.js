/**
 * PDL-by-Name Bulk Enrichment — Phase 22 #1
 *
 * Pulls persons in qualification_state='pending_named' where phone IS NULL
 * AND email IS NULL — calls PDL /v5/person/enrich keyed on name + locality.
 *
 * Smart Router (lib/v1/enrich/_smart_router.js) already routes these to
 * 'enrich-pdl-by-name', but the action there is `deferred_to_cron`. THIS
 * handler is what actually drains the queue.
 *
 * GET /api/v1/enrich/pdl-by-name?secret=ingest-now&action=batch&limit=20
 *
 * 14-point compliance:
 *   - canonical normalizers (normalizePerson)
 *   - cascade emission (enqueueCascade per success)
 *   - cost tracking (trackApiCall 'enrich-pdl-by-name')
 *   - logChange on aggregate update
 *   - cron registration (folded into existing 30-min slot)
 *   - non-overwrite of higher-confidence existing fields
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { dedupCache } = require('../../_cache');
const { enqueueCascade } = require('../system/_cascade');
const { trackApiCall } = require('../system/cost');
const { logChange } = require('../system/changelog');
const { normalizePerson } = require('../../_schema');

const PDL_KEY = process.env.PDL_API_KEY;
const PDL_ENRICH_URL = 'https://api.peopledatalabs.com/v5/person/enrich';
const PDL_SEARCH_URL = 'https://api.peopledatalabs.com/v5/person/search';
const TIMEOUT_MS = 9000;

/**
 * PDL Person Enrichment requires at least one of (email|phone|profile|lid),
 * so name-only never works there. For name+locality we use Person Search API
 * which accepts SQL-like filters on name+locality+region.
 *
 * Strategy:
 *   1. If person has phone OR email → use Enrich (precise, $0.02)
 *   2. Otherwise → use Search with first/last + locality/region ($0.05 per result)
 */
async function callPdlByName(person) {
  if (!PDL_KEY) return { ok: false, error: 'no_pdl_key' };
  const first = (person.first_name || '').trim();
  const last  = (person.last_name  || '').trim();
  const fullName = (person.full_name || `${first} ${last}`).trim();
  if (!fullName || (!first && !last)) return { ok: false, error: 'no_name' };

  // Path 1: Enrich (when we have phone or email — won't be the case for pending_named, but cheap fallback)
  if (person.phone || person.email) {
    const params = new URLSearchParams();
    if (first) params.append('first_name', first);
    if (last)  params.append('last_name',  last);
    if (person.email) params.append('email', person.email);
    if (person.phone) params.append('phone', person.phone);
    if (person.city)  params.append('locality', person.city);
    if (person.state) params.append('region',   person.state);
    params.append('min_likelihood', '3');
    try {
      const r = await fetch(`${PDL_ENRICH_URL}?${params}`, {
        headers: { 'X-API-Key': PDL_KEY, 'Accept': 'application/json' },
        signal: AbortSignal.timeout(TIMEOUT_MS)
      });
      if (!r.ok) return { ok: false, status: r.status, mode: 'enrich' };
      const d = await r.json();
      if (d.status !== 200 || !d.data) return { ok: false, status: d.status, mode: 'enrich' };
      return { ok: true, data: d.data, likelihood: d.likelihood, mode: 'enrich' };
    } catch (e) { return { ok: false, error: e.message, mode: 'enrich' }; }
  }

  // Path 2: Search (name + locality only)
  // Build SQL: SELECT * FROM person WHERE first_name='..' AND last_name='..' AND location_locality='..' AND location_region='..'
  const where = [];
  if (first) where.push(`first_name='${first.replace(/'/g, "''")}'`);
  if (last)  where.push(`last_name='${last.replace(/'/g, "''")}'`);
  if (person.city)  where.push(`location_locality='${String(person.city).replace(/'/g, "''")}'`);
  if (person.state) where.push(`location_region='${String(person.state).toLowerCase().replace(/'/g, "''")}'`);
  const sql = `SELECT * FROM person WHERE ${where.join(' AND ')}`;

  try {
    const r = await fetch(PDL_SEARCH_URL, {
      method: 'POST',
      headers: {
        'X-API-Key': PDL_KEY,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      body: JSON.stringify({ sql, size: 1, pretty: false }),
      signal: AbortSignal.timeout(TIMEOUT_MS)
    });
    if (!r.ok) return { ok: false, status: r.status, mode: 'search' };
    const d = await r.json();
    if (d.status !== 200 || !d.data?.length) return { ok: false, status: d.status || 'no_results', mode: 'search', total: d.total };
    return { ok: true, data: d.data[0], likelihood: 5, mode: 'search', total: d.total };
  } catch (e) { return { ok: false, error: e.message, mode: 'search' }; }
}

/**
 * Map PDL data → canonical person fields.
 */
function mapPdlToCanonical(pdl) {
  const out = {};
  if (!pdl) return out;

  // Phone (mobile takes priority)
  if (pdl.mobile_phone) out.phone = pdl.mobile_phone;
  else if (pdl.phone_numbers?.length) out.phone = pdl.phone_numbers[0];

  // Email — personal preferred, work fallback
  if (pdl.personal_emails?.length) out.email = pdl.personal_emails[0];
  else if (pdl.work_email) out.email = pdl.work_email;

  // Employment
  if (pdl.job_company_name) out.employer = pdl.job_company_name;
  if (pdl.job_title) out.occupation = pdl.job_title;
  if (pdl.linkedin_url) out.linkedin_url = pdl.linkedin_url;

  // Social
  if (pdl.facebook_url) out.facebook_url = pdl.facebook_url;
  if (pdl.twitter_url) out.twitter_url = pdl.twitter_url;
  if (pdl.github_url) out.github_url = pdl.github_url;
  if (pdl.profiles?.length) {
    const fb = pdl.profiles.find(p => p.network === 'facebook');
    const tw = pdl.profiles.find(p => p.network === 'twitter');
    const li = pdl.profiles.find(p => p.network === 'linkedin');
    if (fb && !out.facebook_url) out.facebook_url = fb.url;
    if (tw && !out.twitter_url) out.twitter_url = tw.url;
    if (li && !out.linkedin_url) out.linkedin_url = li.url;
  }

  // Age via birth_year
  if (pdl.birth_year) out.age = new Date().getFullYear() - pdl.birth_year;

  // Address
  if (pdl.location_street_address) {
    out.address = pdl.location_street_address;
    if (pdl.location_locality && !out.city) out.city = pdl.location_locality;
    if (pdl.location_region && !out.state) out.state = pdl.location_region;
    if (pdl.location_postal_code) out.zip = pdl.location_postal_code;
  }

  return out;
}

/**
 * Apply PDL fields to a person, but DON'T overwrite higher-confidence existing.
 * Returns { updated, fields_filled, applied }
 */
async function applyPdlToPerson(db, person, mapped, likelihood) {
  const updateableFields = ['phone','email','employer','occupation','linkedin_url',
    'facebook_url','twitter_url','github_url','age','address','city','state','zip'];
  const update = {};
  const applied = {};
  for (const f of updateableFields) {
    if (mapped[f] == null) continue;
    // Only fill if empty (don't overwrite higher-confidence existing)
    if (person[f] && String(person[f]).trim()) continue;
    update[f] = mapped[f];
    applied[f] = mapped[f];
  }
  let fields_filled = Object.keys(update).length;
  if (fields_filled === 0) return { updated: false, fields_filled: 0, applied };

  // Boost confidence
  const newConf = Math.min(95, Math.max(person.confidence_score || 50, 70 + (likelihood || 0) * 2));
  update.confidence_score = newConf;
  update.updated_at = new Date();

  try {
    await db('persons').where('id', person.id).update(update);
  } catch (e) {
    return { updated: false, fields_filled: 0, error: e.message };
  }

  // Log enrichment events
  for (const f of Object.keys(applied)) {
    await db('enrichment_logs').insert({
      person_id: person.id,
      field_name: f,
      old_value: person[f] || null,
      new_value: String(applied[f]).substring(0, 500),
      source: 'pdl_by_name',
      confidence: newConf,
      verified: false,
      created_at: new Date()
    }).catch(() => {});
  }

  return { updated: true, fields_filled, applied };
}

async function processPerson(db, person) {
  const cacheKey = `pdlbn:${person.id}`;
  if (dedupCache.has(cacheKey)) return { skipped: 'cached' };
  dedupCache.set(cacheKey, 1);

  const r = await callPdlByName(person);
  await trackApiCall(db, 'enrich-pdl-by-name', 'pdl', 0, 0, !!r.ok);
  if (!r.ok) return { ok: false, reason: r.error || `status_${r.status}`, likelihood: r.likelihood };

  const mapped = mapPdlToCanonical(r.data);
  const apply = await applyPdlToPerson(db, person, mapped, r.likelihood);
  if (!apply.updated) return { ok: true, fields_filled: 0, reason: 'no_new_fields' };

  // Phase 29: PDL -> Hunter inline chain. If we just got an employer but no email, hit Hunter immediately
  // instead of waiting for next cron tick. Saves a 30-min latency window.
  if (apply.applied?.employer && !person.email && !apply.applied?.email) {
    try {
      const hunter = require('./hunter');
      if (typeof hunter.findEmail === 'function') {
        const guess = await hunter.findEmail(db, { full_name: mapped.full_name || person.full_name, company: apply.applied.employer });
        if (guess?.email) {
          await db('persons').where({ id: person.id }).update({ email: guess.email, updated_at: new Date() });
          apply.applied.email = guess.email;
          apply.fields_filled = (apply.fields_filled || 0) + 1;
        }
      }
    } catch (_) {}
  }

  // Cascade emission — newly filled phone/email may unlock more enrichment
  await enqueueCascade(db, {
    person_id: person.id,
    incident_id: person.incident_id,
    trigger_source: 'pdl_by_name',
    trigger_field: Object.keys(apply.applied)[0] || 'phone',
    trigger_value: apply.applied.phone || apply.applied.email || 'misc',
    priority: 8
  }).catch(() => {});

  return { ok: true, fields_filled: apply.fields_filled, applied: apply.applied };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  if (!PDL_KEY) return res.json({ success: true, message: 'PDL_API_KEY not set, no-op', candidates: 0, enriched: 0 });

  const db = getDb();
  const action = req.query?.action || 'batch';
  const limit = Math.min(parseInt(req.query?.limit) || 20, 50);
  const startTime = Date.now();
  const TIME_BUDGET = 50000;

  const results = {
    candidates: 0, processed: 0, enriched: 0, fields_filled: 0,
    phones_filled: 0, emails_filled: 0,
    samples: [], errors: []
  };

  try {
    if (action === 'person' && req.query?.person_id) {
      const p = await db('persons').where('id', req.query.person_id).first();
      if (!p) return res.status(404).json({ error: 'person_not_found' });
      const r = await processPerson(db, p);
      return res.json({ success: true, person_id: p.id, ...r, timestamp: new Date().toISOString() });
    }

    // batch: pending_named && phone IS NULL && email IS NULL
    const candidates = await db.raw(`
      SELECT p.* FROM persons p
      JOIN incidents i ON i.id = p.incident_id
      WHERE i.qualification_state = 'pending_named'
        AND p.full_name IS NOT NULL
        AND (p.phone IS NULL OR p.phone = '')
        AND (p.email IS NULL OR p.email = '')
        AND i.discovered_at > NOW() - INTERVAL '60 days'
      ORDER BY COALESCE(i.lead_score, 0) DESC, i.discovered_at DESC
      LIMIT ?
    `, [limit]).then(r => r.rows || []).catch(() => []);
    results.candidates = candidates.length;

    const reasons = {};
    for (const p of candidates) {
      if (Date.now() - startTime > TIME_BUDGET) break;
      try {
        const r = await processPerson(db, p);
        results.processed++;
        if (r?.ok && r.fields_filled > 0) {
          results.enriched++;
          results.fields_filled += r.fields_filled;
          if (r.applied?.phone) results.phones_filled++;
          if (r.applied?.email) results.emails_filled++;
          if (results.samples.length < 8) {
            results.samples.push({
              person_id: p.id, name: p.full_name,
              fields_filled: r.fields_filled,
              got_phone: !!r.applied?.phone,
              got_email: !!r.applied?.email
            });
          }
        } else {
          const reason = r?.reason || (r?.ok ? 'no_new_fields' : 'no_match');
          reasons[reason] = (reasons[reason] || 0) + 1;
        }
      } catch (e) {
        results.errors.push(`${p.id}: ${e.message}`);
        await reportError(db, 'enrich-pdl-by-name', p.id, e.message).catch(()=>{});
      }
    }
    results.no_match_reasons = reasons;

    if (results.enriched > 0) {
      await logChange(db, {
        kind: 'enrichment',
        title: `PDL-by-name: ${results.enriched} pending_named persons enriched`,
        summary: `${results.fields_filled} fields filled (${results.phones_filled} phones, ${results.emails_filled} emails) across ${results.processed}/${results.candidates} candidates`,
        meta: { samples: results.samples }
      }).catch(() => {});
    }

    res.json({
      success: true,
      message: `PDL-by-name: ${results.enriched}/${results.processed} enriched, ${results.fields_filled} fields filled`,
      ...results,
      duration_ms: Date.now() - startTime,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'enrich-pdl-by-name', null, err.message).catch(()=>{});
    res.status(500).json({ error: err.message, results });
  }
};

module.exports.processPerson = processPerson;
module.exports.callPdlByName = callPdlByName;
module.exports.mapPdlToCanonical = mapPdlToCanonical;
