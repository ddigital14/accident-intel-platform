/**
 * Event-driven aggressive enrichment
 *
 * Pulls recent persons that have a name but missing contact info, and fires
 * EVERY enrichment API in parallel for each one. Much faster than waiting for
 * the every-15-min /enrich/run cron.
 *
 * Idempotent — safe to run frequently. Skips persons already enriched in last 6h.
 *
 * GET /api/v1/enrich/trigger?secret=ingest-now
 * Cron: every 5 minutes
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const { reportError } = require('../system/_errors');
const { dedupCache } = require('../../_cache');
const { enqueueCascade } = require('../system/_cascade');
const { enrichPersonViaTrestle, isConfigured: trestleConfigured } = require('./trestle');
const { deepEnrichPerson } = require('./deep');

const PDL_KEY      = process.env.PDL_API_KEY;
const HUNTER_KEY   = process.env.HUNTER_API_KEY;
const NUMVERIFY    = process.env.NUMVERIFY_API_KEY;
const TRACERFY     = process.env.TRACERFY_API_KEY;
const SEARCHBUG    = process.env.SEARCHBUG_API_KEY;
const SEARCHBUG_CO = process.env.SEARCHBUG_CO_CODE;

async function tryTrestle(p, db) {
  try {
    if (!(await trestleConfigured(db))) return null;
    return await enrichPersonViaTrestle(p, db);
  } catch (_) { return null; }
}

async function tryPDL(p) {
  if (!PDL_KEY) return null;
  try {
    const params = new URLSearchParams();
    if (p.first_name) params.append('first_name', p.first_name);
    if (p.last_name)  params.append('last_name', p.last_name);
    if (p.email)      params.append('email', p.email);
    if (p.phone)      params.append('phone', p.phone);
    if (p.state)      params.append('region', p.state);
    if (p.city)       params.append('locality', p.city);
    params.append('min_likelihood', '3');
    const r = await fetch(`https://api.peopledatalabs.com/v5/person/enrich?${params}`, {
      headers: { 'X-API-Key': PDL_KEY }, signal: AbortSignal.timeout(8000)
    });
    if (!r.ok) return null;
    const d = await r.json();
    if (d.status !== 200 || !d.data) return null;
    return {
      source: 'pdl',
      confidence: Math.round((d.likelihood || 0.5) * 100),
      fields: {
        employer: d.data.job_company_name || null,
        occupation: d.data.job_title || null,
        linkedin_url: d.data.linkedin_url || null,
        email: !p.email && d.data.work_email ? d.data.work_email : null,
        phone: !p.phone && d.data.mobile_phone ? d.data.mobile_phone : null,
        address: !p.address && d.data.street_address ? d.data.street_address : null,
        zip: d.data.postal_code || null
      }
    };
  } catch (_) { return null; }
}

async function tryTracerfy(p) {
  if (!TRACERFY || !p.first_name || !p.last_name) return null;
  try {
    const r = await fetch(`https://api.tracerfy.com/v1/person/search`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${TRACERFY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        first_name: p.first_name, last_name: p.last_name,
        state: p.state, city: p.city
      }),
      signal: AbortSignal.timeout(10000)
    });
    if (!r.ok) return null;
    const d = await r.json();
    const m = d.results?.[0];
    if (!m) return null;
    return {
      source: 'tracerfy',
      confidence: 80,
      fields: {
        phone: !p.phone && m.phones?.[0] ? m.phones[0] : null,
        email: !p.email && m.emails?.[0] ? m.emails[0] : null,
        address: !p.address && m.address ? m.address : null,
        relatives: m.relatives || []
      }
    };
  } catch (_) { return null; }
}

async function trySearchBug(p) {
  if (!SEARCHBUG || !SEARCHBUG_CO) return null;
  try {
    if (p.phone) {
      const r = await fetch(`https://api.searchbug.com/api.aspx?TYPE=peoplesearch&CO_CODE=${SEARCHBUG_CO}&PASS=${SEARCHBUG}&PHONE=${encodeURIComponent(p.phone)}&FORMAT=json`, {
        signal: AbortSignal.timeout(8000)
      });
      if (!r.ok) return null;
      const d = await r.json();
      if (d.records?.[0]) {
        const m = d.records[0];
        return {
          source: 'searchbug',
          confidence: 75,
          fields: {
            address: !p.address && m.address ? m.address : null,
            full_name: !p.full_name && m.name ? m.name : null,
            email: !p.email && m.email ? m.email : null
          }
        };
      }
    }
    return null;
  } catch (_) { return null; }
}

async function tryNumVerify(p) {
  if (!NUMVERIFY || !p.phone) return null;
  try {
    const cleaned = p.phone.replace(/\D/g, '');
    const r = await fetch(`http://apilayer.net/api/validate?access_key=${NUMVERIFY}&number=${cleaned}&country_code=US&format=1`, {
      signal: AbortSignal.timeout(8000)
    });
    if (!r.ok) return null;
    const d = await r.json();
    return {
      source: 'numverify',
      confidence: d.valid ? 90 : 30,
      fields: {
        phone_verified: !!d.valid,
        phone_carrier: d.carrier || null,
        phone_line_type: d.line_type || null
      }
    };
  } catch (_) { return null; }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const results = { candidates: 0, enriched: 0, fields_filled: 0, sources_used: {}, errors: [] };
  const startTime = Date.now();

  try {
    const force = req.query?.force === 'true';
    // Persons with name but missing phone, email, or address
    // AND not enriched in last 6 hours (or never), unless force=true
    let q = db('persons')
      .whereNotNull('full_name')
      .where(function() {
        this.whereNull('phone').orWhereNull('email').orWhereNull('address');
      })
      .where('created_at', '>', new Date(Date.now() - 30 * 86400000));
    if (!force) {
      q = q.where(function() {
        this.where('updated_at', '<', new Date(Date.now() - 6 * 3600000))
            .orWhereNull('updated_at');
      });
    }
    const candidates = await q.select('*').limit(force ? 100 : 20);

    results.candidates = candidates.length;

    for (const p of candidates) {
      if (Date.now() - startTime > 50000) break;
      try {
        const cacheKey = `enrich:${p.id}`;
        if (dedupCache.has(cacheKey)) continue;
        dedupCache.set(cacheKey, 1);

        // Use deep multi-step chain if ?deep=true (default for force=true), otherwise fast parallel fan-out
        const useDeep = req.query?.deep === 'true' || force;
        let merged_fields = {};
        let sourcesUsed = [];
        if (useDeep) {
          const deepResult = await deepEnrichPerson(p, db);
          if (deepResult?.ok) {
            merged_fields = deepResult.merged_fields;
            sourcesUsed = deepResult.sources_used;
            // Also save alt_phones to phone_secondary if room
            if (deepResult.alt_phones?.[0] && !p.phone_secondary) {
              merged_fields.phone_secondary = deepResult.alt_phones[0];
            }
          }
        } else {
          // Original fast parallel mode
          const [trestle, pdl, tracerfy, sb, nv] = await Promise.all([
            tryTrestle(p, db), tryPDL(p), tryTracerfy(p), trySearchBug(p), tryNumVerify(p)
          ]);
          const merged = {};
          for (const r of [trestle, pdl, tracerfy, sb, nv].filter(Boolean)) {
            sourcesUsed.push(r.source);
            for (const [k, v] of Object.entries(r.fields || {})) {
              if (v !== null && v !== undefined && (!merged[k] || r.confidence > (merged[k+'_conf'] || 0))) {
                merged[k] = v;
                merged[k+'_conf'] = r.confidence;
              }
            }
          }
          merged_fields = merged;

        }
        if (sourcesUsed.length === 0) continue;
        for (const s of sourcesUsed) results.sources_used[s] = (results.sources_used[s] || 0) + 1;

        // Build update — only fill empty fields
        const update = { updated_at: new Date() };
        let fieldsFilled = 0;
        const updateableFields = ['phone','email','address','city','state','zip','employer','occupation','linkedin_url','age','phone_verified','phone_carrier','phone_line_type','phone_secondary','full_name','first_name','last_name'];
        for (const f of updateableFields) {
          if (merged_fields[f] && !p[f]) {
            update[f] = merged_fields[f];
            fieldsFilled++;
          }
        }

        if (fieldsFilled > 0) {
          await db('persons').where('id', p.id).update(update);
          // Trigger cascade — newly enriched fields may unlock further cross-examination
          await enqueueCascade(db, { person_id: p.id, incident_id: p.incident_id, trigger_source: 'enrich_trigger' }).catch(()=>{});
          results.enriched++;
          results.fields_filled += fieldsFilled;

          // Log enrichment events
          for (const f of updateableFields) {
            if (update[f]) {
              await db('enrichment_logs').insert({
                person_id: p.id,
                field_name: f,
                old_value: p[f] || null,
                new_value: String(update[f]).substring(0, 500),
                source: sourcesUsed.join('+'),
                confidence: merged_fields[f+'_conf'] || 50,
                verified: f === 'phone_verified' && update[f],
                created_at: new Date()
              }).catch(() => {});
            }
          }
        }
      } catch (e) {
        results.errors.push(`${p.id}: ${e.message}`);
        await reportError(db, 'enrich-trigger', p.id, e.message);
      }
    }

    res.json({
      success: true,
      message: `Trigger enrich: ${results.candidates} candidates, ${results.enriched} enriched, ${results.fields_filled} fields filled`,
      ...results,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    await reportError(db, 'enrich-trigger', null, err.message);
    res.status(500).json({ error: err.message, results });
  }
};
