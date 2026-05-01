/**
 * ADVERSARIAL CROSS-CHECK (Phase 60)
 *
 * The existing evidence-cross-checker compares our own enrichment_logs values
 * pairwise. This module goes deeper — it ACTIVELY VALIDATES each contact field
 * (phone / email / address) against independent third-party signals and looks
 * for 2-of-3 disagreement that suggests bad data.
 *
 *   PHONE:   numverify  +  twilio-lookup  +  fcc-carrier
 *            -> consensus carrier+region; area-code/state alignment
 *            -> mismatch / disagreement docks confidence -15
 *
 *   EMAIL:   hunter-verify  +  employer-domain-pattern  +  scraper-domain blacklist
 *            -> invalid OR scraper-domain  ->  -25 confidence
 *
 *   ADDRESS: usps-validate  +  metro-distance-vs-incident (PostGIS)
 *            -> USPS reject OR > 50mi from incident  ->  -10 confidence
 *
 * Endpoints (auth-gated):
 *   GET  ?action=health
 *   POST ?action=validate          body { person_id }
 *   GET  ?action=batch&limit=20    last-updated qualified persons
 *   GET  ?action=stats             7d conflict counts
 *
 * Per AIP CORE_INTENT: emits enqueueCascade after validation.
 * Per AIP enrichment_logs minimal schema: writes a single row with
 *   field_name='adversarial_validation' and the full result inside new_value.
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { enqueueCascade } = require('./_cascade');

let trackApiCall = async () => {};
try { trackApiCall = require('./cost-tracker').trackApiCall || trackApiCall; } catch (_) {}
try { trackApiCall = require('./cost').trackApiCall || trackApiCall; } catch (_) {}

const SECRET_LITERAL = 'ingest-now';
const SCRAPER_DOMAINS_RE = /(thatsthem|fastpeoplesearch|truepeoplesearch|radaris|whitepages|spokeo|beenverified|peoplefinder|usatoday|cnn|nytimes|washingtonpost|reuters|apnews|bbc|cbsnews|abcnews|nbcnews|foxnews|breitbart|huffpost|buzzfeed|vox|theverge|wired|techcrunch)\.com$/i;

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  if (s === SECRET_LITERAL) return true;
  if (process.env.CRON_SECRET && s === process.env.CRON_SECRET) return true;
  return false;
}

function digitsOnly(s) { return String(s || '').replace(/\D+/g, ''); }
function lower(s) { return String(s || '').toLowerCase().trim(); }

async function getCfg(db, key, envName) {
  if (envName && process.env[envName]) return process.env[envName];
  try {
    const row = await db('system_config').where({ key }).first();
    if (row?.value) {
      const v = typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
      return v;
    }
  } catch (_) {}
  return null;
}

let _migrated = false;
async function ensureSchema(db) {
  if (_migrated) return;
  _migrated = true;
}

const AREA_CODE_TO_STATE = {
  '205':'AL','251':'AL','256':'AL','334':'AL','659':'AL','938':'AL',
  '907':'AK',
  '480':'AZ','520':'AZ','602':'AZ','623':'AZ','928':'AZ',
  '479':'AR','501':'AR','870':'AR',
  '209':'CA','213':'CA','310':'CA','323':'CA','408':'CA','415':'CA','424':'CA','510':'CA','530':'CA','559':'CA','562':'CA','619':'CA','626':'CA','628':'CA','650':'CA','657':'CA','661':'CA','669':'CA','707':'CA','714':'CA','747':'CA','760':'CA','805':'CA','818':'CA','820':'CA','831':'CA','840':'CA','858':'CA','909':'CA','916':'CA','925':'CA','949':'CA','951':'CA',
  '303':'CO','719':'CO','720':'CO','970':'CO',
  '203':'CT','475':'CT','860':'CT','959':'CT',
  '302':'DE',
  '202':'DC',
  '239':'FL','305':'FL','321':'FL','352':'FL','386':'FL','407':'FL','561':'FL','727':'FL','754':'FL','772':'FL','786':'FL','813':'FL','850':'FL','863':'FL','904':'FL','941':'FL','954':'FL',
  '229':'GA','404':'GA','470':'GA','478':'GA','678':'GA','706':'GA','762':'GA','770':'GA','912':'GA',
  '808':'HI',
  '208':'ID','986':'ID',
  '217':'IL','224':'IL','309':'IL','312':'IL','331':'IL','447':'IL','464':'IL','618':'IL','630':'IL','708':'IL','730':'IL','773':'IL','779':'IL','815':'IL','847':'IL','872':'IL',
  '219':'IN','260':'IN','317':'IN','463':'IN','574':'IN','765':'IN','812':'IN','930':'IN',
  '319':'IA','515':'IA','563':'IA','641':'IA','712':'IA',
  '316':'KS','620':'KS','785':'KS','913':'KS',
  '270':'KY','364':'KY','502':'KY','606':'KY','859':'KY',
  '225':'LA','318':'LA','337':'LA','504':'LA','985':'LA',
  '207':'ME',
  '240':'MD','301':'MD','410':'MD','443':'MD','667':'MD',
  '339':'MA','351':'MA','413':'MA','508':'MA','617':'MA','774':'MA','781':'MA','857':'MA','978':'MA',
  '231':'MI','248':'MI','269':'MI','313':'MI','517':'MI','586':'MI','616':'MI','679':'MI','734':'MI','810':'MI','906':'MI','947':'MI','989':'MI',
  '218':'MN','320':'MN','507':'MN','612':'MN','651':'MN','763':'MN','952':'MN',
  '228':'MS','601':'MS','662':'MS','769':'MS',
  '314':'MO','417':'MO','573':'MO','636':'MO','660':'MO','816':'MO','975':'MO',
  '406':'MT',
  '308':'NE','402':'NE','531':'NE',
  '702':'NV','725':'NV','775':'NV',
  '603':'NH',
  '201':'NJ','551':'NJ','609':'NJ','640':'NJ','732':'NJ','848':'NJ','856':'NJ','862':'NJ','908':'NJ','973':'NJ',
  '505':'NM','575':'NM',
  '212':'NY','315':'NY','332':'NY','347':'NY','363':'NY','516':'NY','518':'NY','585':'NY','607':'NY','631':'NY','646':'NY','680':'NY','716':'NY','718':'NY','838':'NY','845':'NY','914':'NY','917':'NY','929':'NY','934':'NY',
  '252':'NC','336':'NC','704':'NC','743':'NC','828':'NC','910':'NC','919':'NC','980':'NC','984':'NC',
  '701':'ND',
  '216':'OH','220':'OH','234':'OH','326':'OH','330':'OH','380':'OH','419':'OH','440':'OH','513':'OH','567':'OH','614':'OH','740':'OH','937':'OH',
  '405':'OK','539':'OK','572':'OK','580':'OK','918':'OK',
  '458':'OR','503':'OR','541':'OR','971':'OR',
  '215':'PA','223':'PA','267':'PA','272':'PA','412':'PA','445':'PA','484':'PA','570':'PA','582':'PA','610':'PA','717':'PA','724':'PA','814':'PA','835':'PA','878':'PA',
  '401':'RI',
  '803':'SC','839':'SC','843':'SC','854':'SC','864':'SC',
  '605':'SD',
  '423':'TN','615':'TN','629':'TN','731':'TN','865':'TN','901':'TN','931':'TN',
  '210':'TX','214':'TX','254':'TX','281':'TX','325':'TX','346':'TX','361':'TX','409':'TX','430':'TX','432':'TX','469':'TX','512':'TX','682':'TX','713':'TX','726':'TX','737':'TX','806':'TX','817':'TX','830':'TX','832':'TX','903':'TX','915':'TX','936':'TX','940':'TX','945':'TX','956':'TX','972':'TX','979':'TX',
  '385':'UT','435':'UT','801':'UT',
  '802':'VT',
  '276':'VA','434':'VA','540':'VA','571':'VA','703':'VA','757':'VA','804':'VA','826':'VA','948':'VA',
  '206':'WA','253':'WA','360':'WA','425':'WA','509':'WA','564':'WA',
  '304':'WV','681':'WV',
  '262':'WI','274':'WI','414':'WI','534':'WI','608':'WI','715':'WI','920':'WI',
  '307':'WY'
};

async function runNumVerify(db, phone) {
  const apiKey = await getCfg(db, 'numverify_api_key', 'NUMVERIFY_API_KEY');
  if (!apiKey) return { ok: false, source: 'numverify', error: 'no_key' };
  const ten = digitsOnly(phone).slice(-10);
  if (ten.length !== 10) return { ok: false, source: 'numverify', error: 'invalid_phone' };
  try {
    const r = await fetch(`https://apilayer.net/api/validate?access_key=${apiKey}&number=1${ten}&country_code=US&format=1`,
      { signal: AbortSignal.timeout(8000) });
    await trackApiCall(db, 'adversarial-cross-check', 'numverify', 0, 0, r.ok).catch(() => {});
    if (!r.ok) return { ok: false, source: 'numverify', status: r.status };
    const j = await r.json();
    return {
      ok: true, source: 'numverify',
      valid: j.valid !== false,
      carrier: j.carrier || null,
      line_type: j.line_type || null,
      country: j.country_code || null,
      region: j.location || null
    };
  } catch (e) {
    await trackApiCall(db, 'adversarial-cross-check', 'numverify', 0, 0, false).catch(() => {});
    return { ok: false, source: 'numverify', error: e.message };
  }
}

async function runTwilioLookup(db, phone) {
  try {
    const m = require('../enrich/twilio');
    if (!m.lookupPhone) return { ok: false, source: 'twilio', error: 'unavailable' };
    const ten = digitsOnly(phone).slice(-10);
    if (ten.length !== 10) return { ok: false, source: 'twilio', error: 'invalid_phone' };
    const r = await m.lookupPhone(db, '+1' + ten, { fields: 'line_type_intelligence' });
    if (!r?.ok) return { ok: false, source: 'twilio', error: r?.error || 'lookup_failed' };
    return {
      ok: true, source: 'twilio',
      valid: r.valid !== false,
      carrier: r.carrier_name || null,
      line_type: r.line_type || null,
      country: r.country || null,
      region: null
    };
  } catch (e) {
    return { ok: false, source: 'twilio', error: e.message };
  }
}

async function runFccCarrier(db, phone) {
  try {
    const m = require('../enrich/fcc-carrier');
    if (!m.lookup) return { ok: false, source: 'fcc', error: 'unavailable' };
    const r = await m.lookup(phone, db);
    const fcc = r?.fcc || null;
    const fcl = r?.freecarrierlookup || null;
    const carrier = fcc?.carrier || fcl?.carrier || null;
    const line_type = fcc?.line_type || fcl?.line_type || null;
    return {
      ok: !!(fcc || fcl),
      source: 'fcc',
      valid: !!carrier,
      carrier,
      line_type,
      consensus: r?.consensus || false
    };
  } catch (e) {
    return { ok: false, source: 'fcc', error: e.message };
  }
}

function carrierToken(c) {
  if (!c) return null;
  const t = String(c).toLowerCase().replace(/[^a-z0-9 ]/g, ' ').split(/\s+/).filter(Boolean);
  const noise = new Set(['llc','inc','corp','wireless','communications','telephone','tel','mobile','cellular','co','company','services','group']);
  const sig = t.find(x => !noise.has(x));
  return sig || t[0] || null;
}

function votesAgree(a, b) {
  if (!a || !b) return false;
  return a === b || a.includes(b) || b.includes(a);
}

async function validatePhone(db, phone, expectedState) {
  const result = {
    field: 'phone',
    value: phone,
    valid: null,
    conflicts: [],
    flags: [],
    confidence_delta: 0,
    sources: {}
  };
  if (!phone) return { ...result, skipped: 'no_phone' };

  const [nv, tw, fcc] = await Promise.all([
    runNumVerify(db, phone).catch(e => ({ ok: false, source: 'numverify', error: e.message })),
    runTwilioLookup(db, phone).catch(e => ({ ok: false, source: 'twilio', error: e.message })),
    runFccCarrier(db, phone).catch(e => ({ ok: false, source: 'fcc', error: e.message }))
  ]);
  result.sources.numverify = nv;
  result.sources.twilio = tw;
  result.sources.fcc = fcc;

  const validityVotes = [nv?.valid, tw?.valid, fcc?.valid].filter(v => v != null);
  const trueCount = validityVotes.filter(v => v === true).length;
  const falseCount = validityVotes.filter(v => v === false).length;
  if (validityVotes.length >= 2 && falseCount > trueCount) {
    result.flags.push('majority_invalid');
    result.confidence_delta -= 15;
    result.valid = false;
  } else if (trueCount >= 2) {
    result.valid = true;
  }

  const carriers = [nv?.carrier, tw?.carrier, fcc?.carrier].map(carrierToken).filter(Boolean);
  if (carriers.length >= 2) {
    let pairs = 0, agree = 0;
    for (let i = 0; i < carriers.length; i++)
      for (let j = i + 1; j < carriers.length; j++) {
        pairs++;
        if (votesAgree(carriers[i], carriers[j])) agree++;
      }
    if (pairs > 0 && agree === 0) {
      result.flags.push('carrier_disagreement:' + carriers.join('|'));
      result.confidence_delta -= 15;
      result.conflicts.push('carrier');
    }
  }

  const ten = digitsOnly(phone).slice(-10);
  const npa = ten.length === 10 ? ten.slice(0, 3) : null;
  const acState = npa ? AREA_CODE_TO_STATE[npa] : null;
  if (acState && expectedState && acState !== String(expectedState).toUpperCase()) {
    result.flags.push(`area_code_state_mismatch:${acState}!=${expectedState}`);
    result.confidence_delta -= 15;
    result.conflicts.push('area_code_state');
  }

  return result;
}

async function runHunterVerify(db, email) {
  const apiKey = await getCfg(db, 'hunter_api_key', 'HUNTER_API_KEY');
  if (!apiKey) return { ok: false, source: 'hunter', error: 'no_key' };
  if (!email) return { ok: false, source: 'hunter', error: 'no_email' };
  try {
    const r = await fetch(`https://api.hunter.io/v2/email-verifier?email=${encodeURIComponent(email)}&api_key=${apiKey}`,
      { signal: AbortSignal.timeout(8000) });
    await trackApiCall(db, 'adversarial-cross-check', 'hunter_verify', 0, 0, r.ok).catch(() => {});
    if (!r.ok) return { ok: false, source: 'hunter', status: r.status };
    const j = await r.json();
    const d = j?.data || {};
    return {
      ok: true,
      source: 'hunter',
      status: d.status || d.result || null,
      score: typeof d.score === 'number' ? d.score : null,
      regexp: d.regexp,
      smtp_check: d.smtp_check,
      mx_records: d.mx_records,
      disposable: d.disposable,
      webmail: d.webmail
    };
  } catch (e) {
    await trackApiCall(db, 'adversarial-cross-check', 'hunter_verify', 0, 0, false).catch(() => {});
    return { ok: false, source: 'hunter', error: e.message };
  }
}

function emailDomain(e) {
  const m = String(e || '').toLowerCase().match(/@([^@\s]+)$/);
  return m ? m[1] : null;
}

function looksLikeEmployerDomain(domain, employer) {
  if (!domain || !employer) return null;
  const stem = String(employer).toLowerCase().replace(/[^a-z0-9]+/g, '');
  if (!stem) return null;
  const dStem = String(domain).toLowerCase().replace(/\.(com|net|org|io|co)$/, '').replace(/[^a-z0-9]+/g, '');
  if (!dStem) return null;
  return dStem.includes(stem.slice(0, 6)) || stem.includes(dStem.slice(0, 6));
}

async function validateEmail(db, email, employer) {
  const result = {
    field: 'email',
    value: email,
    valid: null,
    conflicts: [],
    flags: [],
    confidence_delta: 0,
    sources: {}
  };
  if (!email) return { ...result, skipped: 'no_email' };

  const domain = emailDomain(email);
  result.sources.domain = domain;

  if (domain && SCRAPER_DOMAINS_RE.test(domain)) {
    result.flags.push('scraper_domain:' + domain);
    result.confidence_delta -= 25;
    result.conflicts.push('scraper_domain');
    result.valid = false;
  }

  const hunter = await runHunterVerify(db, email).catch(e => ({ ok: false, source: 'hunter', error: e.message }));
  result.sources.hunter = hunter;

  if (hunter?.ok) {
    const status = lower(hunter.status);
    if (status === 'invalid' || status === 'undeliverable') {
      result.flags.push('hunter_invalid');
      result.confidence_delta -= 25;
      result.conflicts.push('hunter_invalid');
      result.valid = false;
    } else if (status === 'catchall' || status === 'accept_all') {
      result.flags.push('hunter_catchall');
      result.confidence_delta -= 5;
    } else if (status === 'valid' || status === 'deliverable') {
      if (result.valid !== false) result.valid = true;
    }
    if (hunter.disposable) {
      result.flags.push('disposable_email');
      result.confidence_delta -= 25;
      result.conflicts.push('disposable_email');
      result.valid = false;
    }
  }

  if (employer && domain) {
    const matchesEmployer = looksLikeEmployerDomain(domain, employer);
    result.sources.employer_match = matchesEmployer;
    if (matchesEmployer === false) {
      result.flags.push('employer_domain_mismatch');
      result.confidence_delta -= 5;
    }
  }

  return result;
}

async function runUspsValidate(db, addr) {
  try {
    const m = require('../enrich/usps-validate');
    if (!m.uspsValidate) return { ok: false, source: 'usps', error: 'unavailable' };
    const v = await m.uspsValidate(addr, db);
    if (!v) return { ok: false, source: 'usps', error: 'rejected' };
    return { ok: true, source: 'usps', ...v };
  } catch (e) {
    return { ok: false, source: 'usps', error: e.message };
  }
}

async function metroDistanceMiles(db, personId, incidentId) {
  if (!incidentId) return null;
  try {
    const r = await db.raw(`
      SELECT ST_DistanceSphere(
        ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326),
        ST_SetSRID(ST_MakePoint(i.longitude, i.latitude), 4326)
      ) / 1609.344 AS miles
      FROM persons p
      JOIN incidents i ON i.id = ?
      WHERE p.id = ?
        AND p.lat IS NOT NULL AND p.lon IS NOT NULL
        AND i.latitude IS NOT NULL AND i.longitude IS NOT NULL
      LIMIT 1
    `, [incidentId, personId]);
    const row = (r.rows || r)[0];
    return row?.miles != null ? Number(row.miles) : null;
  } catch (_) {
    return null;
  }
}

async function validateAddress(db, person, incident) {
  const result = {
    field: 'address',
    value: person?.address || null,
    valid: null,
    conflicts: [],
    flags: [],
    confidence_delta: 0,
    sources: {}
  };
  if (!person?.address) return { ...result, skipped: 'no_address' };

  const addr = {
    street: person.address,
    city: person.city,
    state: person.state,
    zip: person.zip
  };
  const usps = await runUspsValidate(db, addr).catch(e => ({ ok: false, source: 'usps', error: e.message }));
  result.sources.usps = usps;

  if (!usps?.ok) {
    result.flags.push('usps_rejected');
    result.confidence_delta -= 10;
    result.conflicts.push('usps_rejected');
    result.valid = false;
  } else {
    result.valid = true;
  }

  if (incident?.id) {
    const miles = await metroDistanceMiles(db, person.id, incident.id);
    result.sources.metro_distance_miles = miles;
    if (miles != null && miles > 50) {
      result.flags.push(`far_from_incident:${miles.toFixed(1)}mi`);
      result.confidence_delta -= 10;
      result.conflicts.push('far_from_incident');
    }
  }

  return result;
}

async function validateOne(db, personId) {
  await ensureSchema(db);
  if (!personId) return { ok: false, error: 'person_id required' };

  const person = await db('persons').where('id', personId).first().catch(() => null);
  if (!person) return { ok: false, error: 'person_not_found' };

  const incident = person.incident_id
    ? await db('incidents').where('id', person.incident_id).first().catch(() => null)
    : null;

  const [phoneRes, emailRes, addressRes] = await Promise.all([
    person.phone
      ? validatePhone(db, person.phone, person.state || incident?.state).catch(e => ({ field: 'phone', error: e.message, confidence_delta: 0 }))
      : Promise.resolve({ field: 'phone', skipped: 'no_phone', confidence_delta: 0 }),
    person.email
      ? validateEmail(db, person.email, person.employer).catch(e => ({ field: 'email', error: e.message, confidence_delta: 0 }))
      : Promise.resolve({ field: 'email', skipped: 'no_email', confidence_delta: 0 }),
    person.address
      ? validateAddress(db, person, incident).catch(e => ({ field: 'address', error: e.message, confidence_delta: 0 }))
      : Promise.resolve({ field: 'address', skipped: 'no_address', confidence_delta: 0 })
  ]);

  const validations = { phone: phoneRes, email: emailRes, address: addressRes };
  const conflicts_found =
    (phoneRes.conflicts?.length || 0) +
    (emailRes.conflicts?.length || 0) +
    (addressRes.conflicts?.length || 0);
  const confidence_delta =
    (phoneRes.confidence_delta || 0) +
    (emailRes.confidence_delta || 0) +
    (addressRes.confidence_delta || 0);

  if (confidence_delta !== 0) {
    try {
      await db('persons').where('id', personId).update({
        confidence: db.raw('GREATEST(0, LEAST(100, COALESCE(confidence, 50) + ?))', [confidence_delta]),
        updated_at: new Date()
      });
    } catch (_) {}
    try {
      await db('persons').where('id', personId).update({
        identity_confidence: db.raw('GREATEST(0, LEAST(100, COALESCE(identity_confidence, 50) + ?))', [confidence_delta]),
        updated_at: new Date()
      });
    } catch (_) {}
  }

  try {
    await db('enrichment_logs').insert({
      person_id: personId,
      field_name: 'adversarial_validation',
      new_value: JSON.stringify({
        validations,
        conflicts_found,
        confidence_delta,
        source: 'adversarial-cross-check',
        ts: new Date().toISOString()
      }).slice(0, 8000),
      created_at: new Date()
    });
  } catch (e) {
    console.error('[adversarial] log insert failed:', e.message);
  }

  try {
    await enqueueCascade(db, {
      person_id: personId,
      trigger_source: 'adversarial-cross-check',
      trigger_field: 'adversarial_validation',
      trigger_value: conflicts_found ? 'conflicts:' + conflicts_found : 'clean',
      priority: conflicts_found ? 8 : 4
    });
  } catch (_) {}

  await trackApiCall(db, 'adversarial-cross-check', 'validate', 0, 0, true).catch(() => {});

  return {
    ok: true,
    person_id: personId,
    validations,
    conflicts_found,
    confidence_delta
  };
}

async function batch(db, { limit = 20 } = {}) {
  await ensureSchema(db);
  const lim = Math.max(1, Math.min(parseInt(limit) || 20, 100));
  const rows = await db.raw(`
    SELECT DISTINCT p.id
    FROM persons p
    JOIN incidents i ON i.id = p.incident_id
    WHERE i.qualification_state = 'qualified'
      AND (p.phone IS NOT NULL OR p.email IS NOT NULL OR p.address IS NOT NULL)
    ORDER BY p.updated_at DESC NULLS LAST
    LIMIT ${lim}
  `).then(r => r.rows || r).catch(() => []);

  const out = { candidates: rows.length, checked: 0, conflicts_total: 0, samples: [] };
  for (const r of rows) {
    let one;
    try { one = await validateOne(db, r.id); } catch (_) { continue; }
    if (!one?.ok) continue;
    out.checked++;
    out.conflicts_total += one.conflicts_found || 0;
    if (out.samples.length < 10 && one.conflicts_found > 0) {
      out.samples.push({
        person_id: r.id,
        conflicts_found: one.conflicts_found,
        confidence_delta: one.confidence_delta,
        flags: [
          ...(one.validations.phone?.flags || []),
          ...(one.validations.email?.flags || []),
          ...(one.validations.address?.flags || [])
        ]
      });
    }
  }
  return out;
}

async function stats(db) {
  await ensureSchema(db);
  let total = 0, withConflicts = 0;
  try {
    const r = await db.raw(`
      SELECT COUNT(*)::int AS total,
             SUM(CASE WHEN new_value LIKE '%"conflicts_found":0%' THEN 0 ELSE 1 END)::int AS with_conflicts
      FROM enrichment_logs
      WHERE field_name = 'adversarial_validation'
        AND created_at > NOW() - INTERVAL '7 days'
    `);
    const row = (r.rows || r)[0] || {};
    total = row.total || 0;
    withConflicts = row.with_conflicts || 0;
  } catch (_) {}
  return {
    period: '7d',
    total_validations: total,
    with_conflicts: withConflicts,
    clean: Math.max(0, total - withConflicts),
    conflict_rate: total ? +(withConflicts / total).toFixed(3) : 0
  };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const action = String(req.query?.action || 'health').toLowerCase();
  let db;
  try { db = getDb(); } catch (e) {
    return res.status(500).json({ success: false, error: 'db_unavailable:' + e.message });
  }

  if (action === 'health') {
    return res.status(200).json({
      success: true,
      service: 'adversarial-cross-check',
      validators: ['numverify', 'twilio-lookup', 'fcc-carrier', 'hunter-verify', 'usps-validate', 'metro-distance'],
      ts: new Date().toISOString()
    });
  }

  if (action === 'validate') {
    const personId = (req.body && req.body.person_id) || req.query?.person_id;
    if (!personId) return res.status(400).json({ error: 'person_id required' });
    try {
      const out = await validateOne(db, personId);
      return res.status(200).json({ success: true, ...out });
    } catch (e) {
      await reportError(db, 'adversarial-cross-check', null, e.message, { severity: 'error' }).catch(() => {});
      return res.status(500).json({ success: false, error: e.message });
    }
  }

  if (action === 'batch') {
    try {
      const out = await batch(db, { limit: req.query?.limit });
      return res.status(200).json({ success: true, ...out });
    } catch (e) {
      await reportError(db, 'adversarial-cross-check', null, e.message, { severity: 'error' }).catch(() => {});
      return res.status(500).json({ success: false, error: e.message });
    }
  }

  if (action === 'stats') {
    try {
      const out = await stats(db);
      return res.status(200).json({ success: true, ...out });
    } catch (e) {
      return res.status(500).json({ success: false, error: e.message });
    }
  }

  return res.status(400).json({ error: 'unknown action: ' + action });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.validateOne = validateOne;
module.exports.batch = batch;
module.exports.stats = stats;
module.exports.validatePhone = validatePhone;
module.exports.validateEmail = validateEmail;
module.exports.validateAddress = validateAddress;
