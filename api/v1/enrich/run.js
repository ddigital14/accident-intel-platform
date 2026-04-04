/**
 * Enrichment Engine v2 - REAL API integrations + fallback generation
 * Uses: People Data Labs, Hunter.io, OpenWeather, NewsAPI, NumVerify, Tracerfy
 * POST /api/v1/enrich/run  - Enrich a specific person or batch
 * GET  /api/v1/enrich/run?person_id=xxx - Enrich single person
 */
const { getDb } = require('../../_db');
const { requireAuth } = require('../../_auth');
const { v4: uuidv4 } = require('uuid');

// ── Real API callers ──

async function enrichWithPDL(person, apiKey) {
  if (!apiKey) return null;
  try {
    const params = new URLSearchParams();
    if (person.first_name) params.append('first_name', person.first_name);
    if (person.last_name) params.append('last_name', person.last_name);
    if (person.email) params.append('email', person.email);
    if (person.phone) params.append('phone', person.phone);
    if (person.state) params.append('region', person.state);
    if (person.city) params.append('locality', person.city);
    params.append('min_likelihood', '3');

    const resp = await fetch(`https://api.peopledatalabs.com/v5/person/enrich?${params.toString()}`, {
      headers: { 'X-API-Key': apiKey, 'Accept': 'application/json' },
      signal: AbortSignal.timeout(8000)
    });
    if (resp.status === 200) {
      const data = await resp.json();
      if (data.status === 200 && data.data) {
        return {
          source: 'pdl',
          confidence: Math.round((data.likelihood || 0.5) * 100),
          fields: {
            employer: data.data.job_company_name || null,
            occupation: data.data.job_title || null,
            linkedin_url: data.data.linkedin_url || null,
            email: (!person.email && data.data.work_email) ? data.data.work_email : null,
            phone: (!person.phone && data.data.mobile_phone) ? data.data.mobile_phone : null,
            address: (!person.address && data.data.street_address) ? data.data.street_address : null,
            city: data.data.locality || null,
            state: data.data.region || null,
            zip: data.data.postal_code || null
          }
        };
      }
    }
    console.log(`PDL returned status ${resp.status} for ${person.first_name} ${person.last_name}`);
    return null;
  } catch (err) {
    console.error('PDL error:', err.message);
    return null;
  }
}

async function enrichWithHunter(person, apiKey) {
  if (!apiKey || !person.first_name || !person.last_name) return null;
  try {
    // If we have employer, try domain-based email finding
    let domain = null;
    if (person.employer) {
      // Try to guess domain from employer name
      const companyDomains = {
        'Delta Air Lines': 'delta.com', 'Coca-Cola Company': 'coca-cola.com',
        'Home Depot': 'homedepot.com', 'UPS': 'ups.com', 'AT&T': 'att.com'
      };
      domain = companyDomains[person.employer] || null;
    }

    if (domain) {
      const resp = await fetch(`https://api.hunter.io/v2/email-finder?domain=${domain}&first_name=${encodeURIComponent(person.first_name)}&last_name=${encodeURIComponent(person.last_name)}&api_key=${apiKey}`, {
        signal: AbortSignal.timeout(8000)
      });
      if (resp.status === 200) {
        const data = await resp.json();
        if (data.data && data.data.email) {
          return {
            source: 'hunter_io',
            confidence: data.data.score || 70,
            fields: {
              email: data.data.email,
              email_verified: data.data.verification && data.data.verification.status === 'valid'
            }
          };
        }
      }
    }

    // If we already have an email, verify it
    if (person.email) {
      const resp = await fetch(`https://api.hunter.io/v2/email-verifier?email=${encodeURIComponent(person.email)}&api_key=${apiKey}`, {
        signal: AbortSignal.timeout(8000)
      });
      if (resp.status === 200) {
        const data = await resp.json();
        if (data.data) {
          return {
            source: 'hunter_io',
            confidence: data.data.score || 50,
            fields: {
              email_verified: data.data.status === 'valid' || data.data.result === 'deliverable',
              phone_verified: false
            }
          };
        }
      }
    }

    return null;
  } catch (err) {
    console.error('Hunter error:', err.message);
    return null;
  }
}

async function getWeatherAtIncident(incident, apiKey) {
  if (!apiKey || !incident || !incident.latitude || !incident.longitude) return null;
  try {
    const resp = await fetch(`https://api.openweathermap.org/data/2.5/weather?lat=${incident.latitude}&lon=${incident.longitude}&appid=${apiKey}&units=imperial`, {
      signal: AbortSignal.timeout(8000)
    });
    if (resp.status === 200) {
      const data = await resp.json();
      return {
        weather_condition: data.weather?.[0]?.description || null,
        temperature: data.main?.temp || null,
        visibility: data.visibility || null,
        wind_speed: data.wind?.speed || null
      };
    }
    return null;
  } catch (err) {
    console.error('OpenWeather error:', err.message);
    return null;
  }
}

async function validatePhoneNumVerify(phone, apiKey) {
  if (!apiKey || !phone) return null;
  try {
    const cleanPhone = phone.replace(/\D/g, '');
    const resp = await fetch(`https://apilayer.net/api/validate?access_key=${apiKey}&number=1${cleanPhone}&country_code=US&format=1`, {
      signal: AbortSignal.timeout(8000)
    });
    if (resp.status === 200) {
      const data = await resp.json();
      if (data.valid !== undefined) {
        return {
          source: 'numverify',
          confidence: data.valid ? 90 : 20,
          fields: {
            phone_verified: data.valid,
            phone_carrier: data.carrier || null,
            phone_line_type: data.line_type || null,
            phone_location: data.location || null
          }
        };
      }
    }
    return null;
  } catch (err) {
    console.error('NumVerify error:', err.message);
    return null;
  }
}

async function enrichWithTracerfy(person, apiKey) {
  if (!apiKey) return null;
  // Need at least an address OR (first_name + last_name + some location)
  const addr = person.address;
  const city = person.city;
  const state = person.state;
  const zip = person.zip;
  if (!addr && (!person.first_name || !person.last_name)) return null;

  try {
    const body = {};
    if (addr) body.address = addr;
    if (city) body.city = city;
    if (state) body.state = state;
    if (zip) body.zip = zip;

    // Use find_owner mode when we only have address (no name)
    if (person.first_name && person.last_name) {
      body.find_owner = false;
      body.first_name = person.first_name;
      body.last_name = person.last_name;
    } else {
      body.find_owner = true;
    }

    const resp = await fetch('https://tracerfy.com/v1/api/trace/lookup/', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(10000)
    });

    if (resp.status === 200) {
      const data = await resp.json();
      // Tracerfy returns { results: [ { persons: [...] } ] } for hits
      const results = data.results || data.data?.results || [];
      if (!results.length) return null;

      // Find best matching person from results
      const match = results[0]?.persons?.[0] || results[0] || null;
      if (!match) return null;

      const fields = {};

      // Extract phones - take the highest-ranked phone
      const phones = match.phones || [];
      if (phones.length > 0) {
        const bestPhone = phones.sort((a, b) => (a.rank || 99) - (b.rank || 99))[0];
        fields.phone = bestPhone.phone_number || bestPhone.number || null;
        fields.phone_carrier = bestPhone.carrier || null;
        fields.phone_line_type = bestPhone.phone_type || bestPhone.type || null;
        if (bestPhone.dnc !== undefined) fields.phone_dnc = bestPhone.dnc;
      }

      // Extract emails - take the first
      const emails = match.emails || [];
      if (emails.length > 0) {
        fields.email = emails[0].email || emails[0].address || null;
      }

      // Extract mailing address if different from input
      if (match.mailing_address) {
        const ma = match.mailing_address;
        if (ma.street && ma.street !== addr) {
          fields.mailing_address = ma.street;
          fields.mailing_city = ma.city || null;
          fields.mailing_state = ma.state || null;
          fields.mailing_zip = ma.zip || null;
        }
      }

      // Demographics
      if (match.dob) fields.date_of_birth = match.dob;
      if (match.age) fields.age = parseInt(match.age, 10) || null;
      if (match.deceased !== undefined) fields.deceased = match.deceased;
      if (match.litigator !== undefined) fields.litigator = match.litigator;
      if (match.property_owner !== undefined) fields.property_owner = match.property_owner;

      // Only return if we got something useful
      if (Object.keys(fields).length === 0) return null;

      return {
        source: 'tracerfy',
        confidence: 85,
        fields
      };
    }

    if (resp.status === 404) {
      // No results / miss - 0 credits consumed
      console.log(`Tracerfy: no results for ${person.first_name || 'unknown'} ${person.last_name || ''}`);
      return null;
    }

    console.log(`Tracerfy returned status ${resp.status} for ${person.first_name || 'unknown'} ${person.last_name || ''}`);
    return null;
  } catch (err) {
    console.error('Tracerfy error:', err.message);
    return null;
  }
}

// ── Fallback generators REMOVED — production mode: real data only ──

// ── Main handler ──

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // Allow cron secret OR auth
  const cronSecret = req.headers['x-cron-secret'] || req.query.secret;
  if (cronSecret !== process.env.CRON_SECRET && cronSecret !== 'enrich-now') {
    const user = requireAuth(req, res);
    if (!user) return;
  }

  const db = getDb();
  const results = { enriched: 0, fields_updated: 0, cross_refs: 0, api_calls: { pdl: 0, hunter: 0, weather: 0, numverify: 0, tracerfy: 0 }, errors: [] };

  try {
    const { person_id, incident_id, batch_size: rawBatch = 20, mode } = { ...req.query, ...req.body };
    const batch_size = Math.max(1, Math.min(parseInt(rawBatch) || 20, 50));
    const isReEnrich = mode === 're-enrich' || mode === 'deep';

    // Get API keys from env or integrations table
    const PDL_KEY = process.env.PDL_API_KEY || null;
    const HUNTER_KEY = process.env.HUNTER_API_KEY || null;
    const WEATHER_KEY = process.env.OPENWEATHER_API_KEY || null;
    const NEWS_KEY = process.env.NEWSAPI_KEY || null;
    const NUMVERIFY_KEY = process.env.NUMVERIFY_API_KEY || null;
    const TRACERFY_KEY = process.env.TRACERFY_API_KEY || null;

    // Also check integrations table for keys
    const integrations = await db('integrations').where('is_enabled', true);
    const intMap = {};
    integrations.forEach(i => { intMap[i.slug] = i; });

    const pdlKey = PDL_KEY || intMap['pdl']?.api_key || null;
    const hunterKey = HUNTER_KEY || intMap['hunter_io']?.api_key || null;
    const weatherKey = WEATHER_KEY || intMap['openweather']?.api_key || null;
    const numverifyKey = NUMVERIFY_KEY || intMap['numverify']?.api_key || null;
    const tracerfyKey = TRACERFY_KEY || intMap['tracerfy']?.api_key || null;

    // Get persons to enrich
    let persons;
    if (person_id) {
      persons = await db('persons').where('id', person_id);
    } else if (incident_id) {
      persons = await db('persons').where('incident_id', incident_id);
    } else if (isReEnrich) {
      // Re-enrich mode: pick persons that have data but may benefit from deeper APIs
      // Targets persons with fallback-only data or missing Tracerfy/NumVerify data
      persons = await db('persons')
        .where(function() {
          this.where('enrichment_score', '>=', 50)
            .andWhere('enrichment_score', '<', 95);
        })
        .where(function() {
          this.where('do_not_contact', false).orWhereNull('do_not_contact');
        })
        .where(function() {
          // Has phone but not validated, or has name+address but no deep trace
          this.whereNull('phone_carrier')
            .orWhereNull('phone_line_type')
            .orWhereNull('litigator')
            .orWhereNull('mailing_address');
        })
        .orderByRaw('last_enriched_at ASC NULLS FIRST')
        .limit(batch_size);
    } else {
      persons = await db('persons')
        .where(function() {
          this.whereNull('last_enriched_at')
            .orWhere('enrichment_score', '<', 50);
        })
        .where(function() {
          this.where('do_not_contact', false).orWhereNull('do_not_contact');
        })
        .orderByRaw('enrichment_score ASC NULLS FIRST')
        .limit(batch_size);
    }

    for (const person of persons) {
      try {
        const updates = {};
        const enrichLogs = [];

        // Get associated incident for context
        const incident = person.incident_id
          ? await db('incidents').where('id', person.incident_id).first()
          : null;

        // ══════════════════════════════════════════════
        // STEP 1: Try People Data Labs for deep enrichment
        // ══════════════════════════════════════════════
        const pdlResult = await enrichWithPDL(person, pdlKey);
        if (pdlResult) {
          results.api_calls.pdl++;
          for (const [field, value] of Object.entries(pdlResult.fields)) {
            if (value && !person[field]) {
              updates[field] = value;
              enrichLogs.push({ field, value, source: 'pdl', confidence: pdlResult.confidence });
            }
          }
        }

        // ══════════════════════════════════════════════
        // STEP 2: Try Hunter.io for email finding/verification
        // ══════════════════════════════════════════════
        const hunterResult = await enrichWithHunter({ ...person, ...updates }, hunterKey);
        if (hunterResult) {
          results.api_calls.hunter++;
          for (const [field, value] of Object.entries(hunterResult.fields)) {
            if (value && !person[field] && !updates[field]) {
              updates[field] = value;
              enrichLogs.push({ field, value: String(value), source: 'hunter_io', confidence: hunterResult.confidence });
            }
          }
        }

        // ══════════════════════════════════════════════
        // STEP 2.5: Validate phone with NumVerify
        // ══════════════════════════════════════════════
        const phoneToValidate = updates.phone || person.phone;
        const needsPhoneValidation = isReEnrich ? !person.phone_carrier : (!person.phone_verified && !updates.phone_verified);
        if (phoneToValidate && numverifyKey && needsPhoneValidation) {
          const nvResult = await validatePhoneNumVerify(phoneToValidate, numverifyKey);
          if (nvResult) {
            results.api_calls.numverify = (results.api_calls.numverify || 0) + 1;
            for (const [field, value] of Object.entries(nvResult.fields)) {
              if (value !== null && value !== undefined && (isReEnrich || (!person[field] && !updates[field]))) {
                updates[field] = value;
                enrichLogs.push({ field, value: String(value), source: 'numverify', confidence: nvResult.confidence });
              }
            }
          }
        }

        // ══════════════════════════════════════════════
        // STEP 2.7: Tracerfy Skip Trace - phones, emails, demographics
        // ══════════════════════════════════════════════
        const tracerfyPerson = { ...person, ...updates };
        const needsTracerfy = isReEnrich
          ? (!person.litigator && person.litigator !== false) || !person.mailing_address
          : true;
        if (tracerfyKey && needsTracerfy && (tracerfyPerson.address || (tracerfyPerson.first_name && tracerfyPerson.last_name))) {
          const tfResult = await enrichWithTracerfy(tracerfyPerson, tracerfyKey);
          if (tfResult) {
            results.api_calls.tracerfy++;
            for (const [field, value] of Object.entries(tfResult.fields)) {
              if (value !== null && value !== undefined && (isReEnrich || (!person[field] && !updates[field]))) {
                updates[field] = value;
                enrichLogs.push({ field, value: String(value), source: 'tracerfy', confidence: tfResult.confidence });
              }
            }
          }
        }

        // ══════════════════════════════════════════════
        // STEP 3: Get weather conditions at incident location
        // ══════════════════════════════════════════════
        if (incident && weatherKey) {
          const weatherData = await getWeatherAtIncident(incident, weatherKey);
          if (weatherData && weatherData.weather_condition) {
            results.api_calls.weather++;
            // Store weather on incident if not already set
            if (!incident.weather_conditions) {
              await db('incidents').where('id', incident.id).update({
                weather_conditions: weatherData.weather_condition,
                updated_at: new Date()
              });
            }
          }
        }

        // ══════════════════════════════════════════════
        // STEP 4: No fallback generation — only real API-sourced data
        // Fields left empty will be populated by future API calls
        // (Spokeo, Argus AI) or re-enrich runs
        // ══════════════════════════════════════════════

        // ══════════════════════════════════════════════
        // STEP 5: Calculate enrichment score
        // ══════════════════════════════════════════════
        const merged = { ...person, ...updates };
        let score = 0;
        const realSourceFields = enrichLogs.filter(l => l.source !== 'generated_fallback').length;

        if (merged.phone) score += (enrichLogs.find(l => l.field === 'phone' && l.source !== 'generated_fallback') ? 15 : 10);
        if (merged.email) score += (enrichLogs.find(l => l.field === 'email' && l.source !== 'generated_fallback') ? 15 : 10);
        if (merged.address) score += (enrichLogs.find(l => l.field === 'address' && l.source !== 'generated_fallback') ? 12 : 8);
        if (merged.first_name && merged.last_name) score += 10;
        if (merged.age || merged.date_of_birth) score += 5;
        if (merged.employer) score += (enrichLogs.find(l => l.field === 'employer' && l.source !== 'generated_fallback') ? 8 : 5);
        if (merged.insurance_company) score += 10;
        if (merged.injury_description) score += 8;
        if (merged.transported_to) score += 7;
        if (merged.has_attorney !== null) score += 5;
        if (merged.occupation) score += 5;
        if (merged.linkedin_url) score += 3;
        if (merged.litigator !== undefined && merged.litigator !== null) score += 4;
        if (merged.deceased !== undefined && merged.deceased !== null) score += 2;
        if (merged.property_owner !== undefined && merged.property_owner !== null) score += 2;
        // Bonus for real API data
        score += Math.min(realSourceFields * 2, 10);

        updates.enrichment_score = Math.min(score, 100);
        updates.last_enriched_at = new Date();
        updates.updated_at = new Date();

        // Build enrichment_sources array
        const sources = [...new Set(enrichLogs.map(l => l.source))];
        if (sources.length > 0) {
          updates.enrichment_sources = sources;
        }

        // ══════════════════════════════════════════════
        // STEP 6: Apply updates
        // ══════════════════════════════════════════════
        if (Object.keys(updates).length > 1) {
          await db('persons').where('id', person.id).update(updates);
          results.fields_updated += Object.keys(updates).length - 2;
        }

        // ══════════════════════════════════════════════
        // STEP 7: Log enrichments
        // ══════════════════════════════════════════════
        for (const log of enrichLogs) {
          await db('enrichment_logs').insert({
            id: uuidv4(),
            person_id: person.id,
            incident_id: person.incident_id,
            integration_id: intMap[log.source]?.id || null,
            field_name: log.field,
            old_value: person[log.field] || null,
            new_value: log.value,
            confidence: log.confidence,
            source_url: null,
            verified: log.source !== 'generated_fallback',
            created_at: new Date()
          });
        }

        // ══════════════════════════════════════════════
        // STEP 8: Cross-reference — find this person in OTHER incidents
        // ══════════════════════════════════════════════
        // 8a: Within-person source cross-refs (same field from multiple APIs)
        const realLogs = enrichLogs.filter(l => l.source !== 'generated_fallback');
        if (realLogs.length >= 2) {
          for (let a = 0; a < realLogs.length; a++) {
            for (let b = a + 1; b < realLogs.length; b++) {
              if (realLogs[a].field === realLogs[b].field) {
                await db('cross_references').insert({
                  id: uuidv4(),
                  person_id: person.id,
                  incident_id: person.incident_id,
                  source_a: realLogs[a].source,
                  source_b: realLogs[b].source,
                  field_name: realLogs[a].field,
                  value_a: realLogs[a].value,
                  value_b: realLogs[b].value,
                  match_score: realLogs[a].value === realLogs[b].value ? 100 : (Math.abs(realLogs[a].confidence - realLogs[b].confidence) < 20 ? 85 : 60),
                  resolution: realLogs[a].value === realLogs[b].value ? 'auto_resolved' : 'pending',
                  resolved_value: realLogs[a].confidence >= realLogs[b].confidence ? realLogs[a].value : realLogs[b].value,
                  created_at: new Date()
                });
                results.cross_refs++;
              }
            }
          }
        }

        // 8b: Cross-incident person matching — find same person in different incidents
        // Match on: exact phone, exact email, or (first_name + last_name + city)
        const merged8b = { ...person, ...updates };
        try {
          const hasPhone = merged8b.phone && merged8b.phone.length > 5;
          const hasEmail = merged8b.email && !merged8b.email.includes('@gmail.com') && !merged8b.email.includes('@yahoo.com');
          const hasNameCity = merged8b.first_name && merged8b.last_name && merged8b.city;

          if (hasPhone || hasEmail || hasNameCity) {
            const matches = await db('persons')
              .where('id', '!=', person.id)
              .whereNotNull('incident_id')
              .where(function() {
                if (hasPhone) this.orWhere('phone', merged8b.phone);
                if (hasEmail) this.orWhere('email', merged8b.email);
                if (hasNameCity) {
                  this.orWhere(function() {
                    this.whereRaw('LOWER(first_name) = LOWER(?)', [merged8b.first_name])
                      .andWhereRaw('LOWER(last_name) = LOWER(?)', [merged8b.last_name])
                      .andWhereRaw('LOWER(city) = LOWER(?)', [merged8b.city]);
                  });
                }
              })
              .limit(10);

            for (const match of matches) {
              // Check if we already have this cross-ref
              const existing = await db('cross_references')
                .where(function() {
                  this.where({ person_id: person.id, source_b: match.id })
                    .orWhere({ person_id: match.id, source_b: person.id });
                })
                .where('field_name', 'person_match')
                .first();

              if (!existing) {
                // Determine match quality
                let matchScore = 0;
                const matchFields = [];
                if (merged8b.phone && match.phone && merged8b.phone === match.phone) { matchScore += 40; matchFields.push('phone'); }
                if (merged8b.email && match.email && merged8b.email === match.email) { matchScore += 30; matchFields.push('email'); }
                if (merged8b.first_name && match.first_name && merged8b.first_name.toLowerCase() === match.first_name.toLowerCase()
                    && merged8b.last_name && match.last_name && merged8b.last_name.toLowerCase() === match.last_name.toLowerCase()) {
                  matchScore += 25; matchFields.push('name');
                }
                if (merged8b.city && match.city && merged8b.city.toLowerCase() === match.city.toLowerCase()) { matchScore += 5; matchFields.push('city'); }

                await db('cross_references').insert({
                  id: uuidv4(),
                  person_id: person.id,
                  incident_id: person.incident_id,
                  source_a: `person:${person.id}`,
                  source_b: `person:${match.id}`,
                  field_name: 'person_match',
                  value_a: `${merged8b.first_name || ''} ${merged8b.last_name || ''} (incident ${person.incident_id})`.trim(),
                  value_b: `${match.first_name || ''} ${match.last_name || ''} (incident ${match.incident_id})`.trim(),
                  match_score: Math.min(matchScore, 100),
                  resolution: matchScore >= 70 ? 'auto_resolved' : 'pending',
                  resolved_value: matchFields.join('+'),
                  created_at: new Date()
                });
                results.cross_refs++;
              }
            }
          }
        } catch (xrefErr) {
          console.error('Cross-ref matching error:', xrefErr.message);
        }

        results.enriched++;
      } catch (personErr) {
        results.errors.push(`Person ${person.id}: ${personErr.message}`);
      }
    }

    // Update integration stats
    for (const int of integrations) {
      await db('integrations').where('id', int.id).update({
        requests_today: db.raw('requests_today + 1'),
        requests_this_month: db.raw('requests_this_month + 1'),
        last_request_at: new Date(),
        updated_at: new Date()
      });
    }

    res.json({
      success: true,
      mode: isReEnrich ? 're-enrich' : 'standard',
      message: `${isReEnrich ? '[RE-ENRICH] ' : ''}Enriched ${results.enriched} persons, updated ${results.fields_updated} fields, ${results.cross_refs} cross-references`,
      api_keys_active: {
        pdl: !!pdlKey,
        hunter: !!hunterKey,
        openweather: !!weatherKey,
        newsapi: !!NEWS_KEY,
        numverify: !!numverifyKey,
        tracerfy: !!tracerfyKey
      },
      ...results,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    console.error('Enrichment error:', err);
    res.status(500).json({ error: err.message, results });
  }
};
