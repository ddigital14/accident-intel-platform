/**
 * Phase 96: Research-Cascade Engine
 *
 * Mason's insight: we extracted these names FROM articles. The articles are in
 * our raw_description. We don't need Brave/Google to find new sources — we
 * need to MORE THOROUGHLY mine what we already have, then cascade out.
 *
 * Pipeline per person:
 *   1. Fetch person + parent incident (with raw_description = original article)
 *   2. Use Claude Sonnet 4.5 to extract ALL contact context from raw_description:
 *      age, hometown, employer, attorney, GoFundMe URL, funeral home,
 *      hospital, family names + relationships, vehicle info, witness names
 *   3. Write extracted fields to person row
 *   4. Create related-person rows for each family member found
 *   5. For incidents missing geo, look up city → lat/lon (geocoder)
 *   6. If GoFundMe URL found, fetch it and extract organizer phone/email
 *   7. If funeral home named, hit funeral-home-survivors with that home
 *   8. Cascade: each new field triggers more engines via auto-fan-out
 *
 * Endpoints:
 *   GET ?action=health
 *   POST ?action=research&person_id=X — single person deep research
 *   POST ?action=run&limit=N — batch over named-pending persons
 */
const { getDb } = require('../../_db');

const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
const MODEL = 'claude-opus-4-7';

async function claudeExtract(name, articleText, knownFields) {
  if (!ANTHROPIC_KEY) return null;
  const text = (articleText || '').slice(0, 6000);
  const prompt = `You are an OSINT researcher extracting structured contact intel from a news article about a car accident.

VICTIM/SUBJECT: ${name}
KNOWN FIELDS: ${JSON.stringify(knownFields)}

ARTICLE TEXT (raw_description):
"""
${text}
"""

Extract EVERYTHING you can find about ${name} OR their family/witnesses/responders. Return ONLY JSON in this exact shape:
{
  "subject": {
    "age": <number or null>,
    "hometown_city": <string or null>,
    "hometown_state": <2-letter or null>,
    "occupation": <string or null>,
    "employer": <string or null>,
    "attorney_firm": <string or null>,
    "attorney_name": <string or null>,
    "vehicle_year": <number or null>,
    "vehicle_make": <string or null>,
    "vehicle_model": <string or null>,
    "vehicle_color": <string or null>,
    "license_plate": <string or null>,
    "vin": <string or null>,
    "insurance_carrier": <string or null>,
    "hospital": <string or null>,
    "funeral_home": <string or null>,
    "gofundme_url": <string or null>,
    "social_media": <{platform: handle} or null>
  },
  "family": [
    {"name": <full name>, "relationship": <"spouse"|"parent"|"child"|"sibling"|"grandparent"|"in-law"|"cousin"|"uncle/aunt"|"nephew/niece">, "city": <string or null>, "state": <2-letter or null>, "age": <number or null>}
  ],
  "witnesses": [
    {"name": <full name>, "role": <"witness"|"first responder"|"officer"|"reporter">, "phone_or_email_if_mentioned": <string or null>}
  ],
  "responding_agency": <string or null>,
  "incident_address": <string or null>,
  "incident_intersection": <string or null>,
  "officer_name": <string or null>,
  "police_report_number": <string or null>,
  "tow_company": <string or null>,
  "key_quotes_with_names": [<"quoted phrase attributed to NAME">]
}

If a field is not found, use null. Do NOT make up data. Return ONLY the JSON, no preamble.`;
  try {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': ANTHROPIC_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json'
      },
      body: JSON.stringify({
        model: MODEL,
        max_tokens: 3000,
        messages: [{ role: 'user', content: prompt }]
      }),
      signal: AbortSignal.timeout(25000)
    });
    if (!r.ok) return { error: `HTTP ${r.status}` };
    const j = await r.json();
    const text = j.content?.[0]?.text || '';
    const m = text.match(/\{[\s\S]*\}/);
    if (!m) return { error: 'no_json_in_response', preview: text.slice(0, 200) };
    return JSON.parse(m[0]);
  } catch (e) {
    return { error: e.message };
  }
}

async function fetchGoFundMeOrganizer(url) {
  if (!url) return null;
  try {
    const r = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; AccidentCommandCenter/1.0)' },
      signal: AbortSignal.timeout(10000)
    });
    if (!r.ok) return null;
    const html = await r.text();
    // GoFundMe organizer name is in og:title and meta tags
    const orgMatch = html.match(/<meta\s+(?:name|property)="(?:og:title|twitter:title)"\s+content="([^"]+)"/);
    const descMatch = html.match(/<meta\s+(?:name|property)="(?:og:description|description)"\s+content="([^"]+)"/);
    const stateMatch = html.match(/"state":"([^"]+)"/);
    const cityMatch = html.match(/"city":"([^"]+)"/);
    const organizerMatch = html.match(/"organizerName":"([^"]+)"/);
    const beneficiaryMatch = html.match(/"beneficiaryName":"([^"]+)"/);
    return {
      organizer: organizerMatch?.[1] || null,
      beneficiary: beneficiaryMatch?.[1] || null,
      title: orgMatch?.[1] || null,
      description: descMatch?.[1] || null,
      state: stateMatch?.[1] || null,
      city: cityMatch?.[1] || null
    };
  } catch { return null; }
}

async function applyExtractedFields(db, person, incident, extracted) {
  const updates = {};
  const sub = extracted.subject || {};
  // Coerce - person.age may come back as string from PG
  const personAge = person.age ? parseInt(person.age) : null;
  if (!personAge && sub.age) updates.age = parseInt(sub.age);
  if (!person.city && sub.hometown_city) updates.city = sub.hometown_city;
  if (!person.state && sub.hometown_state) updates.state = sub.hometown_state;
  if (!person.employer && sub.employer) updates.employer = sub.employer;
  if (!person.occupation && sub.occupation) updates.occupation = sub.occupation;
  if (!person.attorney_firm && sub.attorney_firm) updates.attorney_firm = sub.attorney_firm;
  if (!person.attorney_name && sub.attorney_name) updates.attorney_name = sub.attorney_name;
  if (!person.insurance_company && sub.insurance_carrier) updates.insurance_company = sub.insurance_carrier;

  let appliedFields = [];
  if (Object.keys(updates).length > 0) {
    try {
      await db('persons').where('id', person.id).update(updates);
      appliedFields = Object.keys(updates);
      // Log to enrichment_logs
      for (const [field, value] of Object.entries(updates)) {
        await db('enrichment_logs').insert({
          person_id: person.id, field_name: field,
          old_value: null, new_value: typeof value === 'string' ? value : JSON.stringify(value),
          created_at: new Date()
        }).catch(() => {});
      }
    } catch (e) {
      return { applied_fields: [], error: e.message };
    }
  }

  // Update incident with extracted address/intersection/officer/agency
  const incUpdates = {};
  if (!incident.address && extracted.incident_address) incUpdates.address = extracted.incident_address;
  if (!incident.intersection && extracted.incident_intersection) incUpdates.intersection = extracted.incident_intersection;
  if (!incident.officer_name && extracted.officer_name) incUpdates.officer_name = extracted.officer_name;
  if (!incident.police_report_number && extracted.police_report_number) incUpdates.police_report_number = extracted.police_report_number;
  if (!incident.responding_agencies && extracted.responding_agency) incUpdates.responding_agencies = [extracted.responding_agency];
  if (Object.keys(incUpdates).length > 0) {
    try { await db('incidents').where('id', incident.id).update(incUpdates); } catch { /* skip */ }
  }

  // Insert family members as related persons
  let familyAdded = 0;
  if (Array.isArray(extracted.family)) {
    const { v4: uuid } = require('uuid');
    for (const f of extracted.family) {
      if (!f.name || f.name.length < 5 || !/\s/.test(f.name)) continue;
      const exists = await db('persons').where({ incident_id: incident.id, full_name: f.name }).first();
      if (exists) continue;
      try {
        await db('persons').insert({
          id: uuid(), incident_id: incident.id, full_name: f.name,
          role: 'family', age: f.age ? parseInt(f.age) : null,
          city: f.city || person.city, state: f.state || person.state,
          relationship_to_victim: f.relationship,
          victim_id: person.id, victim_verified: false, lead_tier: 'related',
          source: 'research-cascade', created_at: new Date()
        });
        familyAdded++;
      } catch { /* skip duplicate */ }
    }
  }

  return { applied_fields: appliedFields, incident_fields_filled: Object.keys(incUpdates), family_added: familyAdded };
}

async function processOne(db, person) {
  const incident = await db('incidents').where('id', person.incident_id).first();
  if (!incident) return { person_id: person.id, status: 'no_incident' };

  // 1. Pull source article text
  const articleText = incident.raw_description || incident.description || '';
  if (articleText.length < 50) return { person_id: person.id, status: 'no_article_text', text_length: articleText.length };

  // 2. Claude extract everything
  const knownFields = {
    age: person.age, city: person.city, state: person.state,
    employer: person.employer, phone: person.phone, email: person.email
  };
  const extracted = await claudeExtract(person.full_name, articleText, knownFields);
  if (!extracted || extracted.error) {
    return { person_id: person.id, status: 'claude_failed', error: extracted?.error };
  }

  // 3. Apply fields + create family members
  const apply = await applyExtractedFields(db, person, incident, extracted);

  // 4. Try GoFundMe organizer scrape if URL was extracted
  let gofundme = null;
  if (extracted.subject?.gofundme_url) {
    gofundme = await fetchGoFundMeOrganizer(extracted.subject.gofundme_url);
    if (gofundme && gofundme.organizer && gofundme.organizer !== person.full_name) {
      // Add organizer as related person
      const { v4: uuid } = require('uuid');
      const exists = await db('persons').where({ incident_id: incident.id, full_name: gofundme.organizer }).first();
      if (!exists) {
        try {
          await db('persons').insert({
            id: uuid(), incident_id: incident.id, full_name: gofundme.organizer, role: 'family',
            relationship_to_victim: 'spouse_or_family', victim_id: person.id,
            city: gofundme.city, state: gofundme.state,
            victim_verified: false, lead_tier: 'related',
            source: 'gofundme-organizer', created_at: new Date()
          });
        } catch { /* skip */ }
      }
    }
  }

  return {
    person_id: person.id,
    name: person.full_name,
    article_text_len: articleText.length,
    apply_attempted_updates: apply._attempted || null,
    fields_filled_on_person: apply.applied_fields,
    incident_fields_filled: apply.incident_fields_filled,
    family_added: apply.family_added,
    gofundme_organizer: gofundme?.organizer || null,
    extracted_summary: {
      age: extracted.subject?.age,
      hometown: extracted.subject?.hometown_city || null,
      employer: extracted.subject?.employer,
      attorney: extracted.subject?.attorney_firm,
      gofundme_url: extracted.subject?.gofundme_url,
      funeral_home: extracted.subject?.funeral_home,
      hospital: extracted.subject?.hospital,
      witness_count: (extracted.witnesses || []).length,
      key_quotes_count: (extracted.key_quotes_with_names || []).length
    }
  };
}

async function findTargets(db, limit) {
  return (await db.raw(`
    SELECT p.*, i.raw_description, i.description, i.state as i_state, i.city as i_city,
           i.address as i_address, i.severity, i.occurred_at, i.lead_score
    FROM persons p JOIN incidents i ON i.id = p.incident_id
    WHERE p.full_name IS NOT NULL AND length(p.full_name) >= 5
      AND (p.full_name ~ ' ')
      AND (i.qualification_state IS NULL OR i.qualification_state != 'qualified')
      AND (p.phone IS NULL OR p.email IS NULL OR p.address IS NULL)
      AND length(COALESCE(i.raw_description, i.description, '')) > 100
    ORDER BY
      CASE i.severity WHEN 'fatal' THEN 1 WHEN 'critical' THEN 2 WHEN 'serious' THEN 3 ELSE 4 END,
      i.lead_score DESC NULLS LAST,
      i.occurred_at DESC NULLS LAST
    LIMIT ${parseInt(limit) || 10}
  `)).rows;
}

module.exports = async function handler(req, res) {
  if (!authed(req)) return res.status(401).json({ error: 'unauthorized' });
  const action = req.query?.action || 'health';
  const db = getDb();

  if (action === 'health') {
    return res.status(200).json({
      ok: true, engine: 'research-cascade',
      anthropic_configured: !!ANTHROPIC_KEY,
      strategy: 'Claude reads source article + extracts subject/family/witness/incident fields, then cascade'
    });
  }

  if (action === 'research') {
    const id = req.query?.person_id;
    if (!id) return res.status(400).json({ error: 'person_id required' });
    const persons = (await db.raw(`
      SELECT p.*, i.raw_description, i.description, i.severity, i.occurred_at, i.address as i_address
      FROM persons p JOIN incidents i ON i.id = p.incident_id WHERE p.id = ?
    `, [id])).rows;
    if (persons.length === 0) return res.status(404).json({ error: 'person not found' });
    const result = await processOne(db, persons[0]);
    return res.status(200).json({ ok: true, result });
  }

  if (action === 'run') {
    const limit = parseInt(req.query?.limit) || 8;
    const persons = await findTargets(db, limit);
    const results = [];
    let total_fields = 0, total_family = 0;
    for (const p of persons) {
      try {
        const r = await processOne(db, p);
        results.push(r);
        total_fields += (r.fields_filled_on_person || []).length;
        total_family += r.family_added || 0;
      } catch (e) {
        results.push({ person_id: p.id, error: e.message });
      }
    }
    return res.status(200).json({
      ok: true, processed: persons.length,
      total_fields_filled: total_fields,
      total_family_added: total_family,
      results
    });
  }

  return res.status(400).json({ error: 'unknown action', valid: ['health','research','run'] });
};
