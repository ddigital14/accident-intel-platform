/**
 * INTEGRATION ROUTING MATRIX
 *
 * Single source of truth for which API to call given what data we have +
 * what we need. Used by trigger.js to compose the enrichment chain.
 *
 * COST RANK (cheapest first):
 *   FREE: NumVerify-fallback, USPS, OpenWeather, news/RSS scraping, people-search
 *   $0.005/q: Twilio Lookup carrier
 *   $0.015/q: Trestle Phone Validation (needs access)
 *   $0.03/q: Trestle Real Contact (needs access)
 *   $0.05/q: Trestle Smart CNAM (enabled)
 *   $0.05/q: SearchBug
 *   $0.05/q: Hunter.io email finder
 *   $0.07/q: Trestle Reverse Phone (enabled) ← BEST coverage per dollar
 *   $0.07/q: Trestle Reverse Address (needs access)
 *   $0.10/q: Tracerfy skip-trace
 *   PDL: based on tier, ~$0.02/match
 */

// Goal → ordered list of API calls to try
// Each entry: { api, endpoint, cost, gives, requires }
const ROUTING = {
  // GOAL: phone in, want owner name + addresses + emails
  'phone_to_full_profile': [
    { api: 'trestle', endpoint: 'reverse_phone', cost: 0.07, gives: ['name','address','email','phone_metadata','age','phone_secondary'], enabled: true },
    { api: 'pdl',     endpoint: 'enrich',         cost: 0.02, gives: ['name','employer','linkedin','work_email'], enabled: true },
    { api: 'searchbug', endpoint: 'phone',        cost: 0.05, gives: ['name','address'], enabled: true },
  ],

  // GOAL: phone in, want just owner name (cheap)
  'phone_to_name_only': [
    { api: 'trestle', endpoint: 'cnam',           cost: 0.05, gives: ['name','phone_verified'], enabled: true },
  ],

  // GOAL: phone in, just verify it
  'phone_validation': [
    { api: 'twilio',  endpoint: 'lookup',         cost: 0.008, gives: ['phone_verified','carrier','line_type','caller_name','caller_type'], enabled: true,
      note: '$0.008 — line_type_intelligence + caller_name in one call. caller_name is carrier-reported owner — feeds cross-exam at weight 88' },
    { api: 'trestle', endpoint: 'reverse_phone',  cost: 0.07, gives: ['phone_verified','carrier','line_type','is_prepaid','owner'], enabled: true,
      note: 'Reverse Phone INCLUDES validation — no need for separate phone_intel call' },
    { api: 'numverify', endpoint: 'validate',     cost: 0,    gives: ['phone_verified','carrier','line_type'], enabled: true },
  ],

  // GOAL: name + city/state in, want phone + address (no phone)
  'name_to_phone_address': [
    { api: 'pdl',     endpoint: 'enrich',         cost: 0.02, gives: ['phone','address','employer','email'], enabled: true,
      note: 'Best when person has work/online presence' },
    { api: 'tracerfy', endpoint: 'search',        cost: 0.10, gives: ['phone','address','relatives'], enabled: true },
    { api: 'people-search', endpoint: 'tps_fps',  cost: 0,    gives: ['phone','address'], enabled: true,
      note: 'Free scraping — TruePeopleSearch + FastPeopleSearch + Whitepages + Spokeo-free cascade. Bot-detected ~50% of time' },
  ],

  // GOAL: address in, want residents (premises liability cases)
  'address_to_residents': [
    { api: 'trestle', endpoint: 'reverse_address', cost: 0.07, gives: ['name','phone','email','age'], enabled: false, note: 'Need to request access in Trestle portal' },
    { api: 'searchbug', endpoint: 'address',       cost: 0.05, gives: ['name','phone'], enabled: true },
  ],

  // GOAL: have name + employer, want work email
  'name_employer_to_email': [
    { api: 'hunter', endpoint: 'email_finder',    cost: 0.04, gives: ['email','email_verified'], enabled: true },
    { api: 'pdl',    endpoint: 'enrich',          cost: 0.02, gives: ['work_email'], enabled: true },
  ],

  // GOAL: phone in, check if it's a litigator (TCPA risk)
  'phone_litigator_check': [
    { api: 'tcpa-litigator-check', endpoint: 'check', cost: 0, gives: ['is_litigator','risk_score'], enabled: true,
      note: 'FREE — homegrown TCPA litigator scrape + court_records cross-ref. Always run BEFORE Twilio Messaging spend.' },
    { api: 'trestle', endpoint: 'litigator_check', cost: 0.005, gives: ['is_litigator'], enabled: false, note: 'Trestle add-on, ~$0.005' },
  ],

  // GOAL: deceased person obituary in, want family relatives (named contacts)
  'obituary_to_relatives': [
    { api: 'family-tree', endpoint: 'extract', cost: 0.0002, gives: ['relatives','full_name','relation_type'], enabled: true,
      note: 'GPT-4o-mini NER — emits cascade per relative INSERT' },
  ],

  // GOAL: VIN in, want full vehicle context + product-liability signal
  'vin_to_history': [
    { api: 'vehicle-history', endpoint: 'lookup', cost: 0, gives: ['recalls','complaints','ncap','safety_score'], enabled: true,
      note: 'NHTSA recalls + complaints + NCAP — all FREE' },
  ],

  // GOAL: named person in, want family network for fallback contacts
  'person_to_relatives': [
    { api: 'relatives-search', endpoint: 'process', cost: 0.05, gives: ['related_persons','confidence'], enabled: true,
      note: 'Same-last-name + 30mi geo + SearchBug confirmation. Fires cascade per confirmed link.' },
  ],

  // GOAL: verify any phone+email+address combo at once
  'verify_combo': [
    { api: 'trestle', endpoint: 'real_contact',   cost: 0.03, gives: ['phone_verified','email_verified','address_verified','contact_grade'], enabled: false, note: 'Need to request access' },
  ],

  // GOAL: about to send an SMS alert to a phone — verify it's reachable + grab carrier+caller_name in one call
  // Used by system/notify.js BEFORE Twilio Messaging spend
  'pre_alert_verify': [
    { api: 'twilio',  endpoint: 'lookup',         cost: 0.008, gives: ['phone_verified','carrier','line_type','caller_name'], enabled: true,
      note: 'Saves $$$ — confirms phone exists + carrier-reported owner before alert spend' },
  ],

  // GOAL: phone owner replied to our SMS — they self-identified
  'inbound_confirmation': [
    { api: 'twilio',  endpoint: 'webhook_sms',    cost: 0,     gives: ['phone_verified','consent','responded_at'], enabled: true,
      note: 'Inbound webhook fires cascade automatically' },
  ],
};

/**
 * Pick the best (cheapest, enabled) chain for a given enrichment goal.
 * Returns ordered list of { api, endpoint, cost } to try.
 */
function getRouting(goal) {
  const candidates = ROUTING[goal] || [];
  return candidates.filter(c => c.enabled !== false);
}

/**
 * Given an AIP person record, infer which goal to pursue (in priority order).
 */
function inferGoals(person) {
  const goals = [];
  const hasPhone = !!person.phone;
  const hasName = !!(person.full_name || (person.first_name && person.last_name));
  const hasAddress = !!person.address;
  const hasEmail = !!person.email;

  if (hasPhone && (!hasName || !hasAddress || !hasEmail)) goals.push('phone_to_full_profile');
  if (hasName && !hasPhone && person.state) goals.push('name_to_phone_address');
  if (hasAddress && !hasPhone) goals.push('address_to_residents');
  if (hasName && person.employer && !hasEmail) goals.push('name_employer_to_email');
  if (hasPhone && !person.phone_verified) goals.push('phone_validation');
  return goals;
}

module.exports = { ROUTING, getRouting, inferGoals };
