/**
 * INSURANCE VERIFICATION ADAPTER
 * Verifies insurance coverage and retrieves policy details
 *
 * Providers:
 *   - Verisk / ISO ClaimSearch ($2,000-5,000/mo)
 *   - TransUnion TLOxp ($1,000-3,000/mo)
 *   - LexisNexis Insurance Exchange
 *   - NICB (National Insurance Crime Bureau)
 */
const BaseAdapter = require('./baseAdapter');
const { logger } = require('../../utils/logger');

class InsuranceVerify extends BaseAdapter {
  async fetch() {
    // Insurance verification is triggered per-person, not polled
    // This adapter pulls from a queue of persons needing verification
    const db = require('../../config/database');
    const persons = await db('persons')
      .whereNull('insurance_company')
      .where('is_injured', true)
      .where('confidence_score', '>=', 40)
      .whereRaw("created_at > NOW() - INTERVAL '7 days'")
      .orderBy('created_at', 'desc')
      .limit(20);

    const results = [];
    for (const person of persons) {
      try {
        const insuranceData = await this.lookupInsurance(person);
        if (insuranceData) {
          results.push({ person_id: person.id, incident_id: person.incident_id, ...insuranceData });
        }
      } catch (err) {
        logger.warn(`Insurance lookup failed for person ${person.id}:`, err.message);
      }
    }
    return results;
  }

  async lookupInsurance(person) {
    const provider = this.config.provider || 'verisk';

    switch (provider) {
      case 'verisk': return this.lookupVerisk(person);
      case 'tlo': return this.lookupTLO(person);
      case 'lexisnexis': return this.lookupLexisNexis(person);
      default: return this.lookupVerisk(person);
    }
  }

  async lookupVerisk(person) {
    const apiKey = process.env.VERISK_API_KEY;
    if (!apiKey) return null;

    const data = await this.apiRequest('https://api.verisk.com/claims/v2/search', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${apiKey}`, 'X-Customer-ID': process.env.VERISK_CUSTOMER_ID },
      body: {
        firstName: person.first_name,
        lastName: person.last_name,
        dateOfBirth: person.date_of_birth,
        state: person.state,
        searchType: 'auto_insurance',
        includePolicy: true
      }
    });

    if (data?.results?.length > 0) {
      const best = data.results[0];
      return {
        insurance_company: best.carrierName,
        insurance_policy_number: best.policyNumber,
        insurance_type: best.coverageType,
        policy_limits: best.bodilyInjuryLimits,
        policy_limits_bodily_injury: best.bodilyInjuryLimits,
        policy_limits_property: best.propertyDamageLimits,
        insurance_agent: best.agentName,
        insurance_agent_phone: best.agentPhone,
        insurance_claim_number: best.claimNumber,
        provider: 'verisk'
      };
    }
    return null;
  }

  async lookupTLO(person) {
    const username = process.env.TLO_USERNAME;
    if (!username) return null;

    const data = await this.apiRequest('https://api.tlo.com/v3/person/insurance', {
      method: 'POST',
      headers: {
        'Authorization': `Basic ${Buffer.from(`${username}:${process.env.TLO_PASSWORD}`).toString('base64')}`,
        'X-Company-ID': process.env.TLO_COMPANY_ID
      },
      body: {
        name: { first: person.first_name, last: person.last_name },
        dob: person.date_of_birth,
        address: { state: person.state, zip: person.zip },
        includeVehicle: true
      }
    });

    if (data?.insurance) {
      return {
        insurance_company: data.insurance.carrier,
        insurance_policy_number: data.insurance.policyNumber,
        insurance_type: data.insurance.type,
        policy_limits: data.insurance.limits,
        phone: data.phones?.[0]?.number || person.phone,
        address: data.addresses?.[0]?.fullAddress || person.address,
        provider: 'tlo'
      };
    }
    return null;
  }

  async lookupLexisNexis(person) {
    const apiKey = process.env.LN_CLAIMS_API_KEY;
    if (!apiKey) return null;

    const data = await this.apiRequest('https://api.lexisnexis.com/insurance/v2/claims-search', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${apiKey}` },
      body: {
        person: { firstName: person.first_name, lastName: person.last_name, dob: person.date_of_birth },
        claimType: 'auto',
        includePolicy: true
      }
    });

    if (data?.claims?.length > 0) {
      const claim = data.claims[0];
      return {
        insurance_company: claim.carrier,
        insurance_policy_number: claim.policy,
        policy_limits: claim.limits,
        insurance_claim_number: claim.claimNumber,
        provider: 'lexisnexis'
      };
    }
    return null;
  }

  normalize(raw) {
    // Insurance data updates existing persons rather than creating incidents
    return {
      _type: 'person_update',
      person_id: raw.person_id,
      incident_id: raw.incident_id,
      insurance_company: raw.insurance_company,
      insurance_policy_number: raw.insurance_policy_number,
      insurance_type: raw.insurance_type,
      policy_limits: raw.policy_limits,
      policy_limits_bodily_injury: raw.policy_limits_bodily_injury,
      policy_limits_property: raw.policy_limits_property,
      insurance_agent: raw.insurance_agent,
      insurance_agent_phone: raw.insurance_agent_phone,
      insurance_claim_number: raw.insurance_claim_number,
      phone: raw.phone,
      address: raw.address,
      provider: raw.provider,
      tags: ['insurance', raw.provider]
    };
  }
}

module.exports = InsuranceVerify;
