/**
 * POLICE / CRASH REPORT ADAPTER
 * Fetches official police crash reports with full details
 *
 * Providers:
 *   - LexisNexis Accurint / C.L.U.E. ($1,000-3,000/mo)
 *   - CrashDocs / BuyCrash ($500-1,500/mo per state)
 *   - Carfax Crash Reports
 *   - State DOT crash report APIs
 */

const BaseAdapter = require('./baseAdapter');
const { logger } = require('../../utils/logger');

class PoliceReports extends BaseAdapter {
  async fetch() {
    const provider = this.config.provider || 'lexisnexis';
    switch (provider) {
      case 'lexisnexis': return this.fetchLexisNexis();
      case 'crashdocs': return this.fetchCrashDocs();
      case 'state_dot': return this.fetchStateDOT();
      default: return this.fetchLexisNexis();
    }
  }

  async fetchLexisNexis() {
    try {
      const apiKey = process.env.LEXISNEXIS_API_KEY;
      if (!apiKey) { logger.warn('LexisNexis API key not configured'); return []; }

      const metroConfig = this.config.metro || {};
      const data = await this.apiRequest(
        `${this.config.endpoint || 'https://api.lexisnexis.com'}/crash-reports/v2/search`,
        {
          method: 'POST',
          headers: { 'Authorization': `Bearer ${apiKey}` },
          body: {
            dateRange: {
              from: this.source.last_polled_at || new Date(Date.now() - 3600000).toISOString(),
              to: new Date().toISOString()
            },
            state: metroConfig.state,
            county: metroConfig.county,
            radius: { lat: metroConfig.lat, lng: metroConfig.lng, miles: metroConfig.radius || 50 },
            includePersons: true,
            includeVehicles: true,
            includeInsurance: true,
            injuryOnly: this.config.injury_only !== false
          }
        }
      );

      return data?.reports || [];
    } catch (err) {
      logger.error('LexisNexis fetch error:', err.message);
      return [];
    }
  }

  async fetchCrashDocs() {
    try {
      const apiKey = process.env.CRASHDOCS_API_KEY;
      if (!apiKey) return [];

      const data = await this.apiRequest(
        'https://api.crashdocs.com/v1/reports',
        {
          headers: { 'X-API-Key': apiKey, 'X-Partner-ID': process.env.CRASHDOCS_PARTNER_ID },
          params: {
            state: this.config.state,
            since: this.source.last_polled_at || new Date(Date.now() - 3600000).toISOString(),
            injury_only: true,
            limit: 100
          }
        }
      );

      return data?.reports || [];
    } catch (err) {
      logger.error('CrashDocs fetch error:', err.message);
      return [];
    }
  }

  async fetchStateDOT() {
    try {
      const endpoint = this.config.state_endpoint;
      if (!endpoint) return [];
      const data = await this.apiRequest(endpoint, {
        params: {
          from_date: this.source.last_polled_at || new Date(Date.now() - 86400000).toISOString(),
          injury_type: 'all_injuries'
        }
      });
      return data?.crashes || data?.reports || [];
    } catch (err) {
      logger.error('State DOT fetch error:', err.message);
      return [];
    }
  }

  normalize(raw) {
    // Police reports are the gold standard - high confidence
    const persons = (raw.persons || raw.parties || raw.individuals || []).map(p => ({
      role: this.mapRole(p.type || p.role),
      is_injured: p.injury_status !== 'none' && p.injury_status !== 'no_injury',
      first_name: p.first_name || p.firstName,
      last_name: p.last_name || p.lastName,
      full_name: p.full_name || `${p.first_name || ''} ${p.last_name || ''}`.trim(),
      date_of_birth: p.dob || p.date_of_birth,
      age: p.age,
      gender: p.gender || p.sex,
      phone: p.phone || p.telephone,
      address: p.address || p.street_address,
      city: p.city,
      state: p.state,
      zip: p.zip || p.zipcode,
      injury_severity: this.mapInjurySeverity(p.injury_status || p.injury_severity),
      injury_description: p.injury_description || p.injury_area,
      transported_to: p.hospital || p.transported_to || p.medical_facility,
      transported_by: p.ambulance_service || p.transport_agency,
      insurance_company: p.insurance_company || p.insurer,
      insurance_policy_number: p.policy_number || p.insurance_policy,
      insurance_type: p.coverage_type,
      policy_limits: p.policy_limits,
      has_attorney: p.attorney_represented === true || !!p.attorney_name,
      attorney_name: p.attorney_name,
      employer: p.employer,
      confidence: 85
    }));

    const vehicles = (raw.vehicles || []).map(v => ({
      year: v.year,
      make: v.make,
      model: v.model,
      color: v.color,
      body_type: v.body_type || v.vehicle_type,
      license_plate: v.tag || v.plate || v.license_plate,
      license_state: v.tag_state || v.plate_state,
      vin: v.vin,
      damage_severity: v.damage_extent || v.damage_severity,
      towed: v.towed === true || v.tow_away === true,
      tow_company: v.tow_company,
      insurance_company: v.insurance_company || v.insurer,
      insurance_policy: v.policy_number,
      is_commercial: v.commercial === true || v.vehicle_use === 'commercial',
      dot_number: v.dot_number || v.usdot,
      carrier_name: v.carrier || v.company_name
    }));

    return {
      source_reference: raw.report_number || raw.case_number || raw.id,
      incident_number: raw.report_number || raw.case_number,
      incident_type: this.classifyIncidentType(raw.collision_type || raw.crash_type || raw.description || ''),
      severity: this.mapCrashSeverity(raw.severity || raw.crash_severity),
      confidence: 85,

      address: raw.location || raw.address || raw.street,
      street: raw.street || raw.on_street,
      city: raw.city || raw.municipality,
      county: raw.county,
      state: raw.state,
      zip: raw.zip || raw.zipcode,
      latitude: parseFloat(raw.latitude || raw.lat),
      longitude: parseFloat(raw.longitude || raw.lng || raw.lon),
      intersection: raw.intersection || raw.at_street,
      highway: raw.highway || raw.route,

      occurred_at: raw.crash_date || raw.date_time || raw.occurred_at,
      reported_at: raw.report_date || raw.filed_date,

      description: raw.narrative || raw.description || raw.summary,

      police_report_number: raw.report_number || raw.case_number,
      police_department: raw.agency || raw.department || raw.investigating_agency,
      officer_name: raw.officer || raw.investigating_officer,
      officer_badge: raw.badge_number,

      vehicles_involved: vehicles.length || raw.vehicle_count,
      persons_involved: persons.length || raw.person_count,
      injuries_count: raw.injury_count || persons.filter(p => p.is_injured).length,
      fatalities_count: raw.fatality_count || raw.fatal_count || 0,

      weather_conditions: raw.weather || raw.weather_condition,
      road_conditions: raw.road_condition || raw.surface_condition,
      lighting_conditions: raw.light_condition || raw.lighting,

      responding_agencies: [raw.agency || 'police'],
      ems_dispatched: raw.ems_called === true || persons.some(p => p.transported_to),

      persons,
      vehicles,

      tags: ['police_report', this.config.provider || 'lexisnexis']
    };
  }

  mapRole(type) {
    const map = { '1': 'driver', '2': 'passenger', '3': 'pedestrian', '4': 'cyclist',
      'driver': 'driver', 'passenger': 'passenger', 'pedestrian': 'pedestrian',
      'bicyclist': 'cyclist', 'witness': 'witness' };
    return map[type?.toLowerCase()] || 'driver';
  }

  mapInjurySeverity(status) {
    const map = {
      'fatal': 'fatal', 'killed': 'fatal', 'K': 'fatal',
      'incapacitating': 'incapacitating', 'serious': 'incapacitating', 'A': 'incapacitating',
      'non-incapacitating': 'non_incapacitating', 'moderate': 'non_incapacitating', 'B': 'non_incapacitating',
      'possible': 'possible', 'complaint': 'possible', 'C': 'possible',
      'none': 'none', 'no_injury': 'none', 'O': 'none'
    };
    return map[status] || 'possible';
  }

  mapCrashSeverity(sev) {
    const map = { 'fatal': 'fatal', 'serious_injury': 'serious', 'injury': 'moderate',
      'possible_injury': 'moderate', 'property_damage': 'minor', 'pdo': 'minor' };
    return map[sev?.toLowerCase()] || this.classifySeverity(sev || '');
  }
}

module.exports = PoliceReports;
