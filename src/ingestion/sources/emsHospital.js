/**
 * EMS / HOSPITAL DATA ADAPTER
 * Hospital run sheets, ambulance transport data, ER admissions
 *
 * Providers:
 *   - ESO (EMS data platform) - $1,000-3,000/mo
 *   - ImageTrend (EMS/fire reporting) - $800-2,500/mo
 *   - NEMSIS (national EMS data) - varies
 *   - Hospital ER feed partnerships - negotiated
 */

const BaseAdapter = require('./baseAdapter');
const { logger } = require('../../utils/logger');

class EMSHospital extends BaseAdapter {
  async fetch() {
    const provider = this.config.provider || 'eso';
    switch (provider) {
      case 'eso': return this.fetchESO();
      case 'imagetrend': return this.fetchImageTrend();
      case 'hospital_feed': return this.fetchHospitalFeed();
      default: return this.fetchESO();
    }
  }

  async fetchESO() {
    try {
      const apiKey = process.env.ESO_API_KEY;
      if (!apiKey) return [];
      const data = await this.apiRequest(
        `https://api.eso.com/v2/incidents`,
        {
          headers: { 'Authorization': `Bearer ${apiKey}`, 'X-Agency-ID': process.env.ESO_AGENCY_ID },
          params: {
            since: this.source.last_polled_at?.toISOString(),
            type: 'trauma,mva,fall,injury',
            include_patient: true
          }
        }
      );
      return (data?.incidents || []).map(i => ({ ...i, provider: 'eso' }));
    } catch (err) {
      logger.error('ESO fetch error:', err.message);
      return [];
    }
  }

  async fetchImageTrend() {
    try {
      const apiKey = process.env.IMAGETREND_API_KEY;
      if (!apiKey) return [];
      const data = await this.apiRequest(
        `https://api.imagetrend.com/v1/runs`,
        {
          headers: { 'X-Api-Key': apiKey, 'X-Site-ID': process.env.IMAGETREND_SITE_ID },
          params: { since: this.source.last_polled_at?.toISOString(), disposition: 'transported' }
        }
      );
      return (data?.runs || []).map(r => ({ ...r, provider: 'imagetrend' }));
    } catch (err) {
      logger.error('ImageTrend fetch error:', err.message);
      return [];
    }
  }

  async fetchHospitalFeed() {
    try {
      const endpoint = this.config.hospital_endpoint;
      if (!endpoint) return [];
      const data = await this.apiRequest(endpoint, {
        headers: this.config.hospital_headers || {},
        params: { since: this.source.last_polled_at?.toISOString(), category: 'trauma,mva' }
      });
      return (data?.admissions || data?.patients || []).map(a => ({ ...a, provider: 'hospital' }));
    } catch (err) {
      logger.error('Hospital feed error:', err.message);
      return [];
    }
  }

  normalize(raw) {
    const patient = raw.patient || raw;
    return {
      source_reference: raw.run_number || raw.incident_id || raw.pcr_number || `ems_${Date.now()}`,
      incident_type: this.classifyIncidentType(raw.dispatch_reason || raw.chief_complaint || raw.mechanism_of_injury || ''),
      severity: this.mapTriageSeverity(raw.triage_code || raw.acuity || raw.esi_level),
      confidence: 65,

      address: raw.scene_address || raw.location,
      city: raw.scene_city || this.config.default_city,
      state: raw.scene_state || this.config.default_state,
      latitude: parseFloat(raw.scene_lat || raw.latitude),
      longitude: parseFloat(raw.scene_lng || raw.longitude),

      occurred_at: raw.dispatch_time || raw.incident_time,
      reported_at: raw.arrival_time || new Date(),

      description: `[EMS] ${raw.dispatch_reason || raw.chief_complaint || 'Transport'}: ${raw.mechanism_of_injury || ''} - Patient transported to ${raw.destination_hospital || raw.hospital || 'hospital'}`.trim(),

      ems_dispatched: true,
      injuries_count: 1,

      persons: [{
        role: 'driver',
        is_injured: true,
        first_name: patient.first_name,
        last_name: patient.last_name,
        full_name: patient.full_name || `${patient.first_name || ''} ${patient.last_name || ''}`.trim(),
        age: patient.age,
        gender: patient.gender,
        phone: patient.phone || patient.contact_phone,
        address: patient.home_address,
        city: patient.home_city,
        state: patient.home_state,
        zip: patient.home_zip,
        injury_severity: this.mapTriageSeverity(raw.triage_code || raw.acuity),
        injury_description: raw.chief_complaint || raw.mechanism_of_injury,
        transported_to: raw.destination_hospital || raw.hospital,
        transported_by: raw.unit_number || raw.ambulance,
        insurance_company: patient.insurance_company || patient.payer,
        insurance_policy_number: patient.insurance_id || patient.member_id,
        insurance_type: patient.insurance_type,
        confidence: 70
      }],

      tags: ['ems', raw.provider || 'unknown']
    };
  }

  mapTriageSeverity(code) {
    const map = { '1': 'critical', '2': 'serious', '3': 'moderate', '4': 'minor', '5': 'minor',
      'red': 'critical', 'yellow': 'serious', 'green': 'moderate', 'black': 'fatal' };
    return map[String(code).toLowerCase()] || 'moderate';
  }
}

module.exports = EMSHospital;
