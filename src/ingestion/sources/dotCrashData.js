/**
 * DOT / GOVERNMENT CRASH DATA ADAPTER
 * NHTSA, FMCSA, state DOT crash databases
 *
 * Providers:
 *   - NHTSA FARS/CRSS API (free)
 *   - FMCSA SAFER System (free - commercial vehicle data)
 *   - State DOT crash databases (varies)
 */
const BaseAdapter = require('./baseAdapter');
const { logger } = require('../../utils/logger');

class DOTCrashData extends BaseAdapter {
  async fetch() {
    const results = [];
    results.push(...await this.fetchNHTSA());
    results.push(...await this.fetchFMCSA());
    if (this.config.state_endpoints) {
      for (const ep of this.config.state_endpoints) {
        results.push(...await this.fetchStateDOT(ep));
      }
    }
    return results;
  }

  async fetchNHTSA() {
    try {
      const data = await this.apiRequest('https://crashviewer.nhtsa.dot.gov/CrashAPI/crashes/GetCaseList', {
        params: {
          states: this.config.state_fips || '13,48,12,17,6,4,36,42',
          fromYear: new Date().getFullYear(),
          toYear: new Date().getFullYear(),
          minNumOfVehicles: 1,
          format: 'json'
        }
      });
      return (data?.Results?.[0] || []).map(r => ({ ...r, provider: 'nhtsa' }));
    } catch (err) {
      logger.error('NHTSA fetch error:', err.message);
      return [];
    }
  }

  async fetchFMCSA() {
    try {
      const apiKey = process.env.FMCSA_API_KEY;
      if (!apiKey) return [];
      const data = await this.apiRequest('https://mobile.fmcsa.dot.gov/qc/services/carriers/crashes', {
        params: {
          webKey: apiKey,
          state: this.config.state_abbrev || 'GA',
          fromDate: this.source.last_polled_at?.toISOString()?.split('T')[0],
          toDate: new Date().toISOString().split('T')[0]
        }
      });
      return (data?.content || []).map(r => ({ ...r, provider: 'fmcsa' }));
    } catch (err) {
      logger.error('FMCSA fetch error:', err.message);
      return [];
    }
  }

  async fetchStateDOT(endpoint) {
    try {
      const data = await this.apiRequest(endpoint.url, {
        params: { ...endpoint.params, since: this.source.last_polled_at?.toISOString() }
      });
      return (data?.crashes || data?.records || []).map(r => ({ ...r, provider: 'state_dot', state: endpoint.state }));
    } catch (err) {
      logger.error(`State DOT (${endpoint.state}) error:`, err.message);
      return [];
    }
  }

  normalize(raw) {
    const isFMCSA = raw.provider === 'fmcsa';
    return {
      source_reference: raw.CaseNumber || raw.reportNumber || raw.crash_id || `dot_${Date.now()}`,
      incident_number: raw.CaseNumber || raw.reportNumber,
      incident_type: isFMCSA ? 'truck_accident' : this.classifyIncidentType(raw.crash_type || ''),
      severity: this.mapDOTSeverity(raw.INJSEV || raw.severity || raw.crashSeverity),
      confidence: 70,

      city: raw.CITY || raw.city,
      county: raw.COUNTY || raw.county,
      state: raw.STATE || raw.state,
      latitude: parseFloat(raw.LATITUDE || raw.latitude),
      longitude: parseFloat(raw.LONGITUD || raw.longitude),
      highway: raw.ROUTE || raw.highway,

      occurred_at: raw.ACCIDENT_DATE || raw.crashDate || raw.date,
      description: `[DOT] ${isFMCSA ? 'Commercial vehicle crash' : 'Crash'}: ${raw.crash_type || ''} - ${raw.CITY || raw.city || ''}, ${raw.STATE || raw.state || ''}`,

      vehicles_involved: parseInt(raw.PEDS || raw.totalVehicles) || 2,
      fatalities_count: parseInt(raw.FATALS || raw.fatalities) || 0,
      injuries_count: parseInt(raw.INJURIES || raw.totalInjuries) || 0,

      persons: isFMCSA ? [{
        role: 'driver',
        is_injured: parseInt(raw.totalInjuries) > 0,
        confidence: 50
      }] : [],

      vehicles: isFMCSA ? [{
        is_commercial: true,
        dot_number: raw.dotNumber,
        carrier_name: raw.carrierName,
        carrier_mc_number: raw.mcNumber
      }] : [],

      tags: ['dot', raw.provider]
    };
  }

  mapDOTSeverity(sev) {
    const map = { '4': 'fatal', 'K': 'fatal', '3': 'serious', 'A': 'serious',
      '2': 'moderate', 'B': 'moderate', '1': 'minor', 'C': 'minor', '0': 'minor', 'O': 'minor' };
    return map[String(sev)] || 'unknown';
  }
}

module.exports = DOTCrashData;
