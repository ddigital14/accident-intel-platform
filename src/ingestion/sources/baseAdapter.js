/**
 * BASE SOURCE ADAPTER
 * All data source adapters extend this class
 */

const axios = require('axios');
const { logger } = require('../../utils/logger');

class BaseAdapter {
  constructor(source) {
    this.source = source;
    this.config = source.config || {};
    this.name = source.name;
    this.type = source.type;
  }

  /**
   * Fetch raw records from the source. Override in subclass.
   * @returns {Array} Array of raw records
   */
  async fetch() {
    throw new Error('fetch() must be implemented by subclass');
  }

  /**
   * Normalize a raw record into our standard format. Override in subclass.
   * @param {Object} raw - Raw record from the source
   * @returns {Object} Normalized record
   */
  normalize(raw) {
    throw new Error('normalize() must be implemented by subclass');
  }

  /**
   * Make an authenticated API request
   */
  async apiRequest(url, options = {}) {
    try {
      const response = await axios({
        url,
        method: options.method || 'GET',
        headers: {
          'Content-Type': 'application/json',
          ...options.headers
        },
        params: options.params,
        data: options.body,
        timeout: options.timeout || 30000
      });
      return response.data;
    } catch (err) {
      logger.error(`API request failed for ${this.name}: ${url}`, {
        status: err.response?.status,
        message: err.message
      });
      throw err;
    }
  }

  /**
   * Classify incident type from description text
   */
  classifyIncidentType(text) {
    if (!text) return 'car_accident';
    const lower = text.toLowerCase();

    if (/motorcycle|motorbike|moped/i.test(lower)) return 'motorcycle_accident';
    if (/semi|tractor.?trailer|18.?wheel|commercial vehicle|cdl|big rig|freight/i.test(lower)) return 'truck_accident';
    if (/work(place|site|er)|osha|construction|industrial|on.?the.?job|fall from/i.test(lower)) return 'work_accident';
    if (/pedestrian|struck.?by.?vehicle|hit.?and.?run.*walk/i.test(lower)) return 'pedestrian';
    if (/bicycle|cyclist|bike/i.test(lower)) return 'bicycle';
    if (/slip|trip|fall|premise/i.test(lower)) return 'slip_fall';
    if (/bus|transit|shuttle/i.test(lower)) return 'bus_accident';
    if (/boat|watercraft|marine/i.test(lower)) return 'boat_accident';
    return 'car_accident';
  }

  /**
   * Classify severity from description text
   */
  classifySeverity(text) {
    if (!text) return 'unknown';
    const lower = text.toLowerCase();

    if (/fatal|death|deceased|doa|dead on|killed|mortality/i.test(lower)) return 'fatal';
    if (/critical|life.?threaten|trauma.?(alert|code)|cpr|cardiac|unresponsive/i.test(lower)) return 'critical';
    if (/serious|major|severe|helicopter|airlift|extrication|entrap|amputat/i.test(lower)) return 'serious';
    if (/minor|fender|scratch|bump|no injur|refused.?transport/i.test(lower)) return 'minor';
    if (/moderate|injur|hospital|ambulance|ems|pain|broken|fracture/i.test(lower)) return 'moderate';
    return 'unknown';
  }

  /**
   * Extract phone numbers from text
   */
  extractPhones(text) {
    if (!text) return [];
    const phoneRegex = /(?:\+?1[-.\s]?)?\(?[2-9]\d{2}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g;
    return (text.match(phoneRegex) || []).map(p => p.replace(/[^\d+]/g, ''));
  }

  /**
   * Parse address components
   */
  parseAddress(address) {
    if (!address) return {};
    // Basic address parsing - could use Google Geocoding API for better results
    const stateAbbrevs = /\b(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\b/i;
    const zipRegex = /\b(\d{5})(?:-\d{4})?\b/;
    const stateMatch = address.match(stateAbbrevs);
    const zipMatch = address.match(zipRegex);

    return {
      state: stateMatch ? stateMatch[1].toUpperCase() : null,
      zip: zipMatch ? zipMatch[1] : null
    };
  }
}

module.exports = BaseAdapter;
