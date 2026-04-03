/**
 * CAD (Computer-Aided Dispatch) ADAPTER
 * Real-time 911/dispatch data from emergency call centers
 *
 * Providers:
 *   - PulsePoint (free for fire/EMS - $0)
 *   - Tyler Technologies / New World CAD ($2,000-5,000/mo)
 *   - Hexagon Safety ($1,500-4,000/mo)
 *   - RapidSOS ($3,000-8,000/mo)
 *   - Active911 ($200-500/mo)
 *   - Many cities publish CAD feeds publicly
 */

const BaseAdapter = require('./baseAdapter');
const { logger } = require('../../utils/logger');

class CADDispatch extends BaseAdapter {
  async fetch() {
    const provider = this.config.provider || 'pulsepoint';
    switch (provider) {
      case 'pulsepoint': return this.fetchPulsePoint();
      case 'tyler_cad': return this.fetchTylerCAD();
      case 'rapidsos': return this.fetchRapidSOS();
      case 'public_cad': return this.fetchPublicCAD();
      default: return this.fetchPulsePoint();
    }
  }

  async fetchPulsePoint() {
    try {
      const agencyIds = this.config.agency_ids || [];
      const results = [];
      for (const agencyId of agencyIds) {
        const data = await this.apiRequest(
          `https://web.pulsepoint.org/DB/giba.php`,
          { params: { agency_id: agencyId } }
        );
        if (data?.incidents) {
          const filtered = data.incidents.filter(i =>
            /TC|MVC|MVA|ACCIDENT|INJURY|RESCUE|EXTRICATION/i.test(i.call_type || i.incident_type || '')
          );
          results.push(...filtered.map(i => ({ ...i, provider: 'pulsepoint', agency_id: agencyId })));
        }
      }
      return results;
    } catch (err) {
      logger.error('PulsePoint fetch error:', err.message);
      return [];
    }
  }

  async fetchTylerCAD() {
    try {
      const endpoint = process.env.TYLER_CAD_ENDPOINT;
      const apiKey = process.env.TYLER_CAD_API_KEY;
      if (!endpoint || !apiKey) return [];

      const data = await this.apiRequest(endpoint, {
        headers: { 'Authorization': `Bearer ${apiKey}` },
        params: {
          since: this.source.last_polled_at?.toISOString(),
          call_types: 'ACCIDENT,MVC,MVA,TC,PI,INJURY,RESCUE',
          include_units: true
        }
      });
      return (data?.calls || []).map(c => ({ ...c, provider: 'tyler_cad' }));
    } catch (err) {
      logger.error('Tyler CAD fetch error:', err.message);
      return [];
    }
  }

  async fetchRapidSOS() {
    try {
      const clientId = process.env.RAPIDSOS_CLIENT_ID;
      const clientSecret = process.env.RAPIDSOS_CLIENT_SECRET;
      if (!clientId) return [];

      // OAuth token
      const tokenResp = await this.apiRequest('https://api.rapidsos.com/oauth/token', {
        method: 'POST',
        body: { grant_type: 'client_credentials', client_id: clientId, client_secret: clientSecret }
      });

      const data = await this.apiRequest('https://api.rapidsos.com/v1/incidents', {
        headers: { 'Authorization': `Bearer ${tokenResp.access_token}` },
        params: {
          since: this.source.last_polled_at?.toISOString(),
          types: 'vehicle_accident,motorcycle_accident,pedestrian,injury'
        }
      });
      return (data?.incidents || []).map(i => ({ ...i, provider: 'rapidsos' }));
    } catch (err) {
      logger.error('RapidSOS fetch error:', err.message);
      return [];
    }
  }

  async fetchPublicCAD() {
    // Many cities publish real-time CAD data on public web pages
    const cheerio = require('cheerio');
    try {
      const url = this.config.public_cad_url;
      if (!url) return [];

      const html = await this.apiRequest(url);
      const $ = cheerio.load(html);
      const results = [];

      // Generic table parser for common CAD formats
      const selector = this.config.row_selector || 'table tbody tr';
      $(selector).each((i, row) => {
        const cells = $(row).find('td').map((j, td) => $(td).text().trim()).get();
        if (cells.length >= 3) {
          const text = cells.join(' ');
          if (/accident|crash|mva|mvc|injury|collision/i.test(text)) {
            results.push({
              call_type: cells[this.config.type_col || 0],
              location: cells[this.config.location_col || 1],
              time: cells[this.config.time_col || 2],
              agency: cells[this.config.agency_col || 3],
              raw_cells: cells,
              provider: 'public_cad'
            });
          }
        }
      });
      return results;
    } catch (err) {
      logger.error('Public CAD fetch error:', err.message);
      return [];
    }
  }

  normalize(raw) {
    return {
      source_reference: raw.incident_id || raw.call_id || raw.id || `cad_${Date.now()}_${Math.random().toString(36).substr(2,6)}`,
      incident_type: this.classifyIncidentType(raw.call_type || raw.incident_type || raw.type_description || ''),
      severity: this.classifySeverity(raw.call_type || raw.type_description || ''),
      confidence: raw.provider === 'rapidsos' ? 60 : 40,

      address: raw.address || raw.location || raw.full_address,
      city: raw.city || this.config.default_city,
      state: raw.state || this.config.default_state,
      latitude: parseFloat(raw.latitude || raw.lat),
      longitude: parseFloat(raw.longitude || raw.lng || raw.lon),

      occurred_at: raw.call_time || raw.received_time || raw.timestamp ? new Date(raw.call_time || raw.received_time || raw.timestamp) : new Date(),
      reported_at: new Date(),

      description: `[CAD Dispatch] ${raw.call_type || raw.incident_type || ''}: ${raw.address || raw.location || ''} - ${raw.type_description || raw.comments || ''}`.trim(),

      dispatch_codes: [raw.call_type, raw.priority_code].filter(Boolean),
      responding_agencies: (raw.units || []).map(u => u.unit_id || u).filter(Boolean),
      ems_dispatched: /ems|medic|rescue|ambulance/i.test(JSON.stringify(raw.units || raw.call_type || '')),
      helicopter_dispatched: /helicopter|medflight/i.test(JSON.stringify(raw)),

      injuries_count: raw.patient_count || 0,
      vehicles_involved: raw.vehicle_count || 2,

      tags: ['cad_dispatch', raw.provider || 'unknown']
    };
  }
}

module.exports = CADDispatch;
