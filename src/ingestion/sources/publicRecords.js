/**
 * PUBLIC RECORDS ADAPTER - Court filings, accident reports from public portals
 */
const BaseAdapter = require('./baseAdapter');
const { logger } = require('../../utils/logger');

class PublicRecords extends BaseAdapter {
  async fetch() {
    const results = [];
    if (this.config.court_portals) {
      for (const portal of this.config.court_portals) {
        try {
          const data = await this.scrapeCourtPortal(portal);
          results.push(...data);
        } catch (err) { logger.warn(`Court portal error ${portal.name}:`, err.message); }
      }
    }
    if (this.config.dot_portals) {
      for (const portal of this.config.dot_portals) {
        try {
          const data = await this.scrapeDOTPortal(portal);
          results.push(...data);
        } catch (err) { logger.warn(`DOT portal error ${portal.name}:`, err.message); }
      }
    }
    return results;
  }

  async scrapeCourtPortal(portal) {
    const cheerio = require('cheerio');
    const html = await this.apiRequest(portal.url, { params: portal.params });
    const $ = cheerio.load(html);
    const results = [];
    $(portal.row_selector || 'table tbody tr').each((i, row) => {
      const cells = $(row).find('td').map((j, td) => $(td).text().trim()).get();
      if (/accident|injury|negligence|motor vehicle/i.test(cells.join(' '))) {
        results.push({ cells, portal: portal.name, provider: 'court_filing' });
      }
    });
    return results;
  }

  async scrapeDOTPortal(portal) {
    const data = await this.apiRequest(portal.url, { params: { ...portal.params, since: this.source.last_polled_at?.toISOString() } });
    return (data?.records || data?.crashes || []).map(r => ({ ...r, provider: 'dot_portal', portal: portal.name }));
  }

  normalize(raw) {
    return {
      source_reference: raw.case_number || raw.id || `public_${Date.now()}`,
      incident_type: this.classifyIncidentType(JSON.stringify(raw)),
      severity: 'unknown',
      confidence: 40,
      city: raw.city || this.config.default_city,
      state: raw.state || this.config.default_state,
      occurred_at: raw.filing_date || raw.crash_date,
      description: `[Public Record] ${raw.case_number || ''}: ${JSON.stringify(raw.cells || raw.description || '').substring(0, 300)}`,
      tags: ['public_records', raw.provider]
    };
  }
}

module.exports = PublicRecords;
