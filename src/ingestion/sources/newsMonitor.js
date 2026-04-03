/**
 * NEWS MONITOR ADAPTER
 * Scrapes news articles about accidents for corroboration and enrichment
 * Providers: NewsAPI ($449/mo), Google News API, Bing News, local news RSS
 */

const BaseAdapter = require('./baseAdapter');
const { logger } = require('../../utils/logger');

class NewsMonitor extends BaseAdapter {
  async fetch() {
    const results = [];
    if (process.env.NEWSAPI_KEY) results.push(...await this.fetchNewsAPI());
    if (process.env.BING_NEWS_API_KEY) results.push(...await this.fetchBingNews());
    if (this.config.rss_feeds) results.push(...await this.fetchRSSFeeds());
    return results;
  }

  async fetchNewsAPI() {
    try {
      const cities = this.config.cities || ['Atlanta', 'Houston', 'Dallas', 'Miami', 'Chicago', 'Los Angeles', 'Phoenix', 'New York', 'Philadelphia', 'San Antonio'];
      const allResults = [];
      for (const city of cities) {
        const data = await this.apiRequest('https://newsapi.org/v2/everything', {
          params: {
            q: `("car accident" OR "vehicle crash" OR "motorcycle accident" OR "truck accident" OR "fatal crash" OR "injury crash" OR "pedestrian struck" OR "work accident") AND "${city}"`,
            sortBy: 'publishedAt',
            from: this.source.last_polled_at?.toISOString()?.split('T')[0] || new Date(Date.now() - 86400000).toISOString().split('T')[0],
            language: 'en',
            pageSize: 20,
            apiKey: process.env.NEWSAPI_KEY
          }
        });
        if (data?.articles) allResults.push(...data.articles.map(a => ({ ...a, search_city: city, provider: 'newsapi' })));
      }
      return allResults;
    } catch (err) {
      logger.error('NewsAPI error:', err.message);
      return [];
    }
  }

  async fetchBingNews() {
    try {
      const data = await this.apiRequest('https://api.bing.microsoft.com/v7.0/news/search', {
        headers: { 'Ocp-Apim-Subscription-Key': process.env.BING_NEWS_API_KEY },
        params: {
          q: 'car accident crash injury',
          freshness: 'Day',
          count: 50,
          mkt: 'en-US'
        }
      });
      return (data?.value || []).map(a => ({ ...a, provider: 'bing' }));
    } catch (err) {
      logger.error('Bing News error:', err.message);
      return [];
    }
  }

  async fetchRSSFeeds() {
    // Parse RSS feeds from local news stations
    const results = [];
    for (const feedUrl of (this.config.rss_feeds || [])) {
      try {
        const xml = await this.apiRequest(feedUrl);
        const cheerio = require('cheerio');
        const $ = cheerio.load(xml, { xmlMode: true });
        $('item').each((i, item) => {
          const title = $(item).find('title').text();
          const desc = $(item).find('description').text();
          const combined = `${title} ${desc}`;
          if (/accident|crash|collision|injur|fatal|wreck/i.test(combined)) {
            results.push({
              title, description: desc,
              url: $(item).find('link').text(),
              publishedAt: $(item).find('pubDate').text(),
              provider: 'rss'
            });
          }
        });
      } catch (err) {
        logger.warn(`RSS feed error for ${feedUrl}:`, err.message);
      }
    }
    return results;
  }

  normalize(raw) {
    const text = `${raw.title || ''} ${raw.description || raw.content || ''}`;
    const location = this.extractLocationFromText(text);

    return {
      source_reference: raw.url || `news_${Date.now()}`,
      incident_type: this.classifyIncidentType(text),
      severity: this.classifySeverity(text),
      confidence: 35,

      address: location.address,
      city: location.city || raw.search_city,
      state: location.state,

      occurred_at: raw.publishedAt ? new Date(raw.publishedAt) : new Date(),
      description: `[News] ${raw.title || ''}: ${(raw.description || '').substring(0, 300)}`,

      injuries_count: this.extractInjuryCount(text),
      fatalities_count: this.extractFatalityCount(text),

      tags: ['news', raw.provider],
      metadata: { news_url: raw.url, news_source: raw.source?.name }
    };
  }

  extractLocationFromText(text) {
    const result = { address: null, city: null, state: null };
    const cityMatch = text.match(/(?:in|near|at)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),?\s*(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)/);
    if (cityMatch) { result.city = cityMatch[1]; result.state = cityMatch[2]; }
    const addrMatch = text.match(/(\d+\s+(?:block of\s+)?[A-Z][a-z]+(?:\s+[A-Z]?[a-z]+)*\s+(?:St|Ave|Blvd|Dr|Rd|Hwy|Highway|Road|Street|Avenue|Drive|Boulevard)\b)/i);
    if (addrMatch) result.address = addrMatch[1];
    return result;
  }

  extractInjuryCount(text) {
    const match = text.match(/(\d+)\s*(?:people\s+)?(?:injur|hurt|hospitalized|wounded)/i);
    return match ? parseInt(match[1]) : (/injur|hurt/i.test(text) ? 1 : 0);
  }

  extractFatalityCount(text) {
    const match = text.match(/(\d+)\s*(?:people\s+)?(?:killed|dead|fatal|died|death)/i);
    return match ? parseInt(match[1]) : (/fatal|killed|dead|died/i.test(text) ? 1 : 0);
  }
}

module.exports = NewsMonitor;
