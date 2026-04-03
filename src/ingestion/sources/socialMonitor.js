/**
 * SOCIAL MEDIA MONITOR - Twitter/X, Reddit, Waze, Citizen app
 */
const BaseAdapter = require('./baseAdapter');
const { logger } = require('../../utils/logger');

class SocialMonitor extends BaseAdapter {
  async fetch() {
    const results = [];
    if (process.env.TWITTER_BEARER_TOKEN) results.push(...await this.fetchTwitter());
    if (process.env.REDDIT_CLIENT_ID) results.push(...await this.fetchReddit());
    if (this.config.waze_partner_feed) results.push(...await this.fetchWaze());
    return results;
  }

  async fetchTwitter() {
    try {
      const data = await this.apiRequest('https://api.twitter.com/2/tweets/search/recent', {
        headers: { 'Authorization': `Bearer ${process.env.TWITTER_BEARER_TOKEN}` },
        params: {
          query: '(accident OR crash OR wreck OR collision) (injury OR injured OR fatal OR killed) -is:retweet',
          max_results: 50,
          'tweet.fields': 'created_at,geo,entities',
          expansions: 'geo.place_id'
        }
      });
      return (data?.data || []).map(t => ({ ...t, provider: 'twitter' }));
    } catch (err) {
      logger.error('Twitter error:', err.message);
      return [];
    }
  }

  async fetchReddit() {
    try {
      // Get OAuth token
      const auth = Buffer.from(`${process.env.REDDIT_CLIENT_ID}:${process.env.REDDIT_CLIENT_SECRET}`).toString('base64');
      const tokenResp = await this.apiRequest('https://www.reddit.com/api/v1/access_token', {
        method: 'POST',
        headers: { 'Authorization': `Basic ${auth}`, 'Content-Type': 'application/x-www-form-urlencoded' },
        body: 'grant_type=client_credentials'
      });

      const cities = ['atlanta', 'houston', 'dallas', 'miami', 'chicago', 'losangeles', 'phoenix', 'nyc', 'philadelphia', 'sanantonio'];
      const results = [];
      for (const city of cities) {
        const data = await this.apiRequest(`https://oauth.reddit.com/r/${city}/search`, {
          headers: { 'Authorization': `Bearer ${tokenResp.access_token}`, 'User-Agent': 'AccidentIntel/1.0' },
          params: { q: 'accident crash injury', sort: 'new', t: 'day', limit: 10 }
        });
        if (data?.data?.children) {
          results.push(...data.data.children.map(c => ({ ...c.data, provider: 'reddit', city })));
        }
      }
      return results;
    } catch (err) {
      logger.error('Reddit error:', err.message);
      return [];
    }
  }

  async fetchWaze() {
    try {
      const data = await this.apiRequest(this.config.waze_partner_feed, {
        headers: this.config.waze_headers || {}
      });
      return (data?.alerts || [])
        .filter(a => a.type === 'ACCIDENT' || a.subtype?.includes('ACCIDENT'))
        .map(a => ({ ...a, provider: 'waze' }));
    } catch (err) {
      logger.error('Waze error:', err.message);
      return [];
    }
  }

  normalize(raw) {
    const text = raw.text || raw.title || raw.selftext || raw.street || '';
    return {
      source_reference: raw.id || `social_${Date.now()}`,
      incident_type: this.classifyIncidentType(text),
      severity: this.classifySeverity(text),
      confidence: 15,
      address: raw.street || raw.location,
      city: raw.city || this.config.default_city,
      state: this.config.default_state,
      latitude: raw.location?.y || raw.lat,
      longitude: raw.location?.x || raw.lng,
      occurred_at: raw.created_at ? new Date(raw.created_at) : new Date(),
      description: `[Social] ${text.substring(0, 300)}`,
      tags: ['social', raw.provider]
    };
  }
}

module.exports = SocialMonitor;
