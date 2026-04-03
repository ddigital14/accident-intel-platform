/**
 * RADIO FREQUENCY MONITOR
 * SDR-based monitoring of emergency radio frequencies
 * Providers: SDR hardware + trunk-recorder, RadioReference API
 */
const BaseAdapter = require('./baseAdapter');
const { logger } = require('../../utils/logger');

class RadioFrequency extends BaseAdapter {
  async fetch() {
    // RadioFrequency ingestion via trunk-recorder output files or webhook
    try {
      if (this.config.mode === 'trunk_recorder') {
        return this.fetchTrunkRecorder();
      }
      return this.fetchRadioReference();
    } catch (err) {
      logger.error('RadioFrequency fetch error:', err.message);
      return [];
    }
  }

  async fetchTrunkRecorder() {
    // trunk-recorder writes JSON files for each captured call
    const fs = require('fs').promises;
    const path = require('path');
    const dir = this.config.output_dir || '/data/trunk-recorder/calls';
    try {
      const files = await fs.readdir(dir);
      const jsonFiles = files.filter(f => f.endsWith('.json')).sort().slice(-100);
      const results = [];
      for (const file of jsonFiles) {
        const data = JSON.parse(await fs.readFile(path.join(dir, file), 'utf8'));
        if (this.isRelevantTalkgroup(data.talkgroup)) {
          results.push({ ...data, provider: 'trunk_recorder', _file: file });
        }
      }
      return results;
    } catch (err) {
      logger.warn('Trunk recorder dir not available:', err.message);
      return [];
    }
  }

  async fetchRadioReference() {
    try {
      const apiKey = this.config.radioreference_key;
      if (!apiKey) return [];
      const data = await this.apiRequest('https://api.radioreference.com/v1/calls', {
        headers: { 'Authorization': `Bearer ${apiKey}` },
        params: { county: this.config.county, since: this.source.last_polled_at?.toISOString() }
      });
      return (data?.calls || []).map(c => ({ ...c, provider: 'radioreference' }));
    } catch (err) {
      logger.error('RadioReference error:', err.message);
      return [];
    }
  }

  isRelevantTalkgroup(tg) {
    const emergencyTGs = this.config.emergency_talkgroups || [];
    return emergencyTGs.includes(tg);
  }

  normalize(raw) {
    const text = raw.transcription || raw.transcript || '';
    return {
      source_reference: raw.id || raw._file || `radio_${Date.now()}`,
      incident_type: this.classifyIncidentType(text),
      severity: this.classifySeverity(text),
      confidence: 20,
      address: raw.location,
      city: this.config.default_city,
      state: this.config.default_state,
      occurred_at: raw.timestamp ? new Date(raw.timestamp * 1000) : new Date(),
      description: `[Radio] TG${raw.talkgroup || '?'}: ${text.substring(0, 300)}`,
      ems_dispatched: /ems|medic|ambulance/i.test(text),
      tags: ['radio', raw.provider]
    };
  }
}

module.exports = RadioFrequency;
