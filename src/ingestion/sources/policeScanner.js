/**
 * POLICE SCANNER ADAPTER
 * Monitors police/fire/EMS radio scanner feeds
 *
 * Primary providers:
 *   - Broadcastify (premium API) - $200-500/mo per metro
 *   - OpenMHz - Free/open source
 *   - Scanner Radio API
 *
 * How it works:
 *   1. Subscribes to live scanner audio streams for target metros
 *   2. Uses speech-to-text (OpenAI Whisper) to transcribe
 *   3. NLP extraction pulls accident details from transcriptions
 *   4. Outputs normalized incident records
 */

const BaseAdapter = require('./baseAdapter');
const { logger } = require('../../utils/logger');

class PoliceScanner extends BaseAdapter {
  async fetch() {
    const provider = this.config.provider || 'broadcastify';
    let records = [];

    switch (provider) {
      case 'broadcastify':
        records = await this.fetchBroadcastify();
        break;
      case 'openmhz':
        records = await this.fetchOpenMHz();
        break;
      case 'transcription_webhook':
        // Records come via webhook POST to our API - fetch from queue
        records = await this.fetchFromQueue();
        break;
      default:
        records = await this.fetchBroadcastify();
    }

    // Filter for accident-related transmissions
    return records.filter(r => this.isAccidentRelated(r));
  }

  async fetchBroadcastify() {
    try {
      const apiKey = process.env.BROADCASTIFY_API_KEY;
      if (!apiKey) { logger.warn('Broadcastify API key not configured'); return []; }

      // Broadcastify Calls API - fetches recent transcribed calls
      const feedIds = this.config.feed_ids || [];
      const results = [];

      for (const feedId of feedIds) {
        const data = await this.apiRequest(
          `https://api.broadcastify.com/calls/v1/feeds/${feedId}/calls`,
          {
            headers: { 'Authorization': `Bearer ${apiKey}` },
            params: {
              since: this.source.last_polled_at || new Date(Date.now() - 5 * 60000).toISOString(),
              limit: 100
            }
          }
        );

        if (data?.calls) {
          results.push(...data.calls.map(call => ({
            ...call,
            feed_id: feedId,
            provider: 'broadcastify'
          })));
        }
      }
      return results;
    } catch (err) {
      logger.error('Broadcastify fetch error:', err.message);
      return [];
    }
  }

  async fetchOpenMHz() {
    try {
      const systems = this.config.systems || [];
      const results = [];

      for (const system of systems) {
        const data = await this.apiRequest(
          `https://api.openmhz.com/${system}/calls`,
          {
            params: {
              time: Math.floor((this.source.last_polled_at || new Date(Date.now() - 5 * 60000)).getTime() / 1000),
              filter_type: 'group',
              filter_code: this.config.talkgroups?.join(',') || ''
            }
          }
        );

        if (data?.calls) {
          results.push(...data.calls.map(call => ({ ...call, system, provider: 'openmhz' })));
        }
      }
      return results;
    } catch (err) {
      logger.error('OpenMHz fetch error:', err.message);
      return [];
    }
  }

  async fetchFromQueue() {
    // For webhook-delivered transcriptions stored in our DB/Redis queue
    const db = require('../../config/database');
    const pending = await db('source_reports')
      .where('data_source_id', this.source.id)
      .whereNull('incident_id')
      .whereNull('processed_at')
      .orderBy('fetched_at', 'asc')
      .limit(50);

    return pending.map(r => ({ ...r.raw_data, _queue_id: r.id }));
  }

  isAccidentRelated(record) {
    const text = (record.transcription || record.transcript || record.text || '').toLowerCase();
    const accidentKeywords = [
      'accident', 'collision', 'crash', 'mva', 'mvc', 'motor vehicle',
      'hit and run', 'rollover', 'overturn', 'pin-in', 'pinned',
      'extrication', 'entrapment', 'injury', 'injuries', 'trauma',
      'pedestrian struck', 'bicycle', 'motorcycle down',
      'doa', 'fatal', 'ejected', 'airbag deploy',
      'ambulance needed', 'ems enroute', 'rescue',
      'personal injury', 'pi accident', 'injury accident',
      'work.?place', 'construction.*injury', 'fall.*injur',
      'signal 4', 'signal 7', '10-50', '10-45', '10-46'
    ];

    return accidentKeywords.some(kw => new RegExp(kw, 'i').test(text));
  }

  normalize(raw) {
    const text = raw.transcription || raw.transcript || raw.text || '';
    const location = this.extractLocation(text);

    return {
      source_reference: raw.id || raw.call_id || `scanner_${Date.now()}`,
      incident_type: this.classifyIncidentType(text),
      severity: this.classifySeverity(text),
      confidence: 25, // Scanner is low confidence, needs corroboration

      address: location.address,
      intersection: location.intersection,
      city: location.city || this.config.default_city,
      state: location.state || this.config.default_state,
      latitude: raw.lat || raw.latitude,
      longitude: raw.lon || raw.lng || raw.longitude,

      occurred_at: raw.timestamp ? new Date(raw.timestamp * 1000) : new Date(),
      reported_at: new Date(),

      description: `[Scanner] ${text.substring(0, 500)}`,

      ems_dispatched: /ems|ambulance|medic|rescue/i.test(text),
      helicopter_dispatched: /helicopter|air.?ambulance|life.?flight|med.?flight|star.?flight/i.test(text),
      extrication_needed: /extrication|entrap|pin.?in|jaws of life/i.test(text),

      responding_agencies: this.extractAgencies(text),
      dispatch_codes: this.extractDispatchCodes(text),

      injuries_count: this.extractInjuryCount(text),
      vehicles_involved: this.extractVehicleCount(text),
      fatalities_count: /fatal|doa|dead on arrival|deceased|signal 7/i.test(text) ? 1 : 0,

      tags: ['scanner', raw.provider || 'broadcastify']
    };
  }

  extractLocation(text) {
    const result = { address: null, intersection: null, city: null, state: null };

    // Look for intersection pattern
    const intersectionMatch = text.match(/(?:at|near|intersection of)\s+([^,]+\s+(?:and|&|at)\s+[^,]+)/i);
    if (intersectionMatch) result.intersection = intersectionMatch[1].trim();

    // Look for address pattern
    const addressMatch = text.match(/(\d+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:St|Ave|Blvd|Dr|Rd|Ln|Way|Ct|Pl|Hwy|Highway|Road|Street|Avenue|Drive|Boulevard|Lane|Circle|Pkwy|Parkway)\.?)/i);
    if (addressMatch) result.address = addressMatch[1].trim();

    // Highway/interstate
    const hwMatch = text.match(/(I-?\d+|(?:Interstate|Highway|US|SR|State Route)\s*\d+)/i);
    if (hwMatch) result.address = result.address || hwMatch[1];

    return result;
  }

  extractAgencies(text) {
    const agencies = [];
    if (/police|pd|officer|deputy|sheriff/i.test(text)) agencies.push('police');
    if (/fire|fd|engine|truck|ladder/i.test(text)) agencies.push('fire');
    if (/ems|ambulance|medic|paramedic|rescue/i.test(text)) agencies.push('ems');
    if (/highway patrol|trooper|state patrol|chp|gsp/i.test(text)) agencies.push('highway_patrol');
    return agencies;
  }

  extractDispatchCodes(text) {
    const codes = [];
    const codePatterns = text.match(/\b(10-\d{1,3}|signal \d{1,2}|code \d{1,2})\b/gi);
    if (codePatterns) codes.push(...codePatterns);
    return codes;
  }

  extractInjuryCount(text) {
    const match = text.match(/(\d+)\s*(?:injur|patient|victim|subject)/i);
    if (match) return parseInt(match[1]);
    if (/multiple.*injur|multiple.*patient/i.test(text)) return 3;
    if (/injur|patient|victim/i.test(text)) return 1;
    return 0;
  }

  extractVehicleCount(text) {
    const match = text.match(/(\d+)\s*(?:vehicle|car|auto)/i);
    if (match) return parseInt(match[1]);
    if (/multi.?vehicle|pile.?up|chain.?reaction/i.test(text)) return 3;
    return 2;
  }
}

module.exports = PoliceScanner;
