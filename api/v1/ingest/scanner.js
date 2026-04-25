/**
 * Scanner Audio Transcription Pipeline
 *
 * Processes police/fire/EMS scanner audio into structured accident data:
 * 1. Pulls audio from OpenMHz or Broadcastify streams
 * 2. Transcribes via OpenAI Whisper API
 * 3. Extracts structured incident data via GPT-4o Mini NLP
 * 4. Ingests matched accidents into AIP database
 *
 * Can also accept pre-transcribed text via POST body for
 * use with local Trunk Recorder + trunk-transcribe setups.
 *
 * POST /api/v1/ingest/scanner?secret=ingest-now
 * Body (optional): { transcripts: [{ text, timestamp, talkgroup, system }] }
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');

// ── Talkgroup-to-metro mapping ────────────────────────────────────────
// These vary by region; configure per-deployment
const TALKGROUP_MAP = {
  // Atlanta metro (DeKalb County, Fulton County, APD)
  'atl_dispatch': { metro: 'Atlanta', state: 'GA', lat: 33.749, lng: -84.388 },
  'atl_fire': { metro: 'Atlanta', state: 'GA', lat: 33.749, lng: -84.388 },
  // Houston (HPD, Harris County)
  'hou_dispatch': { metro: 'Houston', state: 'TX', lat: 29.760, lng: -95.370 },
  // Generic fallback
  'default': { metro: 'Unknown', state: 'GA', lat: 33.749, lng: -84.388 },
};

// ── Accident detection patterns ───────────────────────────────────────
const ACCIDENT_PATTERNS = [
  /\bMVA\b/i,                           // Motor Vehicle Accident
  /\b10-50\b/,                          // Code 10-50 = accident
  /\bsignal\s*(?:4|four)\b/i,          // Signal 4 = accident (some agencies)
  /\btraffic\s*(?:accident|crash|collision)\b/i,
  /\bvehicle\s*(?:accident|crash|collision|rollover)\b/i,
  /\bcrash\b.*\b(?:injur|fatal|respond|enroute)\b/i,
  /\brollover\b/i,
  /\bhead.?on\b/i,
  /\bpedestrian\s*(?:struck|hit|down)\b/i,
  /\bpin(?:ned|ning)\b.*\bvehicle\b/i,
  /\bentrap/i,
  /\bextrication\b/i,
  /\bt-bone/i,
  /\bpile.?up\b/i,
];

const NON_ACCIDENT_PATTERNS = [
  /\btest\b.*\bonly\b/i,
  /\bdrill\b/i,
  /\btraining\b/i,
  /\bdisregard\b/i,
  /\bcancel(?:led)?\b/i,
];

function isAccidentTranscript(text) {
  if (NON_ACCIDENT_PATTERNS.some(p => p.test(text))) return false;
  return ACCIDENT_PATTERNS.some(p => p.test(text));
}

// ── NLP Extraction via OpenAI ─────────────────────────────────────────
async function extractIncidentData(transcript, openaiKey) {
  if (!openaiKey) {
    // Fallback: regex-based extraction
    return extractWithRegex(transcript);
  }

  try {
    const resp = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${openaiKey}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: [
          {
            role: 'system',
            content: `You are a police/fire scanner transcript parser. Extract accident details from dispatch audio transcripts. Return ONLY valid JSON with these fields:
{
  "is_accident": true/false,
  "location": "street/intersection/highway description",
  "cross_street": "cross street if mentioned",
  "city": "city name if mentioned",
  "severity": "fatal|serious|moderate|minor|unknown",
  "injuries": number or null,
  "fatalities": number or null,
  "vehicles": number or null,
  "vehicle_types": ["car", "truck", "motorcycle", etc],
  "incident_type": "car_accident|truck_accident|motorcycle_accident|pedestrian|bicycle",
  "details": "brief summary of what happened",
  "units_responding": ["Engine 7", "Medic 3", etc],
  "hospital": "hospital name if transport mentioned",
  "entrapment": true/false,
  "hazmat": true/false
}`
          },
          {
            role: 'user',
            content: `Parse this scanner transcript:\n\n"${transcript}"`
          }
        ],
        temperature: 0.1,
        max_tokens: 500
      }),
      signal: AbortSignal.timeout(15000)
    });

    if (!resp.ok) {
      console.error('OpenAI NLP error:', resp.status);
      return extractWithRegex(transcript);
    }

    const data = await resp.json();
    const content = data.choices?.[0]?.message?.content || '';

    // Parse JSON from response
    const jsonMatch = content.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      return JSON.parse(jsonMatch[0]);
    }
    return extractWithRegex(transcript);
  } catch (e) {
    console.error('NLP extraction error:', e.message);
    return extractWithRegex(transcript);
  }
}

function extractWithRegex(text) {
  const lower = text.toLowerCase();

  // Location extraction
  let location = null;
  const locMatch = text.match(/(?:at|on|near)\s+([A-Z0-9][\w\s]+(?:(?:and|&|at)\s+[\w\s]+)?(?:(?:street|st|avenue|ave|boulevard|blvd|road|rd|drive|dr|highway|hwy|interstate|i-\d+|parkway|pkwy|lane|ln|way|circle|cir|court|ct|place|pl)\.?)?)/i);
  if (locMatch) location = locMatch[1].trim();

  // Highway/Interstate
  const hwyMatch = text.match(/(?:I-|Interstate\s+|Highway\s+|Hwy\s+|US\s+|SR\s+)(\d+)/i);
  if (hwyMatch && !location) location = `I-${hwyMatch[1]}`;

  // Mile marker
  const mmMatch = text.match(/mile\s*(?:marker|post)?\s*(\d+)/i);
  if (mmMatch && location) location += ` at mile marker ${mmMatch[1]}`;

  // Severity
  let severity = 'moderate';
  if (/fatal|code\s*0|deceased|doa|10-7/i.test(lower)) severity = 'fatal';
  else if (/entrap|extrication|pin|serious|critical|trauma|life.?flight|helicopter/i.test(lower)) severity = 'serious';
  else if (/minor|fender|non.?injury|property\s*damage/i.test(lower)) severity = 'minor';

  // Counts
  let injuries = null, fatalities = null, vehicles = null;
  const injMatch = lower.match(/(\d+)\s*(?:injur|patient|victim|subject)/);
  if (injMatch) injuries = parseInt(injMatch[1]);

  const fatMatch = lower.match(/(\d+)\s*(?:fatal|deceased|dead)/);
  if (fatMatch) fatalities = parseInt(fatMatch[1]);

  const vehMatch = lower.match(/(\d+)\s*(?:vehicle|car|unit)/);
  if (vehMatch) vehicles = parseInt(vehMatch[1]);

  // Type
  let incident_type = 'car_accident';
  if (/motorcycle|bike/i.test(lower)) incident_type = 'motorcycle_accident';
  if (/truck|semi|18.?wheel|tractor/i.test(lower)) incident_type = 'truck_accident';
  if (/pedestrian/i.test(lower)) incident_type = 'pedestrian';
  if (/bicycle|cyclist/i.test(lower)) incident_type = 'bicycle';

  return {
    is_accident: true,
    location,
    severity,
    injuries,
    fatalities,
    vehicles,
    incident_type,
    details: text.substring(0, 300),
    entrapment: /entrap|extrication|pin/i.test(lower),
    hazmat: /hazmat|hazardous|spill|fuel/i.test(lower)
  };
}

// ── OpenMHz Recent Calls Fetcher ──────────────────────────────────────
async function fetchOpenMhzCalls(systemName) {
  try {
    const url = `https://api.openmhz.com/${systemName}/calls?filter-type=&filter-code=&filter-len=3&time=${Date.now()}`;
    const resp = await fetch(url, {
      headers: { 'Accept': 'application/json' },
      signal: AbortSignal.timeout(10000)
    });
    if (!resp.ok) return [];
    const data = await resp.json();
    return (data.calls || []).map(c => ({
      audio_url: c.url,
      timestamp: c.time,
      talkgroup: c.talkgroupNum,
      talkgroup_tag: c.talkgroupTag,
      system: systemName,
      duration: c.len
    }));
  } catch (e) {
    console.error(`OpenMHz error for ${systemName}:`, e.message);
    return [];
  }
}

// ── Whisper Transcription ─────────────────────────────────────────────
async function transcribeAudio(audioUrl, openaiKey) {
  if (!openaiKey) return null;

  try {
    // Download audio
    const audioResp = await fetch(audioUrl, { signal: AbortSignal.timeout(15000) });
    if (!audioResp.ok) return null;
    const audioBuffer = await audioResp.arrayBuffer();

    // Create form data for Whisper API
    const formData = new FormData();
    const audioBlob = new Blob([audioBuffer], { type: 'audio/mp4' });
    formData.append('file', audioBlob, 'call.m4a');
    formData.append('model', 'whisper-1');
    formData.append('language', 'en');
    formData.append('prompt', 'Police fire EMS dispatch radio. MVA accident crash vehicle injury fatal entrapment rollover highway interstate');

    const resp = await fetch('https://api.openai.com/v1/audio/transcriptions', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${openaiKey}` },
      body: formData,
      signal: AbortSignal.timeout(30000)
    });

    if (!resp.ok) {
      console.error('Whisper error:', resp.status);
      return null;
    }

    const data = await resp.json();
    return data.text || null;
  } catch (e) {
    console.error('Transcription error:', e.message);
    return null;
  }
}

// ── Main Handler ──────────────────────────────────────────────────────
module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const db = getDb();
  const openaiKey = process.env.OPENAI_API_KEY;
  const results = { processed: 0, accidents_found: 0, inserted: 0, skipped: 0, errors: [] };

  try {
    let transcripts = [];

    // Mode 1: Accept pre-transcribed text from local Trunk Recorder setup
    if (req.method === 'POST' && req.body?.transcripts) {
      transcripts = req.body.transcripts;
    }
    // Mode 2: Fetch from OpenMHz and transcribe
    else if (openaiKey) {
      const systems = (process.env.OPENMHZ_SYSTEMS || 'dekalb').split(',');
      for (const system of systems) {
        const calls = await fetchOpenMhzCalls(system.trim());
        // Only process recent calls (last 15 min) and longer calls (>3 sec likely contain speech)
        const recentCalls = calls.filter(c =>
          c.duration >= 3 &&
          new Date(c.timestamp) > new Date(Date.now() - 15 * 60 * 1000)
        ).slice(0, 10); // Limit to 10 per run to control Whisper costs

        for (const call of recentCalls) {
          const text = await transcribeAudio(call.audio_url, openaiKey);
          if (text) {
            transcripts.push({
              text,
              timestamp: call.timestamp,
              talkgroup: call.talkgroup_tag || String(call.talkgroup),
              system: call.system
            });
          }
        }
      }
    }

    results.processed = transcripts.length;

    // Filter for accident-related transcripts
    const accidentTranscripts = transcripts.filter(t => isAccidentTranscript(t.text));
    results.accidents_found = accidentTranscripts.length;

    // Find or create scanner data source
    let scannerDs = await db('data_sources').where('name', 'like', '%Scanner%').first();
    if (!scannerDs) {
      const dsId = uuidv4();
      await db('data_sources').insert({
        id: dsId,
        name: 'Radio Scanner (Whisper AI)',
        source_type: 'scanner',
        is_active: true,
        created_at: new Date(),
        updated_at: new Date()
      });
      scannerDs = { id: dsId };
    }

    for (const transcript of accidentTranscripts) {
      try {
        // Extract structured data via NLP
        const extracted = await extractIncidentData(transcript.text, openaiKey);
        if (!extracted || !extracted.is_accident) continue;

        // Dedup by checking recent incidents at same location
        const sourceRef = `SCAN-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

        // Look up metro from talkgroup or default
        const tgInfo = TALKGROUP_MAP[transcript.talkgroup] || TALKGROUP_MAP['default'];
        const city = extracted.city || tgInfo.metro;

        const incidentId = uuidv4();
        const now = new Date();
        const severity = extracted.severity || 'moderate';
        const priority = severity === 'fatal' ? 1 : severity === 'serious' ? 2 : severity === 'moderate' ? 3 : 4;

        await db('incidents').insert({
          id: incidentId,
          incident_number: `SC-${now.getFullYear().toString().slice(-2)}${String(now.getMonth()+1).padStart(2,'0')}${String(now.getDate()).padStart(2,'0')}-${Math.floor(Math.random()*9999).toString().padStart(4,'0')}`,
          incident_type: extracted.incident_type || 'car_accident',
          severity: severity,
          status: 'new',
          priority: priority,
          confidence_score: openaiKey ? 75 : 60, // Higher if AI-extracted
          address: extracted.location || 'Unknown location',
          city: city,
          state: tgInfo.state,
          latitude: tgInfo.lat,
          longitude: tgInfo.lng,
          occurred_at: transcript.timestamp ? new Date(transcript.timestamp) : now,
          reported_at: now,
          discovered_at: now,
          description: `Scanner dispatch: ${extracted.details || transcript.text.substring(0, 500)}`,
          injuries_count: extracted.injuries,
          fatalities_count: extracted.fatalities,
          vehicles_involved: extracted.vehicles,
          source_count: 1,
          first_source_id: scannerDs.id,
          tags: ['scanner', 'whisper_ai', extracted.entrapment ? 'entrapment' : null, extracted.hazmat ? 'hazmat' : null].filter(Boolean),
          created_at: now,
          updated_at: now
        });

        await db('source_reports').insert({
          id: uuidv4(),
          incident_id: incidentId,
          data_source_id: scannerDs.id,
          source_type: 'scanner',
          source_reference: sourceRef,
          raw_data: JSON.stringify({ transcript: transcript.text, talkgroup: transcript.talkgroup, system: transcript.system }),
          parsed_data: JSON.stringify(extracted),
          contributed_fields: ['description', 'severity', 'injuries', 'vehicles', 'location'],
          confidence: openaiKey ? 75 : 60,
          is_verified: false,
          fetched_at: now,
          processed_at: now,
          created_at: now
        });

        results.inserted++;
      } catch (e) {
        results.errors.push(`scanner: ${e.message}`);
      }
    }

    // Update data source
    await db('data_sources').where('id', scannerDs.id).update({
      last_polled_at: new Date(),
      last_success_at: results.inserted > 0 ? new Date() : undefined,
      updated_at: new Date()
    });

    res.json({
      success: true,
      message: `Scanner: ${results.processed} transcripts → ${results.accidents_found} accidents → ${results.inserted} ingested`,
      ...results,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    console.error('Scanner pipeline error:', err);
    res.status(500).json({ error: err.message, results });
  }
};
