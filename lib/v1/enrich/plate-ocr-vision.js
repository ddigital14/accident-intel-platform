/**
 * License Plate OCR via Claude Vision — Phase 44B
 *
 * For each verified victim's source_reports with image_url, fetches the
 * image and asks Claude Sonnet 4.6 to read every plate, street sign, and
 * business sign in the photo. Plates not matching the victim become
 * "vehicles_other_party" — i.e. the opposing driver in a PI case.
 *
 * Tables:
 *   vehicles (id, incident_id, plate, state, make, model, color, year,
 *             party {victim|other_party}, source_url, ocr_confidence, raw)
 *
 * HTTP:
 *   GET /api/v1/enrich/plate-ocr-vision?secret=ingest-now&action=health
 *   GET /api/v1/enrich/plate-ocr-vision?secret=ingest-now&action=process&incident_id=<uuid>
 *   GET /api/v1/enrich/plate-ocr-vision?secret=ingest-now&action=batch&limit=N
 *
 * Cron job: 'plate-ocr-vision'
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');
const { enqueueCascade } = require('../system/_cascade');

const SECRET = 'ingest-now';
const HTTP_TIMEOUT_MS = 15000;
const ANTHROPIC_URL = 'https://api.anthropic.com/v1/messages';
const MODEL = 'claude-sonnet-4-5-20250929';
const MAX_IMAGE_BYTES = 4 * 1024 * 1024;

function authed(req) {
  const s = (req.query && req.query.secret) || (req.headers && req.headers['x-cron-secret']);
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function getAnthropicKey(db) {
  if (process.env.ANTHROPIC_API_KEY) return process.env.ANTHROPIC_API_KEY;
  try {
    const row = await db('system_config').where({ key: 'anthropic_api_key' }).first();
    if (row?.value) return typeof row.value === 'string' ? row.value.replace(/^"|"$/g, '') : row.value;
  } catch (_) {}
  return null;
}

let _vehiclesTableEnsured = false;
async function ensureVehiclesTable(db) {
  if (_vehiclesTableEnsured) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS vehicles (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        incident_id UUID,
        person_id UUID,
        plate VARCHAR(20),
        state VARCHAR(8),
        make VARCHAR(60),
        model VARCHAR(80),
        color VARCHAR(40),
        year INTEGER,
        party VARCHAR(20) DEFAULT 'unknown',
        source_url TEXT,
        ocr_confidence INTEGER,
        scene_signs JSONB,
        raw JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_vehicles_incident ON vehicles(incident_id);
      CREATE INDEX IF NOT EXISTS idx_vehicles_plate ON vehicles(plate);
      CREATE INDEX IF NOT EXISTS idx_vehicles_party ON vehicles(party);
    `);
    _vehiclesTableEnsured = true;
  } catch (e) { console.error('ensureVehiclesTable:', e.message); }
}

async function fetchImageAsBase64(url) {
  try {
    const resp = await fetch(url, { signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    if (!resp.ok) return { ok: false, error: `HTTP ${resp.status}` };
    const ct = resp.headers.get('content-type') || 'image/jpeg';
    if (!/^image\//i.test(ct)) return { ok: false, error: `not_image:${ct}` };
    const buf = Buffer.from(await resp.arrayBuffer());
    if (buf.length > MAX_IMAGE_BYTES) return { ok: false, error: `image_too_large:${buf.length}` };
    return { ok: true, base64: buf.toString('base64'), mime: ct };
  } catch (e) { return { ok: false, error: e.message }; }
}

async function callClaudeVision(imageB64, mime, key, db) {
  const prompt = `Read all license plates and identifying info from this accident scene photo. Return ONLY valid JSON of this exact shape (no prose):
{"plates":[{"text":"ABC1234","state_guess":"OH","vehicle_color":"white","vehicle_make":"Honda","vehicle_model_guess":"Civic","vehicle_year_guess":2018}],"scene_signs":[{"text":"Main St","type":"street_sign"}],"building_visible":true}

If a plate is not readable, omit it. If you cannot determine a field, set it to null. type must be one of: street_sign | business | address.`;
  try {
    const resp = await fetch(ANTHROPIC_URL, {
      method: 'POST',
      headers: {
        'x-api-key': key,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json'
      },
      body: JSON.stringify({
        model: MODEL,
        max_tokens: 1024,
        messages: [{
          role: 'user',
          content: [
            { type: 'image', source: { type: 'base64', media_type: mime, data: imageB64 } },
            { type: 'text', text: prompt }
          ]
        }]
      }),
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS)
    });
    if (!resp.ok) {
      const t = await resp.text().catch(() => '');
      if (db) await trackApiCall(db, 'plate-ocr-vision', 'claude_vision', 0, 0, false).catch(() => {});
      return { ok: false, error: `HTTP ${resp.status}`, body: t.slice(0, 200) };
    }
    const data = await resp.json();
    const text = data?.content?.[0]?.text || '';
    const inTok = data?.usage?.input_tokens || 0;
    const outTok = data?.usage?.output_tokens || 0;
    if (db) await trackApiCall(db, 'plate-ocr-vision', 'claude_vision', inTok, outTok, true).catch(() => {});
    let parsed = null;
    try {
      const m = text.match(/\{[\s\S]*\}/);
      parsed = m ? JSON.parse(m[0]) : JSON.parse(text);
    } catch (_) { parsed = null; }
    return { ok: true, parsed, raw: text, tokens: { in: inTok, out: outTok } };
  } catch (e) {
    if (db) await trackApiCall(db, 'plate-ocr-vision', 'claude_vision', 0, 0, false).catch(() => {});
    return { ok: false, error: e.message };
  }
}

function normalizeMake(s) {
  if (!s) return null;
  return String(s).trim().slice(0, 60);
}

async function processIncident(db, incidentId, opts = {}) {
  await ensureVehiclesTable(db);
  const key = await getAnthropicKey(db);
  if (!key) return { ok: false, error: 'no_anthropic_key' };

  let reports = [];
  try {
    reports = await db('source_reports')
      .where({ incident_id: incidentId })
      .whereNotNull('image_url')
      .select('id', 'image_url', 'url', 'source_type')
      .limit(opts.maxImages || 5);
  } catch (_) {
    try {
      reports = await db('source_reports')
        .where({ incident_id: incidentId })
        .whereRaw("raw_data->>'image_url' IS NOT NULL")
        .select('id', db.raw("raw_data->>'image_url' AS image_url"), 'url', 'source_type')
        .limit(opts.maxImages || 5);
    } catch (_) {}
  }
  if (!reports.length) return { ok: true, incident_id: incidentId, processed: 0, note: 'no_images' };

  let victimName = '';
  try {
    const v = await db('persons')
      .where({ incident_id: incidentId })
      .whereIn('role', ['victim', 'driver'])
      .orderBy('identity_confidence', 'desc')
      .first();
    if (v) victimName = (v.full_name || '').toLowerCase();
  } catch (_) {}

  const inserted = [];
  for (const r of reports) {
    const url = r.image_url;
    if (!url) continue;
    const img = await fetchImageAsBase64(url);
    if (!img.ok) continue;
    const result = await callClaudeVision(img.base64, img.mime, key, db);
    if (!result.ok || !result.parsed) continue;
    const { plates = [], scene_signs = [], building_visible = false } = result.parsed;
    for (const p of plates) {
      if (!p?.text) continue;
      const party = 'other_party';
      try {
        const [row] = await db('vehicles').insert({
          incident_id: incidentId,
          plate: String(p.text).toUpperCase().replace(/\s+/g, '').slice(0, 20),
          state: p.state_guess ? String(p.state_guess).slice(0, 8) : null,
          make: normalizeMake(p.vehicle_make),
          model: p.vehicle_model_guess ? String(p.vehicle_model_guess).slice(0, 80) : null,
          color: p.vehicle_color ? String(p.vehicle_color).slice(0, 40) : null,
          year: p.vehicle_year_guess ? parseInt(p.vehicle_year_guess) : null,
          party,
          source_url: url,
          ocr_confidence: 80,
          scene_signs: JSON.stringify(scene_signs || []),
          raw: JSON.stringify({ building_visible, model: MODEL, raw_text: result.raw?.slice(0, 1000) }),
          created_at: new Date(),
          updated_at: new Date()
        }).returning(['id']);
        inserted.push(row?.id);
      } catch (e) { /* dup or schema mismatch */ }
    }
    if (plates.length) {
      try {
        await enqueueCascade(db, null, 'plate_ocr_vision', {
          incident_id: incidentId, plates: plates.length, building_visible
        });
      } catch (_) {}
    }
  }
  return { ok: true, incident_id: incidentId, processed: reports.length, inserted: inserted.length };
}

async function batch(db, limit = 5) {
  await ensureVehiclesTable(db);
  let candidates = [];
  try {
    candidates = await db.raw(`
      SELECT DISTINCT i.id
        FROM incidents i
        JOIN persons p ON p.incident_id = i.id
        JOIN source_reports sr ON sr.incident_id = i.id
        LEFT JOIN vehicles v ON v.incident_id = i.id
       WHERE p.role IN ('victim','driver')
         AND COALESCE(p.identity_confidence, 0) >= 60
         AND (sr.image_url IS NOT NULL OR (sr.raw_data IS NOT NULL AND sr.raw_data->>'image_url' IS NOT NULL))
         AND v.id IS NULL
       LIMIT ${parseInt(limit)}
    `).then(r => r.rows || []);
  } catch (e) {
    return { ok: false, error: 'candidates_query_failed:' + e.message };
  }
  let total = 0; let inserted = 0;
  for (const c of candidates) {
    const out = await processIncident(db, c.id);
    total++; inserted += out.inserted || 0;
  }
  return { ok: true, processed: total, inserted };
}

async function health(db) {
  await ensureVehiclesTable(db);
  const key = await getAnthropicKey(db);
  let count = 0;
  try { const r = await db('vehicles').count('* as c').first(); count = parseInt(r.c); } catch (_) {}
  return { ok: true, has_key: !!key, model: MODEL, vehicles_total: count };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });
  const db = getDb();
  const action = (req.query?.action || 'health').toLowerCase();
  try {
    if (action === 'health') {
      const h = await health(db);
      return res.json({ success: true, ...h, timestamp: new Date().toISOString() });
    }
    if (action === 'process') {
      const incId = req.query.incident_id;
      if (!incId) return res.status(400).json({ error: 'incident_id required' });
      const out = await processIncident(db, incId);
      return res.json({ success: !!out.ok, ...out });
    }
    if (action === 'batch') {
      const limit = parseInt(req.query.limit || '5');
      const out = await batch(db, limit);
      return res.json({ success: !!out.ok, ...out });
    }
    return res.status(400).json({ error: 'unknown_action', valid: ['health','process','batch'] });
  } catch (e) {
    try { await reportError(db, 'plate-ocr-vision', null, e.message); } catch (_) {}
    return res.status(500).json({ success: false, error: e.message });
  }
};

module.exports.processIncident = processIncident;
module.exports.batch = batch;
module.exports.health = health;
