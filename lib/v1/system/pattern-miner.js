/**
 * Phase 63: Pattern Miner — surfaces leading indicators from historical incidents
 * to boost lead scores BEFORE enrichment runs.
 *
 * Examples it detects:
 *  - "tow company X mentioned" → days-to-contact for those incidents
 *  - severity words ("ejected", "extricated", "med-flighted") → conversion rate
 *  - state + day-of-week patterns
 *  - source patterns (CourtListener mentions = lawyer-already-engaged → DROP)
 *  - article length proxies
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');

const SECRET = 'ingest-now';
const MIN_SAMPLE_SIZE = 3;
const MAX_DELTA = 30;

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

let _migrated = false;
async function ensureSchema(db) {
  if (_migrated) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS lead_score_signals (
        signal_type VARCHAR(64) NOT NULL,
        pattern VARCHAR(128) NOT NULL,
        sample_size INT DEFAULT 0,
        conversion_rate FLOAT DEFAULT 0,
        avg_days_to_contact INT,
        suggested_score_delta INT DEFAULT 0,
        last_computed_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (signal_type, pattern)
      );
      CREATE INDEX IF NOT EXISTS idx_lss_type ON lead_score_signals(signal_type);
    `);
    _migrated = true;
  } catch (e) { console.error('[pattern-miner] migration:', e.message); }
}

const SEVERITY_WORDS = ['fatal','killed','died','deceased','dead','ejected','extricated','med-flighted','medflight','life-flight','lifeflight','critical','life-threatening','intubated','airlift','airlifted','trapped','rolled','overturned','head-on','rollover','t-boned','rear-ended','wrong-way','dui','drunk','impaired','reckless'];
const TOW_REGEX = /\b(towing|tow|wrecker|recovery|hauling)\b/i;

function extractPatterns(inc) {
  const keys = [];
  const desc = (inc.description || '').toLowerCase();
  const src = '';
  const state = inc.state || '';
  const dow = inc.discovered_at ? ['sun','mon','tue','wed','thu','fri','sat'][new Date(inc.discovered_at).getDay()] : null;

  // 1. Severity words
  for (const w of SEVERITY_WORDS) {
    if (desc.includes(w)) keys.push(`severity_word::${w}`);
  }

  // 2. Tow company mention
  if (TOW_REGEX.test(desc)) keys.push('tow_company::mentioned');

  // 3. State + DOW
  if (state && dow) keys.push(`state_dow::${state}_${dow}`);

  // 4. Source
  if (src) keys.push(`source::${src}`);
  if (/courtlistener|court.*listener/i.test(desc) || src.includes('courtlistener')) {
    keys.push('source::courtlistener_hit');
  }

  // 5. Article length bucket
  const len = desc.length;
  const bucket = len < 200 ? 'short' : len < 600 ? 'medium' : len < 1500 ? 'long' : 'xlong';
  keys.push(`article_len::${bucket}`);

  // 6. Incident type from description
  for (const t of ['collision','crash','accident','rollover','pedestrian','motorcycle','dui','hit-and-run','head-on','rear-end']) {
    if (desc.includes(t)) keys.push(`incident_type::${t}`);
  }

  return keys;
}

async function mineSignals(db, lookbackDays = 90) {
  await ensureSchema(db);
  const startedAt = Date.now();
  const cutoff = new Date(Date.now() - lookbackDays * 86400000);

  const incidents = await db('incidents')
    .where('discovered_at', '>', cutoff)
    .select('id', 'discovered_at', 'state', 'description', 'fatalities_count', 'severity');

  if (!incidents.length) return { ok: true, signals_computed: 0, message: 'no incidents in window' };

  // Pull all persons for these incidents
  const incIds = incidents.map(i => i.id);
  const persons = await db('persons')
    .whereIn('incident_id', incIds)
    .select('id', 'incident_id', 'phone', 'email', 'created_at');

  const personsByIncident = {};
  for (const p of persons) {
    (personsByIncident[p.incident_id] = personsByIncident[p.incident_id] || []).push(p);
  }

  // Per-incident: did it produce a contact, and how long?
  let totalIncidents = 0;
  let totalConverted = 0;
  const incOutcomes = incidents.map(inc => {
    const ps = personsByIncident[inc.id] || [];
    const hasContact = ps.some(p => p.phone || p.email);
    let daysToContact = null;
    if (hasContact && inc.discovered_at) {
      const earliestContact = ps.filter(p => p.phone || p.email).map(p => new Date(p.created_at).getTime()).sort()[0];
      daysToContact = Math.round((earliestContact - new Date(inc.discovered_at).getTime()) / 86400000);
    }
    totalIncidents++;
    if (hasContact) totalConverted++;
    return { inc, hasContact, daysToContact, patterns: extractPatterns(inc) };
  });

  const baselineConv = totalConverted / Math.max(1, totalIncidents);

  // Bucket incidents by pattern
  const buckets = {};
  for (const o of incOutcomes) {
    for (const k of o.patterns) {
      const b = buckets[k] = buckets[k] || { count: 0, converted: 0, days: [] };
      b.count++;
      if (o.hasContact) b.converted++;
      if (o.daysToContact != null) b.days.push(o.daysToContact);
    }
  }

  // Compute signal stats + UPSERT
  const signals = [];
  for (const [pattern, b] of Object.entries(buckets)) {
    if (b.count < MIN_SAMPLE_SIZE) continue;
    const [signal_type, ...rest] = pattern.split('::');
    const patternStr = rest.join('::');
    const conv = b.converted / b.count;
    const avgDays = b.days.length ? Math.round(b.days.reduce((a,c) => a+c, 0) / b.days.length) : null;
    let delta = Math.round((conv - baselineConv) * 50);
    delta = Math.max(-MAX_DELTA, Math.min(MAX_DELTA, delta));
    // Hard floor for courtlistener_hit (lawyer already engaged → DROP)
    if (pattern === 'source::courtlistener_hit') delta = Math.min(delta, -20);

    try {
      await db.raw(`
        INSERT INTO lead_score_signals (signal_type, pattern, sample_size, conversion_rate, avg_days_to_contact, suggested_score_delta, last_computed_at)
        VALUES (?, ?, ?, ?, ?, ?, NOW())
        ON CONFLICT (signal_type, pattern) DO UPDATE SET
          sample_size = EXCLUDED.sample_size,
          conversion_rate = EXCLUDED.conversion_rate,
          avg_days_to_contact = EXCLUDED.avg_days_to_contact,
          suggested_score_delta = EXCLUDED.suggested_score_delta,
          last_computed_at = NOW()
      `, [signal_type, patternStr, b.count, Number(conv.toFixed(3)), avgDays, delta]);
    } catch (_) {}

    signals.push({ signal_type, pattern: patternStr, sample_size: b.count, conversion_rate: conv, avg_days_to_contact: avgDays, suggested_score_delta: delta });
  }

  signals.sort((a, b) => Math.abs(b.suggested_score_delta) - Math.abs(a.suggested_score_delta));
  return {
    ok: true,
    incidents_analyzed: totalIncidents,
    baseline_conversion_rate: Number(baselineConv.toFixed(3)),
    signals_computed: signals.length,
    top_signals: signals.slice(0, 20),
    duration_ms: Date.now() - startedAt
  };
}

async function applySignals(db, incidentId) {
  await ensureSchema(db);
  const inc = await db('incidents').where('id', incidentId).first();
  if (!inc) return { ok: false, error: 'incident_not_found' };
  const patterns = extractPatterns(inc);
  if (!patterns.length) return { ok: true, adjusted_score_delta: 0, matched_signals: [] };

  const matched = await db.raw(`
    SELECT signal_type, pattern, sample_size, conversion_rate, suggested_score_delta
    FROM lead_score_signals
    WHERE (signal_type || '::' || pattern) = ANY(?)
  `, [patterns]).then(r => r.rows || []).catch(() => []);

  let total = 0;
  for (const m of matched) total += m.suggested_score_delta || 0;
  total = Math.max(-30, Math.min(30, total));
  return { ok: true, incident_id: incidentId, adjusted_score_delta: total, matched_signals: matched, raw_sum: matched.reduce((s,m) => s + (m.suggested_score_delta || 0), 0) };
}

async function listSignals(db) {
  await ensureSchema(db);
  return db('lead_score_signals')
    .orderByRaw('ABS(suggested_score_delta) DESC, sample_size DESC')
    .limit(200);
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  let db;
  try { db = getDb(); } catch (e) { return res.status(500).json({ error: 'db_unavailable' }); }
  const action = (req.query?.action || 'health').toLowerCase();

  if (action === 'health') return res.json({ success: true, service: 'pattern-miner' });

  if (action === 'mine') {
    const days = Math.min(365, parseInt(req.query?.lookback_days) || 90);
    try { return res.json(await mineSignals(db, days)); }
    catch (e) { await reportError(db, 'pattern-miner', null, e.message).catch(()=>{}); return res.status(500).json({ error: e.message }); }
  }

  if (action === 'signals') {
    return res.json({ success: true, signals: await listSignals(db) });
  }

  if (action === 'apply') {
    let body = req.body;
    if (!body || typeof body !== 'object') {
      body = await new Promise((resolve) => {
        let d=''; req.on('data', c=>d+=c);
        req.on('end', () => { try { resolve(JSON.parse(d || '{}')); } catch { resolve({}); } });
        req.on('error', () => resolve({}));
      });
    }
    const incidentId = body.incident_id || req.query?.incident_id;
    if (!incidentId) return res.status(400).json({ error: 'incident_id required' });
    return res.json(await applySignals(db, incidentId));
  }

  return res.status(400).json({ error: 'unknown action' });
}

module.exports = handler;
module.exports.handler = handler;
module.exports.mineSignals = mineSignals;
module.exports.applySignals = applySignals;
module.exports.extractPatterns = extractPatterns;
