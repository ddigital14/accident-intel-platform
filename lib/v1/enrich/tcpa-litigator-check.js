/**
 * TCPA LITIGATOR CHECK — Phone safety guard before SMS spend
 *
 * The Telephone Consumer Protection Act gives consumers $500-$1500 per
 * unsolicited text. Serial litigators farm SMS campaigns by leaking numbers
 * onto signup forms then suing senders. This engine refuses to send SMS to
 * known litigator numbers.
 *
 * Three combined checks:
 *   1. tcpalitigatorlist.com — public scraped list (free)
 *   2. Local court_records cross-ref — phone tied to plaintiff name in
 *      multiple TCPA filings already in our DB
 *   3. (Future) carrier/disposable-line patterns
 *
 * Result is cached in `tcpa_litigator_cache` for 30 days. notify.js calls
 * `checkLitigator(db, phone)` BEFORE Twilio Messaging spend; if
 * is_litigator OR risk_score>70, the SMS is blocked.
 *
 * Endpoints:
 *   GET /api/v1/enrich/tcpa-litigator-check?phone=+13055551212
 *   GET /api/v1/enrich/tcpa-litigator-check?action=refresh_list  (cron — daily)
 *   GET /api/v1/enrich/tcpa-litigator-check?action=health
 *
 * Cost: $0 (public scraping only). Safety guard, not identity evidence.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { logChange } = require('../system/changelog');
const { normalizePhone } = require('../../_schema');

const TCPA_SOURCES = [
  'https://tcpalitigatorlist.com/',
  'https://www.tcpaworld.com/litigator-list/'
];

let _ensured = false;
async function ensureTables(db) {
  if (_ensured) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS tcpa_litigator_cache (
        phone VARCHAR(20) PRIMARY KEY,
        is_litigator BOOLEAN DEFAULT FALSE,
        risk_score INTEGER DEFAULT 0,
        sources TEXT[] DEFAULT '{}',
        evidence JSONB DEFAULT '{}'::jsonb,
        checked_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_tcpa_cache_checked ON tcpa_litigator_cache(checked_at DESC);
      CREATE TABLE IF NOT EXISTS tcpa_known_litigators (
        id SERIAL PRIMARY KEY,
        phone VARCHAR(20) UNIQUE,
        plaintiff_name VARCHAR(200),
        case_count INTEGER DEFAULT 1,
        source VARCHAR(120),
        first_seen TIMESTAMPTZ DEFAULT NOW(),
        last_seen TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_tcpa_known_phone ON tcpa_known_litigators(phone);
    `);
    _ensured = true;
  } catch (e) { /* non-fatal */ }
}

/**
 * Refresh the local litigator list by scraping public sources. Best-effort
 * regex extraction of US phone numbers from rendered text.
 */
async function refreshLitigatorList(db) {
  await ensureTables(db);
  let inserted = 0;
  for (const url of TCPA_SOURCES) {
    try {
      const resp = await fetch(url, {
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; AIP/1.0; +https://accident-intel-platform.vercel.app)' },
        signal: AbortSignal.timeout(15000)
      });
      await trackApiCall(db, 'enrich-tcpa-check', 'tcpa_scrape', 0, 0, resp.ok);
      if (!resp.ok) continue;
      const html = await resp.text();
      const text = html.replace(/<script[\s\S]*?<\/script>/gi, '').replace(/<[^>]+>/g, ' ');
      const matches = text.match(/\(?\b[2-9]\d{2}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b/g) || [];
      const seen = new Set();
      for (const raw of matches) {
        const norm = normalizePhone(raw);
        if (!norm || seen.has(norm)) continue;
        seen.add(norm);
        try {
          await db('tcpa_known_litigators')
            .insert({ phone: norm, source: url, last_seen: new Date() })
            .onConflict('phone')
            .merge({ last_seen: new Date(), case_count: db.raw('tcpa_known_litigators.case_count + 1') });
          inserted++;
        } catch (_) {}
      }
    } catch (e) {
      await reportError(db, 'enrich-tcpa-check', url, e.message);
    }
  }
  return { ok: true, sources: TCPA_SOURCES.length, inserted };
}

/**
 * Local court records cross-check — same phone tied to TCPA plaintiff role.
 */
async function checkCourtRecords(db, phone) {
  try {
    const rows = await db.raw(`
      SELECT COUNT(*) as ct
      FROM source_reports
      WHERE source_type IN ('court_records','courtlistener')
        AND parsed_data::text ILIKE ?
        AND parsed_data::text ~* '(tcpa|telephone consumer protection)'
    `, [`%${phone}%`]).then(r => r.rows[0] || { ct: 0 });
    return { hits: parseInt(rows.ct || 0) };
  } catch (_) { return { hits: 0 }; }
}

/**
 * Public check — used by notify.js + handler. Returns:
 * { is_litigator, risk_score, sources, blocked, cached }.
 */
async function checkLitigator(db, phoneRaw) {
  await ensureTables(db);
  const phone = normalizePhone(phoneRaw);
  if (!phone) return { is_litigator: false, risk_score: 0, error: 'invalid_phone' };

  // Fresh cache (30d)
  try {
    const cached = await db('tcpa_litigator_cache')
      .where('phone', phone)
      .where('checked_at', '>', new Date(Date.now() - 30 * 86400000))
      .first();
    if (cached) {
      return {
        is_litigator: cached.is_litigator,
        risk_score: cached.risk_score,
        sources: cached.sources || [],
        cached: true,
        blocked: cached.is_litigator || cached.risk_score > 70
      };
    }
  } catch (_) {}

  let listHit = null;
  try { listHit = await db('tcpa_known_litigators').where('phone', phone).first(); } catch (_) {}
  const courtRes = await checkCourtRecords(db, phone);

  let risk = 0;
  const sources = [];
  const evidence = {};
  if (listHit) {
    risk += 80;
    sources.push('tcpalitigatorlist');
    evidence.list = { case_count: listHit.case_count, source: listHit.source };
  }
  if (courtRes.hits >= 2) {
    risk += 50;
    sources.push('court_records');
    evidence.court_hits = courtRes.hits;
  } else if (courtRes.hits === 1) {
    risk += 20;
    sources.push('court_records_single');
  }
  risk = Math.min(100, risk);
  const is_litigator = risk >= 70;

  try {
    await db('tcpa_litigator_cache')
      .insert({
        phone, is_litigator, risk_score: risk,
        sources, evidence: JSON.stringify(evidence),
        checked_at: new Date()
      })
      .onConflict('phone')
      .merge({ is_litigator, risk_score: risk, sources, evidence: JSON.stringify(evidence), checked_at: new Date() });
  } catch (_) {}

  await trackApiCall(db, 'enrich-tcpa-check', 'tcpa_scrape', 0, 0, true);

  return { is_litigator, risk_score: risk, sources, cached: false, blocked: is_litigator, evidence };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const db = getDb();
  const action = req.query.action || (req.query.phone ? 'check' : 'health');
  try {
    if (action === 'health') {
      let listSize = 0;
      try { const r = await db('tcpa_known_litigators').count('* as ct').first(); listSize = parseInt(r?.ct || 0); } catch (_) {}
      return res.status(200).json({ ok: true, engine: 'tcpa-litigator-check', list_size: listSize, sources: TCPA_SOURCES });
    }
    if (action === 'check' || req.query.phone) {
      const phone = req.query.phone;
      if (!phone) return res.status(400).json({ error: 'phone required' });
      const r = await checkLitigator(db, phone);
      return res.status(200).json({ phone, ...r });
    }
    if (action === 'refresh_list') {
      const r = await refreshLitigatorList(db);
      try { await logChange(db, { kind: 'pipeline', title: `tcpa-litigator: +${r.inserted} numbers`, ref: 'tcpa-litigator-check' }); } catch (_) {}
      return res.status(200).json({ success: true, ...r, timestamp: new Date().toISOString() });
    }
    return res.status(400).json({ error: 'unknown action', valid: ['health', 'check', 'refresh_list'] });
  } catch (e) {
    await reportError(db, 'enrich-tcpa-check', null, e.message);
    return res.status(500).json({ error: e.message });
  }
};

module.exports.checkLitigator = checkLitigator;
module.exports.refreshLitigatorList = refreshLitigatorList;
