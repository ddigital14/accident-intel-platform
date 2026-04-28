/**
 * VOTER ROLLS UPLOAD — Phase 40 Module 1
 *
 * Streaming, state-aware loader for voter registration files (TX/FL/GA/MO).
 *
 *   POST  /api/v1/system/voter-rolls-upload?secret=ingest-now
 *         body: raw csv/tsv/pipe (Content-Type: text/csv or text/plain)
 *         optional query: state=TX|FL|GA|MO (forces parser)
 *
 *   POST  /api/v1/system/voter-rolls-upload?secret=ingest-now&file_url=https://...
 *         server-side fetches the file, streams it through the parser
 *
 *   GET   /api/v1/system/voter-rolls-upload?action=status
 *         returns rows-per-state counts
 *
 * Returns: { state, rows_processed, rows_inserted, errors, time_ms }
 *
 * Schema = the existing `voter_rolls` table created by lib/v1/enrich/voter-rolls.js
 * (we re-use ensureTable so writer matches reader).
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { trackApiCall } = require('./cost');
const { batchInsert } = require('../../_batch');
const { ensureTable: ensureVoterTable } = require('../enrich/voter-rolls');

const SECRET = 'ingest-now';
const BATCH_SIZE = 1000;
const HARD_ROW_CAP = 250000;
const FETCH_TIMEOUT_MS = 15000;

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

function detectDelimiter(headerLine) {
  if (!headerLine) return ',';
  const counts = {
    '|': (headerLine.match(/\|/g) || []).length,
    '\t': (headerLine.match(/\t/g) || []).length,
    ',': (headerLine.match(/,/g) || []).length
  };
  let best = ',';
  let bestCount = -1;
  for (const k of Object.keys(counts)) {
    if (counts[k] > bestCount) { bestCount = counts[k]; best = k; }
  }
  return best;
}

function splitLine(line, delim) {
  if (delim !== ',') return line.split(delim);
  const out = [];
  let cur = '';
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      if (inQ && line[i + 1] === '"') { cur += '"'; i++; }
      else inQ = !inQ;
    } else if (c === delim && !inQ) {
      out.push(cur); cur = '';
    } else cur += c;
  }
  out.push(cur);
  return out;
}

function parseDate(s) {
  if (!s) return null;
  const t = String(s).trim();
  if (!t) return null;
  let m = t.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) return `${m[3]}-${m[1].padStart(2,'0')}-${m[2].padStart(2,'0')}`;
  m = t.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (m) return `${m[1]}-${m[2]}-${m[3]}`;
  if (/^\d{4}-\d{2}-\d{2}/.test(t)) return t.slice(0, 10);
  const d = new Date(t);
  return isNaN(d) ? null : d.toISOString().slice(0, 10);
}

function safe(s, n) {
  if (s === null || s === undefined) return null;
  const v = String(s).trim();
  if (!v) return null;
  return n ? v.slice(0, n) : v;
}

function detectState(headerLine) {
  const h = (headerLine || '').toUpperCase();
  if (/VUID/.test(h) && /HOUSENUM|STREETNAME/.test(h)) return 'TX';
  if (/SERIALNUMBER/.test(h)) return 'FL';
  if (/\bVRN\b/.test(h)) return 'GA';
  if (/MO_VOTER_ID|MO_COUNTY/.test(h)) return 'MO';
  return null;
}

const PARSERS = {
  TX(cols) {
    if (cols.length < 11) return null;
    const street = [cols[5], cols[6]].filter(Boolean).join(' ').trim();
    const dob = parseDate(cols[10]);
    return {
      state: 'TX',
      first_name: safe(cols[2], 80),
      middle_name: safe(cols[3], 80),
      last_name: safe(cols[1], 80),
      suffix: safe(cols[4], 10),
      residence_address: safe(street, 240),
      residence_city: safe(cols[7], 100),
      residence_zip: safe(cols[9], 10),
      dob,
      year_of_birth: dob ? parseInt(dob.slice(0, 4)) : null,
      status: safe(cols[16], 20)
    };
  },
  FL(cols) {
    if (cols.length < 9) return null;
    return {
      state: 'FL',
      first_name: safe(cols[2], 80),
      middle_name: safe(cols[3], 80),
      last_name: safe(cols[1], 80),
      suffix: safe(cols[4], 10),
      residence_address: safe(cols[5], 240),
      residence_city: safe(cols[6], 100),
      residence_zip: safe(cols[8], 10),
      party: safe(cols[18] || cols[12], 20),
      dob: parseDate(cols[21] || cols[13]),
      registration_date: parseDate(cols[20] || cols[14])
    };
  },
  GA(cols) {
    if (cols.length < 8) return null;
    const houseNum = cols[10] || '';
    const street = cols[11] || '';
    const fullAddr = [houseNum, street].filter(Boolean).join(' ').trim();
    const dob = parseDate(cols[16]);
    return {
      state: 'GA',
      first_name: safe(cols[2], 80),
      middle_name: safe(cols[3], 80),
      last_name: safe(cols[1], 80),
      suffix: safe(cols[4], 10),
      residence_address: safe(fullAddr || cols[7], 240),
      residence_city: safe(cols[12] || cols[8], 100),
      residence_zip: safe(cols[14] || cols[9], 10),
      county: safe(cols[6], 80),
      registration_date: parseDate(cols[15]),
      dob,
      year_of_birth: dob ? parseInt(dob.slice(0, 4)) : null
    };
  },
  MO(cols) {
    if (cols.length < 7) return null;
    const street = [cols[5], cols[6]].filter(Boolean).join(' ').trim();
    return {
      state: 'MO',
      county: safe(cols[0], 80),
      first_name: safe(cols[2], 80),
      middle_name: safe(cols[3], 80),
      last_name: safe(cols[1], 80),
      suffix: safe(cols[4], 10),
      residence_address: safe(street || cols[5], 240),
      residence_city: safe(cols[7], 100),
      residence_zip: safe(cols[8], 10),
      dob: parseDate(cols[9]),
      registration_date: parseDate(cols[10]),
      party: safe(cols[11], 20),
      status: safe(cols[12], 20)
    };
  }
};

async function* streamLines(asyncIter) {
  let buf = '';
  for await (const chunk of asyncIter) {
    buf += Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk);
    let nl;
    while ((nl = buf.indexOf('\n')) !== -1) {
      const line = buf.slice(0, nl).replace(/\r$/, '');
      buf = buf.slice(nl + 1);
      yield line;
    }
  }
  if (buf.length) yield buf.replace(/\r$/, '');
}

async function* streamLinesFromString(str) {
  let i = 0;
  while (i < str.length) {
    const nl = str.indexOf('\n', i);
    if (nl === -1) { yield str.slice(i).replace(/\r$/, ''); return; }
    yield str.slice(i, nl).replace(/\r$/, '');
    i = nl + 1;
  }
}

async function flushBatch(db, batch, sourceFile) {
  if (!batch.length) return 0;
  for (const r of batch) r.source_file = sourceFile;
  try {
    await batchInsert(db, 'voter_rolls', batch, { chunkSize: BATCH_SIZE });
    return batch.length;
  } catch (e) {
    let ok = 0;
    for (const r of batch) {
      try { await db('voter_rolls').insert(r); ok++; } catch (_) {}
    }
    return ok;
  }
}

async function ingestStream(db, lineIter, opts = {}) {
  const t0 = Date.now();
  const sourceFile = (opts.source_file || `upload_${Date.now()}`).slice(0, 120);
  let stateForced = opts.state ? String(opts.state).toUpperCase() : null;
  let detectedState = null;
  let parser = stateForced ? PARSERS[stateForced] : null;
  let delim = null;
  let headerSeen = false;
  let processed = 0;
  let inserted = 0;
  let errors = 0;
  let batch = [];

  for await (const rawLine of lineIter) {
    if (processed >= HARD_ROW_CAP) break;
    if (!rawLine) continue;
    if (!headerSeen) {
      headerSeen = true;
      delim = detectDelimiter(rawLine);
      detectedState = stateForced || detectState(rawLine);
      if (!stateForced && detectedState) parser = PARSERS[detectedState];
      if (!parser) { parser = PARSERS.TX; detectedState = detectedState || 'TX'; }
      else continue; // skip header line
    }
    let cols;
    try { cols = splitLine(rawLine, delim); } catch (_) { errors++; continue; }
    let row;
    try { row = parser(cols); } catch (_) { errors++; continue; }
    if (!row || !row.last_name) { errors++; continue; }
    batch.push(row);
    processed++;
    if (batch.length >= BATCH_SIZE) {
      inserted += await flushBatch(db, batch, sourceFile);
      batch = [];
    }
  }
  if (batch.length) inserted += await flushBatch(db, batch, sourceFile);

  return {
    state: detectedState || stateForced || 'unknown',
    rows_processed: processed,
    rows_inserted: inserted,
    errors,
    time_ms: Date.now() - t0
  };
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const db = getDb();
  await ensureVoterTable(db);

  const action = (req.query?.action || '').toLowerCase();

  if (req.method === 'GET' || action === 'status') {
    if (req.method === 'GET' && !req.query?.file_url) {
      try {
        const stats = await db.raw(
          `SELECT state, COUNT(*)::int AS rows, MAX(loaded_at) AS last_loaded
           FROM voter_rolls GROUP BY state ORDER BY rows DESC`
        ).then(r => r.rows || []);
        const total = await db('voter_rolls').count('* as c').first().then(r => parseInt(r.c || 0));
        return res.json({
          success: true,
          action: 'status',
          total_voters: total,
          per_state: stats,
          supported_parsers: Object.keys(PARSERS),
          how_to_upload: {
            file_url: 'POST ?secret=ingest-now&file_url=https://... (optional &state=TX)',
            multipart: 'POST ?secret=ingest-now with raw csv/tsv/pipe body'
          },
          timestamp: new Date().toISOString()
        });
      } catch (e) {
        await reportError(db, 'voter-rolls-upload', null, e.message);
        return res.status(500).json({ error: e.message });
      }
    }
  }

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'POST or GET only' });
  }

  const stateForced = req.query?.state ? String(req.query.state).toUpperCase() : null;
  const fileUrl = req.query?.file_url || null;
  const sourceFile = fileUrl
    ? fileUrl.split('/').pop().slice(0, 120)
    : `upload_${stateForced || 'auto'}_${new Date().toISOString().slice(0, 10)}`;

  try {
    let result;
    if (fileUrl) {
      const r = await fetch(fileUrl, {
        headers: { 'User-Agent': 'AIP-VoterRollsUpload/1.0' },
        signal: AbortSignal.timeout(FETCH_TIMEOUT_MS)
      });
      if (!r.ok || !r.body) {
        return res.status(502).json({ error: `fetch failed: ${r.status}` });
      }
      const reader = r.body.getReader();
      async function* iter() {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          yield Buffer.from(value);
        }
      }
      result = await ingestStream(db, streamLines(iter()),
        { state: stateForced, source_file: sourceFile });
    } else {
      let raw = '';
      if (typeof req.body === 'string' && req.body.length) raw = req.body;
      else if (Buffer.isBuffer(req.body)) raw = req.body.toString('utf8');
      else if (req.body && typeof req.body === 'object') raw = req.body.csv || req.body.text || '';
      if (!raw) {
        const chunks = [];
        for await (const c of req) chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c));
        raw = Buffer.concat(chunks).toString('utf8');
      }
      if (!raw) return res.status(400).json({ error: 'no body and no file_url' });
      result = await ingestStream(db, streamLinesFromString(raw),
        { state: stateForced, source_file: sourceFile });
    }

    try { await trackApiCall(db, 'voter-rolls-upload', 'self', 0, 0, true); } catch (_) {}

    return res.json({
      success: true,
      ...result,
      source_file: sourceFile,
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    await reportError(db, 'voter-rolls-upload', null, e.message);
    return res.status(500).json({ success: false, error: e.message });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.PARSERS = PARSERS;
module.exports.detectState = detectState;
module.exports.ingestStream = ingestStream;
