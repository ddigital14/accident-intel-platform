/**
 * GA Voter Roll Loader — Phase 22 #2
 *
 * Georgia Sec of State portal:
 *   https://elections.sos.ga.gov/Elections/voterregistration.do
 *
 * Acquisition:
 *   1. Register a free GA SOS account
 *   2. Submit a voter list request (FREE for individuals, ~$250 commercial)
 *   3. Download .txt files (pipe-delimited, one per county or statewide)
 *
 * GET /api/v1/enrich/ga-voter-loader?action=instructions
 *   → step-by-step instructions
 *
 * POST /api/v1/enrich/ga-voter-loader?action=import&secret=ingest-now
 *   body: { county_file_url, county }
 *
 * GA voter file layout (pipe-delimited, current as of 2024):
 *   1: County
 *   2: Registration Number
 *   3: Voter Status
 *   4: Status Reason
 *   5: Last Name
 *   6: First Name
 *   7: Middle Name
 *   8: Suffix
 *   9: Birth Year
 *  10: Registration Date
 *  11: Residence Address
 *  12: Residence City
 *  13: Residence Zip
 *  14: Mailing Address
 *  15: Mailing City
 *  16: Mailing Zip
 *  17: Race
 *  18: Gender
 *  19: Land District
 *  ...
 *
 * NOTE: GA file format may shift between years. We're tolerant of column-count
 * variation by using sane defaults + normalizePerson.
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { batchInsert } = require('../../_batch');
const { ensureTable: ensureVoterTable } = require('./voter-rolls');
const { trackApiCall } = require('../system/cost');

const GA_PORTAL = 'https://elections.sos.ga.gov/Elections/voterregistration.do';

function parseInteger(v) {
  if (!v) return null;
  const n = parseInt(String(v).trim(), 10);
  return isNaN(n) ? null : n;
}

function parseDate(s) {
  if (!s) return null;
  const m = String(s).match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (m) return new Date(`${m[3]}-${String(m[1]).padStart(2,'0')}-${String(m[2]).padStart(2,'0')}`);
  const d = new Date(s);
  return isNaN(d) ? null : d;
}

async function importFromUrl(db, fileUrl, county) {
  await ensureVoterTable(db);
  const r = await fetch(fileUrl, {
    headers: { 'User-Agent': 'AIP/1.0 (research)' },
    signal: AbortSignal.timeout(25000)
  });
  if (!r.ok) {
    await trackApiCall(db, 'ga-voter-loader', 'ga_sos', 0, 0, false);
    return { error: `Download failed: HTTP ${r.status}` };
  }
  await trackApiCall(db, 'ga-voter-loader', 'ga_sos', 0, 0, true);
  const text = await r.text();
  const lines = text.split(/\r?\n/);
  const rows = [];
  let parsed = 0;

  for (const line of lines) {
    if (!line || line.length < 30) continue;
    const f = line.split('|');
    if (f.length < 13) continue;
    const yob = parseInteger(f[8]);
    rows.push({
      state: 'GA',
      county: (county || f[0] || '').trim() || null,
      first_name: (f[5] || '').trim() || null,
      middle_name: (f[6] || '').trim() || null,
      last_name: (f[4] || '').trim() || null,
      suffix: (f[7] || '').trim() || null,
      gender: (f[17] || '').trim() || null,
      year_of_birth: yob,
      dob: yob ? new Date(`${yob}-01-01`) : null,
      residence_address: (f[10] || '').trim() || null,
      residence_city: (f[11] || '').trim() || null,
      residence_zip: (f[12] || '').trim() || null,
      registration_date: parseDate(f[9]),
      status: (f[2] || '').trim() || null,
      source_file: fileUrl
    });
    parsed++;
    if (rows.length >= 500) {
      await batchInsert(db, 'voter_rolls', rows);
      rows.length = 0;
    }
  }
  if (rows.length) await batchInsert(db, 'voter_rolls', rows);
  return { success: true, county, parsed };
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const db = getDb();

  if (req.method === 'GET' || req.query?.action === 'instructions') {
    return res.json({
      success: true,
      state: 'GA',
      portal_url: GA_PORTAL,
      cost: 'FREE for individuals (~$250 commercial)',
      file_size: '~500MB statewide',
      record_count: '~7M GA registered voters',
      instructions: [
        '1. Visit GA SOS portal: ' + GA_PORTAL,
        '2. Register a free account (or log in)',
        '3. Click "Voter List Request" — fill purpose (Research/Personal — non-commercial)',
        '4. Choose statewide OR specific county files',
        '5. They email you a download link within minutes-hours',
        '6. Save .txt files to a public-readable URL (S3, Vercel Blob, etc.)',
        '7. POST each file: /api/v1/enrich/ga-voter-loader?action=import',
        '   body: { county_file_url: "https://...", county: "FULTON" }'
      ],
      file_format: 'pipe-delimited (|), one row per voter',
    });
  }
  if (req.method === 'POST' && req.query?.action === 'import') {
    const secret = req.query?.secret || req.headers?.['x-cron-secret'];
    if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    const body = typeof req.body === 'string' ? JSON.parse(req.body) : (req.body || {});
    if (!body.county_file_url) return res.status(400).json({ error: 'county_file_url required' });
    try {
      const result = await importFromUrl(db, body.county_file_url, body.county);
      res.json(result);
    } catch (err) {
      await reportError(db, 'ga-voter-loader', body.county, err.message);
      res.status(500).json({ error: err.message });
    }
    return;
  }
  res.status(400).json({ error: 'invalid action', valid: ['instructions', 'import'] });
};

module.exports.importFromUrl = importFromUrl;
