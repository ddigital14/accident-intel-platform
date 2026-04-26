/**
 * FL Voter Roll Loader
 *
 * Florida Sec of State publishes the complete voter file FREE at:
 *   https://dos.myflorida.com/elections/data-statistics/voter-registration-statistics/
 *
 * Process:
 *   1. Visit form, fill purpose statement, agree to terms
 *   2. Download zip (~2GB, contains 67 county .txt files, pipe-delimited)
 *   3. Use this loader to import to voter_rolls table
 *
 * GET /api/v1/enrich/fl-voter-loader?action=instructions
 *   → returns step-by-step download guide
 *
 * POST /api/v1/enrich/fl-voter-loader?action=import&secret=ingest-now
 *   body: { county_file_url: "...", county: "MIAMI-DADE" }
 *   → streams + parses + bulk inserts
 *
 * Field layout (per FL Voter Detail File spec):
 *   1: County Code
 *   2: Voter ID
 *   3: Name Last
 *   4: Name Suffix
 *   5: Name First
 *   6: Name Middle
 *   7: Requested Public Records Exemption (Y/N)
 *   8: Residence Address Line 1
 *   9: Residence Address Line 2
 *   10: Residence City
 *   11: Residence State
 *   12: Residence Zipcode
 *   13: Mailing Address Line 1
 *   14-20: more mailing fields
 *   21: Gender
 *   22: Race
 *   23: Date of Birth
 *   24: Date of Registration
 *   25: Party Affiliation
 *   26: Precinct
 *   27: Precinct Group
 *   28: Precinct Split
 *   29: Precinct Suffix
 *   30: Voter Status
 *   31: Voter Status Reason
 *   ...
 *
 * Full schema: https://dos.myflorida.com/media/704680/voter-extract-format-2014.pdf
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { batchInsert } = require('../../_batch');
const { ensureTable: ensureVoterTable } = require('./voter-rolls');

const FL_DOWNLOAD_PORTAL = 'https://dos.myflorida.com/elections/data-statistics/voter-registration-statistics/voter-extract-disk-request/';

async function importFromUrl(db, fileUrl, county) {
  // Stream + parse pipe-delimited file
  await ensureVoterTable(db);
  const r = await fetch(fileUrl);
  if (!r.ok) return { error: `Download failed: HTTP ${r.status}` };
  const text = await r.text();
  const lines = text.split(/\r?\n/);
  const rows = [];
  for (const line of lines) {
    if (!line || line.length < 50) continue;
    const f = line.split('|');
    if (f.length < 30) continue;
    rows.push({
      state: 'FL',
      county: county || f[0],
      first_name: (f[4] || '').trim() || null,
      middle_name: (f[5] || '').trim() || null,
      last_name: (f[2] || '').trim() || null,
      suffix: (f[3] || '').trim() || null,
      gender: (f[20] || '').trim() || null,
      dob: parseDate(f[22]),
      year_of_birth: parseDate(f[22]) ? parseDate(f[22]).getFullYear() : null,
      residence_address: (f[7] || '').trim() + (f[8] ? ' ' + f[8].trim() : ''),
      residence_city: (f[9] || '').trim() || null,
      residence_zip: (f[11] || '').trim() || null,
      party: (f[24] || '').trim() || null,
      registration_date: parseDate(f[23]),
      status: (f[29] || '').trim() || null,
      source_file: fileUrl
    });
    // Batch every 500
    if (rows.length >= 500) {
      await batchInsert(db, 'voter_rolls', rows);
      rows.length = 0;
    }
  }
  if (rows.length) await batchInsert(db, 'voter_rolls', rows);
  return { success: true, county };
}

function parseDate(s) {
  if (!s) return null;
  const m = String(s).match(/(\d{2})\/(\d{2})\/(\d{4})/);
  if (m) return new Date(`${m[3]}-${m[1]}-${m[2]}`);
  const d = new Date(s);
  return isNaN(d) ? null : d;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const db = getDb();

  if (req.method === 'GET' || req.query?.action === 'instructions') {
    return res.json({
      success: true,
      instructions: [
        '1. Visit FL Sec of State portal: ' + FL_DOWNLOAD_PORTAL,
        '2. Fill out the Voter Extract Disk Request form (free for non-commercial)',
        '3. Email completed form to ElectionRecords@dos.myflorida.com',
        '4. They mail you a CD/DVD or send a download link in 1-2 weeks',
        '5. Once you have the files, POST each county file to /api/v1/enrich/fl-voter-loader?action=import',
        '   body: { county_file_url: "https://...", county: "MIAMI-DADE" }'
      ],
      portal_url: FL_DOWNLOAD_PORTAL,
      cost: 'FREE',
      file_size: '~2GB total (all 67 counties)',
      record_count: '~14M FL registered voters'
    });
  }
  if (req.method === 'POST' && req.query?.action === 'import') {
    const secret = req.query?.secret;
    if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    const body = typeof req.body === 'string' ? JSON.parse(req.body) : (req.body || {});
    if (!body.county_file_url) return res.status(400).json({ error: 'county_file_url required' });
    try {
      const result = await importFromUrl(db, body.county_file_url, body.county);
      res.json(result);
    } catch (err) {
      await reportError(db, 'fl-voter-loader', body.county, err.message);
      res.status(500).json({ error: err.message });
    }
  }
};
