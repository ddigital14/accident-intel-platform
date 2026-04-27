/**
 * TX Voter Roll Loader — Phase 22 #2
 *
 * Texas SOS portal:
 *   https://www.sos.state.tx.us/elections/forms/votreq.shtml
 *
 * Acquisition:
 *   - Statewide voter file: $200 from TX SOS
 *   - Per-county: $20-50 from each county elections office
 *   - File format: pipe-delimited .txt
 *
 * GET /api/v1/enrich/tx-voter-loader?action=instructions
 * POST /api/v1/enrich/tx-voter-loader?action=import&secret=ingest-now
 *   body: { county_file_url, county }
 *
 * TX voter file layout (pipe-delimited, statewide):
 *   1: VUID (Voter Unique ID)
 *   2: Last Name
 *   3: First Name
 *   4: Middle Name
 *   5: Suffix
 *   6: Date of Birth (mm/dd/yyyy)
 *   7: Effective Date of Registration
 *   8: County
 *   9: Residence Address
 *  10: Residence City
 *  11: Residence State
 *  12: Residence Zip
 *  13: Mailing Address
 *  14: Mailing City
 *  15: Mailing State
 *  16: Mailing Zip
 *  17: Status
 *  ...
 *
 * NOTE: Mason hasn't purchased the file yet — this loader is built so the
 * moment he does, ingestion is one POST away.
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { batchInsert } = require('../../_batch');
const { ensureTable: ensureVoterTable } = require('./voter-rolls');
const { trackApiCall } = require('../system/cost');

const TX_PORTAL = 'https://www.sos.state.tx.us/elections/forms/votreq.shtml';

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
    await trackApiCall(db, 'tx-voter-loader', 'tx_sos', 0, 0, false);
    return { error: `Download failed: HTTP ${r.status}` };
  }
  await trackApiCall(db, 'tx-voter-loader', 'tx_sos', 0, 0, true);
  const text = await r.text();
  const lines = text.split(/\r?\n/);
  const rows = [];
  let parsed = 0;

  for (const line of lines) {
    if (!line || line.length < 30) continue;
    const f = line.split('|');
    if (f.length < 12) continue;
    const dob = parseDate(f[5]);
    rows.push({
      state: 'TX',
      county: (county || f[7] || '').trim() || null,
      first_name: (f[2] || '').trim() || null,
      middle_name: (f[3] || '').trim() || null,
      last_name: (f[1] || '').trim() || null,
      suffix: (f[4] || '').trim() || null,
      dob: dob,
      year_of_birth: dob ? dob.getFullYear() : null,
      residence_address: (f[8] || '').trim() || null,
      residence_city: (f[9] || '').trim() || null,
      residence_zip: (f[11] || '').trim() || null,
      mailing_address: (f[12] || '').trim() || null,
      registration_date: parseDate(f[6]),
      status: (f[16] || '').trim() || null,
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
      state: 'TX',
      portal_url: TX_PORTAL,
      cost: 'Statewide $200 (TX SOS) — OR per-county $20-50',
      file_size: '~3GB statewide',
      record_count: '~17M TX registered voters',
      instructions: [
        '1. Visit TX SOS form: ' + TX_PORTAL,
        '2. Print + complete the Voter Registration Information Request form',
        '3. Mail to TX SOS Elections Division with $200 check (statewide) OR contact county elections',
        '4. They mail a CD/DVD or send a download link in 1-2 weeks',
        '5. Major counties also sell direct: Harris (Houston), Travis (Austin), Bexar (San Antonio), Dallas, Tarrant',
        '6. Save .txt files to public-readable URL',
        '7. POST: /api/v1/enrich/tx-voter-loader?action=import',
        '   body: { county_file_url: "https://...", county: "HARRIS" }'
      ],
      file_format: 'pipe-delimited (|), one row per voter'
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
      await reportError(db, 'tx-voter-loader', body.county, err.message);
      res.status(500).json({ error: err.message });
    }
    return;
  }
  res.status(400).json({ error: 'invalid action', valid: ['instructions', 'import'] });
};

module.exports.importFromUrl = importFromUrl;
