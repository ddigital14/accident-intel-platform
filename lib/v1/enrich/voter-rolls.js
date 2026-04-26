/**
 * Voter Registration Files Loader (scaffold)
 *
 * Voter rolls are PUBLIC RECORDS — you can request the file from each state's
 * Secretary of State office. Most states charge $0-50 for the bulk download.
 *
 * Once loaded into Postgres, you have a 200M+ row dataset of:
 *   - Full name (legal)
 *   - Date of birth (or year of birth)
 *   - Residential address
 *   - Voter status, party affiliation (in some states)
 *
 * This is the SINGLE BIGGEST free contact dataset available in the US.
 *
 * Per-state acquisition (manual, not API):
 *   TX: tinyurl.com/tx-voter-files (Texas Sec of State, ~$1100 one-time)
 *   FL: dos.myflorida.com/elections (FREE)
 *   GA: sos.ga.gov/voter-files ($250)
 *   CA: sos.ca.gov ($30)
 *   NY: elections.ny.gov ($300)
 *   PA: dos.pa.gov ($20)
 *
 * GET /api/v1/enrich/voter-rolls?secret=ingest-now&action=stats
 *   → returns counts per state of loaded voters
 *
 * This module provides:
 *   - lookupVoter(firstName, lastName, state) → matching records
 *   - lookupAddress(address, city, state) → residents at address
 *
 * Usage: when a person has name+state but missing address, lookupVoter()
 * returns possible matches with addresses + DOB.
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');

let _tableEnsured = false;
async function ensureTable(db) {
  if (_tableEnsured) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS voter_rolls (
        id BIGSERIAL PRIMARY KEY,
        state VARCHAR(2) NOT NULL,
        county VARCHAR(80),
        first_name VARCHAR(80),
        middle_name VARCHAR(80),
        last_name VARCHAR(80),
        suffix VARCHAR(10),
        full_name VARCHAR(255) GENERATED ALWAYS AS (
          TRIM(COALESCE(first_name, '') || ' ' || COALESCE(middle_name, '') || ' ' ||
               COALESCE(last_name, '') || ' ' || COALESCE(suffix, ''))
        ) STORED,
        dob DATE,
        year_of_birth INTEGER,
        gender VARCHAR(10),
        residence_address TEXT,
        residence_city VARCHAR(100),
        residence_zip VARCHAR(10),
        mailing_address TEXT,
        party VARCHAR(20),
        registration_date DATE,
        status VARCHAR(20),
        source_file VARCHAR(120),
        loaded_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_voter_state ON voter_rolls(state);
      CREATE INDEX IF NOT EXISTS idx_voter_lastname ON voter_rolls(LOWER(last_name));
      CREATE INDEX IF NOT EXISTS idx_voter_firstlast ON voter_rolls(LOWER(first_name), LOWER(last_name));
      CREATE INDEX IF NOT EXISTS idx_voter_zip ON voter_rolls(residence_zip);
      CREATE INDEX IF NOT EXISTS idx_voter_full_trgm ON voter_rolls USING GIN(full_name gin_trgm_ops);
    `);
    _tableEnsured = true;
  } catch (e) { console.error('voter_rolls table ensure failed:', e.message); }
}

async function lookupVoter(db, firstName, lastName, state) {
  await ensureTable(db);
  if (!lastName) return [];
  let q = db('voter_rolls')
    .whereRaw('LOWER(last_name) = LOWER(?)', [lastName])
    .limit(20);
  if (firstName) q = q.whereRaw('LOWER(first_name) = LOWER(?)', [firstName]);
  if (state) q = q.where('state', state.toUpperCase());
  return q;
}

async function lookupAddress(db, address, city, state) {
  await ensureTable(db);
  if (!address) return [];
  let q = db('voter_rolls')
    .whereRaw('LOWER(residence_address) LIKE LOWER(?)', [`%${address.substring(0, 40)}%`])
    .limit(20);
  if (state) q = q.where('state', state.toUpperCase());
  if (city) q = q.whereRaw('LOWER(residence_city) = LOWER(?)', [city]);
  return q;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const db = getDb();
  await ensureTable(db);

  try {
    const action = req.query.action || 'stats';
    if (action === 'stats') {
      const stats = await db.raw(`
        SELECT state, COUNT(*) as count, MAX(loaded_at) as last_loaded
        FROM voter_rolls GROUP BY state ORDER BY count DESC
      `).then(r => r.rows || []);
      const total = await db('voter_rolls').count('* as c').first().then(r => parseInt(r.c||0));
      return res.json({
        success: true,
        total_voters: total,
        per_state: stats,
        instructions: 'Voter rolls must be acquired manually from each state and bulk-loaded via SQL COPY. See module comment header for state URLs.',
        timestamp: new Date().toISOString()
      });
    }
    if (action === 'lookup') {
      const { first, last, state, address, city } = req.query;
      if (address) {
        const matches = await lookupAddress(db, address, city, state);
        return res.json({ success: true, matches, count: matches.length });
      }
      const matches = await lookupVoter(db, first, last, state);
      return res.json({ success: true, matches, count: matches.length });
    }
    res.status(400).json({ error: 'unknown action', valid: ['stats', 'lookup'] });
  } catch (err) {
    await reportError(db, 'voter-rolls', null, err.message);
    res.status(500).json({ error: err.message });
  }
};

module.exports.lookupVoter = lookupVoter;
module.exports.lookupAddress = lookupAddress;
module.exports.ensureTable = ensureTable;
