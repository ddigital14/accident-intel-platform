/**
 * GET /api/v1/enrich/trestle-test?secret=ingest-now&phone=+15555551234
 * GET /api/v1/enrich/trestle-test?secret=ingest-now&address=...&city=...&state=...
 *
 * Sanity check that Trestle integration works.
 */
const { getDb } = require('../../_db');
const trestle = require('./trestle');
const { reportError } = require('../system/_errors');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();

  const configured = await trestle.isConfigured(db);
  if (!configured) {
    return res.status(400).json({
      error: 'Trestle not configured',
      hint: 'POST { trestle_api_key: "..." } to /api/v1/system/setup?secret=ingest-now, OR set TRESTLE_API_KEY env var in Vercel.'
    });
  }

  const { phone, address, city, state, zip, endpoint = 'auto' } = req.query;
  let result = null;
  let used = null;

  try {
    if (endpoint === 'phone' || (endpoint === 'auto' && phone)) {
      result = await trestle.reversePhone(phone, db);
      used = 'reverse_phone';
    } else if (endpoint === 'caller_id' && phone) {
      result = await trestle.callerIdentification(phone, db);
      used = 'caller_id';
    } else if (endpoint === 'cnam' && phone) {
      result = await trestle.smartCnam(phone, db);
      used = 'cnam';
    } else if (endpoint === 'address' || (endpoint === 'auto' && address)) {
      result = await trestle.reverseAddress({ street: address, city, state, postal_code: zip }, db);
      used = 'reverse_address';
    } else if (endpoint === 'real_contact') {
      result = await trestle.realContact({ phone, ...(req.query) }, db);
      used = 'real_contact';
    } else {
      return res.status(400).json({
        error: 'Provide phone= or address=+state= as query params',
        endpoints: ['phone', 'caller_id', 'cnam', 'address', 'real_contact']
      });
    }

    res.json({
      success: !result?.error,
      endpoint_used: used,
      key_configured: true,
      result,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
