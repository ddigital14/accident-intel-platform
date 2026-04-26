/**
 * GET /api/v1/enrich/trestle-test?secret=ingest-now&phone=+15555551234
 * GET /api/v1/enrich/trestle-test?secret=ingest-now&address=...&city=...&state=...
 *
 * Sanity check that TRESTLE_API_KEY env var works. Calls one of the 5
 * Trestle endpoints based on inputs and returns the raw response (with PII
 * masked for safety in logs).
 */
const trestle = require('./trestle');
const { reportError } = require('../system/_errors');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  if (!trestle.isConfigured()) {
    return res.status(400).json({
      error: 'TRESTLE_API_KEY not set in Vercel env vars',
      hint: 'Sign up at https://portal.trestleiq.com/signup, request Reverse Phone API access, then set TRESTLE_API_KEY in Vercel.'
    });
  }

  const { phone, address, city, state, zip, endpoint = 'auto' } = req.query;
  let result = null;
  let used = null;

  try {
    if (endpoint === 'phone' || (endpoint === 'auto' && phone)) {
      result = await trestle.reversePhone(phone);
      used = 'reverse_phone';
    } else if (endpoint === 'caller_id' && phone) {
      result = await trestle.callerIdentification(phone);
      used = 'caller_id';
    } else if (endpoint === 'cnam' && phone) {
      result = await trestle.smartCnam(phone);
      used = 'cnam';
    } else if (endpoint === 'phone_intel' && phone) {
      result = await trestle.phoneValidation(phone);
      used = 'phone_intel';
    } else if (endpoint === 'address' || (endpoint === 'auto' && address)) {
      result = await trestle.reverseAddress({ street: address, city, state, postal_code: zip });
      used = 'reverse_address';
    } else {
      return res.status(400).json({
        error: 'Provide phone= or address=+state= as query params',
        endpoints: ['phone', 'caller_id', 'cnam', 'phone_intel', 'address']
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
