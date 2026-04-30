/**
 * Phase 50b: spanish-detector is now an alias of the multi-language
 * detector (kept for back-compat).
 */
const multi = require('./multilang-detector');
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');

const ENGINE = 'spanish-detector';

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const action = req.query?.action || 'health';

  try {
    if (action === 'health') {
      return res.json({
        success: true,
        engine: ENGINE,
        message: 'spanish-detector (alias of multilang-detector) online',
        alias_of: 'multilang-detector',
        timestamp: new Date().toISOString()
      });
    }

    if (action === 'detect') {
      const text = req.query?.text || (req.body?.text || '');
      return res.json({ success: true, detection: multi.detectSpanish(String(text)) });
    }

    if (action === 'translate') {
      let body = req.body;
      if (!body && req.method === 'POST') {
        body = await new Promise(r => { let d = ''; req.on('data', c => d += c); req.on('end', () => { try { r(JSON.parse(d)); } catch (_) { r({}); } }); });
      }
      body = body || {};
      const text = body.text || req.query?.text || '';
      const url = body.source_url || req.query?.source_url || null;
      if (!text) return res.status(400).json({ error: 'text required (POST body or ?text=)' });
      const out = await multi.translateAndExtract(db, String(text), url, 'es');
      return res.json({ success: !!out.ok, ...out });
    }

    if (action === 'batch') {
      const limit = parseInt(req.query?.limit || '10', 10);
      const out = await multi.batchTranslate(db, limit);
      return res.json({ success: true, message: `multilang batch (via spanish-detector alias): ${out.translated}/${out.scanned} translated`, ...out });
    }

    return res.status(400).json({ error: 'unknown action', allowed: ['health', 'detect', 'translate', 'batch'] });
  } catch (err) {
    await reportError(db, ENGINE, null, err.message).catch(() => {});
    return res.status(500).json({ error: err.message });
  }
};

module.exports.detect = multi.detectSpanish;
module.exports.translateAndExtract = (db, text, url) => multi.translateAndExtract(db, text, url, 'es');
module.exports.batchTranslate = multi.batchTranslate;
