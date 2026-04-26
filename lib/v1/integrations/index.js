/**
 * Integrations Management API
 * GET  /api/v1/integrations       - List all integrations
 * POST /api/v1/integrations       - Update integration config (connect/disconnect/set keys)
 */
const { getDb } = require('../../_db');
const { requireAuth } = require('../../_auth');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const user = requireAuth(req, res);
  if (!user) return;
  const db = getDb();

  if (req.method === 'GET') {
    try {
      const { category, status, is_free } = req.query;
      let query = db('integrations').orderBy('category').orderBy('priority');
      if (category) query = query.where('category', category);
      if (status) query = query.where('status', status);
      if (is_free === 'true') query = query.where('is_free', true);

      const integrations = await query;

      // Mask API keys in response (show last 4 chars only)
      const masked = integrations.map(i => ({
        ...i,
        api_key_encrypted: i.api_key_encrypted ? '••••' + i.api_key_encrypted.slice(-4) : null,
        api_secret_encrypted: i.api_secret_encrypted ? '••••' + i.api_secret_encrypted.slice(-4) : null,
        credentials_json: i.credentials_json ? '(configured)' : null
      }));

      // Group by category
      const byCategory = {};
      masked.forEach(i => {
        if (!byCategory[i.category]) byCategory[i.category] = [];
        byCategory[i.category].push(i);
      });

      const stats = {
        total: integrations.length,
        connected: integrations.filter(i => i.is_connected).length,
        active: integrations.filter(i => i.status === 'active').length,
        free: integrations.filter(i => i.is_free).length,
        total_monthly_cost: integrations.reduce((sum, i) => sum + (i.is_enabled ? parseFloat(i.monthly_cost || 0) : 0), 0)
      };

      res.json({ integrations: masked, byCategory, stats });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  }

  else if (req.method === 'POST') {
    try {
      const { id, action, api_key, api_secret, credentials, config } = req.body;
      if (!id) return res.status(400).json({ error: 'Integration ID required' });

      const integration = await db('integrations').where('id', id).first();
      if (!integration) return res.status(404).json({ error: 'Integration not found' });

      const updates = { updated_at: new Date() };

      if (action === 'connect') {
        if (api_key) updates.api_key_encrypted = api_key; // In production, encrypt this
        if (api_secret) updates.api_secret_encrypted = api_secret;
        if (credentials) updates.credentials_json = JSON.stringify(credentials);
        updates.is_connected = true;
        updates.is_enabled = true;
        updates.status = 'active';
      } else if (action === 'disconnect') {
        updates.is_connected = false;
        updates.is_enabled = false;
        updates.status = 'disconnected';
      } else if (action === 'enable') {
        updates.is_enabled = true;
        updates.status = 'active';
      } else if (action === 'disable') {
        updates.is_enabled = false;
        updates.status = 'paused';
      } else if (action === 'update_config') {
        if (config) updates.config_json = JSON.stringify(config);
        if (api_key) updates.api_key_encrypted = api_key;
        if (api_secret) updates.api_secret_encrypted = api_secret;
      } else if (action === 'test') {
        // Test the connection
        updates.last_request_at = new Date();
        try {
          const testResult = await testIntegration(integration, api_key || integration.api_key_encrypted);
          updates.last_success_at = new Date();
          updates.last_error = null;
          updates.status = 'active';
          updates.is_connected = true;
          await db('integrations').where('id', id).update(updates);
          return res.json({ success: true, message: 'Connection test passed', result: testResult });
        } catch (testErr) {
          updates.last_error = testErr.message;
          updates.status = 'error';
          await db('integrations').where('id', id).update(updates);
          return res.json({ success: false, message: 'Connection test failed', error: testErr.message });
        }
      }

      await db('integrations').where('id', id).update(updates);
      const updated = await db('integrations').where('id', id).first();
      res.json({ success: true, integration: updated });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  }
  else {
    res.status(405).json({ error: 'Method not allowed' });
  }
};

async function testIntegration(integration, apiKey) {
  const slug = integration.slug;
  const timeout = 8000;

  if (slug === 'nhtsa' || slug === 'vin_decoder') {
    const r = await fetch('https://vpic.nhtsa.dot.gov/api/vehicles/decodevin/1HGCM82633A004352?format=json', { signal: AbortSignal.timeout(timeout) });
    return { ok: r.ok, status: r.status };
  }
  if (slug === 'osm') {
    const r = await fetch('https://nominatim.openstreetmap.org/search?q=Atlanta+GA&format=json&limit=1', { signal: AbortSignal.timeout(timeout) });
    return { ok: r.ok, status: r.status };
  }
  if (slug === 'newsapi' && apiKey) {
    const r = await fetch(`https://newsapi.org/v2/top-headlines?country=us&pageSize=1&apiKey=${apiKey}`, { signal: AbortSignal.timeout(timeout) });
    return { ok: r.ok, status: r.status };
  }
  if (slug === 'numverify' && apiKey) {
    const r = await fetch(`https://apilayer.net/api/validate?access_key=${apiKey}&number=14158586273`, { signal: AbortSignal.timeout(timeout) });
    return { ok: r.ok, status: r.status };
  }
  if (slug === 'hunter' && apiKey) {
    const r = await fetch(`https://api.hunter.io/v2/email-count?domain=google.com&api_key=${apiKey}`, { signal: AbortSignal.timeout(timeout) });
    return { ok: r.ok, status: r.status };
  }
  if (slug === 'openweather' && apiKey) {
    const r = await fetch(`https://api.openweathermap.org/data/2.5/weather?q=Atlanta&appid=${apiKey}`, { signal: AbortSignal.timeout(timeout) });
    return { ok: r.ok, status: r.status };
  }
  if (slug === 'pdl' && apiKey) {
    const r = await fetch('https://api.peopledatalabs.com/v5/person/enrich?email=test@test.com', {
      headers: { 'X-Api-Key': apiKey }, signal: AbortSignal.timeout(timeout)
    });
    return { ok: r.ok, status: r.status };
  }
  return { ok: true, message: 'No automated test available — manual verification required' };
}
