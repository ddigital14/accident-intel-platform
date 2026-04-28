/**
 * GET  /api/v1/system/setup — show what's configured
 * POST /api/v1/system/setup?secret=ingest-now — bulk-configure platform
 *
 * Body example:
 * {
 *   "slack_webhook": "https://hooks.slack.com/services/...",
 *   "twilio": { "account_sid": "...", "auth_token": "...", "from_number": "+1..." },
 *   "alert_sms_to": "+1...",
 *   "min_alert_score": 70,
 *   "create_reps": [
 *     { "email": "rep@firm.com", "first_name": "Jane", "last_name": "Doe",
 *       "role": "rep", "phone": "+1...", "assigned_metros": ["uuid1","uuid2"],
 *       "specialization": ["auto","truck"], "max_daily_leads": 50 }
 *   ]
 * }
 *
 * Slack/Twilio config is stored in integrations table (encrypted-at-rest by
 * Neon). On read, /api/v1/system/health pulls from this table to override env vars.
 */
const { getDb } = require('../../_db');
const { v4: uuidv4 } = require('uuid');
const bcrypt = require('bcryptjs');
const { reportError } = require('./_errors');
const { logChange } = require('./changelog');

let _tableEnsured = false;
async function ensureTable(db) {
  if (_tableEnsured) return;
  try {
    await db.raw(`
      CREATE TABLE IF NOT EXISTS system_config (
        key VARCHAR(80) PRIMARY KEY,
        value JSONB NOT NULL,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);
    _tableEnsured = true;
  } catch (_) {}
}

async function getConfig(db, key) {
  await ensureTable(db);
  const row = await db('system_config').where('key', key).first();
  return row ? row.value : null;
}

async function setConfig(db, key, value) {
  await ensureTable(db);
  return db('system_config')
    .insert({ key, value: JSON.stringify(value), updated_at: new Date() })
    .onConflict('key').merge();
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const db = getDb();
  await ensureTable(db);

  try {
    if (req.method === 'GET') {
      // Show current state — but mask secrets
      const slackConfig = await getConfig(db, 'slack');
      const twilioConfig = await getConfig(db, 'twilio');
      const alertConfig = await getConfig(db, 'alerts');
      const reps = await db('users').where('is_active', true)
        .whereIn('role', ['rep', 'manager'])
        .select('id','email','first_name','last_name','role','phone','assigned_metros','specialization','max_daily_leads');

      return res.json({
        success: true,
        slack: slackConfig ? { configured: true, webhook_set: !!(slackConfig.webhook || process.env.SLACK_WEBHOOK_URL) } : { configured: !!process.env.SLACK_WEBHOOK_URL },
        twilio: twilioConfig ? { configured: !!(twilioConfig.account_sid || process.env.TWILIO_ACCOUNT_SID), from_number: twilioConfig.from_number || process.env.TWILIO_FROM_NUMBER } : { configured: !!process.env.TWILIO_ACCOUNT_SID },
        alerts: alertConfig || { min_score: parseInt(process.env.NOTIFY_MIN_SCORE) || 70, alert_sms_to: process.env.ALERT_SMS_TO || null },
        reps,
        trestle: { configured: !!(process.env.TRESTLE_API_KEY) },
        env_keys_set: {
          DATABASE_URL: !!process.env.DATABASE_URL,
          OPENAI_API_KEY: !!process.env.OPENAI_API_KEY,
          TRESTLE_API_KEY: !!process.env.TRESTLE_API_KEY,
          NEWSAPI_KEY: !!process.env.NEWSAPI_KEY,
          PDL_API_KEY: !!process.env.PDL_API_KEY,
          HUNTER_API_KEY: !!process.env.HUNTER_API_KEY,
          NUMVERIFY_API_KEY: !!process.env.NUMVERIFY_API_KEY,
          SEARCHBUG_API_KEY: !!process.env.SEARCHBUG_API_KEY,
          TRACERFY_API_KEY: !!process.env.TRACERFY_API_KEY,
          SLACK_WEBHOOK_URL: !!process.env.SLACK_WEBHOOK_URL,
          TWILIO_ACCOUNT_SID: !!process.env.TWILIO_ACCOUNT_SID,
          TWILIO_AUTH_TOKEN: !!process.env.TWILIO_AUTH_TOKEN,
          TWILIO_FROM_NUMBER: !!process.env.TWILIO_FROM_NUMBER,
          ALERT_SMS_TO: !!process.env.ALERT_SMS_TO,
          ANTHROPIC_API_KEY: !!process.env.ANTHROPIC_API_KEY,
          OPENWEATHER_API_KEY: !!process.env.OPENWEATHER_API_KEY,
          GOOGLE_CSE_API_KEY: !!process.env.GOOGLE_CSE_API_KEY,
          GOOGLE_CSE_ID: !!process.env.GOOGLE_CSE_ID
        },
        timestamp: new Date().toISOString()
      });
    }

    if (req.method === 'POST') {
      const secret = req.query.secret || req.headers['x-cron-secret'];
      if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
        return res.status(401).json({ error: 'Unauthorized' });
      }
      const body = typeof req.body === 'string' ? JSON.parse(req.body) : (req.body || {});
      const updates = {};

      if (body.slack_webhook) {
        await setConfig(db, 'slack', { webhook: body.slack_webhook });
        updates.slack = 'updated';
      }
      if (body.twilio) {
        await setConfig(db, 'twilio', body.twilio);
        updates.twilio = 'updated';
      }
      if (body.plugin_apollo) {
        await setConfig(db, 'plugin_apollo', body.plugin_apollo);
        updates.plugin_apollo = 'updated';
      }
      if (body.plugin_common_room) {
        await setConfig(db, 'plugin_common_room', body.plugin_common_room);
        updates.plugin_common_room = 'updated';
      }
      if (body.plugin_ghl) {
        await setConfig(db, 'plugin_ghl', body.plugin_ghl);
        updates.plugin_ghl = 'updated';
      }
      if (body.crm_export) {
        await setConfig(db, 'crm_export', body.crm_export);
        updates.crm_export = 'updated';
      }
      if (body.property_records) {
        const cur = await getConfig(db, 'property_records') || {};
        await setConfig(db, 'property_records', Object.assign(cur, body.property_records));
        updates.property_records = 'updated';
      }
      if (body.sentry_dsn) {
        await setConfig(db, 'sentry_dsn', body.sentry_dsn);
        updates.sentry = 'set';
      }
      if (body.usps_userid || body.usps_consumer_key) {
        const cur = await getConfig(db, 'usps') || {};
        if (body.usps_userid) cur.userid = body.usps_userid;
        if (body.usps_consumer_key) cur.consumer_key = body.usps_consumer_key;
        if (body.usps_consumer_secret) cur.consumer_secret = body.usps_consumer_secret;
        await setConfig(db, 'usps', cur);
        updates.usps = 'updated';
      }
      if (false) {
        await setConfig(db, 'usps_userid', body.usps_userid);
        updates.usps_userid = 'set';
      }
      if (body.maricopa_api_token) {
        await setConfig(db, 'maricopa_api_token', body.maricopa_api_token);
        updates.maricopa_api_token = 'set';
      }
      if (body.newsapi_key) {
        await setConfig(db, 'newsapi_key', body.newsapi_key);
        updates.newsapi_key = 'rotated';
      }
      if (body.reddit_client_id || body.reddit_client_secret) {
        await setConfig(db, 'reddit_oauth', { client_id: body.reddit_client_id, client_secret: body.reddit_client_secret });
        updates.reddit_oauth = 'set';
      }
      if (body.trestle_api_key) {
        await setConfig(db, 'trestle', { api_key: body.trestle_api_key });
        updates.trestle = 'updated';
      }
      if (body.google_cse_api_key || body.google_cse_id) {
        const cur = await getConfig(db, 'google_cse') || {};
        await setConfig(db, 'google_cse', {
          api_key: body.google_cse_api_key || cur.api_key,
          cse_id: body.google_cse_id || cur.cse_id
        });
        updates.google_cse = 'updated';
      }
      if (body.alert_sms_to || body.min_alert_score) {
        const cur = await getConfig(db, 'alerts') || {};
        await setConfig(db, 'alerts', {
          ...cur,
          alert_sms_to: body.alert_sms_to || cur.alert_sms_to,
          min_score: body.min_alert_score || cur.min_score || 70
        });
        updates.alerts = 'updated';
      }
      if (Array.isArray(body.create_reps)) {
        const created = [];
        for (const r of body.create_reps) {
          if (!r.email) continue;
          const exists = await db('users').where('email', r.email).first();
          if (exists) {
            await db('users').where('id', exists.id).update({
              first_name: r.first_name || exists.first_name,
              last_name: r.last_name || exists.last_name,
              role: r.role || exists.role,
              phone: r.phone || exists.phone,
              assigned_metros: r.assigned_metros || exists.assigned_metros,
              specialization: r.specialization || exists.specialization,
              max_daily_leads: r.max_daily_leads || exists.max_daily_leads,
              is_active: true,
              updated_at: new Date()
            });
            created.push({ id: exists.id, action: 'updated' });
          } else {
            const tempPw = r.password || `${r.first_name?.toLowerCase() || 'rep'}${Date.now() % 100000}!`;
            const hash = await bcrypt.hash(tempPw, 10);
            const id = uuidv4();
            await db('users').insert({
              id, email: r.email,
              password_hash: hash,
              first_name: r.first_name, last_name: r.last_name,
              role: r.role || 'rep',
              phone: r.phone,
              assigned_metros: r.assigned_metros || [],
              specialization: r.specialization || [],
              max_daily_leads: r.max_daily_leads || 50,
              is_active: true,
              created_at: new Date(), updated_at: new Date()
            });
            created.push({ id, action: 'created', temp_password: tempPw });
          }
        }
        updates.reps = created;
      }

      await logChange(db, {
        kind: 'config',
        title: 'System setup updated via API',
        summary: Object.keys(updates).join(', '),
        author: 'system:setup',
        meta: updates
      });

      return res.json({ success: true, updates, timestamp: new Date().toISOString() });
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (err) {
    await reportError(db, 'setup', null, err.message);
    res.status(500).json({ error: err.message });
  }
};

module.exports.getConfig = getConfig;
module.exports.setConfig = setConfig;
