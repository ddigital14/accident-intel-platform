/**
 * BACKGROUND WORKER
 * Handles queued jobs: skip tracing, geocoding, notification delivery
 */
const db = require('../config/database');
const { logger } = require('../utils/logger');
const axios = require('axios');

// Skip Tracing - Find contact info for persons with missing data
async function runSkipTracing() {
  const persons = await db('persons')
    .where('skip_trace_completed', false)
    .where('is_injured', true)
    .where(function () {
      this.whereNull('phone').orWhere('phone', '');
    })
    .where('confidence_score', '>=', 30)
    .whereRaw("created_at > NOW() - INTERVAL '14 days'")
    .limit(20);

  for (const person of persons) {
    try {
      const data = await skipTracePerson(person);
      if (data) {
        await db('persons').where({ id: person.id }).update({
          phone: data.phone || person.phone,
          phone_secondary: data.phone_secondary,
          email: data.email || person.email,
          address: data.address || person.address,
          city: data.city || person.city,
          state: data.state || person.state,
          zip: data.zip || person.zip,
          skip_trace_completed: true,
          skip_trace_data: JSON.stringify(data)
        });
        logger.info(`Skip traced person ${person.id}: ${person.full_name}`);
      }
    } catch (err) {
      logger.warn(`Skip trace failed for person ${person.id}:`, err.message);
    }
  }
}

async function skipTracePerson(person) {
  // TLOxp skip tracing
  if (process.env.TLO_USERNAME) {
    try {
      const resp = await axios.post('https://api.tlo.com/v3/person/search', {
        name: { first: person.first_name, last: person.last_name },
        dob: person.date_of_birth,
        address: { state: person.state },
        includePhones: true,
        includeEmails: true,
        includeAddresses: true
      }, {
        headers: {
          'Authorization': `Basic ${Buffer.from(`${process.env.TLO_USERNAME}:${process.env.TLO_PASSWORD}`).toString('base64')}`,
          'X-Company-ID': process.env.TLO_COMPANY_ID
        }
      });

      const result = resp.data?.persons?.[0];
      if (result) {
        return {
          phone: result.phones?.[0]?.number,
          phone_secondary: result.phones?.[1]?.number,
          email: result.emails?.[0]?.address,
          address: result.addresses?.[0]?.street,
          city: result.addresses?.[0]?.city,
          state: result.addresses?.[0]?.state,
          zip: result.addresses?.[0]?.zip,
          full_data: result
        };
      }
    } catch (err) {
      logger.warn('TLO skip trace error:', err.message);
    }
  }

  // Fallback: LexisNexis Accurint
  if (process.env.LEXISNEXIS_API_KEY) {
    try {
      const resp = await axios.post(`${process.env.LEXISNEXIS_ENVIRONMENT === 'production' ? 'https://api.lexisnexis.com' : 'https://api-sandbox.lexisnexis.com'}/person/v2/search`, {
        firstName: person.first_name,
        lastName: person.last_name,
        state: person.state,
        includePhones: true
      }, {
        headers: { 'Authorization': `Bearer ${process.env.LEXISNEXIS_API_KEY}` }
      });

      const result = resp.data?.results?.[0];
      if (result) {
        return {
          phone: result.phones?.[0],
          email: result.emails?.[0],
          address: result.address,
          city: result.city,
          state: result.state,
          zip: result.zip
        };
      }
    } catch (err) {
      logger.warn('LexisNexis skip trace error:', err.message);
    }
  }

  return null;
}

// Geocoding - Resolve addresses to lat/lng
async function runGeocoding() {
  const incidents = await db('incidents')
    .whereNull('latitude')
    .whereNotNull('address')
    .where('status', '!=', 'invalid')
    .limit(30);

  for (const incident of incidents) {
    try {
      const coords = await geocodeAddress(`${incident.address}, ${incident.city || ''}, ${incident.state || ''}`);
      if (coords) {
        await db('incidents').where({ id: incident.id }).update({
          latitude: coords.lat, longitude: coords.lng
        });
      }
    } catch (err) {
      logger.warn(`Geocode failed for incident ${incident.id}:`, err.message);
    }
  }
}

async function geocodeAddress(address) {
  if (process.env.GOOGLE_MAPS_API_KEY) {
    const resp = await axios.get('https://maps.googleapis.com/maps/api/geocode/json', {
      params: { address, key: process.env.GOOGLE_MAPS_API_KEY }
    });
    if (resp.data.results?.length > 0) {
      const loc = resp.data.results[0].geometry.location;
      return { lat: loc.lat, lng: loc.lng };
    }
  }
  return null;
}

// Notification delivery
async function processNotifications() {
  const notifications = await db('notifications')
    .leftJoin('users', 'notifications.user_id', 'users.id')
    .leftJoin('alert_rules', 'notifications.alert_rule_id', 'alert_rules.id')
    .where('notifications.is_read', false)
    .whereRaw("notifications.created_at > NOW() - INTERVAL '5 minutes'")
    .select('notifications.*', 'users.email', 'users.phone as user_phone',
      'alert_rules.notify_email', 'alert_rules.notify_sms');

  for (const notif of notifications) {
    if (notif.notify_email && notif.email) {
      await sendEmailNotification(notif);
    }
    if (notif.notify_sms && notif.user_phone) {
      await sendSMSNotification(notif);
    }
  }
}

async function sendEmailNotification(notif) {
  try {
    const nodemailer = require('nodemailer');
    const transporter = nodemailer.createTransport({
      host: process.env.SMTP_HOST, port: process.env.SMTP_PORT,
      auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASSWORD }
    });
    await transporter.sendMail({
      from: process.env.ALERT_FROM_EMAIL,
      to: notif.email,
      subject: `🚨 ${notif.title}`,
      html: `<h2>${notif.title}</h2><p>${notif.message}</p><p><a href="${process.env.APP_URL}/incidents/${notif.incident_id}">View Incident</a></p>`
    });
  } catch (err) {
    logger.warn('Email notification failed:', err.message);
  }
}

async function sendSMSNotification(notif) {
  try {
    if (!process.env.TWILIO_ACCOUNT_SID) return;
    const twilio = require('twilio')(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
    await twilio.messages.create({
      body: `${notif.title}: ${notif.message}`,
      from: process.env.TWILIO_PHONE_NUMBER,
      to: notif.user_phone
    });
  } catch (err) {
    logger.warn('SMS notification failed:', err.message);
  }
}

// Main worker loop
if (require.main === module) {
  const cron = require('node-cron');

  cron.schedule('*/3 * * * *', async () => {
    try { await runSkipTracing(); } catch (err) { logger.error('Skip trace cycle error:', err); }
  });

  cron.schedule('*/2 * * * *', async () => {
    try { await runGeocoding(); } catch (err) { logger.error('Geocoding cycle error:', err); }
  });

  cron.schedule('* * * * *', async () => {
    try { await processNotifications(); } catch (err) { logger.error('Notification cycle error:', err); }
  });

  logger.info('🔧 Worker started: skip tracing, geocoding, notifications');
}

module.exports = { runSkipTracing, runGeocoding, processNotifications };
