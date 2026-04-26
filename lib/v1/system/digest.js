/**
 * GET /api/v1/system/digest?secret=ingest-now[&post=true]
 *
 * Daily summary: qualified leads / pipeline performance / errors.
 * If post=true (and SLACK_WEBHOOK_URL set), posts to Slack.
 *
 * Cron: 09:00 UTC daily (4am ET / 1am PT)
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { getConfig } = require('./setup');

async function postSlack(webhook, text, blocks) {
  if (!webhook) return false;
  try {
    const resp = await fetch(webhook, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text, blocks }),
      signal: AbortSignal.timeout(8000)
    });
    return resp.ok;
  } catch (_) { return false; }
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  try {
    const day = new Date(Date.now() - 86400000);

    const [
      newIncidents, qualifiedLast24h, totalQualified, fatalLast24h,
      personsAdded, errorsLast24h, costLast24h, byMetro, byType
    ] = await Promise.all([
      db('incidents').where('discovered_at','>',day).count('* as c').first().then(r=>parseInt(r.c||0)).catch(()=>0),
      db('incidents').where('qualified_at','>',day).count('* as c').first().then(r=>parseInt(r.c||0)).catch(()=>0),
      db('incidents').where('qualification_state','qualified').count('* as c').first().then(r=>parseInt(r.c||0)).catch(()=>0),
      db('incidents').where('discovered_at','>',day).where('severity','fatal').count('* as c').first().then(r=>parseInt(r.c||0)).catch(()=>0),
      db('persons').where('created_at','>',day).count('* as c').first().then(r=>parseInt(r.c||0)).catch(()=>0),
      db.raw(`SELECT COUNT(*) as c FROM system_errors WHERE created_at > $1`,[day]).then(r=>parseInt(r.rows?.[0]?.c||0)).catch(()=>0),
      db.raw(`SELECT SUM(cost_usd) as total FROM system_api_calls WHERE created_at > $1`,[day]).then(r=>parseFloat(r.rows?.[0]?.total||0)).catch(()=>0),
      db.raw(`SELECT i.city, i.state, COUNT(*) as c
              FROM incidents i WHERE i.discovered_at > $1
              GROUP BY i.city, i.state ORDER BY c DESC LIMIT 5`,[day]).then(r=>r.rows||[]).catch(()=>[]),
      db.raw(`SELECT incident_type, COUNT(*) as c
              FROM incidents WHERE discovered_at > $1
              GROUP BY incident_type ORDER BY c DESC LIMIT 5`,[day]).then(r=>r.rows||[]).catch(()=>[])
    ]);

    const summary = {
      new_incidents_24h: newIncidents,
      newly_qualified_24h: qualifiedLast24h,
      total_qualified: totalQualified,
      fatal_24h: fatalLast24h,
      persons_added_24h: personsAdded,
      errors_24h: errorsLast24h,
      cost_24h_usd: costLast24h,
      top_metros: byMetro,
      top_types: byType
    };

    if (req.query.post === 'true') {
      const slackCfg = await getConfig(db, 'slack');
      const webhook = slackCfg?.webhook || process.env.SLACK_WEBHOOK_URL;
      const blocks = [
        { type: 'header', text: { type: 'plain_text', text: '📊 AIP Daily Digest' } },
        {
          type: 'section',
          fields: [
            { type: 'mrkdwn', text: `*New Incidents:*\n${newIncidents}` },
            { type: 'mrkdwn', text: `*Newly Qualified:*\n${qualifiedLast24h}` },
            { type: 'mrkdwn', text: `*Fatals:*\n${fatalLast24h}` },
            { type: 'mrkdwn', text: `*Persons Added:*\n${personsAdded}` },
            { type: 'mrkdwn', text: `*Errors:*\n${errorsLast24h}` },
            { type: 'mrkdwn', text: `*API Cost:*\n$${costLast24h.toFixed(2)}` }
          ]
        },
        {
          type: 'section',
          text: { type: 'mrkdwn', text: `*Top Metros:*\n${byMetro.slice(0,3).map(m => `• ${m.city || '-'}, ${m.state || '-'}: ${m.c}`).join('\n')}` }
        }
      ];
      const text = `AIP Daily: ${newIncidents} new, ${qualifiedLast24h} qualified, ${fatalLast24h} fatal`;
      const sent = await postSlack(webhook, text, blocks);
      summary.slack_sent = sent;
    }

    res.json({ success: true, ...summary, timestamp: new Date().toISOString() });
  } catch (err) {
    await reportError(db, 'digest', null, err.message);
    res.status(500).json({ error: err.message });
  }
};
