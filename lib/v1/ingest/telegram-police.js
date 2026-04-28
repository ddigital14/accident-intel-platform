/**
 * Telegram police-monitor channels. Many cities have public Telegram channels
 * mirroring scanner audio with named victims. Free public RSS-like /s/ slug pages.
 */
const fetch = require('node-fetch');
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');

const CHANNELS = [
  // Public preview pages — t.me/s/CHANNELNAME
  'akronpolicemonitor', 'clevelandscanner', 'houstonpolice', 'atlantascanner',
  'phoenixpolice', 'tampascanner', 'miamipolice', 'columbusscanner', 'dallaspolicemonitor'
];

const ACCIDENT = /accident|crash|MVA|MVC|TC|collision|wreck|rollover|pedestrian|fatality|injury/i;

async function fetchChannel(slug, db) {
  const url = `https://t.me/s/${slug}`;
  let html = null, ok = false;
  try { const r = await fetch(url, { timeout: 10000, headers: { 'User-Agent': 'Mozilla/5.0 AIP' } }); if (r.ok) { html = await r.text(); ok = html.length > 500; } } catch (_) {}
  await trackApiCall(db, 'ingest-telegram-police', slug, 0, 0, ok).catch(() => {});
  if (!html) return [];
  const messages = [];
  const matches = [...html.matchAll(/<div class="tgme_widget_message_text[^"]*"[^>]*>([\s\S]*?)<\/div>/g)];
  for (const m of matches.slice(0, 25)) {
    const text = m[1].replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
    if (ACCIDENT.test(text)) messages.push({ slug, text: text.slice(0, 600) });
  }
  return messages;
}

async function run(db) {
  let total = 0, inserted = 0;
  for (const slug of CHANNELS) {
    const msgs = await fetchChannel(slug, db);
    total += msgs.length;
    for (const m of msgs) {
      try {
        const sourceId = `telegram-${slug}-${m.text.slice(0, 50).replace(/\W+/g, '-')}`;
        await db('source_reports').insert({
          source_type: 'telegram_police',
          source_reference: sourceId,
          parsed_data: JSON.stringify(m),
          created_at: new Date()
        }).onConflict('source_reference').ignore();
        inserted++;
      } catch (_) {}
    }
  }
  return { channels: CHANNELS.length, found: total, inserted };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    if (req.query?.action === 'health') return res.json({ ok: true, engine: 'telegram-police', channels: CHANNELS.length });
    const out = await run(db);
    return res.json({ success: true, ...out });
  } catch (err) { await reportError(db, 'telegram-police', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.run = run;
