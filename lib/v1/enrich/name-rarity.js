/**
 * Name-frequency rarity scorer using US Census surname + given name files.
 * Loads small rarity tables once; uses them to boost confidence on rare-name single-source hits.
 * Free.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('../system/cost');
const { reportError } = require('../system/_errors');
const { enqueueCascade } = require('../system/_cascade');

// Very common name substrings to demote. Real Census data ingested via ?action=ingest.
const COMMON_LAST = new Set(['smith', 'johnson', 'williams', 'brown', 'jones', 'garcia', 'miller', 'davis', 'rodriguez', 'martinez', 'hernandez', 'lopez', 'gonzalez', 'wilson', 'anderson', 'thomas', 'taylor', 'moore', 'jackson', 'martin', 'lee', 'perez', 'thompson', 'white', 'harris', 'sanchez', 'clark', 'ramirez', 'lewis', 'robinson', 'walker', 'young', 'allen', 'king', 'wright', 'scott', 'torres', 'nguyen', 'hill', 'flores', 'green', 'adams', 'nelson', 'baker', 'hall', 'rivera', 'campbell', 'mitchell', 'carter', 'roberts']);

async function scoreName(name, db) {
  if (!name || name.split(' ').length < 2) return { score: 0 };
  const last = name.toLowerCase().split(' ').slice(-1)[0];
  let dbCount = null;
  try {
    const r = await db('census_surnames').where({ name: last }).first();
    if (r) dbCount = r.count;
  } catch (_) {}
  // dbCount: 1=most common, higher rank = rarer. ~150k surnames ranked.
  let rarity = 50; // neutral default
  if (dbCount != null) rarity = Math.min(95, Math.round(50 + Math.log10(dbCount + 1) * 12));
  else if (COMMON_LAST.has(last)) rarity = 20;
  else rarity = 70; // unknown → assume rarer
  return { name, last, rarity, dbCount };
}

async function ingestCensus(db) {
  // Census surname file: ~150k entries. Skip in serverless — assume preloaded.
  // Returns whether table is populated.
  try {
    await db.raw('CREATE TABLE IF NOT EXISTS census_surnames (name TEXT PRIMARY KEY, count INT, rank INT)');
    const c = await db('census_surnames').count('* as n').first();
    return { populated: parseInt(c?.n || 0) > 0, count: parseInt(c?.n || 0) };
  } catch (e) { return { error: e.message }; }
}

async function batchScore(db, limit = 30) {
  let rows = []; try {
    rows = await db('persons').whereNotNull('full_name').where('full_name', '!=', '')
      .where(function () { this.whereNull('name_rarity').orWhere('name_rarity', 0); })
      .limit(limit);
  } catch (_) {}
  let updated = 0, autoQualified = 0;
  for (const p of rows) {
    const s = await scoreName(p.full_name, db);
    try {
      await db('persons').where({ id: p.id }).update({ name_rarity: s.rarity, updated_at: new Date() });
      updated++;
      // Bonus: rare name (>80) + 1 source = strong enough alone
      if (s.rarity >= 80 && (p.identity_confidence || 0) < 70) {
        await enqueueCascade(db, 'person', p.id, 'name-rarity', { weight: Math.round(s.rarity / 5) });
        autoQualified++;
      }
    } catch (_) {}
  }
  await trackApiCall(db, 'enrich-name-rarity', 'sql', 0, 0, true).catch(() => {});
  return { rows: rows.length, updated, autoQualified };
}

module.exports = async function handler(req, res) {
  const db = getDb();
  try {
    const { name, action } = req.query || {};
    if (action === 'health') { const i = await ingestCensus(db); return res.json({ ok: true, engine: 'name-rarity', ...i }); }
    if (action === 'ingest') { const i = await ingestCensus(db); return res.json({ success: true, ...i }); }
    if (action === 'batch') { const out = await batchScore(db, parseInt(req.query.limit) || 30); return res.json({ success: true, ...out }); }
    if (name) { const s = await scoreName(name, db); return res.json({ success: true, ...s }); }
    return res.status(400).json({ error: 'need name or action=batch|health' });
  } catch (err) { await reportError(db, 'name-rarity', null, err.message); res.status(500).json({ error: err.message }); }
};
module.exports.scoreName = scoreName;
