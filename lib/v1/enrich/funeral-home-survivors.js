/**
 * FUNERAL-HOME SURVIVORS — Phase 40 Module 2
 *
 * Fatal accidents → obituaries → "Survived by..." family members.
 * For each verified fatal-accident victim, we Google CSE for the obituary,
 * fetch the page, parse the survivors block, and write each named relative
 * as a `persons` row with `relationship_to_victim` + `victim_id` foreign key.
 *
 * HTTP shape:
 *   GET  /api/v1/enrich/funeral-home-survivors?secret=ingest-now&action=health
 *   GET  /api/v1/enrich/funeral-home-survivors?secret=ingest-now&action=resolve&person_id=<uuid>
 *   GET  /api/v1/enrich/funeral-home-survivors?secret=ingest-now&action=batch&limit=10
 *
 * Each family member queued for downstream contact-finder via cascade.
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { trackApiCall } = require('../system/cost');
const { enqueueCascade } = require('../system/_cascade');

const SECRET = 'ingest-now';
const HTTP_TIMEOUT_MS = 15000;

function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

async function loadCseCfg(db) {
  try {
    const row = await db('system_config').where('key', 'google_cse').first();
    let key = process.env.GOOGLE_CSE_API_KEY;
    let cx = process.env.GOOGLE_CSE_ID;
    if (row?.value) {
      const v = typeof row.value === 'string' ? JSON.parse(row.value) : row.value;
      key = v.api_key || key;
      cx = v.cse_id || cx;
    }
    if (!key || !cx) return null;
    return { key, cx };
  } catch (_) { return null; }
}

let _columnsEnsured = false;
async function ensureColumns(db) {
  if (_columnsEnsured) return;
  try {
    await db.raw(
      'ALTER TABLE persons ADD COLUMN IF NOT EXISTS relationship_to_victim VARCHAR(40); ' +
      'ALTER TABLE persons ADD COLUMN IF NOT EXISTS victim_id UUID; ' +
      'ALTER TABLE persons ADD COLUMN IF NOT EXISTS derived_from VARCHAR(60); ' +
      'CREATE INDEX IF NOT EXISTS idx_persons_victim_id ON persons(victim_id) WHERE victim_id IS NOT NULL; ' +
      'CREATE INDEX IF NOT EXISTS idx_persons_relationship ON persons(relationship_to_victim) WHERE relationship_to_victim IS NOT NULL;'
    );
    _columnsEnsured = true;
  } catch (e) { console.error('funeral-home ensureColumns:', e.message); }
}

// ── Survivors block patterns ──
const SURVIVOR_HEADERS = [
  /survived by[^.\n:]{0,30}[:\.]?\s*([^]{30,1500}?)(?=\b(?:preceded|memorial|service|visitation|interment|burial|funeral|in lieu|donations|will be missed|published)\b|$)/i,
  /survivors include[^.\n:]{0,20}[:\.]?\s*([^]{30,1500}?)(?=\b(?:preceded|memorial|service|visitation|interment|burial|funeral|in lieu|donations|will be missed|published)\b|$)/i,
  /(?:left behind|leaves behind)[^.\n:]{0,20}[:\.]?\s*([^]{30,1500}?)(?=\b(?:preceded|memorial|service|visitation|interment|burial|funeral|in lieu|donations|will be missed|published)\b|$)/i,
  /(?:she|he|they) is survived by[^.\n:]{0,30}[:\.]?\s*([^]{30,1500}?)(?=\b(?:preceded|memorial|service|visitation|interment|burial|funeral|in lieu|donations|will be missed|published)\b|$)/i,
];

const PRECEDED_PATTERN = /preceded in death by[^.\n:]{0,30}[:\.]?\s*([^]{30,1000}?)(?=\b(?:survived|memorial|service|visitation|interment|burial|funeral|in lieu|donations|published)\b|$)/i;

// Roles → keywords that, when seen near a name, set the relationship
const ROLE_PATTERNS = [
  { role: 'spouse',  rx: /\b(?:husband|wife|spouse|partner|fianc[ée]e?)\b/i },
  { role: 'child',   rx: /\b(?:son|daughter|child(?:ren)?|stepson|stepdaughter)\b/i },
  { role: 'parent',  rx: /\b(?:mother|father|mom|dad|stepmother|stepfather|parents?)\b/i },
  { role: 'sibling', rx: /\b(?:brother|sister|sibling|half[- ]?brother|half[- ]?sister|stepbrother|stepsister)\b/i },
  { role: 'grandparent', rx: /\b(?:grandfather|grandmother|grandparents?)\b/i },
  { role: 'grandchild',  rx: /\b(?:grandson|granddaughter|grandchild(?:ren)?)\b/i },
];

// Capture proper-name pattern (First Last with optional middle initial / hyphenated)
const NAME_RX = /\b([A-Z][a-zA-Z'\-]{1,24}(?:\s+[A-Z][a-zA-Z'\-\.]{1,24}){1,3})\b/g;

const NAME_BLACKLIST = new Set([
  'United States','New York','Los Angeles','Las Vegas','San Antonio',
  'Memorial Service','Funeral Home','Funeral Services','In Lieu','Of Flowers',
  'May God','Our Lord','Jesus Christ','Police Department','Sheriff Office',
  'God Bless','Rest In Peace','Final Arrangements','Visitation Will'
]);

function stripHtml(html) {
  return String(html || '')
    .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&[a-z]{2,7};/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function classifyRole(snippet) {
  for (const r of ROLE_PATTERNS) if (r.rx.test(snippet)) return r.role;
  return null;
}

function extractNames(snippet) {
  const out = [];
  let m;
  NAME_RX.lastIndex = 0;
  while ((m = NAME_RX.exec(snippet))) {
    const n = m[1].trim();
    if (n.length < 5 || n.length > 60) continue;
    if (NAME_BLACKLIST.has(n)) continue;
    if (/\b(Funeral|Memorial|Cemetery|Church|Hospital|Department|Society|Service|Hospice|Family|Park|Center|Home)\b/i.test(n)) continue;
    out.push(n);
  }
  return [...new Set(out)];
}

// Parse survivors out of obituary HTML (already stripped).
// Returns: [{ name, relationship, status:'survivor'|'predeceased' }]
function parseSurvivors(plainText) {
  const out = [];
  const text = String(plainText || '').replace(/\s+/g, ' ');

  // Pull each survivors block, scan for role keywords, then names
  for (const rx of SURVIVOR_HEADERS) {
    const m = rx.exec(text);
    if (!m) continue;
    const block = m[1];
    // Split by semicolon / "and" so each clause gets its own role guess
    const clauses = block.split(/[;.]\s+|,\s+(?=(?:his|her|their|husband|wife|son|daughter|brother|sister|father|mother|stepson|stepdaughter|grandson|granddaughter|spouse|partner)\b)/i);
    for (const clause of clauses) {
      const role = classifyRole(clause);
      const names = extractNames(clause);
      for (const n of names) out.push({ name: n, relationship: role || 'family', status: 'survivor' });
    }
    break;
  }

  const pm = PRECEDED_PATTERN.exec(text);
  if (pm) {
    const block = pm[1];
    const clauses = block.split(/[;.]\s+/);
    for (const clause of clauses) {
      const role = classifyRole(clause);
      const names = extractNames(clause);
      for (const n of names) out.push({ name: n, relationship: role || 'family', status: 'predeceased' });
    }
  }

  // Dedup by name
  const seen = new Set();
  return out.filter(r => {
    const k = r.name.toLowerCase();
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

async function cseSearch(db, query) {
  const cfg = await loadCseCfg(db);
  if (!cfg) return { ok: false, error: 'no_cse_creds' };
  const url = `https://www.googleapis.com/customsearch/v1?key=${cfg.key}&cx=${cfg.cx}&q=${encodeURIComponent(query)}&num=5`;
  try {
    const r = await fetch(url, { signal: AbortSignal.timeout(HTTP_TIMEOUT_MS) });
    try { await trackApiCall(db, 'enrich-funeral-home-survivors', 'google_cse', 0, 0, r.ok); } catch (_) {}
    if (!r.ok) return { ok: false, error: `cse_${r.status}` };
    const j = await r.json();
    return { ok: true, items: j.items || [] };
  } catch (e) {
    try { await trackApiCall(db, 'enrich-funeral-home-survivors', 'google_cse', 0, 0, false); } catch (_) {}
    return { ok: false, error: e.message };
  }
}

async function fetchPage(url) {
  try {
    const r = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; AIP-FuneralBot/1.0)',
        'Accept': 'text/html,application/xhtml+xml'
      },
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS),
      redirect: 'follow'
    });
    if (!r.ok) return null;
    return await r.text();
  } catch (_) { return null; }
}

async function resolveOne(db, victim) {
  await ensureColumns(db);
  const fullName = victim.full_name || [victim.first_name, victim.last_name].filter(Boolean).join(' ').trim();
  if (!fullName) return { ok: false, error: 'no_name', victim_id: victim.id };
  const city = victim.city || '';
  const state = victim.state || '';
  const q = `"${fullName}" obituary ${city ? `"${city}"` : ''} ${state ? `"${state}"` : ''}`.trim();

  const search = await cseSearch(db, q);
  if (!search.ok) return { ok: false, error: search.error, victim_id: victim.id };
  if (!search.items.length) return { ok: false, error: 'no_results', victim_id: victim.id };

  // Try the top 3 results — first one with a survivors block wins
  let parsed = [];
  let usedUrl = null;
  for (const item of search.items.slice(0, 3)) {
    const url = item.link;
    if (!url) continue;
    if (!/legacy\.com|dignitymemorial\.com|tributes\.com|obittree\.com|funeral|obituar|memori|forevermissed|tributearchive/i.test(url)) {
      // try anyway but lower-priority
    }
    const html = await fetchPage(url);
    if (!html) continue;
    const text = stripHtml(html);
    const fams = parseSurvivors(text);
    if (fams.length) { parsed = fams; usedUrl = url; break; }
  }

  if (!parsed.length) return { ok: false, error: 'no_survivors_block', victim_id: victim.id };

  // Insert each as a person row, link to victim, queue for contact-finder
  let inserted = 0;
  const samples = [];
  for (const fam of parsed) {
    if (fam.status === 'predeceased') continue; // can't enrich the dead
    try {
      // Dedup against existing relatives for this victim
      const exist = await db('persons')
        .where('victim_id', victim.id)
        .whereRaw('LOWER(full_name) = LOWER(?)', [fam.name])
        .first();
      if (exist) continue;
      const parts = fam.name.split(/\s+/);
      const first = parts[0];
      const last = parts.length > 1 ? parts[parts.length - 1] : null;
      const [row] = await db('persons').insert({
        full_name: fam.name,
        first_name: first,
        last_name: last,
        city: victim.city || null,
        state: victim.state || null,
        incident_id: victim.incident_id || null,
        victim_id: victim.id,
        relationship_to_victim: fam.relationship,
        derived_from: 'obit_survivors',
        victim_verified: false,
        role: 'family_member',
        identity_confidence: 35,
        created_at: new Date(),
        updated_at: new Date()
      }).returning(['id']);
      inserted++;
      samples.push({ name: fam.name, relationship: fam.relationship });
      try {
        await enqueueCascade(db, {
          person_id: row?.id || row,
          incident_id: victim.incident_id || null,
          trigger_source: 'funeral-home-survivors',
          trigger_field: 'family_added',
          trigger_value: fam.relationship
        });
      } catch (_) {}
    } catch (e) {
      // role constraint may reject family_member — fall back to omitting role
      try {
        const parts = fam.name.split(/\s+/);
        const [row] = await db('persons').insert({
          full_name: fam.name,
          first_name: parts[0],
          last_name: parts.length > 1 ? parts[parts.length - 1] : null,
          city: victim.city || null,
          state: victim.state || null,
          incident_id: victim.incident_id || null,
          victim_id: victim.id,
          relationship_to_victim: fam.relationship,
          derived_from: 'obit_survivors',
          victim_verified: false,
          identity_confidence: 35,
          created_at: new Date(),
          updated_at: new Date()
        }).returning(['id']);
        inserted++;
        samples.push({ name: fam.name, relationship: fam.relationship });
        try {
          await enqueueCascade(db, {
            person_id: row?.id || row,
            incident_id: victim.incident_id || null,
            trigger_source: 'funeral-home-survivors',
            trigger_field: 'family_added',
            trigger_value: fam.relationship
          });
        } catch (_) {}
      } catch (_) {}
    }
  }

  return {
    ok: true,
    victim_id: victim.id,
    victim_name: fullName,
    obituary_url: usedUrl,
    family_found: parsed.length,
    family_inserted: inserted,
    samples
  };
}

async function batchResolve(db, limit = 10) {
  await ensureColumns(db);
  // Verified fatal victims with no family rows yet
  const victims = await db('persons as p')
    .leftJoin('incidents as i', 'p.incident_id', 'i.id')
    .where('p.victim_verified', true)
    .where(function() { this.where('i.severity', 'fatal').orWhereRaw("LOWER(i.headline) ~ ?", ['killed|fatal|dies|dead|deceased']); })
    .whereNotExists(function() {
      this.select('*').from('persons as f')
        .whereRaw('f.victim_id = p.id')
        .where('f.derived_from', 'obit_survivors');
    })
    .select('p.id', 'p.full_name', 'p.first_name', 'p.last_name', 'p.city', 'p.state', 'p.incident_id')
    .limit(limit);

  const out = { processed: 0, with_family: 0, total_inserted: 0, samples: [], errors: 0 };
  for (const v of victims) {
    try {
      const r = await resolveOne(db, v);
      out.processed++;
      if (r.ok) {
        out.with_family++;
        out.total_inserted += r.family_inserted || 0;
        if (out.samples.length < 6) out.samples.push({
          victim: r.victim_name, family: r.family_inserted, url: r.obituary_url
        });
      } else if (r.error) out.errors++;
    } catch (e) {
      out.errors++;
      await reportError(db, 'enrich-funeral-home-survivors', v.id, e.message);
    }
  }
  return out;
}

async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const db = getDb();
  await ensureColumns(db);
  const action = (req.query?.action || 'health').toLowerCase();

  try {
    if (action === 'health') {
      const cfg = await loadCseCfg(db);
      const familyCount = await db('persons').where('derived_from', 'obit_survivors').count('* as c').first().then(r => parseInt(r.c||0));
      return res.json({
        success: true,
        action: 'health',
        cse_configured: !!cfg,
        family_members_inserted_total: familyCount,
        valid_actions: ['health','resolve','batch'],
        timestamp: new Date().toISOString()
      });
    }
    if (action === 'resolve') {
      const personId = req.query?.person_id;
      if (!personId) return res.status(400).json({ error: 'person_id required' });
      const v = await db('persons').where('id', personId).first();
      if (!v) return res.status(404).json({ error: 'person_not_found' });
      const r = await resolveOne(db, v);
      return res.json({ success: r.ok, ...r, timestamp: new Date().toISOString() });
    }
    if (action === 'batch') {
      const limit = Math.min(50, parseInt(req.query?.limit || '10'));
      const out = await batchResolve(db, limit);
      return res.json({ success: true, action: 'batch', ...out, timestamp: new Date().toISOString() });
    }
    res.status(400).json({ error: 'unknown action', valid: ['health','resolve','batch'] });
  } catch (e) {
    await reportError(db, 'enrich-funeral-home-survivors', null, e.message);
    res.status(500).json({ success: false, error: e.message });
  }
}

module.exports = handler;
module.exports.handler = handler;
module.exports.resolveOne = resolveOne;
module.exports.batchResolve = batchResolve;
module.exports.parseSurvivors = parseSurvivors;
module.exports.ensureColumns = ensureColumns;
