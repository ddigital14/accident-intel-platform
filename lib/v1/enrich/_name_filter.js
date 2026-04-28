/**
 * NAME FILTER — Deny-list utility for accident-victim name extraction
 *
 * Phase 38 Wave A. Single source of truth for "is this name a real victim,
 * or a journalist/officer/witness/family quote?". Used by every ingest
 * extractor BEFORE it persists a person row.
 *
 * Two layers:
 *   1. applyDenyList(name, surroundingText) -> string|null   (cheap, sync)
 *      Returns name if it survived the deny rules, otherwise null.
 *   2. Stage-A rules (BYLINE_RX, OFFICIAL_RX, etc.) are exported individually
 *      so victim-verifier.js can reuse them.
 *
 * NOTE: this is intentionally aggressive. We'd rather drop 5 real victims
 * than keep 1 journalist on a qualified lead. Anything ambiguous should be
 * sent to victim-verifier (Stage B / Claude) — not stored as a victim.
 */

// Bylines / "by Reporter" — match name immediately after byline keywords
const BYLINE_RX = /\b(?:by|reported\s+by|reporter|updated\s+by|photo\s+by|photos?\s+by|posted\s+by|written\s+by|writer|editor|edited\s+by|correspondent|story\s+by|words\s+by|filed\s+by)\s*[:\-—]?\s*([A-Z][a-z'\-]+(?:\s+[A-Z][a-z'\-]+){1,3})/i;

// Official / law-enforcement / coroner titles — match title before name
const OFFICIAL_RX = /\b(?:officer|sergeant|sgt\.?|detective|det\.?|chief|lieutenant|lt\.?|captain|cpt\.?|spokesperson|spokeswoman|spokesman|trooper|deputy|sheriff|under\s*sheriff|commander|cmdr\.?|inspector|public\s+information\s+officer|p\.?i\.?o\.?|coroner|medical\s+examiner|m\.?e\.?\s+spokesperson|mayor|councilman|councilwoman|council\s*member|fire\s+chief|fire\s+marshal|battalion\s+chief|attorney\s+general|prosecutor|district\s+attorney|d\.?a\.?|asst\.?\s*chief|assistant\s+chief|major|colonel|col\.?|special\s+agent|fbi\s+agent|nhtsa\s+spokesperson|aaa\s+spokesperson|representative)\s+([A-Z][a-z'\-]+(?:\s+[A-Z][a-z'\-]+){0,3})/i;

// Generic "according to X" / "X said" without victim verbs nearby
const ATTRIBUTION_RX = /\b([A-Z][a-z'\-]+(?:\s+[A-Z][a-z'\-]+){1,3})\s+(?:said|told\s+(?:reporters|the\s+(?:press|station|paper|times|news|tribune|herald|post|chronicle|journal|gazette|sun|examiner|inquirer|outlet|outlets)|us|abc\d*|nbc\d*|cbs\d*|fox\d*|cnn|wsbtv|wesh|khou|ktrk|wfaa)|added|noted|wrote|tweeted|posted)\b/i;
const ACCORDING_TO_RX = /\baccording\s+to\s+([A-Z][a-z'\-]+(?:\s+[A-Z][a-z'\-]+){1,3})\b/i;

// Journalist outlet sigils — "Jane Doe, CNN" / "Jane Doe | AP" / "via @handle"
const OUTLET_TAG_RX = /\b([A-Z][a-z'\-]+(?:\s+[A-Z][a-z'\-]+){1,3})\s*(?:[,|\-—]|—)\s*(?:CNN|AP|Reuters|Bloomberg|NBC|ABC|CBS|FOX|PBS|NPR|BBC|MSNBC|AFP|UPI|HuffPost|Politico|WSJ|NYT|WaPo|USA\s+Today|Insider|Axios|Vox|Slate|Vice|TIME|Newsweek)\b/;
const VIA_HANDLE_RX = /via\s+@[A-Za-z0-9_]+/i;

// Verbs/phrases that confirm someone WAS a victim
const VICTIM_VERBS = [
  'was killed', 'were killed', 'killed in', 'died', 'has died', 'was pronounced dead',
  'pronounced dead', 'succumbed to injuries', 'succumbed to his injuries',
  'succumbed to her injuries', 'lost (his|her|their) life', 'fatally injured',
  'was injured', 'were injured', 'critically injured', 'seriously injured',
  'was in critical condition', 'in critical condition', 'in serious condition',
  'was driving', 'were driving', 'the driver', 'driver of the',
  'the victim', 'identified as', 'identified the victim as',
  'passenger', 'pedestrian', 'cyclist', 'bicyclist', 'motorcyclist',
  'was struck', 'were struck', 'struck by', 'was hit', 'were hit',
  'was thrown', 'were ejected', 'ejected from',
  'was airlifted', 'were airlifted', 'airlifted to',
  'transported to', 'taken to', 'rushed to', 'flown to',
  'hospitalized', 'in the hospital',
  'wrongful death of', 'survived by', 'leaves behind', 'is survived by',
  'crash claimed', 'died at the scene', 'died on scene', 'died at the hospital'
];
const VICTIM_VERBS_RX = new RegExp('\\b(?:' + VICTIM_VERBS.map(s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|') + ')\\b', 'i');

// Hard ban list of obvious non-victim names (PR firms, departments, etc.)
const HARD_BAN_NAMES = new Set([
  'john doe', 'jane doe', 'doe doe', 'no name', 'unknown unknown',
  'breaking news', 'staff report', 'staff reports', 'news desk', 'editorial board',
  'press release', 'public information', 'getty images', 'associated press',
  'fox news', 'cbs news', 'abc news', 'nbc news', 'cnn newsroom'
]);

// Names where one or both tokens look like a title/role rather than a person
const ROLEY_TOKENS = new Set([
  'police', 'fire', 'ems', 'sheriff', 'department', 'spokesperson', 'spokeswoman',
  'spokesman', 'pio', 'coroner', 'mayor', 'governor', 'witness', 'witnesses',
  'family', 'relative', 'relatives', 'driver', 'passenger', 'victim', 'reporter',
  'editor', 'photographer', 'producer', 'anchor', 'official', 'officials'
]);

function _normalizeName(n) {
  return String(n || '').trim().replace(/\s+/g, ' ').toLowerCase();
}

function _looksLikeRoleTokens(name) {
  const tokens = String(name || '').toLowerCase().split(/\s+/).filter(Boolean);
  if (!tokens.length) return true;
  return tokens.some(t => ROLEY_TOKENS.has(t));
}

/**
 * applyDenyList(name, surroundingText)
 * Returns the trimmed name if it passes deny rules; null otherwise.
 */
function applyDenyList(name, surroundingText) {
  if (!name) return null;
  const trimmed = String(name).trim().replace(/\s+/g, ' ');
  if (trimmed.length < 3) return null;
  if (!/^[A-Z][a-zA-Z'\-]+(\s+[A-Z][a-zA-Z'\-\.]+){1,3}$/.test(trimmed)) return null;

  const norm = _normalizeName(trimmed);
  if (HARD_BAN_NAMES.has(norm)) return null;
  if (_looksLikeRoleTokens(trimmed)) return null;

  const text = String(surroundingText || '');

  const bylineMatch = text.match(BYLINE_RX);
  if (bylineMatch && _normalizeName(bylineMatch[1]) === norm) return null;

  const officialMatch = text.match(OFFICIAL_RX);
  if (officialMatch && _normalizeName(officialMatch[1]) === norm) return null;

  const outletMatch = text.match(OUTLET_TAG_RX);
  if (outletMatch && _normalizeName(outletMatch[1]) === norm) return null;

  if (VIA_HANDLE_RX.test(text) && text.toLowerCase().includes(norm)) {
    const idx = text.toLowerCase().indexOf(norm);
    const viaIdx = text.toLowerCase().search(/via\s+@/);
    if (idx >= 0 && viaIdx >= 0 && Math.abs(idx - viaIdx) < 60) return null;
  }

  const attrib = text.match(ATTRIBUTION_RX);
  const acc = text.match(ACCORDING_TO_RX);
  const isAttributionOnly =
    (attrib && _normalizeName(attrib[1]) === norm) ||
    (acc && _normalizeName(acc[1]) === norm);
  if (isAttributionOnly && !VICTIM_VERBS_RX.test(text)) return null;

  return trimmed;
}

/**
 * Stage-A classification used by victim-verifier without an LLM call.
 */
function quickClassify(name, text) {
  if (!name) return { decision: 'deny', confidence: 95, reason: 'empty_name' };
  const trimmed = String(name).trim();
  const norm = _normalizeName(trimmed);
  const t = String(text || '');

  if (!/^[A-Z][a-zA-Z'\-]+(\s+[A-Z][a-zA-Z'\-\.]+){1,3}$/.test(trimmed)) {
    return { decision: 'deny', confidence: 95, reason: 'name_not_well_formed' };
  }
  if (HARD_BAN_NAMES.has(norm)) return { decision: 'deny', confidence: 95, reason: 'hard_ban_name' };
  if (_looksLikeRoleTokens(trimmed)) return { decision: 'deny', confidence: 95, reason: 'role_token_in_name' };

  const bylineMatch = t.match(BYLINE_RX);
  if (bylineMatch && _normalizeName(bylineMatch[1]) === norm) {
    return { decision: 'deny', confidence: 95, reason: 'byline_match' };
  }
  const officialMatch = t.match(OFFICIAL_RX);
  if (officialMatch && _normalizeName(officialMatch[1]) === norm) {
    return { decision: 'deny', confidence: 95, reason: 'official_title:' + officialMatch[0].slice(0, 40) };
  }
  const outletMatch = t.match(OUTLET_TAG_RX);
  if (outletMatch && _normalizeName(outletMatch[1]) === norm) {
    return { decision: 'deny', confidence: 95, reason: 'outlet_tag' };
  }

  if (VICTIM_VERBS_RX.test(t)) {
    const idx = t.toLowerCase().indexOf(norm);
    if (idx >= 0) {
      const window = t.slice(Math.max(0, idx - 250), Math.min(t.length, idx + 250));
      if (VICTIM_VERBS_RX.test(window)) {
        return { decision: 'accept', confidence: 90, reason: 'victim_verb_near_name' };
      }
    }
  }

  const attrib = t.match(ATTRIBUTION_RX);
  const acc = t.match(ACCORDING_TO_RX);
  if ((attrib && _normalizeName(attrib[1]) === norm) ||
      (acc && _normalizeName(acc[1]) === norm)) {
    if (!VICTIM_VERBS_RX.test(t)) {
      return { decision: 'deny', confidence: 90, reason: 'attribution_only' };
    }
  }

  return { decision: 'unsure', confidence: 50, reason: 'ambiguous' };
}

module.exports = {
  applyDenyList,
  quickClassify,
  BYLINE_RX,
  OFFICIAL_RX,
  ATTRIBUTION_RX,
  ACCORDING_TO_RX,
  OUTLET_TAG_RX,
  VIA_HANDLE_RX,
  VICTIM_VERBS_RX,
  HARD_BAN_NAMES,
  ROLEY_TOKENS
};
