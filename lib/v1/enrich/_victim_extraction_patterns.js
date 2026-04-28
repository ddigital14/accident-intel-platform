/**
 * VICTIM EXTRACTION PATTERNS — Phase 39
 *
 * Shared regex library for extracting victim names from news/police/obit text.
 * Base extractors (news.js, news-rss.js, obituaries.js) used to depend solely
 * on whatever the LLM returned, which often missed names buried in sentences
 * like "police identified the victim as Jane Smith". This module provides
 * aggressive regex passes that catch those misses.
 *
 * Every extracted name MUST still pass `applyDenyList()` before being
 * persisted — these patterns intentionally over-extract and rely on the
 * deny-list to drop journalists/officers/witnesses.
 */

const IDENTIFIED_AS_RX = new RegExp(
  '(?:identified\\s+(?:the\\s+(?:victim|deceased|driver|passenger|pedestrian|cyclist|motorcyclist)\\s+)?as|' +
  'identified\\s+by\\s+(?:police|authorities|the\\s+(?:coroner|medical\\s+examiner))\\s+as|' +
  'named\\s+(?:the\\s+(?:victim|deceased)\\s+)?as|' +
  'authorities\\s+identified\\s+(?:the\\s+(?:victim|deceased)\\s+)?as)' +
  '\\s+(?:\\d{1,3}-year-old\\s+)?' +
  '(?:Mr\\.?|Mrs\\.?|Ms\\.?|Dr\\.?|Officer|Deputy|Sgt\\.?|Lt\\.?|Cpt\\.?)?\\s*' +
  '([A-Z][a-zA-Z\'\\-]+(?:\\s+[A-Z][a-zA-Z\'\\-\\.]+){1,3})',
  'gi'
);

const AGE_NAME_RX = new RegExp(
  '\\b([A-Z][a-zA-Z\'\\-]+(?:\\s+[A-Z][a-zA-Z\'\\-\\.]+){1,3})\\s*,\\s*' +
  '(?:age\\s+)?(\\d{1,3})(?:[\\s,\\-]|\\b)',
  'g'
);

const KILLED_NAME_RX = new RegExp(
  '\\b([A-Z][a-zA-Z\'\\-]+(?:\\s+[A-Z][a-zA-Z\'\\-\\.]+){1,3})\\s*,\\s*\\d{1,3}\\s*,\\s*' +
  '(?:was|were|of\\s+|a\\s+resident|a\\s+native|formerly)?' +
  '\\s*(?:killed|died|fatally|pronounced|deceased|succumbed|lost\\s+(?:his|her|their)\\s+life|' +
  'transported|airlifted|hospitalized|of\\s+[A-Z])',
  'gi'
);

const VICTIM_NAMED_RX = new RegExp(
  '(?:the\\s+(?:victim|deceased|driver|passenger|pedestrian|cyclist|motorcyclist),?|' +
  'victim\\s+named|' +
  'deceased\\s+(?:was|has\\s+been\\s+)?identified\\s+as|' +
  'victims?\\s+(?:include|are|were|has\\s+been\\s+identified\\s+as))' +
  '\\s+' +
  '(?:Mr\\.?|Mrs\\.?|Ms\\.?|Dr\\.?)?\\s*' +
  '([A-Z][a-zA-Z\'\\-]+(?:\\s+[A-Z][a-zA-Z\'\\-\\.]+){1,3})',
  'gi'
);

const HOSPITALIZED_NAME_RX = new RegExp(
  '\\b([A-Z][a-zA-Z\'\\-]+(?:\\s+[A-Z][a-zA-Z\'\\-\\.]+){1,3})\\s+' +
  '(?:was|were|is|are|remains?)\\s+' +
  '(?:hospitalized|in\\s+(?:critical|serious|stable|fair|grave|guarded)\\s+condition|' +
  'rushed\\s+to|airlifted\\s+to|transported\\s+to|flown\\s+to|taken\\s+to|' +
  'admitted\\s+to|recovering\\s+at)',
  'g'
);

const NEXT_OF_KIN_RX = new RegExp(
  '(?:next\\s+of\\s+kin|survived\\s+by|leaves\\s+behind|preceded\\s+in\\s+death\\s+by|' +
  '(?:son|daughter|wife|husband|spouse|brother|sister|father|mother|parent)\\s+of)' +
  '[\\s:,]+' +
  '(?:Mr\\.?|Mrs\\.?|Ms\\.?)?\\s*' +
  '([A-Z][a-zA-Z\'\\-]+(?:\\s+[A-Z][a-zA-Z\'\\-\\.]+){1,3})',
  'gi'
);

const FAMILY_OF_RX = new RegExp(
  "\\b([A-Z][a-zA-Z'\\-]+(?:\\s+[A-Z][a-zA-Z'\\-\\.]+){1,3})\\'s\\s+(?:family|relatives|loved\\s+ones|next\\s+of\\s+kin|widow|widower)",
  'g'
);

function extractVictimNames(text) {
  if (!text || typeof text !== 'string') return [];
  const out = new Map();
  const PATTERNS = [
    ['identified_as', IDENTIFIED_AS_RX],
    ['victim_named', VICTIM_NAMED_RX],
    ['killed_name', KILLED_NAME_RX],
    ['hospitalized', HOSPITALIZED_NAME_RX],
    ['age_name', AGE_NAME_RX],
    ['next_of_kin', NEXT_OF_KIN_RX],
    ['family_of', FAMILY_OF_RX]
  ];
  // Isolate per-sentence so a name doesn't bleed across sentence boundaries
  const sentences = String(text).split(/(?<=[.!?])\s+/g);
  for (const sentence of sentences) {
    if (!sentence || sentence.length < 5) continue;
    for (const [label, rx] of PATTERNS) {
      const r = new RegExp(rx.source, rx.flags);
      let m;
      let safety = 0;
      while ((m = r.exec(sentence)) !== null && safety++ < 10) {
        let cand = (m[1] || '').trim().replace(/\s+/g, ' ');
        if (!cand) { if (!r.global) break; continue; }
        // Trim trailing/leading words that look like a sentence boundary leak
        cand = cand.replace(/\s+(was|were|is|are|of|the|a|an|in|at|on|by|for|to|from|and|or|but)$/i, '').trim();
        cand = cand.replace(/[\.\,;:!\?]+$/, '').trim();
        // Must be 2-4 capitalized tokens
        if (!/^[A-Z][a-zA-Z'\-]+(\s+[A-Z][a-zA-Z'\-\.]+){1,3}$/.test(cand)) { if (!r.global) break; continue; }
        const key = cand.toLowerCase();
        if (!out.has(key)) out.set(key, { name: cand, sources: new Set() });
        out.get(key).sources.add(label);
        if (!r.global) break;
      }
    }
  }
  return Array.from(out.values()).map(v => ({ name: v.name, sources: Array.from(v.sources) }));
}

function extractAndFilter(text, denyFn) {
  const cands = extractVictimNames(text);
  if (typeof denyFn !== 'function') return cands;
  return cands
    .map(c => {
      const safe = denyFn(c.name, text);
      return safe ? { name: safe, sources: c.sources } : null;
    })
    .filter(Boolean);
}

module.exports = {
  IDENTIFIED_AS_RX,
  AGE_NAME_RX,
  KILLED_NAME_RX,
  VICTIM_NAMED_RX,
  HOSPITALIZED_NAME_RX,
  NEXT_OF_KIN_RX,
  FAMILY_OF_RX,
  extractVictimNames,
  extractAndFilter
};
