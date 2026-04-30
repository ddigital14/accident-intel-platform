/**
 * Phase 50: Centralized PI keyword library.
 *
 * Used by news.js (NewsAPI search queries) and news-rss.js (RSS title pre-filter).
 * Includes Spanish-language accident keywords for Hispanic-market sources.
 *
 * Exports:
 *   PI_KEYWORDS (string[])    - all individual phrases for search-API rotation
 *   PI_KEYWORD_REGEX (RegExp) - boundary-matched, case-insensitive filter for RSS bodies
 *   rotateQueries(n)          - pick N varied keywords for query rotation
 */

const PI_KEYWORDS = [
  // Motor vehicle
  'car accident', 'auto accident', 'crash', 'collision', 'vehicle accident',
  'rear end', 'head-on', 'rollover', 'fender bender', 'multi-vehicle collision',
  'wrong-way driver', 'hit and run', 't-bone crash', 'sideswipe',
  // Trucks
  'truck accident', '18-wheeler', 'tractor trailer', 'big rig', 'semi truck',
  'commercial vehicle', 'box truck', 'delivery truck', 'jackknife',
  // Motorcycles
  'motorcycle accident', 'motorcycle crash', 'bike accident', 'biker killed',
  'scooter accident', 'moped', 'motorcyclist',
  // Pedestrian/cyclist
  'pedestrian struck', 'pedestrian hit', 'pedestrian killed',
  'cyclist struck', 'bicycle accident', 'e-bike', 'bicyclist',
  // Commercial / public transit
  'bus accident', 'school bus crash', 'taxi accident', 'rideshare crash',
  'uber accident', 'lyft accident', 'transit collision',
  // Workplace
  'workplace injury', 'construction accident', 'worker killed',
  'fall at work', 'osha investigation', 'forklift accident', 'electrocuted',
  'scaffolding collapse', 'crane accident',
  // Premises
  'slip and fall', 'dog bite', 'swimming pool drowning',
  'pool drowning', 'apartment fire injury', 'staircase fall',
  // Specific severity
  'fatal crash', 'fatal accident', 'fatality', 'killed in', 'died in',
  'critical condition', 'serious injury', 'airlifted', 'life-threatening',
  // Spanish
  'accidente fatal', 'choque', 'atropellado', 'víctima de accidente',
  'fallecido en accidente', 'muerto en choque', 'accidente automovilístico',
  'colisión', 'accidente de motocicleta', 'peatón atropellado'
];

// Word-boundary regex for body-text pre-filter. Built dynamically from
// PI_KEYWORDS plus a few stem-matchers (so "wrecked", "injured" etc. catch).
const PI_KEYWORD_REGEX = new RegExp(
  '\\b(?:' + [
    // explicit phrases (escape special chars)
    ...PI_KEYWORDS.map(k => k.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/[ -]/g, '[ -]')),
    // English stems
    'crash', 'accident', 'collision', 'wreck', 'wrecked', 'injured', 'injuries',
    'killed', 'kills', 'dead', 'deadly', 'fatal', 'fatalit(?:y|ies)',
    'hospitalized', 'critical', 'trooper', 'state[- ]police', 'sheriff',
    'paralyzed', 'amputat', 'burn(?:ed|s)?', 'dui', 'dwi', 'reckless',
    'wrongful[- ]death', 'personal[- ]injur',
    // Spanish stems
    'muri[oó]', 'fallec[ií]', 'atropell', 'víctim', 'choqu[eé]', 'her[ií]d', 'muert[oa]'
  ].join('|') + ')\\b',
  'i'
);

function rotateQueries(n = 5) {
  const shuffled = [...PI_KEYWORDS].sort(() => Math.random() - 0.5);
  return shuffled.slice(0, Math.max(1, Math.min(PI_KEYWORDS.length, n)));
}

module.exports = { PI_KEYWORDS, PI_KEYWORD_REGEX, rotateQueries };
