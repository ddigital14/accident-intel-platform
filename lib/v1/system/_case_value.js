/**
 * PREDICTIVE CASE VALUE SCORING
 *
 * Beyond-competitor feature: estimate the dollar-value band of each lead.
 * Logistic-style weighting, hand-tuned from PI industry priors. Once we have
 * historical settled-case data we'll retrain coefficients.
 *
 * Public API:
 *   computeCaseValue(incident, persons[], opts) -> { score, band, band_low_usd, band_high_usd, factors }
 *   scoreAndStore(db, incidentId)
 *   timeOfDayBucket(occurred_at)
 */
const SEVERITY_SCORE = { fatal: 10, critical: 8, serious: 5, moderate: 3, minor: 1, unknown: 1 };
const BANDS = [
  { name: 'low',      max: 8,   low_usd: 5000,    high_usd: 25000  },
  { name: 'moderate', max: 15,  low_usd: 25000,   high_usd: 100000 },
  { name: 'high',     max: 25,  low_usd: 100000,  high_usd: 500000 },
  { name: 'premium',  max: 999, low_usd: 500000,  high_usd: 5000000 },
];

function bandOf(score) {
  for (const b of BANDS) if (score < b.max) return b;
  return BANDS[BANDS.length - 1];
}

function computeCaseValue(incident, persons = [], opts = {}) {
  const factors = {};
  let score = 0;

  const sev = String(incident.severity || 'unknown').toLowerCase();
  factors.severity = SEVERITY_SCORE[sev] || 1;
  score += factors.severity;

  const fat = Math.min(4, parseInt(incident.fatalities_count) || 0);
  factors.fatalities = fat * 6;
  score += factors.fatalities;

  const inj = Math.min(5, parseInt(incident.injuries_count) || 0);
  factors.injuries = inj;
  score += factors.injuries;

  const conf = parseInt(incident.confidence_score) || 50;
  factors.identity_confidence = Math.round((conf - 50) / 12);
  score += factors.identity_confidence;

  const src = parseInt(incident.source_count) || 1;
  factors.source_count = Math.min(3, Math.floor(src / 2));
  score += factors.source_count;

  if (incident.incident_type === 'truck_accident') {
    factors.commercial_vehicle = 5;
    score += 5;
  }
  if (incident.helicopter_dispatched) {
    factors.helicopter = 3;
    score += 3;
  }

  let personPenalty = 0;
  for (const p of persons || []) {
    if (p.has_attorney === true) personPenalty -= 10;
    if (p.injury_severity === 'fatal') score += 4;
    if (p.injury_severity === 'incapacitating') score += 3;
  }
  factors.attorney_penalty = personPenalty;
  score += personPenalty;

  if (opts.vehicle_recalls_count > 0) { factors.vehicle_recalls = 4; score += 4; }
  if (opts.weather_factor) { factors.weather = 2; score += 2; }
  if (opts.prior_incidents_at_location > 0) { factors.dangerous_location = 2; score += 2; }

  score = Math.max(0, score);
  const band = bandOf(score);

  return {
    score, band: band.name,
    band_low_usd: band.low_usd, band_high_usd: band.high_usd,
    factors,
    computed_at: new Date().toISOString(),
  };
}

let _ensured = false;
async function ensureColumns(db) {
  if (_ensured) return;
  try {
    await db.raw(`ALTER TABLE incidents ADD COLUMN IF NOT EXISTS case_value_score INTEGER`);
    await db.raw(`ALTER TABLE incidents ADD COLUMN IF NOT EXISTS case_value_band VARCHAR(20)`);
    await db.raw(`ALTER TABLE incidents ADD COLUMN IF NOT EXISTS case_value_low_usd INTEGER`);
    await db.raw(`ALTER TABLE incidents ADD COLUMN IF NOT EXISTS case_value_high_usd INTEGER`);
    await db.raw(`ALTER TABLE incidents ADD COLUMN IF NOT EXISTS case_value_factors JSONB`);
    await db.raw(`ALTER TABLE incidents ADD COLUMN IF NOT EXISTS vehicle_recalls_count INTEGER`);
    await db.raw(`ALTER TABLE incidents ADD COLUMN IF NOT EXISTS weather_at_incident JSONB`);
    await db.raw(`ALTER TABLE incidents ADD COLUMN IF NOT EXISTS time_of_day_bucket VARCHAR(20)`);
    await db.raw(`ALTER TABLE incidents ADD COLUMN IF NOT EXISTS prior_incidents_at_location INTEGER`);
    await db.raw(`ALTER TABLE incidents ADD COLUMN IF NOT EXISTS first_responder_agency VARCHAR(200)`);
    _ensured = true;
  } catch (_) { /* non-fatal */ }
}

async function scoreAndStore(db, incidentId) {
  await ensureColumns(db);
  const inc = await db('incidents').where('id', incidentId).first();
  if (!inc) return null;
  const persons = await db('persons').where('incident_id', incidentId).select('*');
  const result = computeCaseValue(inc, persons, {
    vehicle_recalls_count: inc.vehicle_recalls_count,
    weather_factor: !!inc.weather_at_incident,
    prior_incidents_at_location: inc.prior_incidents_at_location,
  });
  await db('incidents').where('id', incidentId).update({
    case_value_score: result.score,
    case_value_band: result.band,
    case_value_low_usd: result.band_low_usd,
    case_value_high_usd: result.band_high_usd,
    case_value_factors: JSON.stringify(result.factors),
    updated_at: new Date(),
  });
  return result;
}

function timeOfDayBucket(d) {
  if (!d) return null;
  const dt = new Date(d);
  if (isNaN(dt.getTime())) return null;
  const hour = dt.getUTCHours();
  const dow = dt.getUTCDay();
  if (dow === 0 || dow === 6) return 'weekend';
  if (hour >= 22 || hour < 5) return 'overnight';
  if ((hour >= 7 && hour < 10) || (hour >= 16 && hour < 19)) return 'rush_hour';
  if (hour >= 19 && hour < 22) return 'evening';
  return 'day';
}

module.exports = { computeCaseValue, scoreAndStore, ensureColumns, timeOfDayBucket };
