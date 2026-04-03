/**
 * DEDUPLICATION PROCESSOR
 * Identifies and merges duplicate incidents from different sources
 */
const db = require('../../config/database');
const { logger } = require('../../utils/logger');

async function runDeduplication() {
  // Find potential duplicates: same metro, within 1 hour, similar location
  const candidates = await db.raw(`
    SELECT a.id as incident_a, b.id as incident_b,
      ST_Distance(a.geom::geography, b.geom::geography) as distance_meters,
      ABS(EXTRACT(EPOCH FROM (a.occurred_at - b.occurred_at))) as time_diff_seconds,
      similarity(COALESCE(a.address,''), COALESCE(b.address,'')) as address_similarity
    FROM incidents a
    JOIN incidents b ON a.id < b.id
      AND a.metro_area_id = b.metro_area_id
      AND a.created_at > NOW() - INTERVAL '24 hours'
      AND b.created_at > NOW() - INTERVAL '24 hours'
    WHERE
      (
        (a.geom IS NOT NULL AND b.geom IS NOT NULL AND ST_DWithin(a.geom::geography, b.geom::geography, 300))
        OR similarity(COALESCE(a.address,''), COALESCE(b.address,'')) > 0.5
      )
      AND ABS(EXTRACT(EPOCH FROM (a.occurred_at - b.occurred_at))) < 3600
      AND NOT EXISTS (
        SELECT 1 FROM incident_matches im
        WHERE (im.incident_id = a.id AND im.matched_incident_id = b.id)
          OR (im.incident_id = b.id AND im.matched_incident_id = a.id)
      )
    LIMIT 50
  `);

  if (candidates.rows.length === 0) return;
  logger.info(`Deduplication: found ${candidates.rows.length} potential duplicate pairs`);

  for (const pair of candidates.rows) {
    const confidence = calculateMatchConfidence(pair);

    // Record the match
    await db('incident_matches').insert({
      incident_id: pair.incident_a,
      matched_incident_id: pair.incident_b,
      match_confidence: confidence,
      match_reason: buildMatchReason(pair),
      is_confirmed: confidence >= 85
    }).onConflict(['incident_id', 'matched_incident_id']).ignore();

    // Auto-merge if very high confidence
    if (confidence >= 90) {
      await mergeIncidents(pair.incident_a, pair.incident_b);
    }
  }
}

function calculateMatchConfidence(pair) {
  let score = 0;
  if (pair.distance_meters !== null && pair.distance_meters < 100) score += 40;
  else if (pair.distance_meters !== null && pair.distance_meters < 300) score += 25;
  if (pair.time_diff_seconds < 300) score += 30;
  else if (pair.time_diff_seconds < 1800) score += 15;
  if (pair.address_similarity > 0.8) score += 30;
  else if (pair.address_similarity > 0.5) score += 15;
  return Math.min(100, score);
}

function buildMatchReason(pair) {
  const reasons = [];
  if (pair.distance_meters !== null && pair.distance_meters < 300) reasons.push(`location_${Math.round(pair.distance_meters)}m`);
  if (pair.time_diff_seconds < 1800) reasons.push(`time_${Math.round(pair.time_diff_seconds / 60)}min`);
  if (pair.address_similarity > 0.5) reasons.push(`address_${Math.round(pair.address_similarity * 100)}%`);
  return reasons.join(', ');
}

async function mergeIncidents(keepId, mergeId) {
  try {
    const [keep, merge] = await Promise.all([
      db('incidents').where({ id: keepId }).first(),
      db('incidents').where({ id: mergeId }).first()
    ]);

    // Merge: keep the one with higher confidence/more sources
    const primary = keep.confidence_score >= merge.confidence_score ? keep : merge;
    const secondary = primary.id === keep.id ? merge : keep;

    // Update primary with any missing fields from secondary
    const updates = {};
    const fields = ['police_report_number', 'police_department', 'address', 'city', 'state', 'zip',
      'latitude', 'longitude', 'description', 'injuries_count', 'fatalities_count'];

    fields.forEach(f => {
      if (!primary[f] && secondary[f]) updates[f] = secondary[f];
    });

    updates.source_count = (primary.source_count || 1) + (secondary.source_count || 1);
    updates.confidence_score = Math.min(100, (primary.confidence_score || 30) + 20);

    if (primary.description && secondary.description && primary.description !== secondary.description) {
      updates.description = `${primary.description}\n\n[Merged]: ${secondary.description}`;
    }

    await db('incidents').where({ id: primary.id }).update(updates);

    // Move all persons, vehicles, source_reports to primary
    await db('persons').where({ incident_id: secondary.id }).update({ incident_id: primary.id });
    await db('vehicles').where({ incident_id: secondary.id }).update({ incident_id: primary.id });
    await db('source_reports').where({ incident_id: secondary.id }).update({ incident_id: primary.id });
    await db('activity_log').where({ incident_id: secondary.id }).update({ incident_id: primary.id });

    // Mark as merged
    await db('incident_matches')
      .where({ incident_id: keepId, matched_incident_id: mergeId })
      .orWhere({ incident_id: mergeId, matched_incident_id: keepId })
      .update({ merged: true, is_confirmed: true });

    // Soft-delete the secondary
    await db('incidents').where({ id: secondary.id }).update({ status: 'invalid', notes: `Merged into ${primary.id}` });

    logger.info(`Merged incident ${secondary.id} into ${primary.id}`);
  } catch (err) {
    logger.error(`Merge failed for ${keepId} + ${mergeId}:`, err.message);
  }
}

module.exports = { runDeduplication, mergeIncidents };
