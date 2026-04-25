/**
 * Multi-Source Incident Correlation Engine
 *
 * Deduplicates and merges incidents from all data sources:
 * TomTom, Waze, City Open Data, Scanner, NewsAPI, NHTSA
 *
 * Algorithm:
 * 1. Find uncorrelated incidents (source_count = 1) from last 2 hours
 * 2. For each, search for nearby incidents (within 1km + 60 min)
 * 3. If match found: merge (keep higher-confidence record, boost score)
 * 4. Calculate composite confidence based on source agreement
 *
 * GET /api/v1/ingest/correlate?secret=ingest-now
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');
const { dedupCache, geoCache } = require('../_cache');

// Source reliability weights (higher = more trusted)
const SOURCE_WEIGHTS = {
  'opendata_chicago': 95,
  'opendata_seattle': 95,
  'opendata_sf': 95,
  'opendata_dallas': 90,
  'opendata_houston': 90,
  'opendata_atlanta': 88,
  'opendata_cincinnati': 90,
  'state_txdot': 92,
  'state_ga511': 90,
  'state_fl511': 90,
  'scanner': 85,
  'tomtom': 80,
  'waze': 70,
  'newsapi': 60,
  'nhtsa': 65,
};

function getWeight(sourceType) {
  return SOURCE_WEIGHTS[sourceType] || 50;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const db = getDb();
  const results = { correlated: 0, merged: 0, boosted: 0, total_checked: 0 };

  try {
    const twoHoursAgo = new Date(Date.now() - 2 * 60 * 60 * 1000);

    // Get uncorrelated single-source incidents from last 2 hours
    const singles = await db('incidents')
      .where('source_count', '<=', 1)
      .where('created_at', '>', twoHoursAgo)
      .whereNotNull('latitude')
      .whereNotNull('longitude')
      .select('id', 'latitude', 'longitude', 'occurred_at', 'city', 'severity',
              'confidence_score', 'source_count', 'first_source_id', 'incident_type',
              'description', 'injuries_count', 'fatalities_count', 'vehicles_involved')
      .orderBy('created_at', 'desc')
      .limit(100);

    results.total_checked = singles.length;

    for (const incident of singles) {
      // Find all other incidents within 1km radius and 60 min window
      const occTime = new Date(incident.occurred_at);
      const windowStart = new Date(occTime.getTime() - 60 * 60 * 1000);
      const windowEnd = new Date(occTime.getTime() + 60 * 60 * 1000);

      let nearby;
      try {
        const postgisRes = await db.raw(`
          SELECT id, latitude, longitude, occurred_at, severity,
                 confidence_score, source_count, incident_type,
                 description, injuries_count, fatalities_count, vehicles_involved
          FROM incidents
          WHERE id != $1
            AND occurred_at > $2
            AND occurred_at < $3
            AND geom IS NOT NULL
            AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint($4, $5), 4326)::geography, 1000)
          LIMIT 10
        `, [incident.id, windowStart, windowEnd, incident.longitude, incident.latitude]);
        nearby = postgisRes.rows;
      } catch (postgisErr) {
        // Fall back to Haversine
        nearby = await db('incidents')
          .where('id', '!=', incident.id)
          .where('occurred_at', '>', windowStart)
          .where('occurred_at', '<', windowEnd)
          .whereNotNull('latitude')
          .whereNotNull('longitude')
          .whereRaw(`
            (6371000 * acos(
              LEAST(1.0, GREATEST(-1.0,
                cos(radians(?)) * cos(radians(latitude)) *
                cos(radians(longitude) - radians(?)) +
                sin(radians(?)) * sin(radians(latitude))
              ))
            )) < 1000
          `, [incident.latitude, incident.longitude, incident.latitude])
          .select('id', 'latitude', 'longitude', 'occurred_at', 'severity',
                  'confidence_score', 'source_count', 'incident_type',
                  'description', 'injuries_count', 'fatalities_count', 'vehicles_involved')
          .limit(10);
      }

      if (nearby.length === 0) continue;

      // Found matching incidents — decide primary vs secondary
      // Primary = highest confidence + most source counts
      const allCandidates = [incident, ...nearby];
      allCandidates.sort((a, b) => {
        const scoreA = (a.confidence_score || 0) + (a.source_count || 0) * 10;
        const scoreB = (b.confidence_score || 0) + (b.source_count || 0) * 10;
        return scoreB - scoreA;
      });

      const primary = allCandidates[0];
      const secondaries = allCandidates.slice(1);

      // Merge secondaries into primary
      for (const secondary of secondaries) {
        // Collect the best data from each record
        const mergedData = {};

        // Take the most specific severity
        const severityRank = { 'fatal': 1, 'critical': 2, 'serious': 3, 'moderate': 4, 'minor': 5, 'unknown': 6 };
        if ((severityRank[secondary.severity] || 6) < (severityRank[primary.severity] || 6)) {
          mergedData.severity = secondary.severity;
        }

        // Take the highest injury/fatality/vehicle counts
        if ((secondary.injuries_count || 0) > (primary.injuries_count || 0)) {
          mergedData.injuries_count = secondary.injuries_count;
        }
        if ((secondary.fatalities_count || 0) > (primary.fatalities_count || 0)) {
          mergedData.fatalities_count = secondary.fatalities_count;
        }
        if ((secondary.vehicles_involved || 0) > (primary.vehicles_involved || 0)) {
          mergedData.vehicles_involved = secondary.vehicles_involved;
        }

        // Get source reports from secondary and reassign to primary
        await db('source_reports')
          .where('incident_id', secondary.id)
          .update({ incident_id: primary.id });

        // Move persons from secondary to primary
        await db('persons')
          .where('incident_id', secondary.id)
          .update({ incident_id: primary.id });

        // Move vehicles from secondary to primary
        await db('vehicles')
          .where('incident_id', secondary.id)
          .update({ incident_id: primary.id });

        // Count total sources on primary now
        const sourceCount = await db('source_reports')
          .where('incident_id', primary.id)
          .count('* as count')
          .first();

        // Calculate new composite confidence
        const sources = await db('source_reports')
          .where('incident_id', primary.id)
          .select('source_type', 'confidence');

        let compositeConfidence = 0;
        let totalWeight = 0;
        for (const src of sources) {
          const w = getWeight(src.source_type);
          compositeConfidence += (src.confidence || 50) * w;
          totalWeight += w;
        }
        compositeConfidence = totalWeight > 0 ? Math.round(compositeConfidence / totalWeight) : 50;
        // Bonus for multi-source corroboration
        compositeConfidence = Math.min(99, compositeConfidence + sources.length * 5);

        // Update primary incident
        await db('incidents').where('id', primary.id).update({
          ...mergedData,
          source_count: parseInt(sourceCount?.count || 1),
          confidence_score: compositeConfidence,
          description: primary.description + (secondary.description ? `\n[Corroborated: ${secondary.description.substring(0, 200)}]` : ''),
          updated_at: new Date()
        });

        // Delete the secondary incident (now merged)
        try {
          await db('enrichment_logs').where('incident_id', secondary.id).del();
          await db('activity_log').where('incident_id', secondary.id).del();
          await db('incidents').where('id', secondary.id).del();
          results.merged++;
        } catch (delErr) {
          // If delete fails due to remaining FKs, just mark as merged
          await db('incidents').where('id', secondary.id).update({
            status: 'merged',
            description: db.raw(`description || '\n[Merged into ${primary.id}]'`),
            updated_at: new Date()
          });
        }
      }

      results.correlated++;
      results.boosted++;
    }

    // Also boost confidence of incidents with multiple source_reports
    const multiSourceIncidents = await db('incidents')
      .where('created_at', '>', twoHoursAgo)
      .where('source_count', '>', 1)
      .select('id', 'confidence_score', 'source_count');

    for (const inc of multiSourceIncidents) {
      const newConf = Math.min(99, (inc.confidence_score || 50) + (inc.source_count - 1) * 3);
      if (newConf > (inc.confidence_score || 0)) {
        await db('incidents').where('id', inc.id).update({
          confidence_score: newConf,
          updated_at: new Date()
        });
        results.boosted++;
      }
    }

    res.json({
      success: true,
      message: `Correlation: ${results.correlated} groups found, ${results.merged} merged, ${results.boosted} boosted`,
      ...results,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    console.error('Correlation error:', err);
    await reportError(db, 'correlate', null, err.message, { stack: (err.stack||'').substring(0,1000) });
    res.status(500).json({ error: err.message, results });
  }
};
