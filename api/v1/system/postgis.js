/**
 * GET /api/v1/system/postgis?secret=ingest-now — one-time PostGIS setup
 */
const { getDb } = require('../../_db');
const { reportError } = require('./_errors');
const { logChange } = require('./changelog');

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();
  const log = [];
  try {
    try { await db.raw(`CREATE EXTENSION IF NOT EXISTS postgis`); log.push('PostGIS extension enabled'); }
    catch (e) { log.push(`PostGIS extension error: ${e.message}`); }

    try { await db.raw(`ALTER TABLE incidents ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326)`);
          log.push('geom column ensured'); }
    catch (e) { log.push(`geom column error: ${e.message}`); }

    let backfilled = 0;
    try {
      const r = await db.raw(`UPDATE incidents
        SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
        WHERE geom IS NULL AND latitude IS NOT NULL AND longitude IS NOT NULL`);
      backfilled = r.rowCount || 0;
      log.push(`Backfilled geom for ${backfilled} incidents`);
    } catch (e) { log.push(`Backfill error: ${e.message}`); }

    try {
      await db.raw(`
        CREATE OR REPLACE FUNCTION aip_set_geom_from_coords()
        RETURNS TRIGGER AS $func$
        BEGIN
          IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
            NEW.geom = ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
          END IF;
          RETURN NEW;
        END;
        $func$ LANGUAGE plpgsql;
        DROP TRIGGER IF EXISTS tr_aip_incidents_geom ON incidents;
        CREATE TRIGGER tr_aip_incidents_geom
        BEFORE INSERT OR UPDATE ON incidents
        FOR EACH ROW EXECUTE FUNCTION aip_set_geom_from_coords();
      `);
      log.push('geom trigger installed');
    } catch (e) { log.push(`Trigger error: ${e.message}`); }

    try { await db.raw(`CREATE INDEX IF NOT EXISTS idx_incidents_geom ON incidents USING GIST(geom)`);
          log.push('GIST index ensured'); }
    catch (e) { log.push(`GIST index error: ${e.message}`); }

    try {
      await db.raw(`
        CREATE INDEX IF NOT EXISTS idx_incidents_occurred_brin ON incidents USING BRIN(occurred_at);
        CREATE INDEX IF NOT EXISTS idx_incidents_discovered_brin ON incidents USING BRIN(discovered_at);
      `);
      log.push('BRIN time-series indexes ensured');
    } catch (e) { log.push(`BRIN error: ${e.message}`); }

    const stats = await db.raw(`
      SELECT COUNT(*) as total, COUNT(geom) as with_geom, COUNT(*) - COUNT(geom) as without_geom
      FROM incidents
    `).then(r => r.rows[0]).catch(() => ({}));

    await logChange(db, {
      kind: 'schema',
      title: 'PostGIS setup + backfill',
      summary: `Ensured PostGIS, geom trigger, GIST + BRIN indexes. Backfilled ${backfilled} rows.`,
      author: 'system:postgis',
      meta: { backfilled, log_steps: log.length }
    });

    res.json({ success: true, log, backfilled, stats, timestamp: new Date().toISOString() });
  } catch (err) {
    await reportError(db, 'postgis', null, err.message);
    res.status(500).json({ error: err.message, log });
  }
};
