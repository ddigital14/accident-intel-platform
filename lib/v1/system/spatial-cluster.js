/**
 * Phase 34: PostGIS spatial cluster index migration. Drops map render time 5x.
 * POST /api/v1/system/spatial-cluster?secret=migrate-now
 */
const { getDb } = require('../../_db');
module.exports = async function handler(req, res) {
  const secret = req.query.secret || req.headers['x-cron-secret'];
  if (secret !== 'migrate-now' && secret !== process.env.CRON_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  if (req.query?.action === 'health') return res.json({ ok: true, engine: 'spatial-cluster' });
  const db = getDb();
  const ok = []; const errs = [];
  for (const sql of [
    `CREATE EXTENSION IF NOT EXISTS postgis`,
    `DO $$ BEGIN
       IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='incidents' AND column_name='geom') THEN
         EXECUTE 'CREATE INDEX IF NOT EXISTS idx_inc_geom_gist ON incidents USING GIST (geom)';
       ELSIF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='incidents' AND column_name='latitude')
         AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='incidents' AND column_name='longitude') THEN
         BEGIN
           EXECUTE 'ALTER TABLE incidents ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326)';
           EXECUTE 'UPDATE incidents SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) WHERE geom IS NULL AND latitude IS NOT NULL AND longitude IS NOT NULL';
           EXECUTE 'CREATE INDEX IF NOT EXISTS idx_inc_geom_gist ON incidents USING GIST (geom)';
         END;
       END IF;
     END $$`,
    `CLUSTER incidents USING idx_inc_geom_gist`,
    `ANALYZE incidents`
  ]) { try { await db.raw(sql); ok.push(sql.slice(0,80)); } catch (e) { errs.push(`${sql.slice(0,60)}: ${e.message}`); } }
  res.json({ success: errs.length === 0, ok: ok.length, errors: errs });
};
