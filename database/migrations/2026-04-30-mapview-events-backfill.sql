-- Phase 83: backfill lat/lon events for Pillar 4 map-view to pass
INSERT INTO property_change_events (entity, record_id, property, new_value, source_engine, created_at)
SELECT 'Person', p.id, 'lat', p.lat::text, 'backfill_existing', p.created_at
FROM persons p WHERE p.lat IS NOT NULL
ON CONFLICT DO NOTHING;
INSERT INTO property_change_events (entity, record_id, property, new_value, source_engine, created_at)
SELECT 'Person', p.id, 'lon', p.lon::text, 'backfill_existing', p.created_at
FROM persons p WHERE p.lon IS NOT NULL
ON CONFLICT DO NOTHING;
INSERT INTO property_change_events (entity, record_id, property, new_value, source_engine, created_at)
SELECT 'Incident', i.id, 'latitude', i.latitude::text, 'backfill_existing', COALESCE(i.discovered_at, NOW())
FROM incidents i WHERE i.latitude IS NOT NULL
ON CONFLICT DO NOTHING;
INSERT INTO property_change_events (entity, record_id, property, new_value, source_engine, created_at)
SELECT 'Incident', i.id, 'longitude', i.longitude::text, 'backfill_existing', COALESCE(i.discovered_at, NOW())
FROM incidents i WHERE i.longitude IS NOT NULL
ON CONFLICT DO NOTHING;
INSERT INTO property_change_events (entity, record_id, property, new_value, source_engine, created_at)
SELECT 'Incident', i.id, 'severity', i.severity, 'backfill_existing', COALESCE(i.discovered_at, NOW())
FROM incidents i WHERE i.severity IS NOT NULL
ON CONFLICT DO NOTHING;
INSERT INTO property_change_events (entity, record_id, property, new_value, source_engine, created_at)
SELECT 'Incident', i.id, 'incident_city', i.city, 'backfill_existing', COALESCE(i.discovered_at, NOW())
FROM incidents i WHERE i.city IS NOT NULL
ON CONFLICT DO NOTHING;
INSERT INTO property_change_events (entity, record_id, property, new_value, source_engine, created_at)
SELECT 'Incident', i.id, 'incident_state', i.state, 'backfill_existing', COALESCE(i.discovered_at, NOW())
FROM incidents i WHERE i.state IS NOT NULL
ON CONFLICT DO NOTHING;
