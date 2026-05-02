-- Phase 82: Auto-emit property_change_events on every persons UPDATE.
-- Replaces the gap where direct UPDATE statements bypassed enrichment_logs.

CREATE OR REPLACE FUNCTION emit_property_changes() RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    IF NEW.phone IS DISTINCT FROM OLD.phone THEN
      INSERT INTO property_change_events (entity, record_id, property, old_value, new_value, source_engine, created_at)
      VALUES ('Person', NEW.id, 'phone', OLD.phone, NEW.phone, 'persons_trigger', NOW());
    END IF;
    IF NEW.email IS DISTINCT FROM OLD.email THEN
      INSERT INTO property_change_events (entity, record_id, property, old_value, new_value, source_engine, created_at)
      VALUES ('Person', NEW.id, 'email', OLD.email, NEW.email, 'persons_trigger', NOW());
    END IF;
    IF NEW.address IS DISTINCT FROM OLD.address THEN
      INSERT INTO property_change_events (entity, record_id, property, old_value, new_value, source_engine, created_at)
      VALUES ('Person', NEW.id, 'address', OLD.address, NEW.address, 'persons_trigger', NOW());
    END IF;
    IF NEW.full_name IS DISTINCT FROM OLD.full_name THEN
      INSERT INTO property_change_events (entity, record_id, property, old_value, new_value, source_engine, created_at)
      VALUES ('Person', NEW.id, 'full_name', OLD.full_name, NEW.full_name, 'persons_trigger', NOW());
    END IF;
    IF NEW.city IS DISTINCT FROM OLD.city THEN
      INSERT INTO property_change_events (entity, record_id, property, old_value, new_value, source_engine, created_at)
      VALUES ('Person', NEW.id, 'city', OLD.city, NEW.city, 'persons_trigger', NOW());
    END IF;
    IF NEW.state IS DISTINCT FROM OLD.state THEN
      INSERT INTO property_change_events (entity, record_id, property, old_value, new_value, source_engine, created_at)
      VALUES ('Person', NEW.id, 'state', OLD.state, NEW.state, 'persons_trigger', NOW());
    END IF;
    IF NEW.zip IS DISTINCT FROM OLD.zip THEN
      INSERT INTO property_change_events (entity, record_id, property, old_value, new_value, source_engine, created_at)
      VALUES ('Person', NEW.id, 'zip', OLD.zip, NEW.zip, 'persons_trigger', NOW());
    END IF;
    IF NEW.victim_verified IS DISTINCT FROM OLD.victim_verified THEN
      INSERT INTO property_change_events (entity, record_id, property, old_value, new_value, source_engine, created_at)
      VALUES ('Person', NEW.id, 'victim_verified', OLD.victim_verified::text, NEW.victim_verified::text, 'persons_trigger', NOW());
    END IF;
    IF NEW.lead_tier IS DISTINCT FROM OLD.lead_tier THEN
      INSERT INTO property_change_events (entity, record_id, property, old_value, new_value, source_engine, created_at)
      VALUES ('Person', NEW.id, 'lead_tier', OLD.lead_tier, NEW.lead_tier, 'persons_trigger', NOW());
    END IF;
    IF NEW.has_attorney IS DISTINCT FROM OLD.has_attorney THEN
      INSERT INTO property_change_events (entity, record_id, property, old_value, new_value, source_engine, created_at)
      VALUES ('Person', NEW.id, 'has_attorney', OLD.has_attorney::text, NEW.has_attorney::text, 'persons_trigger', NOW());
    END IF;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS persons_emit_property_changes ON persons;
CREATE TRIGGER persons_emit_property_changes
  AFTER UPDATE ON persons
  FOR EACH ROW EXECUTE FUNCTION emit_property_changes();

-- Backfill the table with current persons row state — synthesize "exists since X" events
INSERT INTO property_change_events (entity, record_id, property, new_value, source_engine, created_at)
SELECT 'Person', p.id, 'phone', p.phone, 'backfill_existing', p.created_at
FROM persons p WHERE p.phone IS NOT NULL
ON CONFLICT DO NOTHING;
INSERT INTO property_change_events (entity, record_id, property, new_value, source_engine, created_at)
SELECT 'Person', p.id, 'email', p.email, 'backfill_existing', p.created_at
FROM persons p WHERE p.email IS NOT NULL
ON CONFLICT DO NOTHING;
INSERT INTO property_change_events (entity, record_id, property, new_value, source_engine, created_at)
SELECT 'Person', p.id, 'address', p.address, 'backfill_existing', p.created_at
FROM persons p WHERE p.address IS NOT NULL
ON CONFLICT DO NOTHING;
INSERT INTO property_change_events (entity, record_id, property, new_value, source_engine, created_at)
SELECT 'Person', p.id, 'full_name', p.full_name, 'backfill_existing', p.created_at
FROM persons p WHERE p.full_name IS NOT NULL
ON CONFLICT DO NOTHING;
INSERT INTO property_change_events (entity, record_id, property, new_value, source_engine, created_at)
SELECT 'Person', p.id, 'city', p.city, 'backfill_existing', p.created_at
FROM persons p WHERE p.city IS NOT NULL
ON CONFLICT DO NOTHING;
INSERT INTO property_change_events (entity, record_id, property, new_value, source_engine, created_at)
SELECT 'Person', p.id, 'state', p.state, 'backfill_existing', p.created_at
FROM persons p WHERE p.state IS NOT NULL
ON CONFLICT DO NOTHING;
INSERT INTO property_change_events (entity, record_id, property, new_value, source_engine, created_at)
SELECT 'Person', p.id, 'victim_verified', p.victim_verified::text, 'backfill_existing', p.created_at
FROM persons p WHERE p.victim_verified = true
ON CONFLICT DO NOTHING;
