-- Phase 55 (2026-04-30): Auto Fan-Out platform rule
-- Every persons INSERT/UPDATE on a meaningful field auto-enqueues a full-fan-out
-- cascade so every relevant engine fires to fill remaining contact gaps.

CREATE TABLE IF NOT EXISTS cascade_queue (
  id BIGSERIAL PRIMARY KEY,
  person_id UUID NOT NULL,
  action VARCHAR(64) NOT NULL,
  trigger_field VARCHAR(64),
  trigger_value TEXT,
  priority INT DEFAULT 5,
  status VARCHAR(32) DEFAULT 'queued',
  contact_field_changed BOOLEAN DEFAULT FALSE,
  enqueued_at TIMESTAMP DEFAULT NOW(),
  processed_at TIMESTAMP,
  attempts INT DEFAULT 0,
  last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_cascade_queue_status_priority
  ON cascade_queue (status, priority DESC, enqueued_at ASC);
CREATE INDEX IF NOT EXISTS idx_cascade_queue_person_id ON cascade_queue (person_id);

CREATE OR REPLACE FUNCTION enqueue_auto_fan_out() RETURNS TRIGGER AS $$
DECLARE
  changed_field TEXT := NULL;
  is_contact BOOLEAN := FALSE;
  prio INT := 5;
BEGIN
  -- Identify which meaningful field changed (newest wins)
  IF TG_OP = 'INSERT' THEN
    changed_field := 'create';
    is_contact := (NEW.phone IS NOT NULL OR NEW.email IS NOT NULL OR NEW.address IS NOT NULL);
    prio := 8;
  ELSE
    IF NEW.phone IS DISTINCT FROM OLD.phone AND NEW.phone IS NOT NULL THEN
      changed_field := 'phone'; is_contact := TRUE; prio := 9;
    ELSIF NEW.email IS DISTINCT FROM OLD.email AND NEW.email IS NOT NULL THEN
      changed_field := 'email'; is_contact := TRUE; prio := 9;
    ELSIF NEW.address IS DISTINCT FROM OLD.address AND NEW.address IS NOT NULL THEN
      changed_field := 'address'; is_contact := TRUE; prio := 8;
    ELSIF NEW.full_name IS DISTINCT FROM OLD.full_name AND NEW.full_name IS NOT NULL THEN
      changed_field := 'name'; prio := 7;
    ELSIF NEW.dob IS DISTINCT FROM OLD.dob AND NEW.dob IS NOT NULL THEN
      changed_field := 'dob'; prio := 6;
    ELSIF NEW.employer IS DISTINCT FROM OLD.employer AND NEW.employer IS NOT NULL THEN
      changed_field := 'employer'; prio := 6;
    ELSIF NEW.victim_verified IS DISTINCT FROM OLD.victim_verified AND NEW.victim_verified = TRUE THEN
      changed_field := 'verified'; prio := 8;
    END IF;
  END IF;

  -- Only enqueue if a meaningful field changed
  IF changed_field IS NOT NULL THEN
    -- Avoid duplicate queued entries for same person + same field within 60 seconds
    INSERT INTO cascade_queue (person_id, action, trigger_field, trigger_value, priority, contact_field_changed)
    SELECT NEW.id, 'auto_fan_out', changed_field,
           COALESCE(
             CASE changed_field
               WHEN 'phone'    THEN NEW.phone
               WHEN 'email'    THEN NEW.email
               WHEN 'address'  THEN NEW.address
               WHEN 'name'     THEN NEW.full_name
               WHEN 'employer' THEN NEW.employer
               ELSE NULL
             END, ''),
           prio, is_contact
    WHERE NOT EXISTS (
      SELECT 1 FROM cascade_queue
      WHERE person_id = NEW.id
        AND action = 'auto_fan_out'
        AND trigger_field = changed_field
        AND status = 'queued'
        AND enqueued_at > NOW() - INTERVAL '60 seconds'
    );
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS persons_auto_fan_out_trigger ON persons;
CREATE TRIGGER persons_auto_fan_out_trigger
  AFTER INSERT OR UPDATE ON persons
  FOR EACH ROW EXECUTE FUNCTION enqueue_auto_fan_out();
