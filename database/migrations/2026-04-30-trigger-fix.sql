-- Phase 65d: Fix auto-fan-out trigger that referenced non-existent dob/employer columns.
-- Replace with date_of_birth (actual column) and drop employer reference.

CREATE OR REPLACE FUNCTION enqueue_auto_fan_out() RETURNS TRIGGER AS $$
DECLARE
  changed_field TEXT := NULL;
  is_contact BOOLEAN := FALSE;
  prio INT := 5;
BEGIN
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
    ELSIF NEW.date_of_birth IS DISTINCT FROM OLD.date_of_birth AND NEW.date_of_birth IS NOT NULL THEN
      changed_field := 'date_of_birth'; prio := 6;
    ELSIF NEW.victim_verified IS DISTINCT FROM OLD.victim_verified AND NEW.victim_verified = TRUE THEN
      changed_field := 'verified'; prio := 8;
    END IF;
  END IF;

  IF changed_field IS NOT NULL THEN
    INSERT INTO cascade_queue (person_id, action, trigger_field, trigger_value, priority, contact_field_changed)
    SELECT NEW.id, 'auto_fan_out', changed_field,
           COALESCE(
             CASE changed_field
               WHEN 'phone'    THEN NEW.phone
               WHEN 'email'    THEN NEW.email
               WHEN 'address'  THEN NEW.address
               WHEN 'name'     THEN NEW.full_name
               ELSE NULL
             END, ''),
           prio, is_contact
    WHERE NOT EXISTS (
      SELECT 1 FROM cascade_queue
      WHERE person_id = NEW.id AND action = 'auto_fan_out'
        AND trigger_field = changed_field AND status = 'queued'
        AND enqueued_at > NOW() - INTERVAL '60 seconds'
    );
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
