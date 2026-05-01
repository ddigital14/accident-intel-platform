-- Phase 68: enrichment_logs schema fix — add the missing columns that every engine secretly wanted
ALTER TABLE enrichment_logs ADD COLUMN IF NOT EXISTS source VARCHAR(64);
ALTER TABLE enrichment_logs ADD COLUMN IF NOT EXISTS source_url TEXT;
ALTER TABLE enrichment_logs ADD COLUMN IF NOT EXISTS confidence INT;
ALTER TABLE enrichment_logs ADD COLUMN IF NOT EXISTS verified BOOLEAN;
ALTER TABLE enrichment_logs ADD COLUMN IF NOT EXISTS data JSONB;
CREATE INDEX IF NOT EXISTS idx_el_source ON enrichment_logs(source);
CREATE INDEX IF NOT EXISTS idx_el_field_name ON enrichment_logs(field_name);
CREATE INDEX IF NOT EXISTS idx_el_person_field ON enrichment_logs(person_id, field_name);
CREATE INDEX IF NOT EXISTS idx_el_created_at ON enrichment_logs(created_at DESC);
