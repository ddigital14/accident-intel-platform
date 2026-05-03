-- Phase 95: Add source tracking column for ingest engines
ALTER TABLE persons ADD COLUMN IF NOT EXISTS source VARCHAR(64);
CREATE INDEX IF NOT EXISTS idx_persons_source ON persons(source);
