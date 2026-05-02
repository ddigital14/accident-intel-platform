-- Phase 80: fix property-registry ghost columns + add registry sync table
ALTER TABLE persons ADD COLUMN IF NOT EXISTS vehicle_vin VARCHAR(17);
ALTER TABLE persons ADD COLUMN IF NOT EXISTS vehicle_plate VARCHAR(20);
ALTER TABLE persons ADD COLUMN IF NOT EXISTS lead_tier VARCHAR(20);
ALTER TABLE persons ADD COLUMN IF NOT EXISTS notes TEXT;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS cross_engine_conflict BOOLEAN DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_persons_lead_tier ON persons(lead_tier);
CREATE INDEX IF NOT EXISTS idx_persons_vehicle_vin ON persons(vehicle_vin);

-- DB-backed property registry mirror (synced from JS at deploy)
CREATE TABLE IF NOT EXISTS property_registry_db (
  id VARCHAR(64) PRIMARY KEY,
  entity VARCHAR(20) NOT NULL,
  label VARCHAR(255),
  type VARCHAR(50),
  validation TEXT,
  default_value TEXT,
  is_reportable_to_rep BOOLEAN DEFAULT TRUE,
  is_auditable BOOLEAN DEFAULT FALSE,
  is_public BOOLEAN DEFAULT FALSE,
  surfaces TEXT[],
  producers JSONB,
  consumers TEXT[],
  enum_values TEXT[],
  synced_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_prop_reg_entity ON property_registry_db(entity);
CREATE INDEX IF NOT EXISTS idx_prop_reg_surfaces ON property_registry_db USING GIN(surfaces);
