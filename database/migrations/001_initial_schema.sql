-- ============================================================================
-- ACCIDENT INTEL PLATFORM - Complete Database Schema
-- PostgreSQL with PostGIS for geolocation support
-- ============================================================================

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "postgis";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- For fuzzy text search

-- ============================================================================
-- CORE TABLES
-- ============================================================================

-- Metro areas / cities we monitor
CREATE TABLE metro_areas (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL,
    state VARCHAR(2) NOT NULL,
    fips_code VARCHAR(10),
    center_lat DECIMAL(10, 7),
    center_lng DECIMAL(10, 7),
    radius_miles INTEGER DEFAULT 50,
    geom GEOMETRY(Point, 4326),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Data sources configuration
CREATE TABLE data_sources (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) NOT NULL,
    type VARCHAR(50) NOT NULL, -- 'police_scanner', 'police_report', 'hospital_ems', 'news', 'public_records', 'radio', 'crash_report', 'cad_dispatch', 'social_media', 'dot_data'
    provider VARCHAR(100),     -- 'broadcastify', 'lexisnexis', 'rapidsos', etc.
    api_endpoint TEXT,
    api_key_env VARCHAR(100),  -- env var name for the key
    polling_interval_seconds INTEGER DEFAULT 60,
    is_active BOOLEAN DEFAULT TRUE,
    config JSONB DEFAULT '{}',  -- source-specific configuration
    metro_area_id UUID REFERENCES metro_areas(id),
    last_polled_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    error_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- INCIDENT TRACKING
-- ============================================================================

-- Main incidents table - one row per accident event
CREATE TABLE incidents (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    incident_number VARCHAR(100),       -- External reference number

    -- Classification
    incident_type VARCHAR(50) NOT NULL,  -- 'car_accident', 'motorcycle_accident', 'truck_accident', 'work_accident', 'pedestrian', 'bicycle', 'slip_fall', 'other'
    severity VARCHAR(20) DEFAULT 'unknown', -- 'fatal', 'critical', 'serious', 'moderate', 'minor', 'unknown'
    status VARCHAR(30) DEFAULT 'new',    -- 'new', 'verified', 'enriched', 'assigned', 'contacted', 'in_progress', 'closed', 'invalid'
    priority INTEGER DEFAULT 5,          -- 1 (highest) to 10 (lowest)
    confidence_score DECIMAL(5,2) DEFAULT 0, -- 0-100, how confident we are in the data

    -- Location
    address TEXT,
    street VARCHAR(255),
    city VARCHAR(100),
    county VARCHAR(100),
    state VARCHAR(2),
    zip VARCHAR(10),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    geom GEOMETRY(Point, 4326),
    intersection VARCHAR(255),
    highway VARCHAR(100),
    mile_marker VARCHAR(20),

    -- Timing
    occurred_at TIMESTAMPTZ,
    reported_at TIMESTAMPTZ,
    discovered_at TIMESTAMPTZ DEFAULT NOW(), -- When our system first picked it up

    -- Summary
    description TEXT,                    -- AI-generated summary from all sources
    raw_description TEXT,                -- First raw description received

    -- Response info
    responding_agencies TEXT[],
    dispatch_codes VARCHAR(50)[],
    fire_department BOOLEAN DEFAULT FALSE,
    ems_dispatched BOOLEAN DEFAULT FALSE,
    helicopter_dispatched BOOLEAN DEFAULT FALSE,
    extrication_needed BOOLEAN DEFAULT FALSE,
    hazmat BOOLEAN DEFAULT FALSE,
    road_closure BOOLEAN DEFAULT FALSE,

    -- Counts
    vehicles_involved INTEGER,
    persons_involved INTEGER,
    injuries_count INTEGER,
    fatalities_count INTEGER,

    -- Weather / conditions
    weather_conditions VARCHAR(100),
    road_conditions VARCHAR(100),
    lighting_conditions VARCHAR(50),

    -- Police report
    police_report_number VARCHAR(100),
    police_department VARCHAR(200),
    officer_name VARCHAR(200),
    officer_badge VARCHAR(50),

    -- Assignment
    assigned_to UUID,  -- references users table
    assigned_at TIMESTAMPTZ,

    -- Metro area
    metro_area_id UUID REFERENCES metro_areas(id),

    -- Source tracking
    source_count INTEGER DEFAULT 1,      -- Number of sources confirming this
    first_source_id UUID REFERENCES data_sources(id),

    -- Metadata
    tags TEXT[],
    notes TEXT,
    ai_analysis JSONB,                   -- AI-generated insights
    metadata JSONB DEFAULT '{}',

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Persons involved in incidents
CREATE TABLE persons (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,

    -- Role
    role VARCHAR(30) NOT NULL,           -- 'driver', 'passenger', 'pedestrian', 'cyclist', 'worker', 'witness', 'other'
    is_injured BOOLEAN DEFAULT FALSE,

    -- Personal info
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(255),
    date_of_birth DATE,
    age INTEGER,
    gender VARCHAR(20),

    -- Contact
    phone VARCHAR(20),
    phone_secondary VARCHAR(20),
    email VARCHAR(255),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(2),
    zip VARCHAR(10),

    -- Injury details
    injury_severity VARCHAR(30),         -- 'fatal', 'incapacitating', 'non_incapacitating', 'possible', 'none', 'unknown'
    injury_description TEXT,
    transported_to VARCHAR(200),         -- Hospital name
    transported_by VARCHAR(100),         -- Ambulance company
    treatment_status VARCHAR(50),

    -- Insurance
    insurance_company VARCHAR(200),
    insurance_policy_number VARCHAR(100),
    insurance_type VARCHAR(50),          -- 'liability', 'full_coverage', 'minimum', 'uninsured', 'underinsured', 'unknown'
    policy_limits VARCHAR(100),          -- e.g., '25/50/25', '100/300/100'
    policy_limits_bodily_injury VARCHAR(50),
    policy_limits_property VARCHAR(50),
    insurance_claim_number VARCHAR(100),
    insurance_agent VARCHAR(200),
    insurance_agent_phone VARCHAR(20),

    -- Vehicle (if driver)
    vehicle_id UUID,                     -- references vehicles table

    -- Legal
    has_attorney BOOLEAN,
    attorney_name VARCHAR(200),
    attorney_firm VARCHAR(200),
    attorney_phone VARCHAR(20),

    -- Employment (for work accidents)
    employer VARCHAR(200),
    employer_phone VARCHAR(20),
    occupation VARCHAR(200),
    workers_comp_carrier VARCHAR(200),
    workers_comp_claim VARCHAR(100),

    -- Enrichment
    skip_trace_completed BOOLEAN DEFAULT FALSE,
    skip_trace_data JSONB,
    enrichment_data JSONB,

    -- Status
    contact_status VARCHAR(30) DEFAULT 'not_contacted', -- 'not_contacted', 'attempted', 'contacted', 'interested', 'not_interested', 'retained', 'has_attorney'
    contact_attempts INTEGER DEFAULT 0,
    last_contact_at TIMESTAMPTZ,

    confidence_score DECIMAL(5,2) DEFAULT 0,

    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Vehicles involved
CREATE TABLE vehicles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,

    -- Vehicle info
    year INTEGER,
    make VARCHAR(100),
    model VARCHAR(100),
    color VARCHAR(50),
    body_type VARCHAR(50),              -- 'sedan', 'suv', 'truck', 'motorcycle', 'commercial', 'semi', 'bus', 'other'
    license_plate VARCHAR(20),
    license_state VARCHAR(2),
    vin VARCHAR(17),

    -- Damage
    damage_severity VARCHAR(30),        -- 'totaled', 'severe', 'moderate', 'minor', 'none'
    damage_description TEXT,
    towed BOOLEAN,
    tow_company VARCHAR(200),
    tow_destination VARCHAR(200),

    -- Insurance
    insurance_company VARCHAR(200),
    insurance_policy VARCHAR(100),

    -- Owner (may differ from driver)
    owner_name VARCHAR(200),
    owner_phone VARCHAR(20),
    owner_address TEXT,

    -- Commercial vehicle info
    is_commercial BOOLEAN DEFAULT FALSE,
    dot_number VARCHAR(50),
    carrier_name VARCHAR(200),
    carrier_mc_number VARCHAR(50),

    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Update persons.vehicle_id FK
ALTER TABLE persons ADD CONSTRAINT fk_persons_vehicle
    FOREIGN KEY (vehicle_id) REFERENCES vehicles(id);

-- ============================================================================
-- SOURCE DATA TRACKING
-- ============================================================================

-- Raw data from each source, linked to incidents
CREATE TABLE source_reports (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    incident_id UUID REFERENCES incidents(id) ON DELETE CASCADE,
    data_source_id UUID REFERENCES data_sources(id),

    source_type VARCHAR(50) NOT NULL,
    source_reference VARCHAR(200),       -- External ID from the source

    raw_data JSONB NOT NULL,             -- Complete raw data from source
    parsed_data JSONB,                   -- Normalized/parsed version

    -- What this source contributed
    contributed_fields TEXT[],            -- Which fields this source added

    confidence DECIMAL(5,2) DEFAULT 50,
    is_verified BOOLEAN DEFAULT FALSE,

    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Cross-reference / deduplication tracking
CREATE TABLE incident_matches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    matched_incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    match_confidence DECIMAL(5,2),
    match_reason TEXT,                   -- 'same_location_time', 'same_report_number', 'same_persons', etc.
    is_confirmed BOOLEAN DEFAULT FALSE,
    merged BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(incident_id, matched_incident_id)
);

-- ============================================================================
-- USER & ASSIGNMENT SYSTEM
-- ============================================================================

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    role VARCHAR(30) DEFAULT 'rep',      -- 'admin', 'manager', 'rep', 'viewer'
    phone VARCHAR(20),

    -- Rep specific
    assigned_metros UUID[],              -- Which metros this rep covers
    max_daily_leads INTEGER DEFAULT 50,
    specialization VARCHAR(50)[],        -- 'auto', 'motorcycle', 'truck', 'work_injury'

    is_active BOOLEAN DEFAULT TRUE,
    last_login_at TIMESTAMPTZ,

    settings JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Add FK for incidents.assigned_to
ALTER TABLE incidents ADD CONSTRAINT fk_incidents_assigned_to
    FOREIGN KEY (assigned_to) REFERENCES users(id);

-- Activity log
CREATE TABLE activity_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id),
    incident_id UUID REFERENCES incidents(id),
    person_id UUID REFERENCES persons(id),

    action VARCHAR(50) NOT NULL,         -- 'viewed', 'assigned', 'contacted', 'updated', 'exported', 'noted'
    details JSONB,

    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- ALERTS & NOTIFICATIONS
-- ============================================================================

CREATE TABLE alert_rules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id),
    name VARCHAR(200),

    -- Conditions (all must match)
    conditions JSONB NOT NULL,
    /* Example:
    {
        "incident_types": ["car_accident", "truck_accident"],
        "severity": ["critical", "serious"],
        "metro_areas": ["atlanta", "miami"],
        "min_vehicles": 2,
        "keywords": ["commercial", "semi", "tractor"]
    }
    */

    -- Notification method
    notify_email BOOLEAN DEFAULT TRUE,
    notify_sms BOOLEAN DEFAULT FALSE,
    notify_dashboard BOOLEAN DEFAULT TRUE,

    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE notifications (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id),
    alert_rule_id UUID REFERENCES alert_rules(id),
    incident_id UUID REFERENCES incidents(id),

    type VARCHAR(30) NOT NULL,           -- 'new_incident', 'update', 'assignment', 'system'
    title VARCHAR(255),
    message TEXT,

    is_read BOOLEAN DEFAULT FALSE,
    read_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Incidents
CREATE INDEX idx_incidents_type ON incidents(incident_type);
CREATE INDEX idx_incidents_status ON incidents(status);
CREATE INDEX idx_incidents_severity ON incidents(severity);
CREATE INDEX idx_incidents_occurred_at ON incidents(occurred_at DESC);
CREATE INDEX idx_incidents_discovered_at ON incidents(discovered_at DESC);
CREATE INDEX idx_incidents_metro ON incidents(metro_area_id);
CREATE INDEX idx_incidents_assigned ON incidents(assigned_to);
CREATE INDEX idx_incidents_geom ON incidents USING GIST(geom);
CREATE INDEX idx_incidents_city_state ON incidents(city, state);
CREATE INDEX idx_incidents_confidence ON incidents(confidence_score DESC);
CREATE INDEX idx_incidents_priority ON incidents(priority ASC);
CREATE INDEX idx_incidents_police_report ON incidents(police_report_number);

-- Full text search on descriptions
CREATE INDEX idx_incidents_description_trgm ON incidents USING GIN(description gin_trgm_ops);

-- Persons
CREATE INDEX idx_persons_incident ON persons(incident_id);
CREATE INDEX idx_persons_name ON persons(last_name, first_name);
CREATE INDEX idx_persons_phone ON persons(phone);
CREATE INDEX idx_persons_injured ON persons(is_injured) WHERE is_injured = TRUE;
CREATE INDEX idx_persons_contact_status ON persons(contact_status);
CREATE INDEX idx_persons_insurance ON persons(insurance_company);
CREATE INDEX idx_persons_name_trgm ON persons USING GIN(full_name gin_trgm_ops);

-- Vehicles
CREATE INDEX idx_vehicles_incident ON vehicles(incident_id);
CREATE INDEX idx_vehicles_plate ON vehicles(license_plate);
CREATE INDEX idx_vehicles_vin ON vehicles(vin);
CREATE INDEX idx_vehicles_commercial ON vehicles(is_commercial) WHERE is_commercial = TRUE;

-- Source reports
CREATE INDEX idx_source_reports_incident ON source_reports(incident_id);
CREATE INDEX idx_source_reports_source ON source_reports(data_source_id);
CREATE INDEX idx_source_reports_reference ON source_reports(source_reference);

-- Activity
CREATE INDEX idx_activity_user ON activity_log(user_id);
CREATE INDEX idx_activity_incident ON activity_log(incident_id);
CREATE INDEX idx_activity_created ON activity_log(created_at DESC);

-- Notifications
CREATE INDEX idx_notifications_user_unread ON notifications(user_id) WHERE is_read = FALSE;

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- Live feed view - what reps see
CREATE VIEW v_live_feed AS
SELECT
    i.id,
    i.incident_number,
    i.incident_type,
    i.severity,
    i.status,
    i.priority,
    i.confidence_score,
    i.address,
    i.city,
    i.state,
    i.latitude,
    i.longitude,
    i.occurred_at,
    i.discovered_at,
    i.description,
    i.vehicles_involved,
    i.persons_involved,
    i.injuries_count,
    i.fatalities_count,
    i.ems_dispatched,
    i.helicopter_dispatched,
    i.police_report_number,
    i.police_department,
    i.source_count,
    i.tags,
    i.assigned_to,
    ma.name as metro_area,
    u.first_name || ' ' || u.last_name as assigned_to_name,
    (SELECT COUNT(*) FROM persons p WHERE p.incident_id = i.id AND p.is_injured = TRUE) as confirmed_injuries,
    (SELECT COUNT(*) FROM persons p WHERE p.incident_id = i.id AND p.has_attorney = TRUE) as has_attorney_count,
    (SELECT json_agg(json_build_object(
        'name', p.full_name,
        'phone', p.phone,
        'injured', p.is_injured,
        'severity', p.injury_severity,
        'insurance', p.insurance_company,
        'policy_limits', p.policy_limits,
        'contact_status', p.contact_status,
        'has_attorney', p.has_attorney
    )) FROM persons p WHERE p.incident_id = i.id) as persons_summary,
    (SELECT json_agg(json_build_object(
        'year', v.year,
        'make', v.make,
        'model', v.model,
        'damage', v.damage_severity,
        'commercial', v.is_commercial,
        'insurance', v.insurance_company
    )) FROM vehicles v WHERE v.incident_id = i.id) as vehicles_summary
FROM incidents i
LEFT JOIN metro_areas ma ON i.metro_area_id = ma.id
LEFT JOIN users u ON i.assigned_to = u.id
ORDER BY i.discovered_at DESC;

-- Daily stats view
CREATE VIEW v_daily_stats AS
SELECT
    DATE(discovered_at) as date,
    metro_area_id,
    incident_type,
    COUNT(*) as total_incidents,
    COUNT(*) FILTER (WHERE severity IN ('fatal', 'critical', 'serious')) as high_severity,
    SUM(injuries_count) as total_injuries,
    SUM(fatalities_count) as total_fatalities,
    AVG(confidence_score) as avg_confidence,
    COUNT(*) FILTER (WHERE status = 'assigned') as assigned_count,
    COUNT(*) FILTER (WHERE status = 'contacted') as contacted_count
FROM incidents
GROUP BY DATE(discovered_at), metro_area_id, incident_type;

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_incidents_updated BEFORE UPDATE ON incidents FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER tr_persons_updated BEFORE UPDATE ON persons FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER tr_vehicles_updated BEFORE UPDATE ON vehicles FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER tr_users_updated BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- Auto-set geom from lat/lng
CREATE OR REPLACE FUNCTION set_geom_from_coords()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
        NEW.geom = ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_incidents_geom BEFORE INSERT OR UPDATE ON incidents FOR EACH ROW EXECUTE FUNCTION set_geom_from_coords();
CREATE TRIGGER tr_metro_geom BEFORE INSERT OR UPDATE ON metro_areas FOR EACH ROW EXECUTE FUNCTION set_geom_from_coords();
