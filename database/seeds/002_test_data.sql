-- ============================================================================
-- WARNING — Phase 24 ZERO-FAKE-DATA RULE
-- ============================================================================
-- This file contains TEST DATA ONLY (fake names, fake phones, fake reports).
-- IT MUST NEVER BE APPLIED TO PRODUCTION.
--
-- Hardcoded values: Emily Chen, David Kim, James Tucker, Angela Martinez,
-- Robert Garcia, Tanisha Brown, phone numbers 404555xxxx/770555xxxx/678555xxxx,
-- police reports APD-2026-040301..040308 / CCPD-2026-001122.
--
-- If applied accidentally, purge with:
--   GET /api/v1/system/audit?secret=ingest-now&purge_seeds=true
--
-- deploy.sh applies this only when WITH_TEST_DATA=1 AND NODE_ENV != production.
-- ============================================================================

-- ============================================================================
-- ACCIDENT INTEL PLATFORM - Realistic Test Data
-- Run after migrations: psql -d accident_intel -f database/seeds/002_test_data.sql
-- ============================================================================

-- Create admin user (password: Admin2026!)
INSERT INTO users (email, password_hash, first_name, last_name, role, phone)
VALUES (
  'donovan@donovandigitalsolutions.com',
  '$2a$12$LJ3m5Fq4k8Z0v2t.RwJ7zuQHJP.3G3P.3bY4n9rTk6mVxG9AQXW6O',
  'Donovan',
  'Mason',
  'admin',
  '4045550001'
) ON CONFLICT (email) DO NOTHING;

-- Create test reps
INSERT INTO users (email, password_hash, first_name, last_name, role, phone) VALUES
('rep1@donovandigitalsolutions.com', '$2a$12$LJ3m5Fq4k8Z0v2t.RwJ7zuQHJP.3G3P.3bY4n9rTk6mVxG9AQXW6O', 'Sarah', 'Johnson', 'rep', '4045550002'),
('rep2@donovandigitalsolutions.com', '$2a$12$LJ3m5Fq4k8Z0v2t.RwJ7zuQHJP.3G3P.3bY4n9rTk6mVxG9AQXW6O', 'Marcus', 'Williams', 'rep', '4045550003'),
('manager@donovandigitalsolutions.com', '$2a$12$LJ3m5Fq4k8Z0v2t.RwJ7zuQHJP.3G3P.3bY4n9rTk6mVxG9AQXW6O', 'Lisa', 'Chen', 'manager', '4045550004')
ON CONFLICT (email) DO NOTHING;

-- ============================================================================
-- Configure Data Sources
-- ============================================================================

INSERT INTO data_sources (name, type, provider, polling_interval_seconds, is_active, metro_area_id, config) VALUES
('GA NewsAPI Monitor', 'news', 'newsapi', 120, true, (SELECT id FROM metro_areas WHERE state='GA' LIMIT 1),
 '{"keywords": ["accident", "crash", "collision", "wreck", "injury"], "region": "Georgia"}'),

('Atlanta PulsePoint', 'cad_dispatch', 'pulsepoint', 30, true, (SELECT id FROM metro_areas WHERE state='GA' LIMIT 1),
 '{"agency_id": "APD", "incident_types": ["TC", "MCI"]}'),

('GA DOT Crash Reports', 'dot_data', 'nhtsa', 3600, true, (SELECT id FROM metro_areas WHERE state='GA' LIMIT 1),
 '{"state": "GA", "report_type": "crash"}'),

('FL NewsAPI Monitor', 'news', 'newsapi', 120, true, (SELECT id FROM metro_areas WHERE state='FL' LIMIT 1),
 '{"keywords": ["accident", "crash", "collision", "wreck", "injury"], "region": "Florida"}'),

('Miami-Dade CAD Feed', 'cad_dispatch', 'pulsepoint', 30, true, (SELECT id FROM metro_areas WHERE state='FL' LIMIT 1),
 '{"agency_id": "MDPD", "incident_types": ["TC", "MCI"]}'),

('TX NewsAPI Monitor', 'news', 'newsapi', 120, true, (SELECT id FROM metro_areas WHERE state='TX' LIMIT 1),
 '{"keywords": ["accident", "crash", "collision", "wreck", "injury"], "region": "Texas"}');

-- ============================================================================
-- Realistic Test Incidents (Atlanta metro)
-- ============================================================================

WITH atlanta AS (SELECT id FROM metro_areas WHERE state='GA' LIMIT 1)
INSERT INTO incidents (
  incident_type, severity, status, priority, confidence_score,
  address, city, county, state, latitude, longitude,
  occurred_at, discovered_at, description,
  vehicles_involved, persons_involved, injuries_count, fatalities_count,
  ems_dispatched, helicopter_dispatched, extrication_needed,
  police_report_number, police_department, source_count,
  metro_area_id
) VALUES
-- High priority multi-vehicle
('car_accident', 'serious', 'new', 2, 82,
 'I-285 at I-85 interchange', 'Atlanta', 'Fulton', 'GA', 33.8413, -84.3210,
 NOW() - interval '45 minutes', NOW() - interval '40 minutes',
 '5-vehicle pileup on I-285 at I-85 interchange. 3 people transported to Grady Memorial with serious injuries. One vehicle overturned. All lanes blocked, heavy traffic delay.',
 5, 8, 3, 0, true, false, true,
 'APD-2026-040301', 'Atlanta PD', 3, (SELECT id FROM atlanta)),

-- Fatal motorcycle crash
('motorcycle_accident', 'fatal', 'verified', 1, 95,
 '2100 Peachtree Rd NW', 'Atlanta', 'Fulton', 'GA', 33.8117, -84.3854,
 NOW() - interval '2 hours', NOW() - interval '110 minutes',
 'Fatal motorcycle crash on Peachtree Rd. Rider (M/32) pronounced dead on scene. Car driver uninjured. Speed appears to be a factor.',
 2, 2, 0, 1, true, false, false,
 'APD-2026-040302', 'Atlanta PD', 4, (SELECT id FROM atlanta)),

-- Truck accident (high value PI case)
('truck_accident', 'critical', 'enriched', 1, 88,
 'I-75 S near Exit 246, Forest Park', 'Forest Park', 'Clayton', 'GA', 33.6220, -84.3686,
 NOW() - interval '3 hours', NOW() - interval '170 minutes',
 'Semi-truck rear-ended sedan on I-75 S. Driver of sedan airlifted to Atlanta Medical Center. Truck driver (CDL, employed by FastHaul Logistics, DOT# 3456789) cited for following too closely. Significant property damage.',
 2, 3, 2, 0, true, true, true,
 'CCPD-2026-001122', 'Clayton County PD', 5, (SELECT id FROM atlanta)),

-- Work accident
('work_accident', 'serious', 'new', 3, 70,
 '400 W Peachtree St NW, Construction Site', 'Atlanta', 'Fulton', 'GA', 33.7635, -84.3882,
 NOW() - interval '1 hour', NOW() - interval '50 minutes',
 'Construction worker fell approximately 20ft from scaffolding at high-rise project. Worker transported to Emory Midtown with multiple fractures. OSHA notification pending.',
 0, 1, 1, 0, true, false, false,
 NULL, NULL, 2, (SELECT id FROM atlanta)),

-- Standard auto with good PI indicators
('car_accident', 'moderate', 'new', 4, 65,
 'Buford Hwy at Clairmont Rd', 'Brookhaven', 'DeKalb', 'GA', 33.8529, -84.3013,
 NOW() - interval '30 minutes', NOW() - interval '25 minutes',
 'T-bone collision at intersection. Airbags deployed in both vehicles. One driver complaining of neck and back pain, transported to Northside Hospital.',
 2, 3, 1, 0, true, false, false,
 NULL, 'Brookhaven PD', 1, (SELECT id FROM atlanta)),

-- Hit and run
('car_accident', 'moderate', 'new', 3, 55,
 'Memorial Dr SE at Flat Shoals Rd', 'Atlanta', 'DeKalb', 'GA', 33.7360, -84.3270,
 NOW() - interval '20 minutes', NOW() - interval '15 minutes',
 'Hit and run accident. Witness reports a dark SUV ran red light and struck a Honda Civic. Victim with possible head injury refused transport initially. Police searching for suspect vehicle.',
 2, 2, 1, 0, false, false, false,
 NULL, 'DeKalb County PD', 1, (SELECT id FROM atlanta)),

-- Pedestrian
('pedestrian', 'serious', 'new', 2, 78,
 'North Ave at Techwood Dr', 'Atlanta', 'Fulton', 'GA', 33.7712, -84.3928,
 NOW() - interval '15 minutes', NOW() - interval '10 minutes',
 'Pedestrian struck by vehicle in crosswalk near Georgia Tech campus. Pedestrian (F/22, GT student) transported to Grady with leg fractures and head laceration.',
 1, 2, 1, 0, true, false, false,
 'APD-2026-040307', 'Atlanta PD', 2, (SELECT id FROM atlanta)),

-- Minor fender bender
('car_accident', 'minor', 'new', 7, 40,
 'Lenox Rd at Peachtree Rd', 'Atlanta', 'Fulton', 'GA', 33.8458, -84.3610,
 NOW() - interval '10 minutes', NOW() - interval '5 minutes',
 'Minor rear-end collision in Buckhead. No injuries reported. Both drivers exchanged information. No EMS needed.',
 2, 2, 0, 0, false, false, false,
 NULL, NULL, 1, (SELECT id FROM atlanta));

-- ============================================================================
-- Add persons to incidents
-- ============================================================================

-- Persons for I-285 pileup
INSERT INTO persons (incident_id, role, is_injured, first_name, last_name, full_name, age, gender, phone, injury_severity, transported_to, insurance_company, policy_limits, insurance_type, contact_status, confidence_score)
SELECT i.id, 'driver', true, 'Robert', 'Garcia', 'Robert Garcia', 45, 'male', '4045557890',
       'non_incapacitating', 'Grady Memorial Hospital', 'State Farm', '100/300/100', 'full_coverage', 'not_contacted', 75
FROM incidents i WHERE i.police_report_number = 'APD-2026-040301';

INSERT INTO persons (incident_id, role, is_injured, first_name, last_name, full_name, age, gender, phone, injury_severity, transported_to, insurance_company, policy_limits, insurance_type, contact_status, confidence_score)
SELECT i.id, 'driver', true, 'Tanisha', 'Brown', 'Tanisha Brown', 31, 'female', '7705551234',
       'incapacitating', 'Grady Memorial Hospital', 'Progressive', '50/100/50', 'full_coverage', 'not_contacted', 80
FROM incidents i WHERE i.police_report_number = 'APD-2026-040301';

INSERT INTO persons (incident_id, role, is_injured, first_name, last_name, full_name, age, gender, phone, injury_severity, insurance_company, policy_limits, insurance_type, has_attorney, contact_status, confidence_score)
SELECT i.id, 'driver', false, 'David', 'Kim', 'David Kim', 52, 'male', '6785558888',
       'none', 'GEICO', '25/50/25', 'liability', false, 'not_contacted', 70
FROM incidents i WHERE i.police_report_number = 'APD-2026-040301';

-- Persons for truck accident (high value case)
INSERT INTO persons (incident_id, role, is_injured, first_name, last_name, full_name, age, gender, phone, injury_severity, transported_to, transported_by, insurance_company, policy_limits, insurance_type, contact_status, confidence_score)
SELECT i.id, 'driver', true, 'Angela', 'Martinez', 'Angela Martinez', 38, 'female', '4045553456',
       'incapacitating', 'Atlanta Medical Center', 'AMR Ambulance', 'Allstate', '100/300/100', 'full_coverage', 'not_contacted', 85
FROM incidents i WHERE i.police_report_number = 'CCPD-2026-001122';

INSERT INTO persons (incident_id, role, is_injured, first_name, last_name, full_name, age, gender, phone, employer, insurance_company, policy_limits, insurance_type, contact_status, confidence_score)
SELECT i.id, 'driver', false, 'James', 'Tucker', 'James Tucker', 47, 'male', '7705559999',
       'FastHaul Logistics', 'National Interstate Insurance', '1000/2000/1000', 'commercial', 'not_contacted', 90
FROM incidents i WHERE i.police_report_number = 'CCPD-2026-001122';

-- Vehicles for truck accident
INSERT INTO vehicles (incident_id, year, make, model, color, body_type, damage_severity, towed, is_commercial, dot_number, carrier_name)
SELECT i.id, 2020, 'Freightliner', 'Cascadia', 'White', 'semi', 'minor', false, true, '3456789', 'FastHaul Logistics'
FROM incidents i WHERE i.police_report_number = 'CCPD-2026-001122';

INSERT INTO vehicles (incident_id, year, make, model, color, body_type, damage_severity, towed)
SELECT i.id, 2023, 'Honda', 'Accord', 'Blue', 'sedan', 'totaled', true
FROM incidents i WHERE i.police_report_number = 'CCPD-2026-001122';

-- Persons for pedestrian incident
INSERT INTO persons (incident_id, role, is_injured, first_name, last_name, full_name, age, gender, phone, injury_severity, transported_to, contact_status, confidence_score)
SELECT i.id, 'pedestrian', true, 'Emily', 'Chen', 'Emily Chen', 22, 'female', '4045552222',
       'non_incapacitating', 'Grady Memorial Hospital', 'not_contacted', 80
FROM incidents i WHERE i.police_report_number = 'APD-2026-040307';

-- ============================================================================
-- Alert rules for admin
-- ============================================================================

INSERT INTO alert_rules (user_id, name, conditions, notify_email, notify_sms, notify_dashboard) VALUES
((SELECT id FROM users WHERE email='donovan@donovandigitalsolutions.com'),
 'Fatal/Critical Alerts - All Metros',
 '{"severity": ["fatal", "critical"], "incident_types": ["car_accident", "truck_accident", "motorcycle_accident", "pedestrian"]}',
 true, true, true),

((SELECT id FROM users WHERE email='donovan@donovandigitalsolutions.com'),
 'Truck Accidents - High Value',
 '{"incident_types": ["truck_accident"], "min_vehicles": 1}',
 true, true, true),

((SELECT id FROM users WHERE email='donovan@donovandigitalsolutions.com'),
 'Atlanta Metro - All Serious+',
 '{"severity": ["fatal", "critical", "serious"], "metro_areas": ["atlanta"]}',
 true, false, true);

SELECT 'Test data seeded successfully!' as status,
       (SELECT COUNT(*) FROM incidents) as total_incidents,
       (SELECT COUNT(*) FROM persons) as total_persons,
       (SELECT COUNT(*) FROM vehicles) as total_vehicles,
       (SELECT COUNT(*) FROM users) as total_users;
