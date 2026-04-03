-- Seed top 10 US metro areas for initial deployment
INSERT INTO metro_areas (name, state, fips_code, center_lat, center_lng, radius_miles) VALUES
('Atlanta', 'GA', '13121', 33.7490, -84.3880, 40),
('Houston', 'TX', '48201', 29.7604, -95.3698, 45),
('Dallas-Fort Worth', 'TX', '48113', 32.7767, -96.7970, 50),
('Miami-Fort Lauderdale', 'FL', '12086', 25.7617, -80.1918, 40),
('Chicago', 'IL', '17031', 41.8781, -87.6298, 40),
('Los Angeles', 'CA', '06037', 34.0522, -118.2437, 50),
('Phoenix', 'AZ', '04013', 33.4484, -112.0740, 40),
('New York City', 'NY', '36061', 40.7128, -74.0060, 35),
('Philadelphia', 'PA', '42101', 39.9526, -75.1652, 35),
('San Antonio', 'TX', '48029', 29.4241, -98.4936, 35);
