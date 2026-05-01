# Phase 70 New Ingest Sources Spec (2026-04-30)

## Repo
- /tmp/aip-fb (absolute paths)
- DB: const { getDb } = require('../../_db');
- Errors: const { reportError } = require('../system/_errors');
- Auth: ?secret=ingest-now or x-cron-secret header

## Schemas (CRITICAL)
- incidents: id, severity, fatalities_count, description, raw_description, qualification_state,
  lead_score, occurred_at, discovered_at, state, city, latitude, longitude, source_count.
  NO summary, NO source_type, NO discovery_source on incidents.
- persons: full_name, phone, email, address, city, state — see prior phase docs.
- enrichment_logs: ONLY person_id, field_name, old_value, new_value, created_at + (source, source_url, confidence, verified, data after Phase 68 migration).

## File ownership
- Agent A: /tmp/aip-fb/lib/v1/ingest/ntsb-aviation.js — NTSB aviation accident database. Free public API at https://data.ntsb.gov/avdata/ (or RSS feed if simpler). Returns aviation incidents.
- Agent B: /tmp/aip-fb/lib/v1/ingest/nyc-open-data.js — NYC NYPD Motor Vehicle Collisions OpenData. Free, public CSV API at https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95.json
- Agent C: /tmp/aip-fb/lib/v1/ingest/spanish-news.js — Spanish-language news RSS (Univision, Telemundo, La Opinion). Multi-feed aggregator. Use Spanish accident keywords ("accidente", "choque", "atropellamiento", "víctima").

## Wiring
- Each adds to /tmp/aip-fb/api/router.js
- Each follows standard handler pattern with action=health, action=run (or action=fetch).
- Each writes incidents directly to DB with proper schema.
- Each MUST call _name_filter.applyDenyList() before persisting any extracted name.

## Rate limits
- Be polite. Cron-friendly — single-batch per run, ≤30s function timeout.
