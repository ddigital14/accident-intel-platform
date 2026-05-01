# Phase 67 Interface Contract (2026-04-30)

## Repo paths
- Repo: /tmp/aip-fb (use absolute paths)
- DB: `const { getDb } = require('../../_db');`
- Errors: `const { reportError } = require('./_errors');`
- Auth: `?secret=ingest-now` query OR x-cron-secret header OR process.env.CRON_SECRET
- Cost: `let trackApiCall = async () => {}; try { trackApiCall = require('./cost-tracker').trackApiCall || trackApiCall; } catch(_) {}`

## Schemas (CRITICAL — observed schemas, real columns only):

**persons:** id, incident_id, role, full_name, first_name, last_name, date_of_birth (NOT dob), age, gender, phone, phone_secondary, email, address, city, state, zip, victim_verified, has_attorney, attorney_firm, lat (may exist or not — check first), lon (same), created_at, updated_at. NO `employer` column. NO `dob` column.

**incidents:** id, severity ('fatal'/'critical'/'serious'/'moderate'/'minor'/'unknown'), fatalities_count, description, raw_description, qualification_state, lead_score, occurred_at, discovered_at, state, city, latitude, longitude, source_count. NO `summary`, NO `source_type`, NO `discovery_source`.

**enrichment_logs:** ONLY person_id, field_name, old_value, new_value, created_at. NO `source`, `data`, `verified`, `confidence`, `source_url` columns. Fold metadata into new_value JSON.

## Endpoint pattern
- `module.exports = handler; module.exports.handler = handler; module.exports.<namedFn> = ...;`
- Self-applying schema migration via `_migrated` cache flag and try/catch silent.
- All actions auth-gated.

## File ownership (this phase)
- Agent A: `/tmp/aip-fb/lib/v1/enrich/geocoder.js` — Nominatim (free, OpenStreetMap) geocoder. Adds `lat`/`lon` columns to persons table if missing. Action `?action=geocode&person_id=` and `?action=batch&limit=N`.
- Agent B: `/tmp/aip-fb/lib/v1/system/re-extract-historical.js` — pulls all incidents with raw_description, re-runs name extraction via existing extractors with new deny-list, removes/demotes any persons that fail current deny rules.
- Agent C: `/tmp/aip-fb/lib/v1/enrich/hunter-domain.js` — calls Hunter's domain-search API to enumerate emails for a given employer/domain.
- Agent D: `/tmp/aip-fb/lib/v1/system/family-graph.js` — builds cross-incident family bridges via voyage-similar with role/relationship constraint.

## Wiring requirements
- Each agent MUST add the require + route mapping to `/tmp/aip-fb/api/router.js`.
- Each agent MUST add an entry to ENGINE_MATRIX in `/tmp/aip-fb/lib/v1/system/auto-fan-out.js` with proper adapter signature `(db, person)` and `fires_on` triggers.
- Each agent MUST update ENGINE_CATALOGUE in `/tmp/aip-fb/lib/v1/system/strategist.js` with needs/produces/cost/speed.

## Available APIs (already in system_config)
Anthropic, OpenAI, VoyageAI, Deepgram, Apollo, PDL, Hunter, Trestle phone, Twilio, NumVerify, FCC, CourtListener, Maricopa, Brave Search, Google CSE, NewsAPI, USPS. NO new external integrations.

## Free geocoder option
Nominatim — `https://nominatim.openstreetmap.org/search?q=<addr>&format=json&limit=1` — requires User-Agent header, 1 req/sec rate limit. Built-in fallback to canonical USPS address before falling back to raw input.

## Existing extractor list (for Agent B context)
- lib/v1/enrich/news-rss.js, news.js, obituaries.js, pd-press.js, police-social.js, reddit.js — each calls `applyDenyList()` from `_name_filter.js`. Re-extraction should re-call these with the same input but new deny-list.
