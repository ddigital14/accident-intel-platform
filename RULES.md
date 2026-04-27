# AIP Architecture Rules

These rules MUST be followed for any future change to prevent regressions.
Auto-generated for Claude / human contributors.

## Vercel constraints (Hobby plan)
- **Max 12 serverless functions per deployment**
- **Max 11 cron jobs per project**
- **Underscore-prefixed files in /api/ are EXCLUDED** (use without underscore for handlers)
- **Vercel only bundles files reached via STATIC require()** — no dynamic require with variable paths
- **Catch-all `[...slug].js` syntax doesn't work** in plain Vercel /api/ (it's a Next.js feature)

## How AIP routes requests
Every `/api/v1/*` URL is rewritten to `/api/router.js`. The router has:
1. Static require() at top for every handler module under `/lib/v1/...`
2. A `ROUTES` table mapping URL slugs → handler modules
3. Dynamic incident sub-route handler for `/incidents/:id/{assign,note}`

**To add a new endpoint:**
1. Create `lib/v1/<area>/<name>.js` that exports `module.exports = async function handler(req, res) { ... }`
2. Add `const newHandler = require('../lib/v1/<area>/<name>')` to `api/router.js` (top of file)
3. Add `'<area>/<name>': newHandler,` to the ROUTES table
4. If it should be cron-driven, add to `lib/v1/cron/dispatch.js` JOB_HANDLERS map
5. Update vercel.json crons (only via the dispatcher — NEVER add a new top-level cron, you'll hit the 11 limit)

## Database schema rules
- **Schema migrations live as `ensureColumns()` / `ensureTable()` functions** that run idempotently on first request. We don't use a migrations runner — Vercel serverless makes that fragile.
- **All vocabularies live in `lib/_schema.js`** (severity, incident_type, source_type, etc). Adding a new value requires updating that file.
- **All triggers/procedures are installed via `lib/v1/system/triggers.js`** which is idempotent.
- **Every new pipeline must call `normalizeIncident()` and `normalizePerson()` from `lib/_schema.js`** before INSERT.
- **Every catch block must call `await reportError(db, pipeline, source, message, context)`** so errors flow to the dashboard.

## Enrichment integration rules
- **Use `lib/v1/enrich/_routing.js`** to decide which API to call for which enrichment goal. New APIs added there.
- **Each enrichment API helper returns** `{ source, confidence, fields, cost_usd, endpoint }` or `null`. The trigger merges these in confidence order.
- **API keys are read from env vars FIRST, then `system_config` table** (so `/api/v1/system/setup` POST can configure without redeploying).
- **NEVER hardcode an API key** in source. Always read from env or DB config.

## Cron rules
- **Only the dispatcher pattern**: `vercel.json` crons hit `/api/v1/cron/dispatch?secret=ingest-now&jobs=foo,bar`
- **Each job handler must be statically require'd** in `lib/v1/cron/dispatch.js` JOB_HANDLERS map
- **Job timeout = 50s** (Vercel function limit is 60s for Pro, 30s on Hobby — leave 10s headroom)
- **Heavy OpenAI / scraping jobs go on the per-function 1024MB / 60s overrides** in vercel.json `functions` map

## Schema field rules (canonical)
| Field | Where it lives | Set by | Read by |
|---|---|---|---|
| `incidents.qualification_state` | DB | aip_persons_change_trigger (Postgres) + qualify.js | feed.js, counts.js, dashboard |
| `incidents.lead_score` | DB | qualify.js | feed.js, dashboard, notify.js |
| `incidents.has_contact_info` | DB | trigger | dashboard, audit.js |
| `incidents.qualified_at` | DB | trigger when state→qualified | feed.js, notify.js (alerts only newly qualified) |
| `incidents.notified_at` | DB | notify.js (after Slack/SMS sent) | notify.js (skip already-notified) |
| `incidents.assigned_to` | DB | auto-assign.js / manual via /incidents/:id/assign | dashboard My Leads |
| `incidents.geom` | DB | auto-trigger from latitude/longitude | correlate.js, opendata.js, news.js (PostGIS dedup) |
| `persons.contact_status` | DB | court.js (when has_attorney=true) + manual | qualify.js, contacts UI |
| `persons.has_attorney` | DB | court.js + manual | filter qualified leads |
| `persons.enrichment_score` | DB | enrich/run.js + enrich/trigger.js | dashboard |

## Dashboard data flow
- `/api/v1/dashboard/counts` — public summary (no auth): qualification breakdown, top leads, source breakdown, pipeline health
- `/api/v1/dashboard/feed?state=qualified|pending|pending_named|all` — main lead list, joined with persons
- `/api/v1/system/health` — pipeline cron status, error counts
- `/api/v1/system/smoke-test` — parallel 7-endpoint health check (public)
- `/api/v1/system/audit` — consistency check; `?fix=true` repairs
- `/api/v1/system/cost` — API spend by service
- `/api/v1/system/setup` — GET shows config; POST updates Slack/Twilio/Trestle/reps

## Never break the build — pre-deploy checklist
- [ ] `node -c <file>` passes for every changed JS file
- [ ] Total `find api -name '*.js'` ≤ 12
- [ ] Total `vercel.json crons` ≤ 11
- [ ] All require() paths in `/lib/v1/<area>/...` use `'../../_db'` etc., NOT `'../_db'`
- [ ] All require() paths from `/api/router.js` use `'../lib/v1/<area>/<name>'`
- [ ] New handler added to ROUTES in `api/router.js`
- [ ] If cron-driven, also added to JOB_HANDLERS in `lib/v1/cron/dispatch.js`
- [ ] Schema changes idempotent (CREATE IF NOT EXISTS, ADD COLUMN IF NOT EXISTS)
- [ ] Catch blocks call `reportError(db, ...)`
- [ ] Smoke test still passes after deploy

## Auto-update triggers (Postgres)
Installed by `/api/v1/system/triggers`:
- `tr_persons_qualify` — recomputes incident.qualification_state when persons change
- `tr_sourcereports_count` — keeps incidents.source_count synced
- `tr_persons_attorney` — auto-sets contact_status='has_attorney' when has_attorney flips true
- `tr_aip_incidents_geom` — auto-builds PostGIS geometry from lat/lng

Re-run `GET /api/v1/system/triggers?secret=ingest-now` after any DB connection reset.

## Future-proofing additions
- Always include a "rollback" path for every new pipeline (delete the data_source row + filter incidents by tag)
- Always log to `system_changelog` (`logChange(db, {kind, title, summary, ...})`) for any deploy or schema change
- Always use `batchInsert(db, table, rows)` from `lib/_batch.js` for >1 row insert — NEVER per-row INSERT in a loop
- Always use `dedupCache.has()` / `.set()` from `lib/_cache.js` for source_reference dedup BEFORE hitting DB

## CORE INTENT — Multi-cross-conversion (read CORE_INTENT.md)

Every change to AIP must support the multi-cross-conversion directive:
- New pipelines emit cascade events after linking data
- Cascade engine fires every applicable enricher
- Cross-exam scores identity_confidence
- Repeat until convergence
- Properties stay synced across all engines + UI/UX

If a proposed change WEAKENS this — adds dead-end data, breaks the
linkage chain, or misaligns properties — DO NOT SHIP IT.

## NEW ENGINE INTEGRATION RULE (added 2026-04-26)

Every NEW engine/integration must obey the following — no engine ships without all 14 boxes checked.

1. **Use the FULL surface area of the third-party API**, not just one endpoint. Examples:
   - Twilio → Messaging + MMS + Lookup + Verify + Voice + Conversations webhooks (NOT just SMS)
   - Trestle → Reverse Phone + CNAM + Reverse Address + Caller ID + Real Contact
   - PDL → person_search + work_history + education + skills + social URLs
   - Audit the third-party's full endpoint catalog and pick every endpoint that returns data we can cross-reference.
2. **Emit `enqueueCascade(db, personId, 'engine_name')`** after every INSERT/UPDATE on `persons`.
3. **Use canonical normalizers** from `lib/_schema.js` before any DB write.
4. **Add to `lib/v1/enrich/_routing.js`** with cost + capability matrix.
5. **Add to cross-exam SOURCE_WEIGHTS** in `lib/v1/enrich/cross-exam.js` (50–99).
6. **Auto-instrument every API call with `trackApiCall(db, pipeline, service, tin, tout, success)`**.
7. **Log to `system_changelog` via `logChange(db, {kind:'pipeline', ...})`** when shipped.
8. **Register the cron job** via dispatcher `JOB_HANDLERS` in `lib/v1/cron/dispatch.js`.
9. **Add to `api/router.js` ROUTES table** statically required from `lib/v1/<pipeline>/<engine>.js`.
10. **Run smoke + audit + cross-exam** after deploy (must remain 7/7 pass + 0 audit issues).
11. **Update `RULES.md`, `CORE_INTENT.md`, and Claude memory**.
12. **If engine is two-way (read+write)**: wire inbound webhook to `/api/v1/webhooks/<engine>` and route through cascade.
13. **If engine returns identity-relevant fields**: map to canonical schema, log to `enrichment_logs`, add to `gatherEvidence()`.
14. **Add `service_name` PRICING entry** in `lib/v1/system/cost.js`.

This rule supersedes the older guidance — any engine missing items from this list is OUT OF COMPLIANCE
and MUST be brought into compliance before adding new functionality.


## Phase 19 — AI Router + Claude + Beyond-Competitor Features (2026-04-27)

### AI Router (`lib/v1/enrich/_ai_router.js`) — REQUIRED
EVERY new GPT/Claude call MUST go through the AI router. NO direct fetch() calls
to api.openai.com or api.anthropic.com from any other file. The router gives us:
- Tier routing (cheap=gpt-4o-mini, premium=gpt-4o, auto=fatal->premium)
- Provider failover (OpenAI -> Claude when 5xx/quota)
- Token usage tracked into system_api_calls.tokens_in/out
- Centralized error logging (no more silent catches)

If you're tempted to write `await fetch('https://api.openai.com/...')` again — STOP.
Use `extractJson(db, { pipeline, systemPrompt, userPrompt, tier, severityHint })` instead.

### Model registry (single source of truth)
`MODELS` map in `_ai_router.js`. Update only there when new versions ship.
- gpt-4o-mini    — high-volume cheap extraction
- gpt-4o         — fatal/serious incidents + obituaries + court (high-value cases)
- whisper-1      — scanner audio
- claude-haiku-4-5     — fallback / cheap Claude path
- claude-sonnet-4-6    — Claude cross-reasoner (top leads)
- claude-opus-4-6      — manual long-document reasoning (use sparingly)

### Claude cross-reasoner (`lib/v1/enrich/claude-cross-reasoner.js`)
Cron: every 5 min via `qualify,notify,enrich-trigger,cascade,cross-exam,claude-reason`.
For top-15 leads with score≥50 + no recent reasoning, runs Claude Sonnet over ALL
evidence and produces verdict (high_confidence|moderate|low|contradictory|duplicate).
Boost in [-15,+15] applied to person.confidence_score → emits cascade.

### Cross-wires (`lib/v1/enrich/cross-wires.js`)
Free, fast, applied to every newly qualified incident in qualify.js:
- weatherSnapshot          (OpenWeather, free tier)
- priorIncidentsAtLocation (PostGIS ±100m / 5 years)
- vehicleRecallSummary     (NHTSA, free)
- timeOfDayBucket          (rush_hour | overnight | weekend | day | evening)
- first_responder_agency   (heuristic from police_department)

### Predictive case value (`lib/v1/system/_case_value.js`)
Logistic-style score → band:
- low      ($5k–$25k)
- moderate ($25k–$100k)
- high     ($100k–$500k)
- premium  ($500k+)

Stored on `incidents.case_value_*` columns. Set during qualify cron.

### Test endpoint (`/api/v1/system/test-gpt?secret=ingest-now`)
P0 debug: returns env_keys_set + sample extraction round-trip with timings.
First stop when "GPT extraction returns null" symptoms appear.

### Court → has_attorney → incident.tags
court.js now bubbles `has_attorney` up to `incidents.tags` so dashboards filter
without joining persons. Don't break this contract.

### Required env vars (in addition to existing)
- ANTHROPIC_API_KEY    — for Claude cross-reasoner + provider fallback
- OPENWEATHER_API_KEY  — for weather snapshots (free tier sufficient)


## Phase 21 — Cross-Link Maximization + Smart Router (2026-04-27)

### New cross-links shipped
| # | Wire | File | Trigger | Effect |
|---|------|------|---------|--------|
| 1 | VIN recalls → severity boost | `enrich/vehicle-history.js` | recall_count > 0 | lead_score += 5 (10 if 3+) |
| 2 | Court plaintiff → has_attorney=true | `enrich/court-reverse-link.js` | name match existing victim | persons.has_attorney=true + incidents.has_attorney_known=true |
| 3 | Obit → relatives → cascade | `enrich/obit-backfill.js` | name match | enroll relatives + cascade each |
| 4 | Twilio caller_name agreement | `enrich/twilio.js` | caller_name == full_name | identity_confidence += 15 |
| 5 | PD-press name → news/court pull | `ingest/pd-press.js` | new named person | enqueueCascade priority 8 |
| 6 | Property owner last_name match | `enrich/cross-wires.js` | owner_name LIKE last_name | likely_family_residence=true |
| 7 | Voter DOB validation | `enrich/cross-wires.js` | abs(age-yob)<=2 / >5 | identity_confidence +5 / identity_conflict=true |
| 8 | Cross-source name validation | `enrich/cross-wires.js` | 3+ sources agree | identity_confidence += 20 |
| 9 | Incident-level cascade | `system/_cascade.js` (enqueueIncidentCascade) | qualify_state / case_value / lead_score change | re-cross-exam all persons |
| 10 | contact_quality (cold/warm/hot) | `enrich/twilio.js` | post-Twilio Lookup | persons.contact_quality column |

### Smart router (`lib/v1/enrich/_smart_router.js`)
Pure decision function `pickNextAction(person, incident, ic)` chooses cheapest valuable next-step.
Action priority: backfill-nameless → pdl-by-name → twilio-lookup → hunter-find → searchbug+voter → social-search → family-tree → court-reverse-link → claude-reasoner → ready-for-rep.
Endpoints: `?action=pick&person_id=` and `?action=batch&limit=15`.
Cron: folded into 5-min slot alongside qualify.

### New columns
- persons.identity_confidence (INT, indexed)
- persons.identity_conflict (BOOL)
- persons.likely_family_residence (BOOL)
- persons.contact_quality (cold|warm|hot)
- persons.caller_name (carrier-reported owner)
- persons.has_relatives_searched (BOOL)
- incidents.has_attorney_known (BOOL)
- incidents.vehicle_recalls_count (INT)

All ALTER TABLEs live inside qualify.js ensureColumns flow → applied on every qualify cron tick.

---

## Phase 22 — PDL-by-name + voter loaders + property records expansion + tributes.com swap (2026-04-27)

### New endpoints
| Path | Purpose | Cron |
|---|---|---|
| `/api/v1/enrich/pdl-by-name` | Bulk PDL Person Enrichment for `pending_named` persons missing phone+email | folded into 30-min `pd-press,obituaries,...` slot as `pdl-by-name` job |
| `/api/v1/enrich/ga-voter-loader` | GA voter-roll bulk loader (pipe-delimited) | n/a — manual POST after Mason downloads file |
| `/api/v1/enrich/tx-voter-loader` | TX voter-roll bulk loader (pipe-delimited) | n/a — manual POST after Mason buys file |

### Property records: 4 new counties
`lib/v1/enrich/property-records.js` COUNTY_ENDPOINTS now includes:
- `GA:Fulton` (Atlanta) — Fulton GIS ArcGIS feature service
- `FL:MiamiDade` (Miami) — public assessor address-search
- `TX:Travis` (Austin) — TravisCAD ArcGIS feature service
- `AZ:Maricopa` (Phoenix) — `api.mcassessor.maricopa.gov/parcel/search`

City→county map expanded for the major cities of each.

### Obituary source swap
- `lib/v1/enrich/obit-backfill.js` + `lib/v1/ingest/obituaries.js` now hit **tributes.com** first; legacy.com is fallback. UA changed from `AIP-Backfill/1.0` to `Mozilla/5.0 (compatible; AIP/1.0; research)` to avoid bot blocks.

### Smart-router execution upgrade
`enrich-pdl-by-name` action is now executed inline (not just deferred) when smart router picks it — closes the "router decides + nothing happens" gap.

### Cron count
Still 11/11 (Hobby max) — all new work folded into existing slots.

### 14-point compliance for PDL-by-name engine
| Check | Status |
|---|---|
| Static `require()` | yes (no dynamic) |
| Canonical normalizers | yes (`normalizePerson` import; non-overwrite policy) |
| `trackApiCall(db, 'enrich-pdl-by-name', 'pdl', …)` | yes |
| `enqueueCascade()` per success | yes (priority 8) |
| Cron registered in `dispatch.js` JOB_HANDLERS | yes (`pdl-by-name`) |
| Router registered in `api/router.js` ROUTES | yes (`enrich/pdl-by-name`) |
| `logChange()` aggregate write | yes (kind=enrichment) |
| `reportError()` per failure | yes |
| `dedupCache` to avoid loops | yes (`pdlbn:<id>`) |
| Time-budget guard (<50s) | yes |
| Confidence merge (LEAST/Math.max) | yes |
| <600 lines | 278 lines |
| Idempotent | yes (no overwrite of existing fields) |
| Fallback if env missing | yes (no-op if `PDL_API_KEY` unset) |
