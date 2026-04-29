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

---

## Phase 24 — ZERO-FAKE-DATA RULE (CRITICAL)

**NO seed/dummy/mock/fake/sample/placeholder data may EVER be applied to production.**

### What counts as fake data
- Any rows from `database/seeds/002_test_data.sql`
- Hardcoded names: Emily Chen, David Kim, James Tucker, Angela Martinez, Robert Garcia, Tanisha Brown, Sarah Johnson, Marcus Williams, Lisa Chen
- Hardcoded phone patterns: `404555xxxx`, `770555xxxx`, `678555xxxx` (555-prefix area)
- Hardcoded police_report_numbers: `APD-2026-040301..040308`, `CCPD-2026-001122`
- Any incident with `tags = ['test'|'seed'|'demo']`
- Any "fallback" mock response from a pipeline when an API returns no data

### Enforcement
1. `database/seeds/002_test_data.sql` is **DEV/TEST only**. NEVER run against production DATABASE_URL.
2. `deploy.sh` (and any CI workflow) MUST NOT call `psql ... -f database/seeds/002_test_data.sql`.
3. Verify with: `GET /api/v1/system/audit?secret=ingest-now` -> issues.seed_incidents + issues.seed_persons must be 0.
4. To purge if found: `GET /api/v1/system/audit?secret=ingest-now&purge_seeds=true`.
5. Pipelines MUST return real values OR null/skip — never substitute placeholder strings.
6. New ingestion engines MUST NOT include test fixture rows in their first run.
7. `lib/v1/ingest/_homegrown_rotation.js` and all RSS/news pipelines extract from REAL feed responses only — no inline sample objects.

### Audit cron
`audit` cron runs daily at 5 AM UTC; it now reports `seed_incidents` and `seed_persons` counts. Any non-zero value triggers an error in `system_errors`.

---

## Phase 24 — PI-broad keyword filter

`lib/v1/ingest/news-rss.js` `CRASH_KEYWORDS` regex was widened from ~15 vehicle terms
to a full personal-injury surface area (vehicle, pedestrian/cyclist, water, workplace,
premises, medical malpractice, product liability, catastrophic injury, DUI/wrongful death).

`lib/v1/ingest/_homegrown_rotation.js` keyword filters in 5 places were unified to the
same broad PI regex — captures truck/motorcycle/work/dog-bite/elevator/boat etc.

When adding a new ingest source, copy the canonical PI regex. Do NOT use a narrow
"crash|accident|collision" filter — Mason wants ANY personal-injury lead.

---

## Phase 24 — Auto-assign re-rotate

`lib/v1/system/auto-assign.js` now releases stale assignments before assigning new ones:
- Assigned >7d ago AND status in (new|unclaimed|assigned) -> release back to pool
- Status='declined' -> release immediately
Override stale window via `?stale_days=N`.

---

## Phase 24 — WhitePages structured-data parser

`lib/v1/enrich/people-search.js` now extracts persons from `<script id="__NEXT_DATA__">`
JSON blocks before falling back to GPT regex extraction. Much more reliable when WP
returns a Next.js-rendered page.

---

## Phase 24 — identity_confidence backfill + canonicalization

`lib/v1/enrich/claude-identity-investigator.js` now exposes:
- `?action=backfill_ic&limit=200` -> backfill NULL identity_confidence via `crossExamine`
- `?action=batch` auto-runs a 50-row backfill on each call

`lib/v1/enrich/_smart_router.js` already reads `identity_confidence` first in the
`<70 -> claude-cross-reasoner` threshold check. Confirmed canonical column.

When adding a new enrichment engine, write to `persons.identity_confidence` (NOT just
`confidence_score`) when you have multi-source verification.

## ROLLBACK PATHS (Phase 31)

Every shipped engine MUST document a rollback path. The standard pattern:

1. **Disable the engine via cron removal:** Remove the engine slug from `vercel.json` cron jobs. Engine remains queryable via `/api/v1/{path}` for manual debug, but stops auto-firing.

2. **Disable via system_config feature-flag:**
   ```sql
   INSERT INTO system_config (key, value) VALUES ('feature_flags', '{"engine_x_disabled": true}'::jsonb)
     ON CONFLICT (key) DO UPDATE SET value = system_config.value || EXCLUDED.value;
   ```
   Engines should check this flag on entry and short-circuit if disabled.

3. **Hard rollback via git revert:** `git revert <commit-sha>` and push. Vercel auto-redeploys in ~30s.

4. **Drop new schema columns/tables:** Each engine that creates tables/columns must include the corresponding `DROP` SQL in its module-level comment.

## DEPLOY LOG REQUIREMENT (Phase 31)

Every shipped engine MUST log via `lib/v1/system/_deploy.deployLog()` on first run after deploy, OR via the global deploy hook in `system/changelog`. Pattern:
```js
const { deployLog } = require('../system/_deploy');
await deployLog({ name: 'my-engine', version: 'commit-sha', summary: 'what changed', files: ['lib/v1/...'] });
```
This produces a `system_changelog` row with `kind='deploy'` for audit / blame.

## ⚠️ VICTIM-ONLY DATA RULE — ABSOLUTE (Phase 38)

**Mason directive 2026-04-28:** Contact data attached to a lead MUST be the accident victim's data (or a directly-involved party — driver, passenger, pedestrian). NEVER the contact data of journalists, news authors, officers, witnesses, family quoted, or bystanders.

### Hard rules (no exceptions)

1. **No name extraction skips the deny-list filter.** Every extractor that parses names from raw text MUST call `applyDenyList(name, surroundingText)` from `lib/v1/enrich/_name_filter.js` before storing the name. The filter rejects byline patterns, official titles, journalist tags, and attribution-only mentions ("according to X said").

2. **No contact enrichment runs on unverified persons.** The `victim-resolver` and any new enrichment engine MUST filter on `persons.victim_verified = true`. The verifier is `lib/v1/enrich/victim-verifier.js` (Stage A regex hard rules + Stage B Claude Sonnet fallback).

3. **No incident reaches `qualified` state without ≥1 person where `victim_verified = true`.** The ensemble qualifier query MUST include this constraint.

4. **`persons.victim_role` is set on every verified person.** Valid values: `victim`, `driver`, `passenger`, `pedestrian`, `family` (only for next-of-kin in fatal cases). Anything else (`author`, `officer`, `witness`, `unknown`) means the person is NOT a lead candidate and contact enrichment is skipped.

5. **Cross-source contact validation is mandatory.** When 2+ sources return contact data for the same verified victim, `evidence-cross-checker` runs. Conflicts dock confidence -10 and flag for review. Matches confirm at +25 weight.

6. **Quarantine endpoint exists for retroactive cleanup.** `/api/v1/system/quarantine-fake-victims` — re-runs verification on existing qualified persons; demotes incidents with no remaining verified victims to `pending_unverified`.

### When adding a new ingest source

- If the source contains free text that names people (news, social, court filings, scanner transcripts), wire it through `_name_filter.js` BEFORE storing.
- After the verifier batch runs, only then can downstream enrichers (PDL, Apollo, Trestle, Maricopa, voter rolls, people-search-multi, Hunter, Google CSE) attach contact data.
- New extractors MUST add `applyDenyList()` call AND add the surrounding text context to the candidate so Stage B (Claude) has enough signal to classify edge cases.

### When adding a new enrichment engine

- The engine MUST gate on `victim_verified = true` in its candidate selection SQL.
- The engine MUST emit `enqueueCascade(db, 'person', personId, '<engine>', { weight: N })` so cross-checker can validate.
- The engine MUST log conflicts when its returned data disagrees with existing person fields, not silently overwrite.

### Smart victim pipeline (composite)

`/api/v1/system/smart-victim-pipeline?secret=ingest-now` runs all stages in order:
1. `victim-verifier` (Stage A regex + Stage B Claude classification)
2. `victim-resolver` (PDL Pro Enrichment → Apollo → Maricopa → voter-rolls → people-search-multi → Hunter → Google CSE → Trestle, in priority)
3. `evidence-cross-checker` (validate phone area code vs state, address city vs incident city, dock conflicts, confirm matches)
4. `ensemble-qualifier` (promote victim_verified=true persons with evidence_sum >= 120 to qualified)

Anything else (per-engine cron jobs) is supplementary — the composite endpoint is the canonical victim flow.

---

## Phase 41 — AI EXTRACTION LAYER (2026-04-28)

Replaces deterministic regex extractors with Claude Sonnet/Opus for higher recall, structured family/vehicle data, and multi-source synthesis. Augments the existing pipeline; does NOT replace it.

### Modules

| Module | File | Model | Job key | Cron interval |
|---|---|---|---|---|
| AI News Extractor | `lib/v1/enrich/ai-news-extractor.js` | Claude Sonnet 4.6 | `ai-news-extractor` | every 30 min |
| AI Obituary Parser | `lib/v1/enrich/ai-obituary-parser.js` | Claude Sonnet 4.6 | `ai-obituary-parser` | hourly |
| AI Cross-Source Merge | `lib/v1/system/ai-cross-source-merge.js` | Claude Opus 4.6 | `ai-cross-source-merge` | every 2 h |
| Rep Pre-Call Brief | `lib/v1/dashboard/rep-call-brief.js` | Claude Sonnet 4.6 | (HTTP only, on-demand) | n/a |

### Mandatory rules

1. **Every AI extraction MUST go through `_ai_router.js extract()` / `extractJson()`.** Never call `fetch('https://api.anthropic.com')` directly. The router handles model selection, failover, cost tracking, and JSON parsing.
2. **Every AI-extracted name MUST be re-checked through `applyDenyList(name, surroundingText)` from `_name_filter.js`** before insert. Claude is asked to skip journalists/officials but the deny-list is a safety net.
3. **AI-extracted persons get `derived_from='ai-news-extractor'` (or `ai-obituary-parser`, `ai-cross-source-merge`)** so cleanup queries can isolate them and Phase 38 victim_only rule traces source.
4. **Brief output is cached for 24 hours** in `enrichment_logs` (`source='rep-call-brief'`). Pass `?force=1` to regenerate.
5. **AI cross-source merge NEVER overwrites high-confidence existing data** — only fills nulls or upgrades when `confidence_per_field >= 80`.
6. **All AI tokens are logged via `trackApiCall(db, pipeline, model, input_tokens, output_tokens, ok)`** through the router. Cost tab shows per-pipeline breakdown.
7. **Conflicts surfaced by Module 3 are written to `enrichment_logs.data->>'cross_source_conflicts'`** for audit and manual review.

### Failure-mode contract

- AI returns 401/quota → router falls over to OpenAI per existing logic
- AI returns malformed JSON → `extractJson` returns null, handler returns `{ ok: false, error: 'ai_no_parse' }` with 200 status (NEVER 500)
- Network timeout → caught, person row not inserted, error logged via `reportError`
- Insert constraint failure → retry path strips `victim_verified`/`derived_from` then plain insert; if still fails, skipped row is reported in `samples`

### Cascade fan-out

Every newly-inserted person from an AI module fires `enqueueCascade()` with `trigger_source='ai-<module>'`. This auto-runs the contact-finder + cross-exam chain so AI extraction stays consistent with Phase 39's event-driven trigger model.


---

## Phase 43 — Brave fallback + 5 free OSINT extras + best-lead-synthesizer + Opus 4.7

### Modules

| Module | File | Model | Job key | Cron interval |
|---|---|---|---|---|
| Brave Search fallback | `lib/v1/enrich/brave-search.js` | n/a (HTTP) | `brave-search` | (lib only — auto-fallback) |
| Free OSINT Extras (5x) | `lib/v1/enrich/free-osint-extras.js` | n/a (HTTP) | `free-osint-extras` | health-only — invoked from synthesizer |
| Best Lead Synthesizer | `lib/v1/system/best-lead-synthesizer.js` | Claude Opus 4.7 | `best-lead-synthesizer` | every 4 h, limit 2 |

### Mandatory rules

1. **`_ai_router.js MODELS.premium_anth` is now `claude-opus-4-7`.** Every "tier=premium with provider=claude" or `tier='opus'` request routes here.
2. **CSE auto-fallback**: every Google CSE call inside `homegrown-osint-miner.js` goes through `searchWithFallback(db, cfg, q, num)`. On 429/403 it transparently calls Brave. Both code paths track via `trackApiCall`.
3. **No new API keys hardcoded.** Brave/FEC/CScore keys read from `system_config.brave_api_key`, env fallback, then graceful no-op.
4. **`best-lead-synthesizer` is the closer.** Every other engine MUST keep producing structured JSON output that fits inside the `gather()` slice budget. If a new engine ships, hook its `*One()` function into `synthesizeOne`'s `Promise.all`.
5. **Opus 4.7 is reserved for premium reasoning paths only** — synthesizer, claude-identity-investigator, premium ai-cross-source-merge. Everything else stays on Sonnet/Haiku/GPT-mini for cost.

### Failure-mode contract additions

- CSE 429 → log info-severity in `system_errors`, call Brave, continue
- Brave 401 / no key → return `{ ok: false, skipped: true }`, miner records error and moves on
- Synthesizer's parsed JSON missing required fields → still write log, return raw alongside `parsed`, never 500
