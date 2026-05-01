# Phase 59-63 Interface Contract (2026-04-30)

## Shared environment

- Repo at /tmp/aip-fb (cwd path may differ for agents — use absolute paths with /tmp/aip-fb/...)
- DB pattern: `const { getDb } = require('../../_db');`
- Errors: `const { reportError } = require('./_errors');`
- Auth: header X-Cron-Secret OR query.secret === 'ingest-now' OR === process.env.CRON_SECRET
- Cost tracking: `let trackApiCall = async () => {}; try { trackApiCall = require('./cost-tracker').trackApiCall || trackApiCall; } catch (_) {}`
- enrichment_logs schema is MINIMAL: only person_id, field_name, old_value, new_value, created_at exist. Fold metadata into new_value JSON. NEVER use source/data/verified/confidence columns.
- Existing engine matrix is in lib/v1/system/auto-fan-out.js (ENGINE_MATRIX with adapter functions)
- Strategist is in lib/v1/system/strategist.js — owns ENGINE_CATALOGUE, COMBO_RECIPES, planForPerson, recordOutcome, engine_performance table, strategist_decisions table
- Anthropic API key: from system_config.anthropic_api_key (use Claude Sonnet 4.6 default, Opus 4.7 for hard reasoning)
- VoyageAI key: from system_config.voyageai_api_key

## Each new file MUST:

1. Export: `module.exports = handler; module.exports.handler = handler;` plus the named functions.
2. Self-apply migration if it needs new tables (ensureSchema(db) with `_migrated` cache flag).
3. Auth-gate every action (return 401 unless authed).
4. Log every Postgres insert with try/catch silent (existing pattern).
5. Wire into router by adding the require + the route mapping in api/router.js.
6. Action endpoints follow ?action=health|...
7. Update RULES.md if introducing a new platform pattern.

## Phase ownership

- Phase 59 → /tmp/aip-fb/lib/v1/system/hypothesis-generator.js
- Phase 60 → /tmp/aip-fb/lib/v1/system/adversarial-cross-check.js
- Phase 61 → /tmp/aip-fb/lib/v1/system/person-merge-finder.js
- Phase 62 → extend /tmp/aip-fb/lib/v1/system/strategist.js (NEW recipe-bandit table + 2 new actions)
- Phase 63 → /tmp/aip-fb/lib/v1/system/pattern-miner.js

## Available engines (only use these — NO new external integrations)

Anthropic Claude (claude-sonnet-4-6, claude-opus-4-7), OpenAI GPT-4o + Whisper, VoyageAI embeddings + rerank, Deepgram, Apollo, PDL, Hunter, Trestle phone (NOT address — denied), Twilio Lookup, NumVerify, FCC carrier, CourtListener, Maricopa Assessor, Brave Search Answers Plan, Google CSE, NewsAPI, USPS (just wired). All available in system_config.

## Test contract

After all 5 phases land, the integration test must:
- Hit health endpoint of each new file
- Hit any "list" endpoint of each
- Run an end-to-end scenario on a real qualified person from master-lead-list

Fail-loud. Don't catch exceptions silently in the test runner.
