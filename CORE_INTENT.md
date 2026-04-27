# AIP Core Intent — The Multi-Cross-Conversion Directive

> **The platform's primary differentiator and design directive.**
> Stated by Mason Donovan 2026-04-26.

## The directive

If one source is pulled and matched to an existing person, accident, case,
or file, then another data point is linked — use whatever data is found,
analyze it, and look at all other integrations or homegrown systems to see
if that data can further fill in accident information and puzzle together
victim name and contact info.

Use all engines and integrations existing back-and-forth to cross-examine
new data. We will be the only true multi-cross-conversion accident intel
platform.

## Why this beats BuyCrash and LexisNexis

| Them | AIP |
|---|---|
| Single proprietary source | 18+ sources cross-examined |
| Static lookups | Cascading enrichment chain |
| Vendor lock-in | Add new sources in hours |
| End-of-week extracts | Real-time event-driven |
| Opaque pricing | Auto-tracked $/call |

## Implementation rules — every future change MUST follow

1. **Every new ingestion source emits a cascade event** when it links data
   to an existing person/incident. Call `enqueueCascade(db, {person_id,
   trigger_source})` immediately after person INSERT/UPDATE.

2. **Cascade engine fires every applicable enricher** in parallel:
   Trestle Reverse Phone, Trestle CNAM, Trestle Reverse Address (when
   approved), PDL, Hunter, Tracerfy, SearchBug, NumVerify, people-search
   (4-site cascade), obituaries, court records, voter rolls (when loaded),
   social search.

3. **Cross-exam runs after every cascade iteration** to score
   identity_confidence and detect contradictions. The score is the truth.

4. **Cascade repeats until convergence**: no new fields filled OR confidence
   ≥95 OR 3 iterations elapsed (cost guardrail).

5. **Properties map consistently** across every engine. Schema vocabulary
   in `lib/_schema.js` is authoritative. Every pipeline calls
   `normalizeIncident()` / `normalizePerson()` before INSERT.

6. **Frontend reads canonical field names**. When adding a new field,
   update normalizers + dashboard renderers + audit checks together.

7. **Never overwrite higher-confidence data with lower**. Cross-exam
   confidence per source is the arbiter.

8. **Log every change** to `system_changelog`. Track API spend via
   `trackApiCall(db, ...)`.

9. **Pre-deploy checklist** in RULES.md must pass before commit.

## Architecture (cascade flow)

```
NEW DATA LINKED (any pipeline inserts/updates a person)
    │
    ▼
enqueueCascade(db, { person_id, trigger_source })
    │
    ▼
cascade_queue table — pending row
    │
    ▼ (every 5 min via dispatcher)
processCascadeQueue() drains 4 jobs at a time
    │
    ▼ (per job)
runCascadeForPerson(db, person_id)
    │
    ▼
Iteration 1: cross-exam → if confidence < 95
    │     → deepEnrichPerson() fires Trestle + PDL + Hunter + Tracerfy
    │       + SearchBug + NumVerify in parallel
    │     → merge new fields (only fill empties)
    │     → log to enrichment_logs
    │     → log to cascade_queue.fields_filled
    │
    ▼
Iteration 2 (if Iteration 1 added fields): repeat
    │
    ▼
Iteration 3 (cap): final cross-exam
    │
    ▼
qualify.js auto-promotes person → qualified state
    │
    ▼
notify.js sends Slack + SMS alert
    │
    ▼
auto-assign.js routes to rep
```

## Files

- `lib/v1/system/_cascade.js` — engine
- `lib/v1/system/cascade.js` — public endpoint
- `lib/v1/enrich/cross-exam.js` — confidence scorer
- `lib/v1/enrich/deep.js` — multi-step chain
- `lib/v1/enrich/_routing.js` — cost/capability matrix
- `lib/_schema.js` — canonical normalizers
- `feedback_aip_core_intent.md` (memory) — same directive saved for Claude continuity

