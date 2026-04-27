# State Crash Report — FOIA / Manual Request Paths

**Phase 20 #3 — state DOT crash feeds.** Some states do not expose a public real-time crash API. This document tracks the FOIA request paths so a human at Donovan Digital Solutions can complete the workflow on the platform's behalf.

## Live API Status (as of 2026-04-27)

| State | Source | Endpoint | Auth | Cost | Status |
|-------|--------|----------|------|------|--------|
| TX | TxDOT ArcGIS Active Crashes | `services1.arcgis.com/.../Active_Crashes/FeatureServer/0/query` | none | FREE | LIVE |
| FL | FL511 Events (Crash) | `fl511.com/map/data/Events?eventCategory=Crash` | none | FREE | LIVE (Phase 20) |
| OH | OHGO Incidents | `publicapi.ohgo.com/api/v1/incidents` | `OHGO_API_KEY` | FREE | LIVE if `OHGO_API_KEY` env set |
| GA | 511GA Events | `511ga.org/api/v2/getevents` | `GA511_KEY` | FREE | LIVE if `GA511_KEY` env set |

## States Requiring Manual / FOIA Workflow

### Florida — DHSMV crash reports (post-incident officer reports)

The live FL511 feed is real-time but lacks officer-submitted detail. For the
full crash report (with driver/passenger names, insurance, citation info), the
DHSMV requires a request **per incident**.

- **Form**: https://www.flhsmv.gov/forms/90510.pdf (Florida Crash Report Request)
- **Cost**: $10 per report; sworn statement required
- **Turnaround**: 7-10 days mailed; 1-2 days online portal
- **Online**: https://services.flhsmv.gov/CrashPortal — requires account
- **Bulk feed**: NOT publicly available. Bulk access requires an MOA via the
  Florida DHSMV Office of Information Services
  (https://www.flhsmv.gov/contact-us/ → Records Department).
- **What to do (manual)**: Mason creates a DHSMV CrashPortal account using
  donovan@donovandigitalsolutions.com + 330-814-5683. Per-incident pulls
  for fatal incidents in FL only — $10 each, ~5/wk = $200/mo manageable.

### Ohio — ODPS public crash report search

OHGO covers active dispatch incidents. For finalized officer crash reports
(OH-1 form) the ODPS portal is the source.

- **Portal**: https://services.dps.ohio.gov/oats — free for crash reports
- **Form**: OH-1 (Ohio Traffic Crash Report)
- **Cost**: $4 per report online (or free at the police agency)
- **Turnaround**: Reports posted 5-7 days after the crash
- **What to do (manual)**: Mason creates an OATS account using
  donovan@donovandigitalsolutions.com + 95 N Howard St A, Akron OH 44308.
  Works for any Akron / Cleveland / Columbus / Cincinnati metro fatal we want
  hard documentation on.

### Texas — beyond TxDOT live feed

TxDOT ArcGIS = real-time only. For the actual CR-3 officer crash report:

- **Portal**: https://cris.dot.state.tx.us/public/Purchase/
- **Cost**: $6 per report (certified $8)
- **Turnaround**: 10-14 days after the crash
- **What to do (manual)**: Same — Mason makes a CRIS purchaser account.
  Live ArcGIS handles 90% of leads. CR-3 buy is only for high-value cases
  where insurance attorney needs the certified copy.

### Georgia — beyond 511GA

511GA is real-time. For the GA accident form (GA-PT2):

- **Form**: GA-PT2 — Statewide Crash Reporting System
- **Portal**: https://services.dps.georgia.gov/orr — Open Records Request
- **Cost**: Varies by agency; GSP typically $5
- **What to do (manual)**: Mason emails the relevant GA county sheriff or GSP
  troop ORR coordinator with case number. No bulk feed available.

### California — no statewide live feed

CHP runs an internal feed (CAD/IRIS) not publicly exposed. Per-incident:

- **Form**: CHP 555-03 (Traffic Collision Report Request)
- **Portal**: https://www.chp.ca.gov/programs-services/services-information/crash-reports-collision-reports
- **Cost**: FREE for involved parties; $10 for others
- **What to do**: For LA / SF / SD high-value fatals, send 555-03 with case #.

## Pipeline Wiring

When a state has a working free-by-default API → wire it directly into
`lib/v1/ingest/state-crash.js` like FL/TX/OH/GA. When FOIA-only → log the
incident with `tags=['needs_foia']` and surface in the dashboard so Mason or a
rep can manually pull the report.

## Environment Variables

Add to Vercel env (no redeploy needed — variables are read at runtime):

- `OHGO_API_KEY` — request at https://publicapi.ohgo.com (free, instant)
- `GA511_KEY` — already requested per task #112
- `FL511_KEY` — not currently required (public endpoint)
- `TXDOT_KEY` — not currently required (public ArcGIS)
