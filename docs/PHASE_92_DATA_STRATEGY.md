# Phase 92 Data-Source Strategy: End-to-End Car Accident Lifecycle

After Phase 91 measurement showed ingest growing 85/day but qualified count flat at 11.

## Goal
Get more **named** accident victims and more **contact info** per name.

## End-to-end map

| Stage | Time | Data emitted | Public? | Captured today? |
|---|---|---|---|---|
| Pre-impact | -m | Driver behavior in Waze/TomTom | yes | yes (Waze) |
| Impact | t=0 | 911 dispatch, scanner radio, CHP CAD | yes | yes (scanner, CHP) |
| Impact | t=0 | Real-time witness tweets, TikToks | yes | NO |
| First responders | +5m | EMS dispatch, fire dept call | yes | yes (scanner) |
| First responders | +15m | Tow truck dispatch | partial | NO |
| Hour 1 | +1h | News website breaking story (no name) | yes | yes (news-rss) |
| Hour 2-4 | +3h | Police press release | yes | yes (pd-press) |
| Hour 4-12 | +6h | Hospital admission (HIPAA, but family posts) | partial | Caringbridge planned |
| Day 1 | +24h | Coroner case open (fatals), name in record | yes | yes (Cook IL live, others planned) |
| Day 1-2 | +1-2d | Sheriff booking log if DUI/HR | yes | DUI scraper planned |
| Day 2-3 | +2-3d | Family social media posts | yes | NO |
| Day 2-7 | +1w | GoFundMe campaign | yes | yes |
| Day 3-10 | +1w | Obituary published | yes | yes |
| Week 1-2 | +2w | Funeral home announcement | yes | yes |
| Week 2-4 | +3w | NHTSA FARS pre-release | yes | yes |
| Week 4-12 | +months | Civil PI filing | yes | yes |
| Month 2-6 | +6mo | Probate filed | yes | NO |

## Tier 1: SHIPPED in Phase 92

1. **Crash<->News Bridge** — Match nameless Socrata records to news within +/-48h/5km, extract names with Claude. Solves the largest immediate gap.
2. **Deep-Dive-Narrow** — Triangulate name+state through voter rolls + Apollo + PDL + Brave + Google CSE Facebook. Confidence-weighted fusion. Apply at >=0.75.
3. **Patch.com hyperlocal** — 50 markets x 2-3 crash articles/week each.
4. **Cook County Medical Examiner** — Socrata API, decedent names of motor-vehicle fatalities.

## Tier 2: PLANNED for Phase 93

5. **Sheriff DUI booking logs** — DRIVER name + DOB + address (defendant in PI cases, often deeper pockets than victim).
6. **Caringbridge** — public CarePages with hospitalized victim updates.
7. **Twitter/X witness monitor** — searches for "witnessed crash on [highway]" tweets.

## Tier 3: BUILD LATER

8. State court PI civil filings RSS feeds (where available)
9. Probate filings for fatals 0-12mo old
10. Local TV affiliate websites (KTLA, WSB, etc.)
11. County tow yard inventory
12. Reddit local subs (r/NYC, r/LosAngeles, etc.)
13. Bluesky/X searches for witnesses
14. YouTube comment scraping on local news crash videos
15. Nextdoor public posts
16. Legacy.com obituary aggregator
17. OSHA accident reports (work vehicle)
18. FMCSA crash data (commercial trucks)

## Tier 4: PAID OPTIONS (skip unless ROI proven)

19. LexisNexis Risk View (~$5K/mo)
20. Thomson Reuters CLEAR
21. CARFAX corporate access

## Deep-narrowing strategy (for the 107 partial persons)

When we have just `name + state`, triangulate:
- Voter rolls by name+state -> likely city + age + party
- Property records by owner name+state -> home address
- Apollo people-search by name+state -> employer + LinkedIn
- PDL by name+state -> email + sometimes phone
- Brave Search obituaries if recent fatal -> next-of-kin
- Google CSE Facebook by name+city -> Facebook URL
- Hunter person-search by name+employer-domain
- Whitepages free by name+state
- TruePeopleSearch by name+age+state

Confidence weighting (deep-dive-narrow.js):
- voter rolls: 0.35
- PDL match: 0.25
- Apollo match: 0.20
- Brave obit: 0.15
- Google CSE FB: 0.05
- Same-state bonus: +0.15
- Threshold to apply: 0.75

## Cross-conversion principle

Per Mason's primary directive: every new field discovered MUST trigger fan-out cascade.
Phase 92 engines all write to enrichment_logs, fires Postgres trigger, cascade queue,
auto-fan-out (deep-dive-narrow now in ENGINE_MATRIX as a name-trigger engine).
Compounding example: coroner record gives us a name -> deep-dive-narrow finds voter
rolls -> voter rolls give address -> fan-out fires Apollo+Trestle+Hunter on the
address -> phone + email materialize.

## Measurement targets

30-day window after Phase 92 deploys:
- crash_news_bridge_persons_added > 30
- deep_dive_narrow_apply_rate > 25%
- patch_news_inserted_24h > 50
- cook_me_persons_added > 20
- qualified count: 11 -> 30+
