# INCIDENT COMMAND - Data Source Integration Guide

## Overview

This document covers every data source the platform integrates with, including pricing, signup process, and what data each provides. Sources are prioritized by speed and data quality.

---

## TIER 1: REAL-TIME SOURCES (0-5 minute delay)

### 1. Police/Fire/EMS Scanner Feeds

**Broadcastify (Premium)**
- **Cost:** $200-500/month per metro area
- **Data:** Live audio from 7,000+ scanner feeds, transcribed to text
- **Signup:** https://www.broadcastify.com/api/ - Apply for API access
- **What you get:** Real-time dispatch calls, incident details, addresses, unit assignments
- **Config:** Set `feed_ids` per metro in source config

**OpenMHz (Free/Open Source)**
- **Cost:** Free
- **Data:** Community-operated scanner feeds with API access
- **Signup:** https://openmhz.com - Free API key
- **What you get:** Similar to Broadcastify but community-dependent coverage
- **Best for:** Supplementing Broadcastify in metros with active communities

### 2. CAD Dispatch Systems

**PulsePoint (Free)**
- **Cost:** Free
- **Data:** Real-time fire/EMS dispatch data from participating agencies
- **Signup:** https://www.pulsepoint.org/agency - Partner with local agencies
- **What you get:** Incident type, location, responding units, timestamps
- **Coverage:** 4,400+ agencies nationwide
- **Best for:** Fastest possible notification of accidents requiring EMS

**RapidSOS (Premium)**
- **Cost:** $3,000-8,000/month
- **Data:** 911 call data platform, caller location, call transcripts
- **Signup:** https://rapidsos.com/partners - Enterprise partnership
- **What you get:** Precise caller location (from mobile phones), call type, dispatch data
- **Best for:** Most accurate real-time location data

**Tyler Technologies / New World CAD**
- **Cost:** $2,000-5,000/month per jurisdiction
- **Data:** Direct CAD system feed from police/fire/EMS dispatchers
- **Signup:** Contact Tyler Technologies sales - requires agency partnership
- **What you get:** Complete dispatch data before it hits any public feed

### 3. Radio Frequency Monitoring (SDR)

**Self-Hosted with trunk-recorder**
- **Cost:** $500-1,000 hardware per metro + hosting
- **Data:** Raw radio frequencies decoded and transcribed
- **Setup:** RTL-SDR dongle + trunk-recorder software + Whisper for transcription
- **What you get:** Every transmission on emergency talkgroups
- **Best for:** Metros without Broadcastify coverage or for redundancy

---

## TIER 2: NEAR-REAL-TIME SOURCES (5-60 minute delay)

### 4. EMS/Hospital Transport Data

**ESO (Emergency Services Platform)**
- **Cost:** $1,000-3,000/month
- **Data:** EMS patient care reports, transport records
- **Signup:** https://www.eso.com - Enterprise sales
- **What you get:** Patient name, DOB, injury details, hospital destination, insurance

**ImageTrend (EMS Reporting)**
- **Cost:** $800-2,500/month
- **Data:** EMS run sheets and hospital data
- **Signup:** https://www.imagetrend.com - Sales contact
- **What you get:** Similar to ESO, depends on agency participation

**Hospital ER Feed Partnerships**
- **Cost:** Negotiated per hospital system
- **Data:** ER admission data for trauma/accident patients
- **Signup:** Direct partnership with hospital systems
- **What you get:** Patient info, admission time, injury type, insurance on file

### 5. Official Police/Crash Reports

**LexisNexis Accurint (Gold Standard)**
- **Cost:** $1,000-3,000/month + per-report fees ($2-15 each)
- **Data:** Official police crash reports with full details
- **Signup:** https://www.lexisnexis.com/en-us/products/accurint.page
- **What you get:** Complete crash reports: all persons, vehicles, insurance, injuries, narrative
- **Confidence:** 85% - This is the most reliable data source
- **Delay:** 1-7 days after incident (depends on jurisdiction)

**CrashDocs / BuyCrash**
- **Cost:** $500-1,500/month per state
- **Data:** Electronic crash reports as soon as filed
- **Signup:** https://www.crashdocs.org or https://buycrash.com - Partner API
- **What you get:** Crash reports typically 24-72 hours after filing
- **Best for:** States where LexisNexis coverage is limited

---

## TIER 3: ENRICHMENT SOURCES (used to fill gaps)

### 6. Insurance Verification

**Verisk / ISO ClaimSearch**
- **Cost:** $2,000-5,000/month
- **Data:** Insurance policy verification, claim history
- **Signup:** https://www.verisk.com - Enterprise sales
- **What you get:** Carrier name, policy number, coverage type, policy limits, agent info
- **Critical for:** Knowing policy limits before outreach

**TransUnion TLOxp**
- **Cost:** $1,000-3,000/month
- **Data:** Skip tracing + insurance verification combined
- **Signup:** https://www.transunion.com/solution/tlo
- **What you get:** Phone numbers, addresses, insurance, associates, employment

**LexisNexis Insurance Exchange**
- **Cost:** Per-lookup pricing ($5-25 per search)
- **Data:** Insurance claim history, coverage verification
- **Signup:** Through LexisNexis Accurint account

### 7. Skip Tracing (Contact Enrichment)

**TransUnion TLOxp** (see above - dual purpose)
- **Best for:** Finding phone numbers, current addresses, email addresses

**LexisNexis Accurint Person Search**
- **Cost:** Included with Accurint subscription + per-search
- **Data:** Current phone, address, email, associates, employment
- **Best for:** When TLO doesn't return results

### 8. News Monitoring

**NewsAPI**
- **Cost:** $449/month (Business plan)
- **Data:** 150,000+ news sources worldwide
- **Signup:** https://newsapi.org
- **What you get:** Accident articles with details for corroboration
- **Best for:** Verifying incidents and enriching descriptions

**Bing News Search API**
- **Cost:** $250/month (S2 tier)
- **Data:** Microsoft news index
- **Signup:** https://azure.microsoft.com/en-us/products/ai-services/ai-search

### 9. Government/DOT Data

**NHTSA Crash API (Free)**
- **Cost:** Free
- **Data:** Fatal crash data (FARS), crash estimates (CRSS)
- **Signup:** https://crashviewer.nhtsa.dot.gov/CrashAPI
- **Delay:** Weeks to months - use for historical analysis

**FMCSA SAFER System (Free)**
- **Cost:** Free with API key
- **Data:** Commercial vehicle crash data, carrier information
- **Signup:** https://mobile.fmcsa.dot.gov/QCDevsite/
- **Best for:** Truck accident cases - carrier info, DOT numbers, safety records

### 10. Social Media / Crowdsourced

**Waze Partner Feed**
- **Cost:** Free for data partners
- **Data:** Real-time user-reported accidents
- **Signup:** https://www.waze.com/ccp - Connected Citizens Program
- **What you get:** Accident locations, severity estimates, road closure info

**Twitter/X API**
- **Cost:** $100-5,000/month depending on tier
- **Data:** Public posts about accidents
- **Best for:** Breaking news corroboration

---

## RECOMMENDED STARTER PACKAGE

For $5,000-7,000/month covering top 10 metros:

| Source | Monthly Cost | Purpose |
|--------|-------------|---------|
| Broadcastify API | $2,000 | Real-time scanner for 10 metros |
| PulsePoint | Free | Real-time EMS dispatch |
| LexisNexis Accurint | $1,500 | Official crash reports + skip tracing |
| Verisk ClaimSearch | $2,000 | Insurance verification |
| NewsAPI | $449 | News corroboration |
| NHTSA + FMCSA | Free | Government crash data |
| OpenMHz | Free | Supplemental scanner |
| Waze CCP | Free | Crowdsourced reports |
| **TOTAL** | **~$6,000/mo** | |

### Scale-Up Path

Add these as revenue grows:

1. **RapidSOS** ($5,000/mo) - Precise 911 caller location
2. **ESO or ImageTrend** ($2,000/mo) - Patient details from ambulance
3. **TransUnion TLOxp** ($2,000/mo) - Better skip tracing + insurance
4. **CrashDocs** ($1,000/mo/state) - Faster police reports
5. **Tyler CAD feeds** ($3,000/mo) - Direct dispatch data

---

## DATA FLOW ARCHITECTURE

```
Scanner Feeds ──┐
CAD Dispatch ───┤
EMS/Hospital ───┤──▶ INGESTION ENGINE ──▶ DEDUPLICATION ──▶ AI ENRICHMENT ──▶ LIVE DASHBOARD
News Sources ───┤        │                     │                  │              │
Social Media ───┤        ▼                     ▼                  ▼              ▼
DOT Data ───────┘   Source Reports      Incident Matches    Confidence      WebSocket Push
                         │                     │              Scoring         to Reps
                         ▼                     ▼                  │
                    Raw Data Store      Merged Incidents          ▼
                                                            Lead Scoring
                                                                  │
                    Police Reports ─────────────────────────────┐  │
                    Insurance Verify ───────────────────────────┤  ▼
                    Skip Tracing ───────────────────────────────┴──▶ ENRICHED LEADS
```

## CONFIDENCE SCORING

Each source contributes to overall incident confidence:

| Source Type | Base Confidence | Notes |
|------------|----------------|-------|
| Police Report | 85% | Gold standard |
| EMS/Hospital | 65% | Verified transport |
| CAD Dispatch | 40-60% | Depends on provider |
| News | 35% | Corroboration only |
| Scanner | 25% | Needs verification |
| Radio (raw) | 20% | Lowest, needs corroboration |
| Social Media | 15% | Supplemental only |

Multi-source confirmation adds +15% per additional source (capped at 100%).
