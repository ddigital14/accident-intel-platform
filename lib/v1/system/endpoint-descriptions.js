/**
 * Phase 52: Smart endpoint descriptions — rep-friendly, plain English,
 * brand-voice (confident, intelligent, slight personality).
 *
 * GET /api/v1/system/endpoint-descriptions?secret=ingest-now
 *   → returns full map
 *
 * GET /api/v1/system/endpoint-descriptions?secret=ingest-now&endpoint=/api/v1/enrich/deep-phone-research
 *   → returns single description
 *
 * Used in tooltips, dashboard help drawer, integration cards, browser
 * extension popovers, and the public docs page.
 */
const { getDb } = require('../../_db');
const { trackApiCall } = require('./cost');
const { bumpCounter } = require('./_cei_telemetry');

const DESCRIPTIONS = {
  // Dashboard
  '/api/v1/dashboard/awaiting-contact': 'Verified accident victims who have a name in the system but no contact yet. Reps work this list — every name here has been triple-checked as a real victim, not a journalist or witness.',
  '/api/v1/dashboard/feed':             'The live wire. Every incident, source ping, and enrichment hit in chronological order. Refreshes every 10s. This is the platform thinking out loud.',
  '/api/v1/dashboard/counts':           'KPI tile data: qualified leads, pending verification, awaiting contact, total today. Drives the dashboard top strip.',
  '/api/v1/dashboard/stats':            'Aggregate stats over the selected time window — totals by metro, by qualification state, by source, with daily/hourly buckets.',
  '/api/v1/dashboard/my-assignments':   'Leads currently claimed by the logged-in rep. Atomic claim — once you grab it, no other rep sees it as available.',
  '/api/v1/dashboard/admin-overview':   'Full-platform health snapshot for admins: engine success rates, daily spend, cron freshness, error rates by source.',
  '/api/v1/dashboard/caller-id':        'Live phone-call enrichment. When a number rings, this returns the matched person + linked incident in <300ms.',

  // Incidents
  '/api/v1/incidents':                  'List of every accident on file with rich filters — qualification state, metro, severity, date range, contact-info status. Backbone of the All Incidents tab.',

  // Ingest pipelines
  '/api/v1/ingest/news-rss':            'Pulls 200+ local news RSS feeds for fresh accident articles. Extracts victim names, cities, vehicles, severity. Filters out journalists/witnesses upfront.',
  '/api/v1/ingest/multicommunity-news': 'Same as news-rss but tuned for non-English communities — Spanish, Vietnamese, Korean, Tagalog, Haitian Creole, Mandarin, Russian, Portuguese, Arabic, French.',
  '/api/v1/ingest/obituaries':          'Scrapes legacy.com, tributes, local funeral home pages for fatal-crash obituaries. High-signal — names + family next-of-kin in one shot.',
  '/api/v1/ingest/state-crash':         'Hits TX, GA, FL, AZ DOT crash report APIs for officially-reported incidents. Names + plates when available.',
  '/api/v1/ingest/courtlistener':       'FREE federal + state court records. Pulls accident-related filings: PI complaints, attorney appearances, settlement notices.',
  '/api/v1/ingest/court':               'State-level court records (county clerks, e-filing portals) for new PI cases — surfaces leads competitors haven’t signed yet.',
  '/api/v1/ingest/scanner':             'Live police-scanner audio piped through Deepgram + Claude. Catches incidents 5-15 minutes before news breaks.',
  '/api/v1/ingest/waze':                'Real-time Waze incident feed. Coarse (no names) but the fastest signal for "an accident is happening RIGHT NOW".',
  '/api/v1/ingest/opendata':            'Houston + Atlanta city open-data 911 dispatch feeds. Coarse address + incident type, often lands minutes after dispatch.',
  '/api/v1/ingest/funeral-homes':       'Scrapes 800+ funeral-home websites for next-of-kin contact info on fatal-crash victims. Pure homegrown — zero API cost.',
  '/api/v1/ingest/gofundme':            'GoFundMe accident-victim campaigns. The organizer is almost always immediate family — gold-tier next-of-kin signal.',
  '/api/v1/ingest/fars':                'NHTSA FARS database for fatal crashes. Slow (annual lag) but ground-truth for historical case research.',
  '/api/v1/ingest/pulsepoint':          'PulsePoint EMS dispatch. Real-time, hyper-local — see the ambulance roll before the news writes the story.',
  '/api/v1/ingest/nextdoor':            'NextDoor neighborhood posts about local accidents. Often surface witness contacts and victim names.',
  '/api/v1/ingest/police-social':       'Police department Twitter/X + Facebook posts. Press-release-tier accuracy with photo attachments.',
  '/api/v1/ingest/pd-press':            'Police press release pages (replaces dead Nitter). Official department announcements with names + plates.',
  '/api/v1/ingest/reddit':              'r/PoliceScanner + city subreddits. Locals posting in real time — caught hundreds of leads before mainstream news.',

  // Enrichment
  '/api/v1/enrich/deep-phone-research':   'Multi-step Opus 4.7 reasoning loop. Hands the victim’s full case to Claude, which generates 5+ search hypotheses, fires them across 12 engines, synthesizes results, iterates up to 4 cycles. ~60s per victim. Use when standard enrichment dries up.',
  '/api/v1/enrich/multilang-detector':    'Detects Spanish, French, Haitian Creole, Vietnamese, Tagalog, Korean, Mandarin, Russian, Portuguese, and Arabic accident articles. Translates with Claude Sonnet, preserving names. Unlocks accident victims in non-English communities.',
  '/api/v1/enrich/spanish-detector':      'Specialized Spanish-language accident detection — finer-grained than the generic multilang router. Tuned for TX/AZ/FL/CA Latino communities.',
  '/api/v1/enrich/people-search':         'Smart router across PDL, Apollo, Trestle, Whitepages, TruePeopleSearch. Picks the cheapest/most-confident path per victim and dedupes results.',
  '/api/v1/enrich/voter-rolls':           'Cross-references uploaded state voter files (FL/GA/TX/AZ) for victim address + DOB confirmation. 100M+ records, FREE after one-time download.',
  '/api/v1/enrich/property-records':      'County tax-roll lookups across 30+ counties. Confirms address + finds spouse/co-owner names. FREE homegrown.',
  '/api/v1/enrich/address-to-residents':  'Reverse-address fallback chain — Trestle, Whitepages, TruePeopleSearch, Spokeo, FastPeopleSearch, voter rolls, property records. Tries in cost order until it finds a hit.',
  '/api/v1/enrich/trestle-test':          'Trestle API live-fire test — phone, email, name, address. Use to verify integration after API key rotation.',
  '/api/v1/enrich/apollo-unlock':         'Burns one Apollo Professional credit to unlock a contact’s direct dial + email. Use only on high-score qualified leads ($120+ threshold).',
  '/api/v1/enrich/plate-ocr-vision':      'GPT-4o Vision reads license plates + VINs from accident scene photos. Passes plates to vehicle-owner lookup for instant identity.',
  '/api/v1/enrich/auto-purchase':         'Decides per-victim whether to spend on Trestle/Apollo/PDL. Weighs lead score, current confidence, and remaining budget.',
  '/api/v1/enrich/fraud-filter':          'Rejects fake-name patterns, witness misclassifications, and known scammer profiles before they pollute the lead list.',
  '/api/v1/enrich/contradiction-detector':'Cross-references all evidence on a victim — flags conflicts (different DOB across sources, mismatched addresses) for QA before contact.',
  '/api/v1/enrich/temporal-corroborate':  'Confirms a person was actually in the right place at the right time — geofence + obituary date + social-media timestamp triangulation.',
  '/api/v1/enrich/smart-cross-ref':       'Phase 50 next-best-action engine. Looks at the gap in a victim’s profile (no phone? no address?) and picks the engine most likely to fill it.',
  '/api/v1/enrich/ensemble-qualifier':    'Weighted vote across 9 sub-classifiers — fatal? injury? PI-eligible? US-jurisdiction? journalist? Final score sets qualification_state.',
  '/api/v1/enrich/active-learning':       'Feeds rep "wrong lead" / "good lead" feedback back into the qualifier weights. The platform gets smarter every week without retraining.',
  '/api/v1/enrich/predictive-at-source':  'Predicts at ingest-time whether an article will yield a qualified victim — skips low-value scrapes before burning compute.',
  '/api/v1/enrich/fatal-family-tree':     'For fatal crashes, expands to spouse + children + parents using obit + voter rolls + property records. Rep contacts next-of-kin, not the deceased.',
  '/api/v1/enrich/attorney-heatmap':      'Detects whether the victim already has counsel. If yes — flag and demote. If no — flag as cold-open opportunity.',
  '/api/v1/enrich/insurance-doi':         'State Department of Insurance lookup for the at-fault driver’s carrier. Tells the rep who they’ll be negotiating against.',

  // System / orchestration
  '/api/v1/system/master-lead-list':            'The flagship daily email — every qualified lead, ranked, formatted for forward-to-intake. Sent via Resend. The single artifact reps work from each morning.',
  '/api/v1/system/cross-intel-orchestrator':    'CEI hub. Hand it a victim_id, it fans out to every relevant engine in parallel, deduplicates, and returns the consolidated identity card.',
  '/api/v1/system/cei-counters':                'Engine telemetry — invocations, success rate, p95 latency, last-error per engine. The dashboard’s "is the platform healthy?" answer.',
  '/api/v1/system/cei-poll':                    'Cron-driven CEI runner. Polls cascade_queue every minute, fires cross-exam on any victim with new evidence.',
  '/api/v1/system/cron-staleness':              'Watchdog that pages Slack if any cron hasn’t run in 2x its expected interval. Catches silent failures.',
  '/api/v1/system/schema-drift-check':          'Runs on every deploy — diffs canonical schema vs. live database. Blocks the deploy if a column was renamed or dropped.',
  '/api/v1/system/embedding-queue':             'VoyageAI semantic-dedup queue. Embeds new persons + incidents nightly so we never create duplicate leads from differently-worded source articles.',
  '/api/v1/system/qualify':                     'The qualification engine. Walks every pending lead, runs ensemble-qualifier, promotes/demotes based on score threshold (currently 60).',
  '/api/v1/system/auto-assign':                 'Distributes new qualified leads to reps based on metro coverage + current-load. Atomic — no two reps get the same lead.',
  '/api/v1/system/notify':                      'Slack + SMS + email alert dispatch. Routes by lead score, rep on-call, and metro.',
  '/api/v1/system/cost':                        'Per-engine spend rollup with daily/weekly/monthly breakdowns. Drives the Cost tab.',
  '/api/v1/system/health':                      'Top-level health JSON — green/yellow/red for db, queue, cron, email, top 10 engines.',
  '/api/v1/system/audit':                       'Cross-engine consistency audit. Spots when engine A says X and engine B says not-X for the same victim.',
  '/api/v1/system/smoke-test':                  'E2E smoke — fires one trace through every active pipeline. Run on every deploy.',
  '/api/v1/system/realtime-feed':               'Server-Sent Events stream. Browser dashboard subscribes for instant lead-arrival animations without polling.',
  '/api/v1/system/realtime-victim-alerts':      'WebSocket-style high-score alerts to reps’ phones via Twilio + Slack DM. <30s from extraction to ping.',
  '/api/v1/system/atomic-claim':                'Postgres-row-level lock for "I’m calling this lead now". Prevents two reps from double-dialing.',
  '/api/v1/system/refresh-mv':                  'Refreshes the dashboard’s materialized views every 60s. Keeps KPI tiles fast under heavy ingest.',
  '/api/v1/system/spatial-cluster':             'PostGIS-powered map clustering. Groups nearby pins so the map view doesn’t melt at 10k+ leads.',
  '/api/v1/system/digest':                      'Daily Slack digest — top leads, top engines, errors of the day. Posts at 8am ET to the rep channel.',
  '/api/v1/system/setup':                       'Admin endpoint for wiring Slack webhook, Twilio creds, rep metros, API keys. UI lives in the Integrations tab.',
  '/api/v1/system/changelog':                   'Append-only feature/fix log. Every shipped change writes one row. Drives the in-app "what’s new" panel.',
  '/api/v1/system/saved-alerts':                'Per-rep saved-search alerts (e.g. "ping me on any fatal in Houston > 80 score"). Fires through notify when matched.',
  '/api/v1/system/crm-export':                  'Export qualified leads to Salesforce, HubSpot, Lawmatics, or CSV. Pluggable destination.',
  '/api/v1/system/design-tokens':               'Returns the canonical ACC design system as JSON — colors, typography, shadows, logo URLs. Used by embeds, emails, plugins.',
  '/api/v1/system/endpoint-descriptions':       'This very endpoint. Returns a plain-English description for every public AIP endpoint so the dashboard can show smart tooltips and help text everywhere.',
  '/api/v1/system/error-watchdog':              'Sentry + Postgres error tail. Pages on-call when error rate spikes 3x baseline.',
  '/api/v1/system/sentry':                      'Sentry SDK integration endpoint — captures unhandled rejections + slow queries.',
  '/api/v1/system/lookup-cache':                'Shared LRU cache for expensive lookups (PDL, Apollo, Trestle). Cuts duplicate-spend by ~40%.',
  '/api/v1/system/triggers':                    'Postgres triggers manager — keeps cascade_queue, mv refreshes, and audit hooks in sync after schema changes.',
  '/api/v1/system/test-gpt':                    'Live-fires the AI router (Claude + GPT-4o + Sonnet) with a fixed test article. Use to verify model-key health.',
  '/api/v1/system/voter-rolls-upload':          'Multipart-upload endpoint for new state voter files. Streams parse + insert without blowing the function memory limit.',
  '/api/v1/system/resend-domain-setup':         'Verifies the accidentcommandcenter.com sending domain in Resend — DKIM, SPF, DMARC checks.',

  // Auth
  '/api/v1/auth/login':                         'JWT login. Email + password → 7-day token. No password storage on our end if SSO is enabled.',
  '/api/v1/auth/me':                            'Current user info from JWT. Used by the navbar to render name/role.',

  // Cron + meta
  '/api/v1/cron/dispatch':                      'In-process cron router. Vercel Hobby caps at 11 crons — this lets us run 40+ schedules from one entry point.',
  '/api/v1/health':                             'Public health check. 200 OK if the platform is up. No auth required.'
};

const SECRET = 'ingest-now';
function authed(req) {
  const s = req.query?.secret || req.headers?.['x-cron-secret'];
  return s === SECRET || s === process.env.CRON_SECRET;
}

function describe(endpoint) {
  if (!endpoint) return null;
  if (DESCRIPTIONS[endpoint]) return DESCRIPTIONS[endpoint];
  // strip trailing slash + query
  const norm = endpoint.replace(/\?.*$/, '').replace(/\/$/, '');
  if (DESCRIPTIONS[norm]) return DESCRIPTIONS[norm];
  // try with /api/v1 prefix
  const withPrefix = norm.startsWith('/api/v1') ? norm : '/api/v1' + (norm.startsWith('/') ? norm : '/' + norm);
  return DESCRIPTIONS[withPrefix] || null;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'public, max-age=300, s-maxage=300');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (!authed(req)) return res.status(401).json({ error: 'Unauthorized' });

  const t0 = Date.now();
  let db = null;
  try { db = getDb(); } catch (_) {}

  try {
    const single = req.query?.endpoint;
    if (single) {
      const desc = describe(single);
      if (db) {
        await trackApiCall(db, 'endpoint-descriptions', 'lookup', 0, 0, true).catch(() => {});
        await bumpCounter(db, 'endpoint-descriptions', !!desc, Date.now() - t0).catch(() => {});
      }
      if (!desc) return res.status(404).json({ success: false, endpoint: single, description: null, message: 'No description on file. Add one to lib/v1/system/endpoint-descriptions.js' });
      return res.status(200).json({ success: true, endpoint: single, description: desc });
    }
    if (db) {
      await trackApiCall(db, 'endpoint-descriptions', 'list', 0, 0, true).catch(() => {});
      await bumpCounter(db, 'endpoint-descriptions', true, Date.now() - t0).catch(() => {});
    }
    return res.status(200).json({
      success: true,
      count: Object.keys(DESCRIPTIONS).length,
      voice: 'confident, intelligent, slightly cinematic, with personality but professional',
      descriptions: DESCRIPTIONS
    });
  } catch (e) {
    if (db) await bumpCounter(db, 'endpoint-descriptions', false, Date.now() - t0).catch(() => {});
    return res.status(500).json({ error: e.message });
  }
};

module.exports.describe = describe;
module.exports.DESCRIPTIONS = DESCRIPTIONS;
