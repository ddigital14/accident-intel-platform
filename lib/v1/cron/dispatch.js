/**
 * GET /api/v1/cron/dispatch?secret=ingest-now&jobs=foo,bar,baz
 *
 * Calls handlers IN-PROCESS (not via HTTP) — avoids self-looping back to
 * the same Vercel router function, which caused timeouts/failures.
 *
 * Each job is fired in parallel with isolated try/catch.
 */
const { getDb } = require('../../_db');
const { reportError } = require('../system/_errors');

// Static handler imports — Vercel needs these for bundling
const ingestRun = require('../ingest/run');
const ingestWaze = require('../ingest/waze');
const ingestOpendata = require('../ingest/opendata');
const ingestScanner = require('../ingest/scanner');
const ingestNews = require('../ingest/news');
const ingestNewsRss = require('../ingest/news-rss');
const ingestReddit = require('../ingest/reddit');
const ingestPdPress = require('../ingest/pd-press');
const ingestPoliceSocial = require('../ingest/police-social');
const ingestStateCrash = require('../ingest/state-crash');
const ingestCourt = require('../ingest/court');
const ingestObituaries = require('../ingest/obituaries');
const ingestTrauma = require('../ingest/trauma');
const ingestCorrelate = require('../ingest/correlate');
const sysQualify = require('../system/qualify');
const sysNotify = require('../system/notify');
const sysAutoAssign = require('../system/auto-assign');
const enrichRun = require('../enrich/run');
const enrichTrigger = require('../enrich/trigger');
const enrichPeopleSearch = require('../enrich/people-search');
const enrichAddressToResidents = require('../enrich/address-to-residents');
const enrichNominatim = require('../enrich/nominatim-geocode');
const enrichFccCarrier = require('../enrich/fcc-carrier');
const enrichDevProfiles = require('../enrich/dev-profiles');
const enrichArchiveSearch = require('../enrich/archive-search');
const enrichStateCourts = require('../enrich/state-courts');
const enrichBusinessRegistry = require('../enrich/business-registry');
const enrichUspsValidate = require('../enrich/usps-validate');
const enrichRedditHistory = require('../enrich/reddit-history');
const enrichCoResidence = require('../enrich/co-residence');
const enrichNameRarity = require('../enrich/name-rarity');
const enrichVehicleOwner = require('../enrich/vehicle-owner');
const enrichTemporalCorroborate = require('../enrich/temporal-corroborate');
const enrichConfidenceDecay = require('../enrich/confidence-decay');
const enrichPersonMerge = require('../enrich/person-merge');
const enrichCensusIncome = require('../enrich/census-income');
const enrichEnsembleQualifier = require('../enrich/ensemble-qualifier');
const enrichActiveLearning = require('../enrich/active-learning');
const enrichContradictionDetector = require('../enrich/contradiction-detector');
const enrichFlCountyCourts = require('../enrich/fl-county-courts');
const enrichVoterRolls = require('../enrich/voter-rolls');
const ingestPulsepoint = require('../ingest/pulsepoint');
const enrichSalvageListings = require('../enrich/salvage-listings');
const ingestGoFundMe = require('../ingest/gofundme');
const ingestFuneralHomes = require('../ingest/funeral-homes');
const enrichInsuranceDOI = require('../enrich/insurance-doi');
const enrichWorkersComp = require('../enrich/workers-comp');
const sysSavedAlerts = require('../system/saved-alerts');
const sysCrmExport = require('../system/crm-export');
const ingestNextdoor = require('../ingest/nextdoor');
const enrichYoutubeComments = require('../enrich/youtube-comments');
const enrichLeadStaleRecycler = require('../enrich/lead-stale-recycler');
const enrichEngagementScore = require('../enrich/engagement-score');
const enrichFatalFamilyTree = require('../enrich/fatal-family-tree');
const enrichAttorneyHeatmap = require('../enrich/attorney-heatmap');
const enrichFraudFilter = require('../enrich/fraud-filter');
const ingestTelegramPolice = require('../ingest/telegram-police');
const ingestWhoisLLC = require('../ingest/whois-llc');
const ingestHospitalRSS = require('../ingest/hospital-rss');
const enrichPredictiveAtSource = require('../enrich/predictive-at-source');
const sysRefreshMv = require('../system/refresh-mv');
const sysTranscriptionQueue = require('../system/transcription-queue');
const sysVacuumNightly = require('../system/vacuum-nightly');
const sysCeiPoll = require('../system/cei-poll');
const ingestFbiUcr = require('../ingest/fbi-ucr');
const ingestOsha = require('../ingest/osha');
const ingestFars = require('../ingest/fars');
const ingestCitizenProbe = require('../ingest/citizen-probe');
const sysRepStats = require('../system/rep-stats');
const sysBidirectionalCascade = require('../system/bidirectional-cascade');
const sysAudit = require('../system/audit');
const sysCascade = require('../system/cascade');
const sysTrestleProbe = require('../system/trestle-probe');
const enrichTwilio = require('../enrich/twilio');
const enrichSocialSearch = require('../enrich/social-search');
const enrichCrossExam = require('../enrich/cross-exam');
const enrichFamilyTree = require('../enrich/family-tree');
const enrichVehicleHistory = require('../enrich/vehicle-history');
const enrichRelativesSearch = require('../enrich/relatives-search');
const enrichTcpaCheck = require('../enrich/tcpa-litigator-check');
const sysDigest = require('../system/digest');
const sysErrors = require('../system/errors');
const sysBackfillNameless = require('../system/backfill-nameless');
const enrichCourtReverseLink = require('../enrich/court-reverse-link');
const enrichObitBackfill = require('../enrich/obit-backfill');
const claudeCrossReasoner = require('../enrich/claude-cross-reasoner');
const enrichSmartRouter = require('../enrich/_smart_router');
const enrichPdlByName = require('../enrich/pdl-by-name');
const enrichPdlIdentify = require('../enrich/pdl-identify');
const claudeIdentityInvestigator = require('../enrich/claude-identity-investigator');
const enrichPeopleSearchMulti = require('../enrich/people-search-multi');
const enrichApolloCrossPollinate = require('../enrich/apollo-cross-pollinate');
const enrichPropertyToFamily = require('../enrich/property-to-family');
const sysConstantCrossLoop = require('../system/_constant_cross_loop');
const ingestHomegrownRotation = require('../ingest/_homegrown_rotation');
const enrichVictimVerifier = require('../enrich/victim-verifier');
const enrichVictimResolver = require('../enrich/victim-resolver');
const enrichVictimContactFinder = require('../enrich/victim-contact-finder');
const enrichHomegrownOsintMiner = require('../enrich/homegrown-osint-miner');
const enrichDeepPhoneResearch = require('../enrich/deep-phone-research');
const enrichFuneralHomeSurvivors = require('../enrich/funeral-home-survivors');
const enrichAiNewsExtractor = require('../enrich/ai-news-extractor');
const enrichAiObituaryParser = require('../enrich/ai-obituary-parser');
const sysAiCrossSourceMerge = require('../system/ai-cross-source-merge');
const enrichEvidenceCrossChecker = require('../enrich/evidence-cross-checker');
const enrichBraveSearch = require('../enrich/brave-search');
const enrichFreeOsintExtras = require('../enrich/free-osint-extras');
const sysBestLeadSynthesizer = require('../system/best-lead-synthesizer');
// Phase 44A — new keys + alerts + unlock
const enrichApolloUnlock = require('../enrich/apollo-unlock');
const enrichVoyageRouter = require('../enrich/_voyage_router');
const sysRealtimeVictimAlerts = require('../system/realtime-victim-alerts');
const { batchInShards } = require('../system/smart-batcher');
// Phase 44B
const enrichPlateOcrVision = require('../enrich/plate-ocr-vision');
const enrichAutoPurchase   = require('../enrich/auto-purchase');
const sysLookupCache       = require('../system/_lookup_cache');
const sysCrossIntelOrch    = require('../system/cross-intel-orchestrator');
// Phase 48 — master lead list digest
const sysMasterLeadList    = require('../system/master-lead-list');
// Phase 50 — Spanish detector + smart cross-ref + CEI counters + error watchdog
const enrichSpanishDetector = require('../enrich/spanish-detector');
const enrichSmartCrossRef   = require('../enrich/smart-cross-ref');
const sysCeiCounters        = require('../system/cei-counters');
const sysErrorWatchdog      = require('../system/error-watchdog');
// Phase 50b — multi-language detector + multi-community ingest + Resend domain setup
const enrichMultilangDetector  = require('../enrich/multilang-detector');
const ingestMultiCommunityNews = require('../ingest/multicommunity-news');
const sysResendDomainSetup     = require('../system/resend-domain-setup');
// Phase 51 — embedding queue + cron staleness watchdog + schema drift
const sysEmbeddingQueue   = require('../system/embedding-queue');
const sysCronStaleness    = require('../system/cron-staleness');
const sysSchemaDriftCheck = require('../system/schema-drift-check');


// Map: job-name → { handler, defaultQuery }
const JOB_HANDLERS = {
  'tomtom':         { handler: ingestRun,         query: { secret: 'ingest-now' } },
  'waze':           { handler: ingestWaze,        query: { secret: 'ingest-now' } },
  'opendata':       { handler: ingestOpendata,    query: { secret: 'ingest-now' } },
  'scanner':        { handler: ingestScanner,     query: { secret: 'ingest-now' } },
  'news':           { handler: ingestNews,        query: { secret: 'ingest-now' } },
  'news-rss':       { handler: ingestNewsRss,     query: { secret: 'ingest-now' } },
  'reddit':         { handler: ingestReddit,      query: { secret: 'ingest-now' } },
  'pd-press':       { handler: ingestPdPress,     query: { secret: 'ingest-now' } },
  'police-social':  { handler: ingestPoliceSocial,query: { secret: 'ingest-now' } },
  'state-crash':    { handler: ingestStateCrash,  query: { secret: 'ingest-now' } },
  'court':          { handler: ingestCourt,       query: { secret: 'ingest-now' } },
  'obituaries':     { handler: ingestObituaries,  query: { secret: 'ingest-now' } },
  'trauma':         { handler: ingestTrauma,      query: { secret: 'ingest-now' } },
  'correlate':      { handler: ingestCorrelate,   query: { secret: 'ingest-now' } },
  'qualify':        { handler: sysQualify,        query: { secret: 'ingest-now' } },
  'notify':         { handler: sysNotify,         query: { secret: 'ingest-now' } },
  'auto-assign':    { handler: sysAutoAssign,     query: { secret: 'ingest-now' } },
  'enrich':         { handler: enrichRun,         query: { secret: 'enrich-now' } },
  'enrich-trigger': { handler: enrichTrigger,     query: { secret: 'ingest-now' } },
  'people-search':  { handler: enrichPeopleSearch,query: { secret: 'ingest-now' } },
  'address-to-residents':{ handler: enrichAddressToResidents, query: { secret: 'ingest-now', action: 'batch', limit: '15' } },
  'nominatim': { handler: enrichNominatim, query: { secret: 'ingest-now', action: 'batch', limit: '15' } },
  'fcc-carrier': { handler: enrichFccCarrier, query: { secret: 'ingest-now', action: 'batch', limit: '15' } },
  'dev-profiles': { handler: enrichDevProfiles, query: { secret: 'ingest-now', action: 'batch', limit: '12' } },
  'archive-search': { handler: enrichArchiveSearch, query: { secret: 'ingest-now', action: 'batch', limit: '8' } },
  'state-courts': { handler: enrichStateCourts, query: { secret: 'ingest-now', action: 'batch', limit: '8' } },
  'business-registry': { handler: enrichBusinessRegistry, query: { secret: 'ingest-now', action: 'batch', limit: '6' } },
  'usps-validate': { handler: enrichUspsValidate, query: { secret: 'ingest-now', action: 'batch', limit: '20' } },
  'reddit-history': { handler: enrichRedditHistory, query: { secret: 'ingest-now', action: 'batch', limit: '8' } },
  'co-residence': { handler: enrichCoResidence, query: { secret: 'ingest-now', action: 'batch', limit: '50' } },
  'name-rarity': { handler: enrichNameRarity, query: { secret: 'ingest-now', action: 'batch', limit: '30' } },
  'vehicle-owner': { handler: enrichVehicleOwner, query: { secret: 'ingest-now', action: 'batch', limit: '50' } },
  'temporal-corroborate': { handler: enrichTemporalCorroborate, query: { secret: 'ingest-now', action: 'batch', limit: '30' } },
  'confidence-decay': { handler: enrichConfidenceDecay, query: { secret: 'ingest-now', action: 'batch', limit: '50' } },
  'person-merge': { handler: enrichPersonMerge, query: { secret: 'ingest-now', action: 'batch', limit: '25' } },
  'census-income': { handler: enrichCensusIncome, query: { secret: 'ingest-now', action: 'batch', limit: '20' } },
  'ensemble-qualifier': { handler: enrichEnsembleQualifier, query: { secret: 'ingest-now', action: 'batch', limit: '50' } },
  'active-learning': { handler: enrichActiveLearning, query: { secret: 'ingest-now', action: 'enqueue', limit: '30' } },
  'contradiction-detector': { handler: enrichContradictionDetector, query: { secret: 'ingest-now', action: 'batch', limit: '30' } },
  'fl-county-courts': { handler: enrichFlCountyCourts, query: { secret: 'ingest-now', action: 'batch', limit: '8' } },
  'voter-rolls':       { handler: enrichVoterRolls,     query: { secret: 'ingest-now', action: 'stats' } },
  'pulsepoint': { handler: ingestPulsepoint, query: { secret: 'ingest-now' } },
  'salvage-listings': { handler: enrichSalvageListings, query: { secret: 'ingest-now', action: 'batch', limit: '12' } },
  'gofundme': { handler: ingestGoFundMe, query: { secret: 'ingest-now' } },
  'funeral-homes': { handler: ingestFuneralHomes, query: { secret: 'ingest-now' } },
  'insurance-doi': { handler: enrichInsuranceDOI, query: { secret: 'ingest-now', action: 'batch', limit: '25' } },
  'workers-comp': { handler: enrichWorkersComp, query: { secret: 'ingest-now', action: 'batch', limit: '30' } },
  'saved-alerts': { handler: sysSavedAlerts, query: { secret: 'ingest-now', action: 'evaluate' } },
  'crm-export': { handler: sysCrmExport, query: { secret: 'ingest-now', action: 'batch', limit: '20' } },
  'nextdoor': { handler: ingestNextdoor, query: { secret: 'ingest-now' } },
  'youtube-comments': { handler: enrichYoutubeComments, query: { secret: 'ingest-now' } },
  'lead-stale-recycler': { handler: enrichLeadStaleRecycler, query: { secret: 'ingest-now', hours: '24' } },
  'engagement-score': { handler: enrichEngagementScore, query: { secret: 'ingest-now', limit: '100' } },
  'fatal-family-tree': { handler: enrichFatalFamilyTree, query: { secret: 'ingest-now', limit: '5' } },
  'attorney-heatmap': { handler: enrichAttorneyHeatmap, query: { secret: 'ingest-now', days: '30' } },
  'fraud-filter': { handler: enrichFraudFilter, query: { secret: 'ingest-now', days: '90' } },
  'telegram-police': { handler: ingestTelegramPolice, query: { secret: 'ingest-now' } },
  'whois-llc': { handler: ingestWhoisLLC, query: { secret: 'ingest-now', action: 'batch', limit: '10' } },
  'hospital-rss': { handler: ingestHospitalRSS, query: { secret: 'ingest-now' } },
  'predictive-at-source': { handler: enrichPredictiveAtSource, query: { secret: 'ingest-now', action: 'batch', limit: '15' } },
  'refresh-mv': { handler: sysRefreshMv, query: { secret: 'ingest-now' } },
  'transcription-drain': { handler: sysTranscriptionQueue, query: { secret: 'ingest-now', action: 'drain', limit: '15' } },
  'vacuum-nightly': { handler: sysVacuumNightly, query: { secret: 'ingest-now' } },
  'cei-poll': { handler: sysCeiPoll, query: { secret: 'ingest-now' } },
  'fbi-ucr': { handler: ingestFbiUcr, query: { secret: 'ingest-now' } },
  'osha': { handler: ingestOsha, query: { secret: 'ingest-now' } },
  'fars': { handler: ingestFars, query: { secret: 'ingest-now' } },
  'citizen-probe': { handler: ingestCitizenProbe, query: { secret: 'ingest-now' } },
  'bidirectional-cascade': { handler: sysBidirectionalCascade, query: { secret: 'ingest-now', action: 'batch', limit: '30' } },
  'cascade':        { handler: sysCascade,        query: { secret: 'ingest-now' } },
  'audit':          { handler: sysAudit,          query: { secret: 'ingest-now', fix: 'true' } },
  'digest':         { handler: sysDigest,         query: { secret: 'ingest-now', post: 'true' } },
  'trestle-probe':  { handler: sysTrestleProbe, query: { secret: 'ingest-now' } },
  'errors-clean':   { handler: sysErrors,         query: { secret: 'ingest-now', action: 'clear', days: '14' } },
  'twilio-lookup':  { handler: enrichTwilio,      query: { secret: 'ingest-now', action: 'enrich_pending', limit: '25' } },
  'social-search':  { handler: enrichSocialSearch,query: { secret: 'ingest-now', action: 'batch', limit: '15' } },
  'cross-exam':     { handler: enrichCrossExam,   query: { secret: 'ingest-now', action: 'examine_all' } },
  'family-tree':    { handler: enrichFamilyTree,    query: { secret: 'ingest-now', action: 'process', limit: '20' } },
  'vehicle-history':{ handler: enrichVehicleHistory,query: { secret: 'ingest-now', action: 'process', limit: '20' } },
  'relatives-search':{ handler: enrichRelativesSearch,query: { secret: 'ingest-now', action: 'process', limit: '20' } },
  'tcpa-refresh':   { handler: enrichTcpaCheck,     query: { secret: 'ingest-now', action: 'refresh_list' } },
  'claude-reason':  { handler: claudeCrossReasoner, query: { secret: 'ingest-now', action: 'top', limit: '15' } },
  'smart-router':   { handler: enrichSmartRouter, query: { secret: 'ingest-now', action: 'batch', limit: '15' } },
  'backfill-nameless': { handler: sysBackfillNameless, query: { secret: 'ingest-now', limit: '20' } },
  'court-reverse-link':{ handler: enrichCourtReverseLink, query: { secret: 'ingest-now', limit: '15' } },
  'obit-backfill':     { handler: enrichObitBackfill,    query: { secret: 'ingest-now', limit: '12' } },
  'pdl-by-name':       { handler: enrichPdlByName,       query: { secret: 'ingest-now', action: 'batch', limit: '20' } },
  'pdl-identify':      { handler: enrichPdlIdentify,     query: { secret: 'ingest-now', action: 'batch', limit: '15' } },
  'identity-investigator': { handler: claudeIdentityInvestigator, query: { secret: 'ingest-now', action: 'batch', limit: '10' } },
  'constant-cross-loop':   { handler: sysConstantCrossLoop,       query: { secret: 'ingest-now', minutes: '30' } },
  'homegrown-rotation':    { handler: ingestHomegrownRotation,    query: { secret: 'ingest-now' } },
  'people-search-multi':   { handler: enrichPeopleSearchMulti,    query: { secret: 'ingest-now', action: 'batch', limit: '15' } },
  'apollo-cross-pollinate':{ handler: enrichApolloCrossPollinate, query: { secret: 'ingest-now', action: 'batch', limit: '20' } },
  'victim-verifier':       { handler: enrichVictimVerifier,        query: { secret: 'ingest-now', action: 'batch', limit: '20' } },
  'victim-resolver':       { handler: enrichVictimResolver,        query: { secret: 'ingest-now', action: 'batch', limit: '10' } },
  'victim-contact-finder': { handler: enrichVictimContactFinder,    query: { secret: 'ingest-now', action: 'batch', limit: '10' } },
  'homegrown-osint-miner': { handler: enrichHomegrownOsintMiner, query: { secret: 'ingest-now', action: 'batch', limit: '3' } },
  'deep-phone-research':   { handler: enrichDeepPhoneResearch,    query: { secret: 'ingest-now', action: 'batch', limit: '3' } },
  'evidence-cross-checker':{ handler: enrichEvidenceCrossChecker,  query: { secret: 'ingest-now', action: 'batch', limit: '30' } },
  'property-to-family':    { handler: enrichPropertyToFamily,     query: { secret: 'ingest-now', action: 'batch', limit: '15' } },
  'funeral-home-survivors':{ handler: enrichFuneralHomeSurvivors,query: { secret: 'ingest-now', action: 'batch', limit: '10' } },
  'ai-news-extractor':     { handler: enrichAiNewsExtractor,   query: { secret: 'ingest-now', action: 'batch', limit: '10' } },
  'ai-obituary-parser':    { handler: enrichAiObituaryParser,  query: { secret: 'ingest-now', action: 'batch', limit: '5' } },
  'ai-cross-source-merge': { handler: sysAiCrossSourceMerge,   query: { secret: 'ingest-now', action: 'batch', limit: '5' } },
  'brave-search':          { handler: enrichBraveSearch,         query: { secret: 'ingest-now', action: 'health' } },
  'free-osint-extras':     { handler: enrichFreeOsintExtras,     query: { secret: 'ingest-now', action: 'health' } },
  'best-lead-synthesizer': { handler: sysBestLeadSynthesizer,    query: { secret: 'ingest-now', action: 'batch', limit: '2' } },
  // Phase 44A
  'apollo-unlock':         { handler: enrichApolloUnlock,        query: { secret: 'ingest-now', action: 'batch', limit: '5' } },
  'voyage-health':         { handler: enrichVoyageRouter,        query: { secret: 'ingest-now', action: 'health' } },
  'realtime-victim-alerts':{ handler: sysRealtimeVictimAlerts,   query: { secret: 'ingest-now', action: 'scan' } },
  // Phase 44B
  'plate-ocr-vision':      { handler: enrichPlateOcrVision,     query: { secret: 'ingest-now', action: 'batch', limit: '5' } },
  'auto-purchase':         { handler: enrichAutoPurchase,       query: { secret: 'ingest-now', action: 'run', limit: '10' } },
  'cache-clean':           { handler: sysLookupCache,           query: { secret: 'ingest-now', action: 'clean' } },
  'cross-intel-plan':      { handler: sysCrossIntelOrch,        query: { secret: 'ingest-now', action: 'plan' } },
  // Phase 48
  'master-lead-list':      { handler: sysMasterLeadList,         query: { secret: 'ingest-now', action: 'cron-digest' } },
  // Phase 50 — multi-language + reasoning + telemetry + watchdog
  'spanish-detector':      { handler: enrichSpanishDetector,     query: { secret: 'ingest-now', action: 'batch', limit: '10' } },
  'smart-cross-ref':       { handler: enrichSmartCrossRef,       query: { secret: 'ingest-now', action: 'batch', limit: '5' } },
  'cei-counters':          { handler: sysCeiCounters,            query: { secret: 'ingest-now', action: 'summary' } },
  'error-watchdog':        { handler: sysErrorWatchdog,          query: { secret: 'ingest-now', action: 'scan', minutes: '10' } },
  // Phase 50b — 10-language detector + multi-community ingest + Resend domain status
  'multilang-detector':    { handler: enrichMultilangDetector,   query: { secret: 'ingest-now', action: 'batch', limit: '10' } },
  'multicommunity-news':   { handler: ingestMultiCommunityNews,  query: { secret: 'ingest-now', action: 'run' } },
  'resend-domain-status':  { handler: sysResendDomainSetup,      query: { secret: 'ingest-now', action: 'status' } },
  // Phase 51 — Wave 12 patterns: embedding-drain + cron-staleness watchdog + schema-drift
  'embedding-drain':         { handler: sysEmbeddingQueue,    query: { secret: 'ingest-now', action: 'drain', limit: '25' } },
  'cron-staleness-check':    { handler: sysCronStaleness,     query: { secret: 'ingest-now', action: 'scan', minutes: '60' } },
  'schema-drift-check':      { handler: sysSchemaDriftCheck,  query: { secret: 'ingest-now', action: 'check' } },
};

// Phase 44A: heavy-scraper jobs that benefit from sub-shard slicing.
const SHARDED_JOBS = new Set(['news-rss', 'pd-press', 'people-search-multi', 'homegrown-osint-miner']);


// Build a fake res object that captures status + body
function makeFakeRes() {
  const fakeRes = {
    _statusCode: 200,
    _body: null,
    _headers: {},
    setHeader(k, v) { this._headers[k] = v; return this; },
    status(code) { this._statusCode = code; return this; },
    json(obj) { this._body = obj; return this; },
    end() { return this; },
  };
  return fakeRes;
}

function callableOf(mod) {
  if (typeof mod === 'function') return mod;
  if (mod && typeof mod.default === 'function') return mod.default;
  if (mod && typeof mod.handler === 'function') return mod.handler;
  return null;
}

async function runJob(jobName, parentReq) {
  const startT = Date.now();
  const entry = JOB_HANDLERS[jobName];
  if (!entry) {
    return { job: jobName, status: 'unknown_job', latency_ms: 0 };
  }
  try {
    const fn = callableOf(entry.handler);
    if (!fn) return { job: jobName, status: 'fail', error: 'no callable export', latency_ms: 0 };

    const isSharded = SHARDED_JOBS.has(jobName);
    const fakeReq = {
      method: 'GET',
      query: { ...entry.query, ...(isSharded ? { _sharded: '1', shard_ms: '5000' } : {}) },
      headers: { ...(parentReq?.headers || {}), 'x-internal-cron': '1', ...(isSharded ? { 'x-shard-mode': '1' } : {}) },
      body: null,
      url: ''
    };
    const fakeRes = makeFakeRes();

    // Phase 25: per-job safety timeout. Default 25s; trivially-fast jobs get 8s.
    // Keeps a parallel batch under 30s so the 60s function envelope is safe.
    const FAST_JOBS = new Set(['notify','qualify','auto-assign','errors-clean']);
    const perJobTimeoutMs = FAST_JOBS.has(jobName) ? 8000 : 25000;
    await Promise.race([
      fn(fakeReq, fakeRes),
      new Promise((_, rej) => setTimeout(() => rej(new Error(`job timeout ${perJobTimeoutMs}ms`)), perJobTimeoutMs))
    ]);

    const latency = Date.now() - startT;
    const body = fakeRes._body;

    if (fakeRes._statusCode === 200 && body?.success) {
      const out = {
        job: jobName,
        status: 'pass',
        latency_ms: latency,
        message: body.message || 'OK',
        stats: extractStats(body)
      };
      if (isSharded) {
        out.sharded = true;
        if (body.shards) out.shards = body.shards;
        if (body.items_per_shard) out.items_per_shard = body.items_per_shard;
      }
      return out;
    }
    return {
      job: jobName,
      status: 'fail',
      latency_ms: latency,
      error: body?.error || `status ${fakeRes._statusCode}`
    };
  } catch (e) {
    const isTimeout = /job timeout/i.test(e.message || '');
    return {
      job: jobName,
      status: isTimeout ? 'timeout' : 'fail',
      latency_ms: Date.now() - startT,
      error: e.message
    };
  }
}

function extractStats(data) {
  const out = {};
  for (const k of [
    'inserted', 'corroborated', 'persons_added', 'incidents_created',
    'evaluated', 'promoted', 'enriched', 'matched', 'victims_extracted',
    'fields_filled', 'crashes_found', 'pds_polled', 'subs_polled',
    'accounts_polled', 'feeds_polled', 'candidates'
  ]) {
    if (data[k] !== undefined) out[k] = data[k];
  }
  return out;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();
  const secret = req.query?.secret || req.headers?.['x-cron-secret'];
  if (secret !== 'ingest-now' && secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  const db = getDb();

  const jobsParam = (req.query?.jobs || '').toString();
  const requested = jobsParam.split(',').map(s => s.trim()).filter(Boolean);
  if (requested.length === 0) {
    return res.status(400).json({
      error: 'no jobs requested',
      registry: Object.keys(JOB_HANDLERS)
    });
  }

  const startAll = Date.now();

  // Run all in parallel — IN-PROCESS, not HTTP
  const results = await Promise.all(requested.map(j => runJob(j, req)));

  const passed = results.filter(r => r.status === 'pass').length;
  const failed = results.filter(r => r.status === 'fail').length;
  const timedout = results.filter(r => r.status === 'timeout').length;

  if (failed > 0) {
    await reportError(db, 'cron-dispatch', null,
      `${failed}/${results.length} jobs failed`,
      { failed: results.filter(r => r.status === 'fail') });
  }
  if (timedout > 0) {
    await reportError(db, 'cron-dispatch', null,
      `${timedout}/${results.length} jobs timed out`,
      { timeouts: results.filter(r => r.status === 'timeout').map(r => r.job) });
  }

  res.json({
    success: true,
    summary: `${passed}/${results.length} jobs passed (${failed} failed, ${timedout} timed out)`,
    timedout_count: timedout,
    requested,
    results,
    total_latency_ms: Date.now() - startAll,
    timestamp: new Date().toISOString()
  });
};
// phase 39 deploy-bust 1777408432
