/**
 * AIP Master Router
 * Vercel only auto-includes files referenced via STATIC require().
 * So every handler must be require()d at the top, not dynamically.
 */

// ── Auth ──
const authLogin = require('../lib/v1/auth/login');
const authMe = require('../lib/v1/auth/me');

// ── Dashboard ──
const dashCounts = require('../lib/v1/dashboard/counts');
const dashAwaitingContact = require('../lib/v1/dashboard/awaiting-contact');
const dashFeed = require('../lib/v1/dashboard/feed');
const dashMetros = require('../lib/v1/dashboard/metro-areas');
const dashAssign = require('../lib/v1/dashboard/my-assignments');
const dashStats = require('../lib/v1/dashboard/stats');

// ── Incidents (static + dynamic) ──
const incidentsIndex = require('../lib/v1/incidents/index');
const incidentDetail = require('../lib/v1/incidents/[id]');
const incidentAssign = require('../lib/v1/incidents/[id]/assign');
const incidentNote = require('../lib/v1/incidents/[id]/note');

// ── Contacts / Integrations / Alerts / Migrate ──
const contacts = require('../lib/v1/contacts/index');
const integrations = require('../lib/v1/integrations/index');
const alertsNotif = require('../lib/v1/alerts/notifications');
const migrateColumns = require('../lib/v1/migrate/columns');

// ── Ingest ──
const ingestRun = require('../lib/v1/ingest/run');
const ingestWaze = require('../lib/v1/ingest/waze');
const ingestOpendata = require('../lib/v1/ingest/opendata');
const ingestScanner = require('../lib/v1/ingest/scanner');
const ingestNews = require('../lib/v1/ingest/news');
const ingestNewsRss = require('../lib/v1/ingest/news-rss');
const ingestStateCrash = require('../lib/v1/ingest/state-crash');
const ingestCourt = require('../lib/v1/ingest/court');
const ingestCorrelate = require('../lib/v1/ingest/correlate');
const ingestObituaries = require('../lib/v1/ingest/obituaries');
const ingestTrauma = require('../lib/v1/ingest/trauma');
const ingestReddit = require('../lib/v1/ingest/reddit');
const ingestPoliceSocial = require('../lib/v1/ingest/police-social');
const ingestPdPress = require('../lib/v1/ingest/pd-press');
const ingestCourtListener = require('../lib/v1/ingest/courtlistener');
const ingestFars = require('../lib/v1/ingest/fars');

// ── Enrich ──
const enrichRun = require('../lib/v1/enrich/run');
const enrichTrigger = require('../lib/v1/enrich/trigger');
const enrichPeopleSearch = require('../lib/v1/enrich/people-search');
const enrichTrestleTest = require('../lib/v1/enrich/trestle-test');
const enrichVoterRolls = require('../lib/v1/enrich/voter-rolls');
const enrichPropertyRecords = require('../lib/v1/enrich/property-records');
const enrichAddressToResidents = require('../lib/v1/enrich/address-to-residents');
const enrichNominatim = require('../lib/v1/enrich/nominatim-geocode');
const enrichFccCarrier = require('../lib/v1/enrich/fcc-carrier');
const enrichDevProfiles = require('../lib/v1/enrich/dev-profiles');
const enrichArchiveSearch = require('../lib/v1/enrich/archive-search');
const enrichStateCourts = require('../lib/v1/enrich/state-courts');
const enrichBusinessRegistry = require('../lib/v1/enrich/business-registry');
const enrichUspsValidate = require('../lib/v1/enrich/usps-validate');
const enrichGeocoder = require('../lib/v1/enrich/geocoder');
const enrichHunterDomain = require('../lib/v1/enrich/hunter-domain');
const sysMigrationRunner = require('../lib/v1/system/migration-runner');
const sysMeasurement = require('../lib/v1/system/measurement');
const sysDailyIntelEmail = require('../lib/v1/system/daily-intel-email');
const sysUniversalResolver = require('../lib/v1/system/universal-resolver');
const sysRelationshipDetector = require('../lib/v1/system/relationship-detector');
const sysTriangulationVerifier = require('../lib/v1/system/triangulation-verifier');
const sysPropertyRegistry = require('../lib/v1/system/property-registry');
const sysPropertyCoverage = require('../lib/v1/system/property-coverage');
const sysPropertyEvents = require('../lib/v1/system/property-events');
const sysPropertyRegistrySync = require('../lib/v1/system/property-registry-sync');
const sysFourPillarGate = require('../lib/v1/system/four-pillar-gate');
const sysPropertyEventsBackfill = require('../lib/v1/system/property-events-backfill');
const sysDeployGate = require('../lib/v1/system/deploy-gate');
const sysCallScriptGenerator = require('../lib/v1/system/call-script-generator');
const sysLeadQualityScorer = require('../lib/v1/system/lead-quality-scorer');
const enrichAddressSonnet = require('../lib/v1/enrich/address-sonnet-extractor');
const ingestNewSources = require('../lib/v1/ingest/new-sources');
const enrichTextExtractors = require('../lib/v1/enrich/text-extractors');
const enrichOpenCNAM = require('../lib/v1/enrich/opencnam');
const enrichHunterCampaign = require('../lib/v1/enrich/hunter-campaign');
const enrichWhitepagesScrape = require('../lib/v1/enrich/whitepages-scrape');
const sysAttorneyCrossLink = require('../lib/v1/system/attorney-cross-link');
const enrichRedditHistory = require('../lib/v1/enrich/reddit-history');
const enrichCoResidence = require('../lib/v1/enrich/co-residence');
const enrichNameRarity = require('../lib/v1/enrich/name-rarity');
const enrichVehicleOwner = require('../lib/v1/enrich/vehicle-owner');
const enrichTemporalCorroborate = require('../lib/v1/enrich/temporal-corroborate');
const enrichConfidenceDecay = require('../lib/v1/enrich/confidence-decay');
const enrichPersonMerge = require('../lib/v1/enrich/person-merge');
const enrichCensusIncome = require('../lib/v1/enrich/census-income');
const enrichEnsembleQualifier = require('../lib/v1/enrich/ensemble-qualifier');
const enrichActiveLearning = require('../lib/v1/enrich/active-learning');
const enrichContradictionDetector = require('../lib/v1/enrich/contradiction-detector');
const enrichFlCountyCourts = require('../lib/v1/enrich/fl-county-courts');
const enrichQpublic = require('../lib/v1/enrich/qpublic-property');
const ingestPulsepoint = require('../lib/v1/ingest/pulsepoint');
const enrichSalvageListings = require('../lib/v1/enrich/salvage-listings');
const ingestGoFundMe = require('../lib/v1/ingest/gofundme');
const ingestFuneralHomes = require('../lib/v1/ingest/funeral-homes');
const enrichInsuranceDOI = require('../lib/v1/enrich/insurance-doi');
const enrichWorkersComp = require('../lib/v1/enrich/workers-comp');
const sysSavedAlerts = require('../lib/v1/system/saved-alerts');
const sysCrmExport = require('../lib/v1/system/crm-export');
const ingestNextdoor = require('../lib/v1/ingest/nextdoor');
const enrichYoutubeComments = require('../lib/v1/enrich/youtube-comments');
const sysRealtimeFeed = require('../lib/v1/system/realtime-feed');
const sysAtomicClaim = require('../lib/v1/system/atomic-claim');
const enrichLeadStaleRecycler = require('../lib/v1/enrich/lead-stale-recycler');
const enrichEngagementScore = require('../lib/v1/enrich/engagement-score');
const enrichFatalFamilyTree = require('../lib/v1/enrich/fatal-family-tree');
const enrichAttorneyHeatmap = require('../lib/v1/enrich/attorney-heatmap');
const enrichFraudFilter = require('../lib/v1/enrich/fraud-filter');
const enrichSpanishExtraction = require('../lib/v1/enrich/spanish-extraction');
const enrichWhatsappOutreach = require('../lib/v1/enrich/whatsapp-outreach');
const ingestTelegramPolice = require('../lib/v1/ingest/telegram-police');
const ingestCitizenProbe = require('../lib/v1/ingest/citizen-probe');
const ingestWhoisLLC = require('../lib/v1/ingest/whois-llc');
const ingestHospitalRSS = require('../lib/v1/ingest/hospital-rss');
const enrichPredictiveAtSource = require('../lib/v1/enrich/predictive-at-source');
const sysAbSmsOptimizer = require('../lib/v1/system/ab-sms-optimizer');
const sysRepStats = require('../lib/v1/system/rep-stats');
const sysConfidenceTrail = require('../lib/v1/system/confidence-trail');
const sysPluginExport = require('../lib/v1/system/plugin-export');
const migratePerfViews = require('../lib/v1/migrate/perf-views');
const sysRefreshMv = require('../lib/v1/system/refresh-mv');
const sysTranscriptionQueue = require('../lib/v1/system/transcription-queue');
const sysKvCursor = require('../lib/v1/system/kv-cursor');
const sysSpatialCluster = require('../lib/v1/system/spatial-cluster');
const sysVacuumNightly = require('../lib/v1/system/vacuum-nightly');
const sysSentry = require('../lib/v1/system/sentry');
const sysSseStream = require('../lib/v1/system/sse-stream');
const sysCeiPoll = require('../lib/v1/system/cei-poll');
const ingestVoterStates = require('../lib/v1/ingest/voter-states');
const ingestOsha = require('../lib/v1/ingest/osha');
const ingestCdcWonder = require('../lib/v1/ingest/cdc-wonder');
const ingestFbiUcr = require('../lib/v1/ingest/fbi-ucr');
const sysBidirectionalCascade = require('../lib/v1/system/bidirectional-cascade');
const sysFeedFreshness = require('../lib/v1/system/feed-freshness');
const sysModelRegistry = require('../lib/v1/system/model-registry');
const enrichCrossExam = require('../lib/v1/enrich/cross-exam');
const enrichSocialSearch = require('../lib/v1/enrich/social-search');
const enrichFlVoterLoader = require('../lib/v1/enrich/fl-voter-loader');
const enrichCrossref = require('../lib/v1/enrich/crossref');
const enrichTwilio = require('../lib/v1/enrich/twilio');
const enrichFamilyTree = require('../lib/v1/enrich/family-tree');
const enrichVehicleHistory = require('../lib/v1/enrich/vehicle-history');
const enrichRelativesSearch = require('../lib/v1/enrich/relatives-search');
const enrichTcpaCheck = require('../lib/v1/enrich/tcpa-litigator-check');
const webhookTwilioSms = require('../lib/v1/webhooks/twilio-sms');

// ── System ──
const sysHealth = require('../lib/v1/system/health');
const sysPostgis = require('../lib/v1/system/postgis');
const sysQualify = require('../lib/v1/system/qualify');
const sysNotify = require('../lib/v1/system/notify');
const sysAutoAssign = require('../lib/v1/system/auto-assign');
const sysChangelog = require('../lib/v1/system/changelog');
const sysErrors = require('../lib/v1/system/errors');
const sysCost = require('../lib/v1/system/cost');
const sysCostDebug = require('../lib/v1/system/cost-debug');
const sysCascade = require('../lib/v1/system/cascade');
const sysSetup = require('../lib/v1/system/setup');
const sysTrestleProbe = require('../lib/v1/system/trestle-probe');
const sysDigest = require('../lib/v1/system/digest');
const sysSmoke = require('../lib/v1/system/smoke-test');
const sysTriggers = require('../lib/v1/system/triggers');
const sysAudit = require('../lib/v1/system/audit');
const sysResync = require('../lib/v1/system/resync');
const sysTestGpt = require('../lib/v1/system/test-gpt');
const sysBackfillNameless = require('../lib/v1/system/backfill-nameless');
const enrichCourtReverseLink = require('../lib/v1/enrich/court-reverse-link');
const enrichObitBackfill = require('../lib/v1/enrich/obit-backfill');
const claudeCrossReasoner = require('../lib/v1/enrich/claude-cross-reasoner');
const enrichSmartRouter = require('../lib/v1/enrich/_smart_router');
const enrichPdlByName = require('../lib/v1/enrich/pdl-by-name');
const enrichPdlIdentify = require('../lib/v1/enrich/pdl-identify');
const enrichPeopleSearchMulti = require('../lib/v1/enrich/people-search-multi');
const enrichApolloCrossPollinate = require('../lib/v1/enrich/apollo-cross-pollinate');
const enrichPropertyToFamily = require('../lib/v1/enrich/property-to-family');
const enrichGaVoterLoader = require('../lib/v1/enrich/ga-voter-loader');
const enrichTxVoterLoader = require('../lib/v1/enrich/tx-voter-loader');
const claudeIdentityInvestigator = require('../lib/v1/enrich/claude-identity-investigator');
const sysConstantCrossLoop = require('../lib/v1/system/_constant_cross_loop');
const ingestHomegrownRotation = require('../lib/v1/ingest/_homegrown_rotation');

// ── Cron ──
const enrichVictimVerifier = require('../lib/v1/enrich/victim-verifier');
const enrichVictimResolver = require('../lib/v1/enrich/victim-resolver');
const enrichEvidenceCrossChecker = require('../lib/v1/enrich/evidence-cross-checker');
const enrichVictimContactFinder = require('../lib/v1/enrich/victim-contact-finder');
const enrichHomegrownOsintMiner = require('../lib/v1/enrich/homegrown-osint-miner');
const enrichDeepPhoneResearch = require('../lib/v1/enrich/deep-phone-research');
const sysVoterRollsUpload = require('../lib/v1/system/voter-rolls-upload');
const enrichFuneralHomeSurvivors = require('../lib/v1/enrich/funeral-home-survivors');
const sysRunAllSources = require('../lib/v1/system/run-all-sources');
const enrichAiNewsExtractor = require('../lib/v1/enrich/ai-news-extractor');
const enrichAiObituaryParser = require('../lib/v1/enrich/ai-obituary-parser');
const sysAiCrossSourceMerge = require('../lib/v1/system/ai-cross-source-merge');
const dashRepCallBrief = require('../lib/v1/dashboard/rep-call-brief');
const dashRepQuickLinks = require('../lib/v1/dashboard/rep-quick-links');
const dashVictimLeads = require('../lib/v1/dashboard/victim-leads');
const sysRunContactFinder = require('../lib/v1/system/run-contact-finder');
const sysSmartVictimPipeline = require('../lib/v1/system/smart-victim-pipeline');
const sysResend = require('../lib/v1/system/resend');
const sysReverifyOfficerDenials = require('../lib/v1/system/reverify-officer-denials');
const sysPhase47Fixes = require('../lib/v1/system/phase47-fixes');
const sysCleanupFalsePositives46 = require('../lib/v1/system/cleanup-false-positives-46');
const sysCleanupBadContacts = require('../lib/v1/system/cleanup-bad-contacts');
const sysDemoteOrphanQualified = require('../lib/v1/system/demote-orphan-qualified');
const sysQuarantineFakeVictims = require('../lib/v1/system/quarantine-fake-victims');
const sysReExtractHistorical = require('../lib/v1/system/re-extract-historical');
const enrichBraveSearch = require('../lib/v1/enrich/brave-search');
const enrichFreeOsintExtras = require('../lib/v1/enrich/free-osint-extras');
const sysBestLeadSynthesizer = require('../lib/v1/system/best-lead-synthesizer');
// ── Phase 44A: Apollo unlock + smart batcher + realtime alerts + Deepgram + VoyageAI
const enrichApolloUnlock = require('../lib/v1/enrich/apollo-unlock');
const enrichVoyageRouter = require('../lib/v1/enrich/_voyage_router');
const sysRealtimeVictimAlerts = require('../lib/v1/system/realtime-victim-alerts');
const cronDispatch = require('../lib/v1/cron/dispatch');
// ── Phase 44B: plate OCR + admin overview + caller-id + auto-purchase + cross-intel orchestrator + lookup cache
const enrichPlateOcrVision     = require('../lib/v1/enrich/plate-ocr-vision');
const enrichAutoPurchase       = require('../lib/v1/enrich/auto-purchase');
const dashAdminOverview        = require('../lib/v1/dashboard/admin-overview');
const dashCallerId             = require('../lib/v1/dashboard/caller-id');
const sysLookupCache           = require('../lib/v1/system/_lookup_cache');
const sysCrossIntelOrchestrator= require('../lib/v1/system/cross-intel-orchestrator');
// ── Phase 48: Master lead list digest
const sysMasterLeadList        = require('../lib/v1/system/master-lead-list');
const sysAutoFanOut             = require('../lib/v1/system/auto-fan-out');
const sysStrategist             = require('../lib/v1/system/strategist');
// ── Phase 60: Adversarial cross-check (independent third-party validation)
const sysAdversarialCrossCheck  = require('../lib/v1/system/adversarial-cross-check');
const sysHypothesisGenerator    = require('../lib/v1/system/hypothesis-generator');
const sysPersonMergeFinder      = require('../lib/v1/system/person-merge-finder');
const sysPatternMiner           = require('../lib/v1/system/pattern-miner');
const sysAdvancedSweepAll       = require('../lib/v1/system/advanced-sweep-all');
const sysFamilyGraph            = require('../lib/v1/system/family-graph');
// ── Phase 50: Spanish detector + smart cross-ref + CEI counters + error watchdog
const enrichSpanishDetector    = require('../lib/v1/enrich/spanish-detector');
const enrichSmartCrossRef      = require('../lib/v1/enrich/smart-cross-ref');
const sysCeiCounters           = require('../lib/v1/system/cei-counters');
const sysErrorWatchdog         = require('../lib/v1/system/error-watchdog');
// ── Phase 50b: multi-language detector + multi-community ingest + Resend domain setup
const enrichMultilangDetector  = require('../lib/v1/enrich/multilang-detector');
const ingestMultiCommunityNews = require('../lib/v1/ingest/multicommunity-news');
const sysResendDomainSetup     = require('../lib/v1/system/resend-domain-setup');
// ── Phase 51: embedding queue + cron staleness watchdog + schema drift detector
const sysEmbeddingQueue        = require('../lib/v1/system/embedding-queue');
const sysCronStaleness         = require('../lib/v1/system/cron-staleness');
const sysSchemaDriftCheck      = require('../lib/v1/system/schema-drift-check');
// Phase 52: design tokens + endpoint descriptions (smart copy everywhere)
const sysDesignTokens          = require('../lib/v1/system/design-tokens');
const sysEndpointDescriptions  = require('../lib/v1/system/endpoint-descriptions');

const ROUTES = {
  'auth/login': authLogin,
  'auth/me': authMe,
  'dashboard/counts': dashCounts,
  'dashboard/awaiting-contact': dashAwaitingContact,
  'dashboard/feed': dashFeed,
  'dashboard/metro-areas': dashMetros,
  'dashboard/my-assignments': dashAssign,
  'dashboard/stats': dashStats,
  'incidents': incidentsIndex,
  'contacts': contacts,
  'integrations': integrations,
  'alerts/notifications': alertsNotif,
  'migrate/columns': migrateColumns,
  'ingest/run': ingestRun,
  'ingest/waze': ingestWaze,
  'ingest/opendata': ingestOpendata,
  'ingest/scanner': ingestScanner,
  'ingest/news': ingestNews,
  'ingest/news-rss': ingestNewsRss,
  'ingest/state-crash': ingestStateCrash,
  'ingest/court': ingestCourt,
  'ingest/courtlistener': ingestCourtListener,
  'ingest/correlate': ingestCorrelate,
  'ingest/obituaries': ingestObituaries,
  'ingest/trauma': ingestTrauma,
  'ingest/reddit': ingestReddit,
  'ingest/police-social': ingestPoliceSocial,
  'ingest/pd-press': ingestPdPress,
  'ingest/fars': ingestFars,
  'enrich/run': enrichRun,
  'enrich/trigger': enrichTrigger,
  'enrich/people-search': enrichPeopleSearch,
  'enrich/trestle-test': enrichTrestleTest,
  'enrich/voter-rolls': enrichVoterRolls,
  'enrich/property-records': enrichPropertyRecords,
  'enrich/address-to-residents': enrichAddressToResidents,
  'enrich/nominatim-geocode': enrichNominatim,
  'enrich/fcc-carrier': enrichFccCarrier,
  'enrich/dev-profiles': enrichDevProfiles,
  'enrich/archive-search': enrichArchiveSearch,
  'enrich/state-courts': enrichStateCourts,
  'enrich/business-registry': enrichBusinessRegistry,
  'enrich/usps-validate': enrichUspsValidate,
  'enrich/geocoder': enrichGeocoder,
  'enrich/hunter-domain': enrichHunterDomain,
  'system/migration-runner': sysMigrationRunner,
  'system/measurement': sysMeasurement,
  'system/daily-intel-email': sysDailyIntelEmail,
  'system/universal-resolver': sysUniversalResolver,
  'system/relationship-detector': sysRelationshipDetector,
  'system/triangulation-verifier': sysTriangulationVerifier,
  'system/property-registry': sysPropertyRegistry,
  'system/property-coverage': sysPropertyCoverage,
  'system/property-events': sysPropertyEvents,
  'system/property-registry-sync': sysPropertyRegistrySync,
  'system/four-pillar-gate': sysFourPillarGate,
  'system/property-events-backfill': sysPropertyEventsBackfill,
  'system/deploy-gate': sysDeployGate,
  'system/call-script-generator': sysCallScriptGenerator,
  'system/lead-quality-scorer': sysLeadQualityScorer,
  'enrich/address-sonnet-extractor': enrichAddressSonnet,
  'ingest/new-sources': ingestNewSources,
  'enrich/text-extractors': enrichTextExtractors,
  'enrich/opencnam': enrichOpenCNAM,
  'enrich/hunter-campaign': enrichHunterCampaign,
  'enrich/whitepages-scrape': enrichWhitepagesScrape,
  'system/attorney-cross-link': sysAttorneyCrossLink,
  'enrich/reddit-history': enrichRedditHistory,
  'enrich/co-residence': enrichCoResidence,
  'enrich/name-rarity': enrichNameRarity,
  'enrich/vehicle-owner': enrichVehicleOwner,
  'enrich/temporal-corroborate': enrichTemporalCorroborate,
  'enrich/confidence-decay': enrichConfidenceDecay,
  'enrich/person-merge': enrichPersonMerge,
  'enrich/census-income': enrichCensusIncome,
  'enrich/ensemble-qualifier': enrichEnsembleQualifier,
  'enrich/active-learning': enrichActiveLearning,
  'enrich/contradiction-detector': enrichContradictionDetector,
  'enrich/fl-county-courts': enrichFlCountyCourts,
  'enrich/qpublic-property': enrichQpublic,
  'ingest/pulsepoint': ingestPulsepoint,
  'enrich/salvage-listings': enrichSalvageListings,
  'ingest/gofundme': ingestGoFundMe,
  'ingest/funeral-homes': ingestFuneralHomes,
  'enrich/insurance-doi': enrichInsuranceDOI,
  'enrich/workers-comp': enrichWorkersComp,
  'system/saved-alerts': sysSavedAlerts,
  'system/crm-export': sysCrmExport,
  'ingest/nextdoor': ingestNextdoor,
  'enrich/youtube-comments': enrichYoutubeComments,
  'system/realtime-feed': sysRealtimeFeed,
  'system/atomic-claim': sysAtomicClaim,
  'enrich/lead-stale-recycler': enrichLeadStaleRecycler,
  'enrich/engagement-score': enrichEngagementScore,
  'enrich/fatal-family-tree': enrichFatalFamilyTree,
  'enrich/attorney-heatmap': enrichAttorneyHeatmap,
  'enrich/fraud-filter': enrichFraudFilter,
  'enrich/spanish-extraction': enrichSpanishExtraction,
  'enrich/whatsapp-outreach': enrichWhatsappOutreach,
  'ingest/telegram-police': ingestTelegramPolice,
  'ingest/citizen-probe': ingestCitizenProbe,
  'ingest/whois-llc': ingestWhoisLLC,
  'ingest/hospital-rss': ingestHospitalRSS,
  'enrich/predictive-at-source': enrichPredictiveAtSource,
  'system/ab-sms-optimizer': sysAbSmsOptimizer,
  'system/rep-stats': sysRepStats,
  'system/confidence-trail': sysConfidenceTrail,
  'system/plugin-export': sysPluginExport,
  'migrate/perf-views': migratePerfViews,
  'system/refresh-mv': sysRefreshMv,
  'system/transcription-queue': sysTranscriptionQueue,
  'system/kv-cursor': sysKvCursor,
  'system/spatial-cluster': sysSpatialCluster,
  'system/vacuum-nightly': sysVacuumNightly,
  'system/sentry': sysSentry,
  'system/sse-stream': sysSseStream,
  'system/cei-poll': sysCeiPoll,
  'ingest/voter-states': ingestVoterStates,
  'ingest/osha': ingestOsha,
  'ingest/cdc-wonder': ingestCdcWonder,
  'ingest/fbi-ucr': ingestFbiUcr,
  'system/bidirectional-cascade': sysBidirectionalCascade,
  'system/feed-freshness': sysFeedFreshness,
  'system/model-registry': sysModelRegistry,
  'enrich/cross-exam':      enrichCrossExam,
  'enrich/social-search':   enrichSocialSearch,
  'enrich/fl-voter-loader': enrichFlVoterLoader,
  'enrich/crossref': enrichCrossref,
  'enrich/twilio': enrichTwilio,
  'enrich/family-tree': enrichFamilyTree,
  'enrich/vehicle-history': enrichVehicleHistory,
  'enrich/relatives-search': enrichRelativesSearch,
  'enrich/tcpa-litigator-check': enrichTcpaCheck,
  'webhooks/twilio-sms': webhookTwilioSms,
  'system/health': sysHealth,
  'system/postgis': sysPostgis,
  'system/qualify': sysQualify,
  'system/notify': sysNotify,
  'system/auto-assign': sysAutoAssign,
  'system/changelog': sysChangelog,
  'system/errors': sysErrors,
  'system/cost': sysCost,
  'system/cost-debug': sysCostDebug,
  'system/cascade': sysCascade,
  'system/setup': sysSetup,
  'system/trestle-probe': sysTrestleProbe,
  'system/digest': sysDigest,
  'system/smoke-test': sysSmoke,
  'system/triggers': sysTriggers,
  'system/audit': sysAudit,
  'system/resync': sysResync,
  'system/test-gpt': sysTestGpt,
  'system/backfill-nameless': sysBackfillNameless,
  'enrich/court-reverse-link': enrichCourtReverseLink,
  'enrich/obit-backfill': enrichObitBackfill,
  'enrich/claude-cross-reasoner': claudeCrossReasoner,
  'enrich/smart-router': enrichSmartRouter,
  'enrich/pdl-by-name': enrichPdlByName,
  'enrich/pdl-identify': enrichPdlIdentify,
  'enrich/ga-voter-loader': enrichGaVoterLoader,
  'enrich/tx-voter-loader': enrichTxVoterLoader,
  'enrich/claude-identity-investigator': claudeIdentityInvestigator,
  'system/constant-cross-loop': sysConstantCrossLoop,
  'ingest/homegrown-rotation': ingestHomegrownRotation,
  'enrich/people-search-multi': enrichPeopleSearchMulti,
  'enrich/apollo-cross-pollinate': enrichApolloCrossPollinate,
  'enrich/property-to-family': enrichPropertyToFamily,
  'enrich/victim-verifier': enrichVictimVerifier,
  'enrich/victim-resolver': enrichVictimResolver,
  'enrich/evidence-cross-checker': enrichEvidenceCrossChecker,
  'enrich/victim-contact-finder': enrichVictimContactFinder,
  'enrich/homegrown-osint-miner': enrichHomegrownOsintMiner,
  'enrich/deep-phone-research': enrichDeepPhoneResearch,
  'dashboard/victim-leads': dashVictimLeads,
  'system/run-contact-finder': sysRunContactFinder,
  'system/smart-victim-pipeline': sysSmartVictimPipeline,
  'system/quarantine-fake-victims': sysQuarantineFakeVictims,
  'system/re-extract-historical': sysReExtractHistorical,
  'system/demote-orphan-qualified': sysDemoteOrphanQualified,
  'system/cleanup-bad-contacts': sysCleanupBadContacts,
  'system/cleanup-false-positives-46': sysCleanupFalsePositives46,
  'system/phase47-fixes': sysPhase47Fixes,
  'system/reverify-officer-denials': sysReverifyOfficerDenials,
  'system/resend': sysResend.handler,
  'system/voter-rolls-upload': sysVoterRollsUpload,
  'enrich/funeral-home-survivors': enrichFuneralHomeSurvivors,
  'system/run-all-sources': sysRunAllSources,
  'enrich/ai-news-extractor': enrichAiNewsExtractor,
  'enrich/ai-obituary-parser': enrichAiObituaryParser,
  'system/ai-cross-source-merge': sysAiCrossSourceMerge,
  'dashboard/rep-call-brief': dashRepCallBrief,
  'dashboard/rep-quick-links': dashRepQuickLinks,
  'enrich/brave-search': enrichBraveSearch,
  'enrich/free-osint-extras': enrichFreeOsintExtras,
  'system/best-lead-synthesizer': sysBestLeadSynthesizer,
  'cron/dispatch': cronDispatch,
  // Phase 44A
  'enrich/apollo-unlock': enrichApolloUnlock,
  'enrich/_voyage_router': enrichVoyageRouter,
  'enrich/voyage-router': enrichVoyageRouter,
  'system/realtime-victim-alerts': sysRealtimeVictimAlerts,
  // Phase 44B
  'enrich/plate-ocr-vision':     enrichPlateOcrVision,
  'enrich/auto-purchase':        enrichAutoPurchase,
  'dashboard/admin-overview':    dashAdminOverview,
  'dashboard/caller-id':         dashCallerId,
  'system/lookup-cache':         sysLookupCache,
  'system/cross-intel-orchestrator': sysCrossIntelOrchestrator,
  'system/master-lead-list': sysMasterLeadList,
  'system/auto-fan-out': sysAutoFanOut,
  'system/strategist': sysStrategist,
  'system/adversarial-cross-check': sysAdversarialCrossCheck,
  'system/hypothesis-generator': sysHypothesisGenerator,
  'system/person-merge-finder': sysPersonMergeFinder,
  'system/pattern-miner': sysPatternMiner,
  'system/advanced-sweep-all': sysAdvancedSweepAll,
  'system/family-graph': sysFamilyGraph,
  // Phase 50
  'enrich/spanish-detector':  enrichSpanishDetector,
  'enrich/smart-cross-ref':   enrichSmartCrossRef,
  'system/cei-counters':      sysCeiCounters,
  'system/error-watchdog':    sysErrorWatchdog,
  // Phase 50b
  'enrich/multilang-detector':    enrichMultilangDetector,
  'ingest/multicommunity-news':   ingestMultiCommunityNews,
  'system/resend-domain-setup':   sysResendDomainSetup,
  // Phase 51
  'system/embedding-queue':       sysEmbeddingQueue,
  'system/cron-staleness':        sysCronStaleness,
  'system/schema-drift-check':    sysSchemaDriftCheck,
  // Phase 52
  'system/design-tokens':         sysDesignTokens,
  'system/endpoint-descriptions': sysEndpointDescriptions,
};

function tryDynamicIncident(slug, req) {
  if (slug[0] !== 'incidents' || slug.length < 2) return null;
  req.query = req.query || {};
  req.query.id = slug[1];
  const action = slug[2];
  if (!action) return incidentDetail;
  if (action === 'assign') return incidentAssign;
  if (action === 'note') return incidentNote;
  return null;
}

function getCallable(mod) {
  if (typeof mod === 'function') return mod;
  if (mod && typeof mod.default === 'function') return mod.default;
  if (mod && typeof mod.handler === 'function') return mod.handler;
  return null;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, PATCH, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Cron-Secret');
  if (req.method === 'OPTIONS') return res.status(200).end();

  // Path comes from x-forwarded-uri (set by Vercel rewrite) or query param
  let slug = req.query?.slug || req.query?.path;
  if (!slug) {
    const fwd = req.headers['x-forwarded-uri'] || req.headers['x-original-uri'] || req.url || '';
    const path = fwd.split('?')[0];
    const m = path.match(/\/api\/v1\/(.+)$/);
    slug = m ? m[1] : '';
  }
  if (typeof slug === 'string') slug = slug.split('/');
  slug = (slug || []).filter(Boolean);

  if (slug.length === 0) {
    return res.status(200).json({
      ok: true, service: 'AIP', version: 1,
      routes: Object.keys(ROUTES).sort()
    });
  }

  const key = slug.join('/');
  try {
    if (ROUTES[key]) {
      const fn = getCallable(ROUTES[key]);
      if (!fn) return res.status(500).json({ error: `Handler ${key} has no callable export` });
      return fn(req, res);
    }
    const dyn = tryDynamicIncident(slug, req);
    if (dyn) {
      const fn = getCallable(dyn);
      return fn(req, res);
    }
    return res.status(404).json({ error: 'Not found', path: key });
  } catch (err) {
    console.error('Router error:', err);
    return res.status(500).json({ error: err.message, path: key });
  }
};
