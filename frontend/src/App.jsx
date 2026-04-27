import React, { useState, useEffect, useCallback, useRef } from "react";

const API = "/api/v1";

// ============================================================================
// ANIMATED STYLES (injected as <style> tag)
// ============================================================================
const GLOBAL_CSS = `
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&family=Orbitron:wght@700;900&display=swap');

:root {
  --bg-primary: #0b0f1a;
  --bg-secondary: #101729;
  --bg-card: #151d32;
  --bg-card-hover: #1b2540;
  --bg-input: #0d1225;
  --border: #1c2b4d;
  --text-primary: #f4f7ff;
  --text-secondary: #a0b0d0;
  --text-muted: #5e739e;
  --brand-blue: #4f6bff;
  --brand-purple: #a855f7;
  --brand-magenta: #e040fb;
  --brand-pink: #ff4da6;
  --brand-orange: #ff7b3a;
  --brand-yellow: #fbbf24;
  --brand-green: #34d399;
  --brand-cyan: #22d3ee;
  --brand-red: #ff4757;
  --brand-teal: #14b8a6;
}

body {
  background: linear-gradient(135deg, rgba(79,107,255,0.05) 10% 0%, rgba(168,85,247,0.05) 90% 100%, rgba(224,64,251,0.05) 50% 50%);
}

@keyframes gradientShift {
  0% { background-position: 0% 50%; }
  50% { background-position: 100% 50%; }
  100% { background-position: 0% 50%; }
}
@keyframes rainbowShift {
  0% { background-position: 0% 50%; }
  100% { background-position: 100% 50%; }
}
@keyframes pulseGlow {
  0%, 100% { box-shadow: 0 0 15px rgba(79,107,255,0.15); }
  50% { box-shadow: 0 0 30px rgba(79,107,255,0.35); }
}
@keyframes slideUp {
  from { opacity: 0; transform: translateY(20px); }
  to { opacity: 1; transform: translateY(0); }
}
@keyframes fadeIn {
  from { opacity: 0; }
  to { opacity: 1; }
}
@keyframes shimmer {
  0% { background-position: -200% 0; }
  100% { background-position: 200% 0; }
}
@keyframes borderGlow {
  0%, 100% { border-color: rgba(79,107,255,0.2); }
  50% { border-color: rgba(79,107,255,0.6); }
}
@keyframes countUp {
  from { opacity: 0; transform: scale(0.5); }
  to { opacity: 1; transform: scale(1); }
}
@keyframes liveIndicator {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.3; }
}
.kpi-card { transition: all 0.3s ease; }
.kpi-card:hover { transform: translateY(-4px); box-shadow: 0 12px 40px rgba(0,0,0,0.4) !important; }
.incident-row { transition: all 0.2s ease; }
.incident-row:hover { background: rgba(79,107,255,0.05) !important; transform: translateX(4px); }
.nav-link { transition: all 0.2s ease; position: relative; }
.nav-link:hover { color: #4f6bff !important; }
.nav-link.active::after { content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 3px; background: linear-gradient(90deg, #ff4757, #ff7b3a, #fbbf24, #34d399, #22d3ee, #4f6bff, #a855f7, #e040fb); border-radius: 1px; animation: rainbowShift 4s linear infinite; background-size: 200% 100%; }
.sidebar-item { transition: all 0.2s ease; }
.sidebar-item:focus { border-color: #4f6bff !important; box-shadow: 0 0 0 2px rgba(79,107,255,0.15); }
.btn-action { transition: all 0.2s ease; }
.btn-action:hover { transform: translateY(-1px); filter: brightness(1.15); }
.detail-panel { animation: slideInRight 0.3s ease; }
@keyframes slideInRight {
  from { opacity: 0; transform: translateX(40px); }
  to { opacity: 1; transform: translateX(0); }
}
.table-row { transition: all 0.15s ease; }
.table-row:hover { background: rgba(79,107,255,0.04) !important; }
::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: #0b0f1a; }
::-webkit-scrollbar-thumb { background: #1c2b4d; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: #2d4a7b; }
* { scrollbar-width: thin; scrollbar-color: #1c2b4d #0b0f1a; }
`;

// ============================================================================
// API HELPER
// ============================================================================
async function api(path, opts = {}) {
  const token = localStorage.getItem("aip_token");
  try {
    const res = await fetch(`${API}${path}`, {
      ...opts,
      headers: {
        "Content-Type": "application/json",
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
        ...opts.headers,
      },
      body: opts.body ? JSON.stringify(opts.body) : undefined,
    });
    if (res.status === 401) {
      // Drop token + force re-render to login screen — DO NOT reload (causes infinite loop)
      localStorage.removeItem("aip_token");
      // Trigger a global event so the App component can clear user state
      window.dispatchEvent(new CustomEvent('aip:auth-expired'));
      return { error: 'Unauthorized', data: [], _expired: true };
    }
    if (!res.ok) {
      const text = await res.text();
      try { return JSON.parse(text); } catch { return { error: `HTTP ${res.status}`, data: [] }; }
    }
    const text = await res.text();
    try { return JSON.parse(text); } catch { return { error: "Invalid response", data: [] }; }
  } catch (err) {
    console.error(`API error: ${path}`, err);
    return { error: err.message, data: [] };
  }
}

// ============================================================================
// MAIN APP
// ============================================================================
export default function App() {
  const [user, setUser] = useState(null);
  // Listen for token-expired events from api() helper — clears user without reload
  useEffect(() => {
    const handler = () => { setUser(null); };
    window.addEventListener('aip:auth-expired', handler);
    return () => window.removeEventListener('aip:auth-expired', handler);
  }, []);
  const [page, setPage] = useState("dashboard");
  const [incidents, setIncidents] = useState([]);
  const [stats, setStats] = useState(null);
  const [selectedIncident, setSelectedIncident] = useState(null);
  const [filters, setFilters] = useState({});
  const [metros, setMetros] = useState([]);
  const [notifications, setNotifs] = useState([]);
  const [loading, setLoading] = useState(false);
  const [contacts, setContacts] = useState([]);
  const [contactSummary, setContactSummary] = useState({});
  const [contactFilters, setContactFilters] = useState({});
  const [integrations, setIntegrations] = useState([]);
  const [integrationStats, setIntegrationStats] = useState({});
  const [enriching, setEnriching] = useState(false);
  const [systemHealth, setSystemHealth] = useState(null);
  const [recentErrors, setRecentErrors] = useState([]);
  const [changelogEntries, setChangelogEntries] = useState([]);
  const [feedView, setFeedView] = useState('qualified'); // 'qualified' | 'pending' | 'all'
  const [feedIncidents, setFeedIncidents] = useState([]);
  const [incidentsView, setIncidentsView] = useState('qualified'); // tab on Incidents page
  const [resyncing, setResyncing] = useState(false);
  const [resyncResult, setResyncResult] = useState(null);
  const [costData, setCostData] = useState(null);
  const loadDataInflightRef = useRef(false);

  // Inject global CSS
  useEffect(() => {
    const style = document.createElement("style");
    style.textContent = GLOBAL_CSS;
    document.head.appendChild(style);
    return () => document.head.removeChild(style);
  }, []);

  // Auth check
  useEffect(() => {
    const token = localStorage.getItem("aip_token");
    if (token) {
      api("/auth/me").then((d) => d.user && setUser(d.user));
    }
  }, []);

  // Load metro areas
  useEffect(() => {
    if (user) api("/dashboard/metro-areas").then((d) => setMetros(d.data || []));
  }, [user]);

  // Load feed by qualification state (tab filter)
  const loadFeed = useCallback(async (state = feedView) => {
    try {
      const d = await api(`/dashboard/feed?state=${state}&limit=50&minutes=10080`);
      if (Array.isArray(d.data)) setFeedIncidents(d.data);
    } catch (err) {
      console.warn('feed load failed', err);
    }
  }, [feedView]);

  useEffect(() => {
    loadFeed(feedView);
    const t = setInterval(() => loadFeed(feedView), 30000);
    return () => clearInterval(t);
  }, [feedView, loadFeed]);

  // Load pipeline health, errors, and changelog
  const loadSystemPanels = useCallback(async () => {
    try {
      const [h, e, c] = await Promise.all([
        api('/system/health'),
        api('/system/errors?limit=20&secret=ingest-now'),
        api('/system/changelog?limit=10&secret=ingest-now'),
      ]);
      if (h?.success || h?.status) setSystemHealth(h);
      if (Array.isArray(e?.errors) || Array.isArray(e?.rows)) setRecentErrors(e.errors || e.rows || []);
      if (Array.isArray(c?.entries) || Array.isArray(c?.rows)) setChangelogEntries(c.entries || c.rows || []);
    } catch (err) {
      console.warn('System panel load failed:', err);
    }
  }, []);

  useEffect(() => {
    loadSystemPanels();
    const t = setInterval(loadSystemPanels, 60000);
    return () => clearInterval(t);
  }, [loadSystemPanels]);

  // Load public dashboard stats (no auth required)
  const loadPublicStats = useCallback(async () => {
    setLoading(true);
    try {
      const resp = await fetch(`${API}/dashboard/counts`);
      if (resp.ok) {
        const data = await resp.json();
        if (data.success) {
          // Map counts endpoint data to the stats format the DashboardView expects
          const enrichment = data.enrichment || {};
          const fc = data.field_completeness || {};
          setStats({
            totals: {
              total_incidents: data.counts?.incidents || 0,
              new_incidents: data.counts?.persons ? Math.round((data.counts.persons - parseInt(enrichment.enriched || 0))) : 0,
              total_injuries: 0,
              total_fatalities: 0,
              high_severity_count: 0,
              active_reps: 0,
              total_persons: data.counts?.persons || 0,
              total_vehicles: data.counts?.vehicles || 0,
              avg_enrichment: parseFloat(enrichment.avg_score) || 0,
              enriched_count: parseInt(enrichment.enriched) || 0,
              cross_references: data.counts?.cross_references || 0,
            },
            byType: (data.source_breakdown || []).map(s => ({ incident_type: s.source, count: parseInt(s.count) })),
            byMetro: [],
            bySeverity: data.by_severity || [],
            recentHighPriority: (data.recent_incidents || []).slice(0, 5).map(inc => ({
              id: inc.id,
              incident_type: inc.incident_type || inc.source || 'unknown',
              severity: inc.severity || 'unknown',
              city: inc.city || '',
              state: inc.state || '',
              description: `Confidence: ${inc.confidence_score || 'N/A'}`,
              discovered_at: inc.timestamp,
            })),
            fieldCompleteness: fc,
            enrichmentHealth: data.pipeline_health || [],
            crossRefStats: data.cross_references || {},
          });
          setIncidents((data.recent_incidents || []).map(inc => ({
            id: inc.id,
            incident_type: inc.incident_type || inc.source || 'unknown',
            severity: inc.severity || 'unknown',
            city: inc.city || '',
            state: inc.state || '',
            description: `Source: ${inc.source || 'unknown'} | Confidence: ${inc.confidence_score || 'N/A'}`,
            discovered_at: inc.timestamp,
            confidence_score: inc.confidence_score,
          })));
        }
      }
    } catch (err) {
      console.error('Public stats error:', err);
    }
    setLoading(false);
  }, []);

  // Load data (authenticated mode)
  const loadData = useCallback(async () => {
    if (!user) return;
    if (loadDataInflightRef.current) return; // skip if previous still running
    if (typeof document !== 'undefined' && document.hidden) return; // skip while tab hidden
    loadDataInflightRef.current = true;
    setLoading(true);
    const params = new URLSearchParams(
      Object.entries(filters).filter(([, v]) => v)
    );
    if (incidentsView && incidentsView !== 'all') {
      params.set('qualification_state', incidentsView);
    }
    const [incData, statsData, notifData] = await Promise.all([
      api(`/incidents?${params}&limit=100`),
      api(`/dashboard/stats?period=${filters.period || "today"}&metro=${filters.metro || ""}`),
      api("/alerts/notifications?unreadOnly=true"),
    ]);
    setIncidents(incData.data || []);
    setStats(statsData);
    setNotifs(notifData.data || []);
    setLoading(false);
    loadDataInflightRef.current = false;
  }, [user, filters, incidentsView]);

  // Refresh Sync — re-runs all enrichment APIs against existing leads
  const handleResync = useCallback(async () => {
    setResyncing(true);
    setResyncResult(null);
    try {
      const d = await api('/system/resync?secret=ingest-now');
      setResyncResult(d);
      if (user) loadData(); else loadPublicStats();
      loadFeed(feedView);
    } catch (err) {
      setResyncResult({ success: false, error: err.message });
    }
    setResyncing(false);
  }, [user, loadData, loadPublicStats, loadFeed, feedView]);

  // Load on auth or fallback to public
  useEffect(() => {
    if (user) {
      loadData();
    } else {
      loadPublicStats();
    }
  }, [user, loadData, loadPublicStats]);

  // Auto-refresh every 30s
  useEffect(() => {
    const interval = setInterval(user ? loadData : loadPublicStats, 30000);
    return () => clearInterval(interval);
  }, [user, loadData, loadPublicStats]);

  // Load contacts
  const loadContacts = useCallback(async () => {
    if (!user) return;
    const params = new URLSearchParams(Object.entries(contactFilters).filter(([, v]) => v));
    const data = await api(`/contacts?${params}&limit=200`);
    setContacts(data.data || []);
    setContactSummary(data.summary || {});
  }, [user, contactFilters]);

  useEffect(() => { if (user && page === "contacts") loadContacts(); }, [page, user, loadContacts]);

  // Load integrations
  const loadIntegrations = useCallback(async () => {
    if (!user) return;
    const data = await api("/integrations");
    setIntegrations(data.integrations || []);
    setIntegrationStats(data.stats || {});
  }, [user]);

  const loadCost = useCallback(async () => {
    if (!user) return;
    try {
      const r = await fetch(`${API}/system/cost`);
      if (r.ok) {
        const d = await r.json();
        setCostData(d);
      }
    } catch (e) { /* ignore */ }
  }, [user]);

  useEffect(() => {
    if (user && page === 'cost') loadCost();
  }, [page, user, loadCost]);

  useEffect(() => {
    if (!user || page !== 'cost') return;
    const t = setInterval(loadCost, 60000);
    return () => clearInterval(t);
  }, [page, user, loadCost]);

  useEffect(() => { if (user && page === "integrations") loadIntegrations(); }, [page, user, loadIntegrations]);

  // Enrich person
  const enrichPerson = useCallback(async (personId) => {
    setEnriching(true);
    const data = await api("/enrich/run", { method: "POST", body: personId ? { person_id: personId } : { batch_size: 20 } });
    setEnriching(false);
    if (data.success) { loadContacts(); loadData(); }
    return data;
  }, [loadContacts, loadData]);

  // Integration actions
  const integrationAction = useCallback(async (id, action, apiKey) => {
    const body = { id, action };
    if (apiKey) body.api_key = apiKey;
    const data = await api("/integrations", { method: "POST", body });
    if (data.success) loadIntegrations();
    return data;
  }, [loadIntegrations]);

  if (!user) return <LoginScreen onLogin={setUser} />;

  return (
    <div style={{ minHeight: "100vh", background: "#0b0f1a", color: "#f4f7ff", fontFamily: "'Inter', system-ui, -apple-system, sans-serif" }}>
      <NavBar user={user} page={page} setPage={setPage} notifications={notifications} onLogout={() => { localStorage.removeItem("aip_token"); setUser(null); }} />

      <div style={{ display: "flex" }}>
        <Sidebar filters={filters} setFilters={setFilters} metros={metros} onRefresh={loadData} />
        <main style={{ flex: 1, padding: "24px 28px", overflow: "auto", maxHeight: "calc(100vh - 64px)" }}>
          {page === "dashboard" && <DashboardView stats={stats} incidents={incidents} onSelect={setSelectedIncident} loading={loading} systemHealth={systemHealth} recentErrors={recentErrors} changelogEntries={changelogEntries} feedView={feedView} setFeedView={setFeedView} feedIncidents={feedIncidents} />}
          {page === "incidents" && <IncidentList incidents={incidents} onSelect={setSelectedIncident} filters={filters} setFilters={setFilters} incidentsView={incidentsView} setIncidentsView={setIncidentsView} onResync={handleResync} resyncing={resyncing} resyncResult={resyncResult} />}
          {page === "my-leads" && <MyLeads user={user} onSelect={setSelectedIncident} />}
          {page === "cost" && <CostView costData={costData} onRefresh={loadCost} />}
          {page === "contacts" && <ContactsView contacts={contacts} summary={contactSummary} filters={contactFilters} setFilters={setContactFilters} onEnrich={enrichPerson} enriching={enriching} onRefresh={loadContacts} onSelect={setSelectedIncident} />}
          {page === "integrations" && <IntegrationsView integrations={integrations} stats={integrationStats} onAction={integrationAction} onRefresh={loadIntegrations} />}
        </main>
      </div>

      {selectedIncident && (
        <IncidentDetail incident={selectedIncident} onClose={() => setSelectedIncident(null)} user={user} onUpdate={loadData} />
      )}
    </div>
  );
}

// ============================================================================
// LOGIN SCREEN — Branded with animated gradient
// ============================================================================
function LoginScreen({ onLogin }) {
  const [email, setEmail] = useState("");
  const [pass, setPass] = useState("");
  const [error, setError] = useState("");

  const handleLogin = async (e) => {
    e.preventDefault();
    setError("");
    const data = await api("/auth/login", { method: "POST", body: { email, password: pass } });
    if (data.token) {
      localStorage.setItem("aip_token", data.token);
      onLogin(data.user);
    } else {
      setError(data.error || "Login failed");
    }
  };

  return (
    <div style={{
      minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center",
      background: "linear-gradient(-45deg, #0b0f1a, #101729, #151d32, #0d1225, #0b0f1a)",
      backgroundSize: "400% 400%", animation: "gradientShift 15s ease infinite"
    }}>
      <div style={{
        background: "rgba(20,24,41,0.85)", backdropFilter: "blur(20px)", borderRadius: 20, padding: 52, width: 440,
        boxShadow: "0 25px 80px rgba(0,0,0,0.6), 0 0 60px rgba(79,107,255,0.08)",
        border: "1px solid rgba(79,107,255,0.1)", animation: "slideUp 0.6s ease"
      }}>
        <div style={{ textAlign: "center", marginBottom: 36 }}>
          {/* Donovan Digital Solutions Brand */}
          <div style={{
            width: 64, height: 64, margin: "0 auto 16px", borderRadius: 16,
            background: "linear-gradient(135deg, #4f6bff, #a855f7)", display: "flex", alignItems: "center", justifyContent: "center",
            boxShadow: "0 8px 32px rgba(79,107,255,0.3)", animation: "pulseGlow 3s ease-in-out infinite"
          }}>
            <span style={{ fontSize: 32, filter: "drop-shadow(0 2px 4px rgba(0,0,0,0.3))" }}>&#x1F6A8;</span>
          </div>
          <h1 style={{ fontSize: 26, fontWeight: 800, color: "#fff", margin: 0, letterSpacing: "-0.5px" }}>INCIDENT COMMAND</h1>
          <p style={{ background: "linear-gradient(135deg, #4f6bff, #a855f7, #e040fb)", WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent", margin: "6px 0 0", fontSize: 12, fontWeight: 600, letterSpacing: "2px", textTransform: "uppercase" }}>Donovan Digital Solutions</p>
          <p style={{ color: "#a0b0d0", margin: "8px 0 0", fontSize: 13 }}>Accident Intelligence Platform</p>
        </div>
        <form onSubmit={handleLogin}>
          {error && <div style={{ background: "rgba(255,71,87,0.15)", border: "1px solid rgba(255,71,87,0.3)", color: "#ff7b9c", padding: "10px 14px", borderRadius: 10, marginBottom: 16, fontSize: 13 }}>{error}</div>}
          <input value={email} onChange={(e) => setEmail(e.target.value)} placeholder="Email" type="email" required style={inputStyle} className="sidebar-item" />
          <input value={pass} onChange={(e) => setPass(e.target.value)} placeholder="Password" type="password" required style={{ ...inputStyle, marginTop: 12 }} className="sidebar-item" />
          <button type="submit" style={{
            ...btnPrimary,
            background: "linear-gradient(135deg, #4f6bff, #a855f7)",
            borderRadius: 10, fontSize: 15, fontWeight: 700, letterSpacing: "0.5px"
          }} className="btn-action">Sign In</button>
        </form>
        <div style={{ textAlign: "center", marginTop: 24 }}>
          <a href="https://donovan-ai-growth-platform.vercel.app" target="_blank" rel="noopener noreferrer" style={{ color: "#a0b0d0", fontSize: 12, textDecoration: "none" }}>
            &larr; Back to Donovan AI Growth Platform
          </a>
        </div>
      </div>
    </div>
  );
}

// ============================================================================
// NAVIGATION BAR — Branded with gradient accent
// ============================================================================
function NavBar({ user, page, setPage, notifications, onLogout }) {
  return (
    <nav style={{
      height: 64, background: "rgba(20,24,41,0.95)", backdropFilter: "blur(12px)",
      borderBottom: "1px solid rgba(79,107,255,0.1)", display: "flex", alignItems: "center",
      justifyContent: "space-between", padding: "0 28px", position: "sticky", top: 0, zIndex: 100
    }}>
      {/* Top rainbow gradient accent line */}
      <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 4, background: "linear-gradient(90deg, #ff4757, #ff7b3a, #fbbf24, #34d399, #22d3ee, #4f6bff, #a855f7, #e040fb)", backgroundSize: "200% 100%", animation: "rainbowShift 4s linear infinite" }} />

      <div style={{ display: "flex", alignItems: "center", gap: 32 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{
            width: 32, height: 32, borderRadius: 8, background: "linear-gradient(135deg, #4f6bff, #a855f7)",
            display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14, fontWeight: 900, color: "#fff", fontFamily: "'Orbitron', monospace"
          }}>MD</div>
          <div>
            <div style={{ fontSize: 14, fontWeight: 900, color: "#fff", letterSpacing: "-0.3px", lineHeight: 1, fontFamily: "'Orbitron', monospace", background: "linear-gradient(135deg, #4f6bff, #a855f7, #e040fb)", WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent" }}>MD2020 INCIDENT</div>
            <div style={{ fontSize: 9, fontWeight: 600, color: "#a0b0d0", letterSpacing: "1.5px", textTransform: "uppercase" }}>Donovan Digital</div>
          </div>
        </div>
        <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
          {["dashboard", "incidents", "cost", "my-leads", "contacts", "integrations"].map((p) => (
            <button key={p} onClick={() => setPage(p)}
              className={`nav-link ${page === p ? "active" : ""}`}
              style={{
                background: page === p ? "rgba(79,107,255,0.1)" : "none", border: "none", cursor: "pointer",
                fontSize: 11, fontWeight: 600, letterSpacing: "0.3px", padding: "10px 12px", borderRadius: 8,
                color: page === p ? "#4f6bff" : "#a0b0d0", whiteSpace: "nowrap"
              }}>
              {p === "dashboard" && "\u25A3 "}
              {p === "incidents" && "\u26A0 "}
              {p === "my-leads" && "\u2605 "}
              {p === "contacts" && "\uD83D\uDCCB "}
              {p === "integrations" && "\u2699 "}
              {p === "cost" && "\uD83D\uDCB0 "}
              {p.replace("-", " ").toUpperCase()}
            </button>
          ))}
        </div>
      </div>

      <div style={{ display: "flex", alignItems: "center", gap: 20 }}>
        {/* Platform switcher */}
        <a href="https://donovan-ai-growth-platform.vercel.app" target="_blank" rel="noopener noreferrer"
          style={{ color: "#a0b0d0", fontSize: 11, textDecoration: "none", padding: "6px 12px", borderRadius: 6, border: "1px solid #1c2b4d", background: "linear-gradient(135deg, #4f6bff, #a855f7)", backgroundClip: "border-box", transition: "all 0.2s" }}
          className="btn-action">
          AI Growth Platform &rarr;
        </a>
        <span style={{ position: "relative", cursor: "pointer" }}>
          <span style={{ fontSize: 18 }}>&#x1F514;</span>
          {notifications.length > 0 && <span style={{
            position: "absolute", top: -6, right: -10, background: "linear-gradient(135deg, #ff4757, #ff7b3a)",
            color: "#fff", fontSize: 10, borderRadius: 10, padding: "1px 6px", fontWeight: 700, animation: "pulseGlow 2s ease-in-out infinite"
          }}>{notifications.length}</span>}
        </span>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{
            width: 32, height: 32, borderRadius: 8, background: "linear-gradient(135deg, #ff7b3a, #ff4da6)",
            display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14, color: "#fff", fontWeight: 700
          }}>{(user.firstName || "U")[0]}</div>
          <div>
            <div style={{ color: "#f4f7ff", fontSize: 13, fontWeight: 600 }}>{user.firstName} {user.lastName}</div>
            <div style={{ color: "#a0b0d0", fontSize: 10 }}>{user.role || "Agent"}</div>
          </div>
        </div>
        <button onClick={onLogout} className="btn-action" style={{ background: "rgba(100,116,139,0.1)", border: "1px solid #1c2b4d", color: "#a0b0d0", cursor: "pointer", fontSize: 12, padding: "6px 12px", borderRadius: 6 }}>Logout</button>
      </div>
    </nav>
  );
}

// ============================================================================
// SIDEBAR FILTERS — Enhanced with glow effects
// ============================================================================
function Sidebar({ filters, setFilters, metros, onRefresh }) {
  const update = (key, val) => setFilters((f) => ({ ...f, [key]: val }));

  return (
    <aside style={{
      width: 250, background: "rgba(20,24,41,0.6)", backdropFilter: "blur(8px)",
      borderRight: "1px solid rgba(79,107,255,0.1)", padding: "24px 18px", minHeight: "calc(100vh - 64px)"
    }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 20 }}>
        <h3 style={{ color: "#4f6bff", fontSize: 11, textTransform: "uppercase", letterSpacing: "2px", margin: 0, fontWeight: 700 }}>Filters</h3>
        <div style={{ width: 6, height: 6, borderRadius: 3, background: "#34d399", animation: "liveIndicator 2s ease-in-out infinite" }} />
      </div>

      <label style={labelStyle}>Time Period</label>
      <select value={filters.period || "today"} onChange={(e) => update("period", e.target.value)} style={selectStyle} className="sidebar-item">
        <option value="today">Today</option>
        <option value="week">Last 7 Days</option>
        <option value="month">Last 30 Days</option>
      </select>

      <label style={labelStyle}>Metro Area</label>
      <select value={filters.metro || ""} onChange={(e) => update("metro", e.target.value)} style={selectStyle} className="sidebar-item">
        <option value="">All Metros</option>
        {metros.map((m) => <option key={m.id} value={m.id}>{m.name}, {m.state}</option>)}
      </select>

      <label style={labelStyle}>Incident Type</label>
      <select value={filters.type || ""} onChange={(e) => update("type", e.target.value)} style={selectStyle} className="sidebar-item">
        <option value="">All Types</option>
        <option value="car_accident">Car Accident</option>
        <option value="motorcycle_accident">Motorcycle</option>
        <option value="truck_accident">Truck/Commercial</option>
        <option value="work_accident">Work Injury</option>
        <option value="pedestrian">Pedestrian</option>
        <option value="bicycle">Bicycle</option>
        <option value="slip_fall">Slip &amp; Fall</option>
      </select>

      <label style={labelStyle}>Severity</label>
      <select value={filters.severity || ""} onChange={(e) => update("severity", e.target.value)} style={selectStyle} className="sidebar-item">
        <option value="">All</option>
        <option value="fatal">Fatal</option>
        <option value="critical">Critical</option>
        <option value="serious">Serious</option>
        <option value="moderate">Moderate</option>
        <option value="minor">Minor</option>
      </select>

      <label style={labelStyle}>Status</label>
      <select value={filters.status || ""} onChange={(e) => update("status", e.target.value)} style={selectStyle} className="sidebar-item">
        <option value="">All</option>
        <option value="new">New</option>
        <option value="verified">Verified</option>
        <option value="assigned">Assigned</option>
        <option value="contacted">Contacted</option>
      </select>

      <div style={{ marginTop: 16, display: "flex", flexDirection: "column", gap: 8 }}>
        <label style={{ display: "flex", alignItems: "center", gap: 8, color: "#a0b0d0", fontSize: 13, cursor: "pointer" }}>
          <input type="checkbox" checked={!!filters.has_contact_info} onChange={(e) => update("has_contact_info", e.target.checked ? "true" : "")} style={{ accentColor: "#34d399" }} />
          Has Contact Info
        </label>
        <label style={{ display: "flex", alignItems: "center", gap: 8, color: "#a0b0d0", fontSize: 13, cursor: "pointer" }}>
          <input type="checkbox" checked={filters.hasAttorney === "false"} onChange={(e) => update("hasAttorney", e.target.checked ? "false" : "")} style={{ accentColor: "#4f6bff" }} />
          No Attorney
        </label>
      </div>

      <button onClick={onRefresh} className="btn-action" style={{
        width: "100%", background: "linear-gradient(135deg, #4f6bff, #a855f7)", color: "#0b0f1a",
        border: "none", borderRadius: 10, padding: "10px 12px", fontWeight: 700, fontSize: 13,
        cursor: "pointer", marginTop: 20, letterSpacing: "0.5px"
      }}>&#x21BB; Refresh Data</button>

      {/* Branding footer */}
      <div style={{ marginTop: "auto", paddingTop: 32, textAlign: "center" }}>
        <div style={{ width: "100%", height: 1, background: "linear-gradient(90deg, transparent, #1c2b4d, transparent)", marginBottom: 16 }} />
        <div style={{ fontSize: 9, color: "#5e739e", letterSpacing: "1px", textTransform: "uppercase" }}>Powered by</div>
        <div style={{ fontSize: 11, color: "#a0b0d0", fontWeight: 600, marginTop: 2 }}>Donovan Digital Solutions</div>
      </div>
    </aside>
  );
}

// ============================================================================
// DASHBOARD VIEW — Color-changing KPIs, animated cards
// ============================================================================
function DashboardView({ stats, incidents, onSelect, loading, systemHealth, recentErrors, changelogEntries, feedView, setFeedView, feedIncidents }) {
  const [time, setTime] = useState(Date.now());
  useEffect(() => {
    const t = setInterval(() => setTime(Date.now()), 3000);
    return () => clearInterval(t);
  }, []);

  if (!stats) return (
    <div style={{ textAlign: "center", padding: 80 }}>
      <div style={{ width: 48, height: 48, border: "3px solid #1c2b4d", borderTopColor: "#4f6bff", borderRadius: "50%", margin: "0 auto 16px", animation: "gradientShift 1s linear infinite" }} />
      <div style={{ color: "#a0b0d0", fontSize: 14 }}>Loading dashboard...</div>
    </div>
  );
  const t = stats.totals || {};

  // Color cycling for KPI cards
  const hueShift = (time / 50) % 360;
  const kpiConfigs = [
    { label: "Total Incidents", value: t.total_incidents || 0, gradient: "linear-gradient(135deg, #4f6bff, #a855f7, #e040fb)", icon: "\u25A3" },
    { label: t.total_persons ? "Persons Tracked" : "New (Unassigned)", value: t.total_persons || t.new_incidents || 0, gradient: "linear-gradient(135deg, #22d3ee, #4f6bff, #a855f7)", icon: "\u2605" },
    { label: t.total_vehicles ? "Vehicles" : "Total Injuries", value: t.total_vehicles || t.total_injuries || 0, gradient: "linear-gradient(135deg, #ff4757, #ff7b3a, #fbbf24)", icon: "\u2764" },
    { label: t.avg_enrichment ? "Avg Enrichment" : "Fatalities", value: t.avg_enrichment ? `${t.avg_enrichment}%` : (t.total_fatalities || 0), gradient: "linear-gradient(135deg, #34d399, #22d3ee)", icon: "\u26A0" },
    { label: t.enriched_count ? "Enriched" : "High Severity", value: t.enriched_count || t.high_severity_count || 0, gradient: "linear-gradient(135deg, #ff7b3a, #ff4da6, #a855f7)", icon: "\u26A1" },
    { label: t.cross_references !== undefined ? "Cross-Refs" : "Active Reps", value: t.cross_references !== undefined ? t.cross_references : (t.active_reps || 0), gradient: "linear-gradient(135deg, #14b8a6, #22d3ee, #4f6bff)", icon: "\u263A" },
  ];

  return (
    <div style={{ animation: "fadeIn 0.4s ease" }}>
      {/* Dashboard Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24 }}>
        <div>
          <h1 style={{ color: "#f4f7ff", margin: 0, fontSize: 24, fontWeight: 800, letterSpacing: "-0.5px" }}>
            Command Dashboard
          </h1>
          <p style={{ color: "#a0b0d0", margin: "4px 0 0", fontSize: 13 }}>
            Real-time accident intelligence overview
          </p>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <div style={{ width: 8, height: 8, borderRadius: 4, background: "#34d399", animation: "liveIndicator 2s ease-in-out infinite" }} />
          <span style={{ color: "#34d399", fontSize: 12, fontWeight: 600 }}>LIVE</span>
          {loading && <span style={{ color: "#4f6bff", fontSize: 12 }}>&middot; Syncing...</span>}
        </div>
      </div>

      {/* KPI Cards — Animated gradient borders */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 16, marginBottom: 28 }}>
        {kpiConfigs.map((kpi, idx) => (
          <div key={kpi.label} className="kpi-card" style={{
            background: "#151d32", borderRadius: 14, padding: "22px 20px",
            border: "1px solid #1c2b4d", position: "relative", overflow: "hidden",
            animation: `slideUp ${0.3 + idx * 0.1}s ease`
          }}>
            {/* Subtle gradient top accent */}
            <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 3, background: kpi.gradient, opacity: 0.8 }} />
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
              <div>
                <div style={{ color: "#a0b0d0", fontSize: 11, textTransform: "uppercase", letterSpacing: "1px", fontWeight: 600 }}>{kpi.label}</div>
                <div style={{
                  background: kpi.gradient, WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent",
                  fontSize: 36, fontWeight: 800, marginTop: 6, lineHeight: 1, animation: "countUp 0.6s ease"
                }}>{kpi.value}</div>
              </div>
              <div style={{ fontSize: 20, opacity: 0.3 }}>{kpi.icon}</div>
            </div>
          </div>
        ))}
      </div>

      {/* Metro & Type breakdown */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 24 }}>
        <div style={enhancedCardStyle}>
          <h3 style={enhancedCardTitle}>
            <span style={{ marginRight: 8 }}>&#x1F3D9;</span>Incidents by Metro
          </h3>
          {(stats.byMetro || []).map((m, i) => (
            <div key={m.metro} style={{
              display: "flex", justifyContent: "space-between", alignItems: "center",
              padding: "10px 0", borderBottom: "1px solid rgba(28,43,77,0.4)"
            }}>
              <span style={{ color: "#f4f7ff", fontSize: 13 }}>{m.metro || "Unknown"}</span>
              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <div style={{ width: 60, height: 4, background: "#1c2b4d", borderRadius: 2, overflow: "hidden" }}>
                  <div style={{
                    width: `${Math.min(100, (m.count / Math.max(...(stats.byMetro || []).map(x => x.count), 1)) * 100)}%`,
                    height: "100%", background: "linear-gradient(90deg, #ff7b3a, #ff4da6)", borderRadius: 2
                  }} />
                </div>
                <span style={{ color: "#ff7b3a", fontWeight: 700, fontSize: 14, minWidth: 24, textAlign: "right" }}>{m.count}</span>
              </div>
            </div>
          ))}
          {(!stats.byMetro || stats.byMetro.length === 0) && <EmptyState text="No metro data" />}
        </div>
        <div style={enhancedCardStyle}>
          <h3 style={enhancedCardTitle}>
            <span style={{ marginRight: 8 }}>&#x1F4CA;</span>By Type
          </h3>
          {(stats.byType || []).map((t) => (
            <div key={t.incident_type} style={{
              display: "flex", justifyContent: "space-between", alignItems: "center",
              padding: "10px 0", borderBottom: "1px solid rgba(28,43,77,0.4)"
            }}>
              <span style={{ color: "#f4f7ff", fontSize: 13 }}>{formatType(t.incident_type)}</span>
              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <div style={{ width: 60, height: 4, background: "#1c2b4d", borderRadius: 2, overflow: "hidden" }}>
                  <div style={{
                    width: `${Math.min(100, (t.count / Math.max(...(stats.byType || []).map(x => x.count), 1)) * 100)}%`,
                    height: "100%", background: "linear-gradient(90deg, #4f6bff, #a855f7)", borderRadius: 2
                  }} />
                </div>
                <span style={{ color: "#4f6bff", fontWeight: 700, fontSize: 14, minWidth: 24, textAlign: "right" }}>{t.count}</span>
              </div>
            </div>
          ))}
          {(!stats.byType || stats.byType.length === 0) && <EmptyState text="No type data" />}
        </div>
      </div>

      {/* Enrichment & Data Quality Panel */}
      {stats.fieldCompleteness && (
        <div style={{ ...enhancedCardStyle, marginBottom: 16, borderColor: "rgba(52,211,153,0.15)" }}>
          <h3 style={enhancedCardTitle}>
            <span style={{ marginRight: 8 }}>&#x1F9EA;</span>Data Quality & Enrichment
          </h3>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))", gap: 12 }}>
            {[
              { label: "Has Phone", val: stats.fieldCompleteness.has_phone, total: stats.fieldCompleteness.total, color: "#4f6bff" },
              { label: "Has Email", val: stats.fieldCompleteness.has_email, total: stats.fieldCompleteness.total, color: "#a855f7" },
              { label: "Has Address", val: stats.fieldCompleteness.has_address, total: stats.fieldCompleteness.total, color: "#22d3ee" },
              { label: "Has Employer", val: stats.fieldCompleteness.has_employer, total: stats.fieldCompleteness.total, color: "#ff7b3a" },
              { label: "Has Insurance", val: stats.fieldCompleteness.has_insurance, total: stats.fieldCompleteness.total, color: "#34d399" },
              { label: "Has Attorney", val: stats.fieldCompleteness.has_attorney, total: stats.fieldCompleteness.total, color: "#ff4da6" },
              { label: "Litigator", val: stats.fieldCompleteness.is_litigator, total: stats.fieldCompleteness.total, color: "#fbbf24" },
              { label: "Property Owner", val: stats.fieldCompleteness.is_property_owner, total: stats.fieldCompleteness.total, color: "#14b8a6" },
            ].map(item => {
              const pct = item.total > 0 ? Math.round((parseInt(item.val || 0) / parseInt(item.total)) * 100) : 0;
              return (
                <div key={item.label} style={{ background: "#0d1424", borderRadius: 10, padding: "12px 14px" }}>
                  <div style={{ color: "#a0b0d0", fontSize: 10, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 6 }}>{item.label}</div>
                  <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                    <div style={{ flex: 1, height: 4, background: "#1c2b4d", borderRadius: 2, overflow: "hidden" }}>
                      <div style={{ width: `${pct}%`, height: "100%", background: item.color, borderRadius: 2, transition: "width 0.6s ease" }} />
                    </div>
                    <span style={{ color: item.color, fontWeight: 700, fontSize: 12, minWidth: 32, textAlign: "right" }}>{pct}%</span>
                  </div>
                  <div style={{ color: "#a0b0d0", fontSize: 10, marginTop: 4 }}>{parseInt(item.val || 0)} / {parseInt(item.total || 0)}</div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Pipeline Health Panel */}
      {systemHealth && (
        <div style={{ ...enhancedCardStyle, marginBottom: 16, borderColor: "rgba(79,107,255,0.2)" }}>
          <h3 style={enhancedCardTitle}>
            <span style={{ marginRight: 8 }}>&#x1F4E1;</span>Pipeline Health
            <span style={{ fontSize: 11, color: "#a0b0d0", fontWeight: 400, marginLeft: 8 }}>
              ({systemHealth.counts?.active_sources || 0} sources / {systemHealth.counts?.incidents_24h || 0} incidents 24h / {systemHealth.counts?.errors_24h || 0} errors)
            </span>
          </h3>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 8 }}>
            {(systemHealth.pipelines || []).map(p => {
              const errCount = parseInt(p.errors_24h) || 0;
              const color = errCount === 0 ? "#34d399" : errCount < 5 ? "#fbbf24" : "#ff4757";
              return (
                <div key={p.name} style={{ background: "#0d1424", borderRadius: 8, padding: "10px 12px", borderLeft: `3px solid ${color}` }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                    <div style={{ width: 6, height: 6, borderRadius: 3, background: color }} />
                    <span style={{ color: "#f4f7ff", fontSize: 12, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.5px" }}>{p.name}</span>
                  </div>
                  <div style={{ color: "#a0b0d0", fontSize: 10, marginTop: 4 }}>cron: {p.cron}</div>
                  <div style={{ color: errCount > 0 ? "#ff4757" : "#34d399", fontSize: 11, fontWeight: 600, marginTop: 2 }}>
                    {errCount} errors / 24h
                  </div>
                </div>
              );
            })}
          </div>
          {systemHealth.database && (
            <div style={{ marginTop: 12, padding: "8px 12px", background: "rgba(52,211,153,0.06)", borderRadius: 8, fontSize: 11, color: "#a0b0d0" }}>
              <strong style={{ color: "#34d399" }}>Database:</strong> PostGIS {systemHealth.database.postgis ? "ENABLED" : "MISSING"} | geom column {systemHealth.database.geom_column ? "OK" : "MISSING"}
            </div>
          )}
        </div>
      )}

      {/* Source Breakdown Panel */}
      {systemHealth?.source_breakdown_24h && systemHealth.source_breakdown_24h.length > 0 && (
        <div style={{ ...enhancedCardStyle, marginBottom: 16 }}>
          <h3 style={enhancedCardTitle}>
            <span style={{ marginRight: 8 }}>&#x1F4CA;</span>Sources Contributing (last 24h)
          </h3>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: 10 }}>
            {systemHealth.source_breakdown_24h.map(s => {
              const max = Math.max(...systemHealth.source_breakdown_24h.map(x => parseInt(x.count) || 0), 1);
              const pct = (parseInt(s.count) / max) * 100;
              const color = sourceTypeColor(s.source_type);
              return (
                <div key={s.source_type} style={{ background: "#0d1424", borderRadius: 8, padding: "10px 12px" }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
                    <span style={{ color: "#f4f7ff", fontSize: 11, fontWeight: 600 }}>{s.source_type}</span>
                    <span style={{ color, fontWeight: 700, fontSize: 13 }}>{s.count}</span>
                  </div>
                  <div style={{ width: "100%", height: 4, background: "#1c2b4d", borderRadius: 2, overflow: "hidden" }}>
                    <div style={{ width: `${pct}%`, height: "100%", background: color, borderRadius: 2 }} />
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Recent Errors Panel */}
      {recentErrors && recentErrors.length > 0 && (
        <div style={{ ...enhancedCardStyle, marginBottom: 16, borderColor: "rgba(255,71,87,0.2)" }}>
          <h3 style={enhancedCardTitle}>
            <span style={{ marginRight: 8 }}>&#x26A0;&#xFE0F;</span>Recent Pipeline Errors
            <span style={{ fontSize: 11, color: "#a0b0d0", fontWeight: 400, marginLeft: 8 }}>(last 20)</span>
          </h3>
          <div style={{ maxHeight: 200, overflowY: "auto" }}>
            {recentErrors.slice(0, 10).map(e => (
              <div key={e.id} style={{ padding: "8px 10px", borderBottom: "1px solid rgba(28,43,77,0.4)", fontSize: 12 }}>
                <div style={{ display: "flex", justifyContent: "space-between" }}>
                  <span style={{ color: "#ff7b3a", fontWeight: 600 }}>{e.pipeline}{e.source ? ` / ${e.source}` : ""}</span>
                  <span style={{ color: "#a0b0d0", fontSize: 10 }}>{formatTime(e.created_at)}</span>
                </div>
                <div style={{ color: "#a0b0d0", marginTop: 2 }}>{(e.message || "").substring(0, 200)}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Changelog Panel */}
      {changelogEntries && changelogEntries.length > 0 && (
        <div style={{ ...enhancedCardStyle, marginBottom: 16 }}>
          <h3 style={enhancedCardTitle}>
            <span style={{ marginRight: 8 }}>&#x1F4DD;</span>Update Log
          </h3>
          {changelogEntries.slice(0, 5).map(c => (
            <div key={c.id} style={{ padding: "8px 10px", borderBottom: "1px solid rgba(28,43,77,0.4)" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <span style={{ background: changelogKindColor(c.kind), color: "#0b0f1a", fontSize: 9, fontWeight: 700, textTransform: "uppercase", padding: "2px 8px", borderRadius: 10 }}>{c.kind}</span>
                <span style={{ color: "#a0b0d0", fontSize: 10 }}>{formatTime(c.created_at)}</span>
              </div>
              <div style={{ color: "#f4f7ff", fontSize: 13, fontWeight: 600, marginTop: 4 }}>{c.title}</div>
              {c.summary && <div style={{ color: "#a0b0d0", fontSize: 11, marginTop: 2 }}>{c.summary.substring(0, 200)}</div>}
            </div>
          ))}
        </div>
      )}

      {/* High Priority Feed */}
      <div style={{ ...enhancedCardStyle, borderColor: "rgba(255,71,87,0.15)" }}>
        <h3 style={enhancedCardTitle}>
          <span style={{ marginRight: 8, animation: "liveIndicator 1.5s ease-in-out infinite" }}>&#x1F525;</span>
          High Priority Incidents
          {loading && <span style={{ fontSize: 11, color: "#a0b0d0", fontWeight: 400, marginLeft: 8 }}>(syncing...)</span>}
        </h3>
        <div>
          {(stats.recentHighPriority || []).map((inc) => (
            <IncidentRow key={inc.id} incident={inc} onSelect={onSelect} compact />
          ))}
          {(!stats.recentHighPriority || stats.recentHighPriority.length === 0) && <EmptyState text="No high-priority incidents in this period" />}
        </div>
      </div>

      {/* Qualified vs Pending Lead Tabs */}
      <div style={{ ...enhancedCardStyle, marginTop: 16, borderColor: feedView === 'qualified' ? "rgba(52,211,153,0.3)" : "rgba(251,191,36,0.3)" }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12 }}>
          <h3 style={{ ...enhancedCardTitle, marginBottom: 0 }}>
            <span style={{ marginRight: 8 }}>&#x1F4CB;</span>Lead Pipeline
          </h3>
          <div style={{ display: "flex", gap: 6, background: "#0d1424", borderRadius: 10, padding: 4 }}>
            {[
              { key: "qualified", label: "Qualified", color: "#34d399" },
              { key: "pending_named", label: "Has Name", color: "#22d3ee" },
              { key: "pending", label: "Pending", color: "#fbbf24" },
              { key: "all", label: "All", color: "#a0b0d0" }
            ].map(t => (
              <button key={t.key}
                onClick={() => setFeedView(t.key)}
                style={{
                  background: feedView === t.key ? t.color : "transparent",
                  color: feedView === t.key ? "#0b0f1a" : "#a0b0d0",
                  border: "none", padding: "6px 14px", borderRadius: 8,
                  fontSize: 12, fontWeight: 700, cursor: "pointer",
                  transition: "all 0.2s ease"
                }}>{t.label}</button>
            ))}
          </div>
        </div>

        {feedView === 'qualified' && (
          <div style={{ background: "rgba(52,211,153,0.06)", padding: "8px 12px", borderRadius: 8, marginBottom: 12, fontSize: 11, color: "#a0b0d0" }}>
            Showing only incidents with at least one person + verified contact info (phone/email/address). Scored 0-100 by severity × contact × recency.
          </div>
        )}
        {feedView === 'pending' && (
          <div style={{ background: "rgba(251,191,36,0.06)", padding: "8px 12px", borderRadius: 8, marginBottom: 12, fontSize: 11, color: "#a0b0d0" }}>
            Awaiting victim names + contact info. These auto-promote to Qualified once enrichment fills enough data.
          </div>
        )}

        {feedIncidents.slice(0, 30).map((inc) => (
          <QualifiedLeadRow key={inc.id} incident={inc} onSelect={onSelect} />
        ))}
        {feedIncidents.length === 0 && <EmptyState text={`No ${feedView} leads`} />}
      </div>

      {/* Recent Feed (raw) */}
      <div style={{ ...enhancedCardStyle, marginTop: 16 }}>
        <h3 style={enhancedCardTitle}>
          <span style={{ marginRight: 8 }}>&#x23F1;&#xFE0F;</span>All Recent &mdash; Raw Feed
        </h3>
        {incidents.slice(0, 10).map((inc) => (
          <IncidentRow key={inc.id} incident={inc} onSelect={onSelect} />
        ))}
        {incidents.length === 0 && <EmptyState text="No incidents to display" />}
      </div>
    </div>
  );
}

// ============================================================================
// INCIDENT LIST VIEW
// ============================================================================
function IncidentList({ incidents, onSelect, filters, setFilters, incidentsView, setIncidentsView, onResync, resyncing, resyncResult }) {
  return (
    <div style={{ animation: "fadeIn 0.3s ease" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
        <div>
          <h2 style={{ color: "#f4f7ff", margin: 0, fontSize: 22, fontWeight: 800 }}>Incidents</h2>
          <p style={{ color: "#a0b0d0", margin: "4px 0 0", fontSize: 13 }}>{incidents.length} {incidentsView === 'qualified' ? 'qualified leads (with name + contact)' : incidentsView === 'pending_named' ? 'incidents with name (awaiting contact info)' : incidentsView === 'pending' ? 'incidents pending name extraction' : 'total'}</p>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <button onClick={onResync} disabled={resyncing} style={{
            background: resyncing ? "#1c2b4d" : "linear-gradient(135deg, #34d399, #22d3ee)",
            color: "#0b0f1a", border: "none", padding: "8px 16px", borderRadius: 10,
            fontSize: 12, fontWeight: 800, cursor: resyncing ? "wait" : "pointer",
            display: "flex", alignItems: "center", gap: 6, transition: "all 0.2s"
          }} title="Re-runs Trestle + PDL + Tracerfy + SearchBug + people-search against all named leads, then re-qualifies">
            {resyncing ? "⏳ Syncing..." : "🔄 Re-Sync All APIs"}
          </button>
        </div>
      </div>

      {resyncResult && (
        <div style={{ background: resyncResult.success ? "rgba(52,211,153,0.1)" : "rgba(255,71,87,0.1)", border: `1px solid ${resyncResult.success ? "rgba(52,211,153,0.3)" : "rgba(255,71,87,0.3)"}`, padding: "10px 14px", borderRadius: 10, marginBottom: 16, fontSize: 12, color: "#f4f7ff" }}>
          {resyncResult.summary || resyncResult.error}
        </div>
      )}

      {/* Tab strip */}
      <div style={{ display: "flex", gap: 8, marginBottom: 16, background: "rgba(13,20,36,0.6)", padding: 6, borderRadius: 10, width: "fit-content" }}>
        {[
          { key: "qualified", label: "Qualified", color: "#34d399", desc: "Name + contact" },
          { key: "pending_named", label: "Has Name", color: "#22d3ee", desc: "Awaiting contact" },
          { key: "pending", label: "Pending", color: "#fbbf24", desc: "No name yet" },
          { key: "all", label: "All", color: "#a0b0d0", desc: "Everything" }
        ].map(t => (
          <button key={t.key} onClick={() => setIncidentsView(t.key)} title={t.desc}
            style={{
              background: incidentsView === t.key ? t.color : "transparent",
              color: incidentsView === t.key ? "#0b0f1a" : "#a0b0d0",
              border: "none", padding: "8px 16px", borderRadius: 8,
              fontSize: 12, fontWeight: 700, cursor: "pointer"
            }}>{t.label}</button>
        ))}
      </div>

      <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: 16 }}>
        <div style={{ position: "relative" }}>
          <input
            placeholder="Search address, report #, description..."
            value={filters.search || ""}
            onChange={(e) => setFilters((f) => ({ ...f, search: e.target.value }))}
            style={{ ...inputStyle, width: 340, paddingLeft: 36 }}
            className="sidebar-item"
          />
          <span style={{ position: "absolute", left: 12, top: "50%", transform: "translateY(-50%)", color: "#a0b0d0", fontSize: 14 }}>&#x1F50D;</span>
        </div>
      </div>
      <div style={enhancedCardStyle}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "2px solid rgba(79,107,255,0.15)" }}>
              {["State", "Priority", "Type", "Severity", "Location", "Time", "Injuries", "Sources", "Status", "Persons"].map((h) => (
                <th key={h} style={{ textAlign: "left", padding: "12px 8px", color: "#4f6bff", fontSize: 10, textTransform: "uppercase", letterSpacing: "1px", fontWeight: 700 }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {incidents.map((inc) => (
              <tr key={inc.id} onClick={() => onSelect(inc)} className="table-row"
                style={{ borderBottom: "1px solid rgba(28,43,77,0.4)", cursor: "pointer" }}>
                <td style={tdStyle}><QualificationBadge state={inc.qualification_state} score={inc.lead_score} /></td>
                <td style={tdStyle}><PriorityBadge p={inc.priority} /></td>
                <td style={tdStyle}><TypeBadge type={inc.incident_type} /></td>
                <td style={tdStyle}><SeverityBadge severity={inc.severity} /></td>
                <td style={tdStyle}><span style={{ color: "#f4f7ff", fontSize: 13 }}>{inc.city}, {inc.state}</span><br /><span style={{ color: "#a0b0d0", fontSize: 11 }}>{inc.address?.substring(0, 40)}</span></td>
                <td style={{ ...tdStyle, color: "#a0b0d0", fontSize: 12 }}>
                  {inc.occurred_at && (
                    <div style={{ color: "#fbbf24", fontSize: 11, fontWeight: 700, fontFamily: "monospace" }}>
                      ⏱ {formatAccidentDateTime(inc.occurred_at)}
                    </div>
                  )}
                  <div style={{ fontSize: 10, color: "#5e739e" }}>discovered {formatTime(inc.discovered_at)}</div>
                </td>
                <td style={tdStyle}><span style={{ color: inc.injuries_count > 0 ? "#ff4757" : "#a0b0d0", fontWeight: inc.injuries_count > 0 ? 700 : 400 }}>{inc.injuries_count || 0}</span></td>
                <td style={tdStyle}><span style={{ color: "#4f6bff" }}>{inc.source_count || 1}</span></td>
                <td style={tdStyle}><StatusBadge status={inc.status} /></td>
                <td style={tdStyle}>
                  {(inc.persons || []).length === 0 ? (
                    <span style={{ color: "#5e739e", fontSize: 11 }}>—</span>
                  ) : (
                    <div>
                      {(inc.persons || []).slice(0, 2).map(p => (
                        <div key={p.id || p.full_name} style={{ marginBottom: 3 }}>
                          <div style={{ color: "#f4f7ff", fontSize: 12, fontWeight: 600 }}>{p.full_name || "unknown"}</div>
                          <div style={{ display: "flex", gap: 6, flexWrap: "wrap", fontSize: 10 }}>
                            {p.phone && <span style={{ color: "#34d399" }}>☎ {p.phone}</span>}
                            {p.email && <span style={{ color: "#22d3ee" }}>✉ {p.email.substring(0, 20)}</span>}
                            {p.address && <span style={{ color: "#fbbf24" }}>⌂ {p.address.substring(0, 25)}</span>}
                            {!p.phone && !p.email && !p.address && <span style={{ color: "#5e739e" }}>no contact yet</span>}
                          </div>
                        </div>
                      ))}
                      {(inc.persons || []).length > 2 && <span style={{ color: "#5e739e", fontSize: 10 }}>+ {(inc.persons || []).length - 2} more</span>}
                    </div>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {incidents.length === 0 && <EmptyState text="No incidents match your filters" />}
      </div>
    </div>
  );
}

// ============================================================================
// MY LEADS VIEW
// ============================================================================
function MyLeads({ user, onSelect }) {
  const [leads, setLeads] = useState([]);
  useEffect(() => {
    api("/dashboard/my-assignments").then((d) => setLeads(d.data || []));
  }, [user]);

  return (
    <div style={{ animation: "fadeIn 0.3s ease" }}>
      <div style={{ marginBottom: 20 }}>
        <h2 style={{ color: "#f4f7ff", margin: 0, fontSize: 22, fontWeight: 800 }}>My Assigned Leads</h2>
        <p style={{ color: "#a0b0d0", margin: "4px 0 0", fontSize: 13 }}>{leads.length} leads assigned to you</p>
      </div>
      <div style={enhancedCardStyle}>
        {leads.map((inc) => <IncidentRow key={inc.id} incident={inc} onSelect={onSelect} />)}
        {leads.length === 0 && <EmptyState text="No leads assigned to you yet" />}
      </div>
    </div>
  );
}

// ============================================================================
// INCIDENT DETAIL MODAL — Enhanced slide-in panel
// ============================================================================
function IncidentDetail({ incident, onClose, user, onUpdate }) {
  const [detail, setDetail] = useState(null);
  const [note, setNote] = useState("");

  useEffect(() => {
    api(`/incidents/${incident.id}`).then(setDetail);
  }, [incident.id]);

  const addNote = async () => {
    if (!note.trim()) return;
    await api(`/incidents/${incident.id}/note`, { method: "POST", body: { note } });
    setNote("");
    const d = await api(`/incidents/${incident.id}`);
    setDetail(d);
  };

  const assignToMe = async () => {
    await api(`/incidents/${incident.id}/assign`, { method: "POST", body: { userId: user.id } });
    onUpdate();
    const d = await api(`/incidents/${incident.id}`);
    setDetail(d);
  };

  const updateStatus = async (status) => {
    await api(`/incidents/${incident.id}`, { method: "PATCH", body: { status } });
    onUpdate();
    const d = await api(`/incidents/${incident.id}`);
    setDetail(d);
  };

  const d = detail || incident;

  return (
    <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.75)", backdropFilter: "blur(4px)", zIndex: 200, display: "flex", justifyContent: "flex-end" }} onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div className="detail-panel" style={{
        width: 660, background: "#101729", height: "100vh", overflow: "auto",
        borderLeft: "1px solid rgba(79,107,255,0.15)", padding: "28px 24px"
      }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24 }}>
          <h2 style={{ color: "#f4f7ff", margin: 0, fontSize: 20, fontWeight: 800 }}>Incident Detail</h2>
          <button onClick={onClose} className="btn-action" style={{
            background: "rgba(100,116,139,0.1)", border: "1px solid #1c2b4d", color: "#a0b0d0",
            fontSize: 18, cursor: "pointer", width: 32, height: 32, borderRadius: 8,
            display: "flex", alignItems: "center", justifyContent: "center"
          }}>&times;</button>
        </div>

        {/* Header info */}
        <div style={{ display: "flex", gap: 8, marginBottom: 16, flexWrap: "wrap" }}>
          <TypeBadge type={d.incident_type} />
          <SeverityBadge severity={d.severity} />
          <PriorityBadge p={d.priority} />
          <StatusBadge status={d.status} />
          {d.qualification_state && <QualificationBadge state={d.qualification_state} score={d.lead_score} />}
          {d.lead_score > 0 && <span style={{ ...badgeBase, background: "linear-gradient(135deg, #34d399, #22d3ee)", color: "#0b0f1a", fontWeight: 800 }}>SCORE {d.lead_score}</span>}
          {d.confidence_score && <span style={{ ...badgeBase, background: "rgba(79,107,255,0.15)", color: "#7dd3fc", border: "1px solid rgba(79,107,255,0.2)" }}>{Math.round(d.confidence_score)}% confidence</span>}
          {d.source_count > 1 && <span style={{ ...badgeBase, background: "rgba(79,107,255,0.15)", color: "#7dd3fc", border: "1px solid rgba(79,107,255,0.2)" }}>{d.source_count} sources</span>}
        </div>

        {/* When did the accident happen */}
        {(d.occurred_at || d.discovered_at) && (
          <div style={{
            background: "rgba(251,191,36,0.08)", border: "1px solid rgba(251,191,36,0.25)",
            borderRadius: 10, padding: "12px 16px", marginBottom: 16
          }}>
            <div style={{ color: "#fbbf24", fontSize: 10, fontWeight: 700, textTransform: "uppercase", letterSpacing: "1px", marginBottom: 4 }}>⏱ Accident Time</div>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: 8 }}>
              <div>
                <div style={{ color: "#fef3c7", fontSize: 16, fontWeight: 700, fontFamily: "monospace" }}>
                  {formatAccidentDateTime(d.occurred_at || d.discovered_at)}
                </div>
                <div style={{ color: "#a0b0d0", fontSize: 11, marginTop: 2 }}>
                  {formatAccidentDate(d.occurred_at || d.discovered_at)}
                </div>
              </div>
              {d.discovered_at && d.occurred_at && d.discovered_at !== d.occurred_at && (
                <div style={{ textAlign: "right", fontSize: 10, color: "#5e739e" }}>
                  <div>discovered:</div>
                  <div style={{ color: "#a0b0d0" }}>{formatAccidentDateTime(d.discovered_at)}</div>
                </div>
              )}
            </div>
          </div>
        )}

        {/* What's missing for qualification */}
        {d.qualification_state !== 'qualified' && (
          <div style={{ background: "rgba(251,191,36,0.08)", border: "1px solid rgba(251,191,36,0.25)", borderRadius: 10, padding: "12px 16px", marginBottom: 16 }}>
            <div style={{ color: "#fbbf24", fontSize: 11, fontWeight: 700, textTransform: "uppercase", letterSpacing: "1px", marginBottom: 6 }}>⏳ Awaiting</div>
            <div style={{ color: "#fef3c7", fontSize: 13, lineHeight: 1.5 }}>
              {(detail?.persons || d.persons || []).length === 0 ? (
                <>No victim names extracted yet. Sources: {d.tags?.join(', ') || 'unknown'}. Names typically come in via news, PD press releases, or obituaries within 24-72h.</>
              ) : (
                <>Have {(detail?.persons || d.persons).length} named person(s) but missing contact info. <strong>Re-Sync All APIs</strong> on the Incidents page will run Trestle + PDL + Tracerfy + people-search to fill in phone/email/address.</>
              )}
            </div>
          </div>
        )}

        {/* Actions */}
        <div style={{ display: "flex", gap: 8, marginBottom: 24, flexWrap: "wrap" }}>
          <button onClick={assignToMe} className="btn-action" style={{ ...btnSmall, background: "linear-gradient(135deg, #4f6bff, #a855f7, #e040fb)" }}>&#x1F464; Assign to Me</button>
          <button onClick={() => updateStatus("contacted")} className="btn-action" style={{ ...btnSmall, background: "linear-gradient(135deg, #34d399, #22d3ee)" }}>&#x2714; Mark Contacted</button>
          <button onClick={() => updateStatus("in_progress")} className="btn-action" style={{ ...btnSmall, background: "linear-gradient(135deg, #ff7b3a, #ff4da6)" }}>&#x23F3; In Progress</button>
          <button onClick={() => updateStatus("closed")} className="btn-action" style={{ ...btnSmall, background: "linear-gradient(135deg, #5e739e, #a0b0d0)" }}>&#x2716; Close</button>
        </div>

        {/* Location */}
        <Section title="Location">
          <InfoRow label="Address" value={d.address} />
          <InfoRow label="City/State" value={`${d.city || ""}, ${d.state || ""} ${d.zip || ""}`} />
          <InfoRow label="Metro" value={d.metro_area_name} />
          {d.police_report_number && <InfoRow label="Police Report #" value={d.police_report_number} />}
          {d.police_department && <InfoRow label="Department" value={d.police_department} />}
        </Section>

        {/* Description */}
        <Section title="Description">
          <p style={{ color: "#f4f7ff", fontSize: 14, lineHeight: 1.6, whiteSpace: "pre-wrap" }}>{d.description || "No description available"}</p>
        </Section>

        {/* Persons Involved */}
        <Section title={`Persons Involved (${(detail?.persons || d.persons || []).length})`}>
          {(detail?.persons || d.persons || []).map((p, i) => (
            <div key={p.id || i} style={{
              background: "rgba(15,18,25,0.8)", borderRadius: 10, padding: 16, marginBottom: 12,
              border: "1px solid rgba(28,43,77,0.5)", transition: "border-color 0.2s"
            }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
                <span style={{ color: "#f4f7ff", fontWeight: 700, fontSize: 14 }}>{p.full_name || "Unknown"}</span>
                <span style={{ ...badgeBase, background: p.is_injured ? "rgba(255,71,87,0.3)" : "rgba(28,43,77,0.6)", color: p.is_injured ? "#ff7b9c" : "#a0b0d0" }}>
                  {p.is_injured ? `Injured (${p.injury_severity || "unknown"})` : p.role}
                </span>
              </div>
              {p.phone && <InfoRow label="Phone" value={p.phone} />}
              {p.email && <InfoRow label="Email" value={p.email} />}
              {p.insurance_company && <InfoRow label="Insurance" value={`${p.insurance_company} - ${p.policy_limits || "limits unknown"}`} />}
              {p.transported_to && <InfoRow label="Hospital" value={p.transported_to} />}
              {p.has_attorney && <InfoRow label="Attorney" value={`${p.attorney_name || "Yes"} ${p.attorney_firm ? `(${p.attorney_firm})` : ""}`} />}
              {!p.has_attorney && p.is_injured && (
                <div style={{ marginTop: 8, padding: "6px 12px", background: "rgba(52,211,153,0.1)", border: "1px solid rgba(52,211,153,0.2)", borderRadius: 6 }}>
                  <span style={{ color: "#34d399", fontSize: 12, fontWeight: 700 }}>&#x2714; NO ATTORNEY - POTENTIAL LEAD</span>
                </div>
              )}
              <div style={{ marginTop: 8 }}>
                <span style={{ ...badgeBase, background: contactStatusColor(p.contact_status).bg, color: contactStatusColor(p.contact_status).fg, fontSize: 11 }}>
                  {(p.contact_status || "not_contacted").replace(/_/g, " ")}
                </span>
              </div>
            </div>
          ))}
        </Section>

        {/* Vehicles */}
        {(detail?.vehicles || []).length > 0 && (
          <Section title={`Vehicles (${detail.vehicles.length})`}>
            {detail.vehicles.map((v, i) => (
              <div key={v.id || i} style={{ background: "rgba(15,18,25,0.8)", borderRadius: 10, padding: 14, marginBottom: 10, border: "1px solid rgba(28,43,77,0.5)" }}>
                <span style={{ color: "#f4f7ff", fontWeight: 700 }}>{v.year} {v.make} {v.model}</span>
                {v.color && <span style={{ color: "#a0b0d0" }}> ({v.color})</span>}
                {v.is_commercial && <span style={{ ...badgeBase, background: "rgba(168,85,247,0.15)", color: "#d8b4fe", marginLeft: 8, fontSize: 11 }}>COMMERCIAL</span>}
                {v.damage_severity && <InfoRow label="Damage" value={v.damage_severity} />}
                {v.insurance_company && <InfoRow label="Insurance" value={v.insurance_company} />}
                {v.carrier_name && <InfoRow label="Carrier" value={`${v.carrier_name} (DOT: ${v.dot_number || "N/A"})`} />}
              </div>
            ))}
          </Section>
        )}

        {/* Source Reports */}
        {(detail?.sourceReports || []).length > 0 && (
          <Section title={`Data Sources (${detail.sourceReports.length})`}>
            {detail.sourceReports.map((sr) => (
              <div key={sr.id} style={{ padding: "10px 0", borderBottom: "1px solid rgba(28,43,77,0.4)" }}>
                <div style={{ display: "flex", justifyContent: "space-between" }}>
                  <span style={{ color: "#7dd3fc", fontWeight: 600, fontSize: 13 }}>{sr.source_name || sr.source_type}</span>
                  <span style={{ color: "#a0b0d0", fontSize: 11 }}>{formatTime(sr.fetched_at)}</span>
                </div>
                {sr.contributed_fields && <span style={{ color: "#a0b0d0", fontSize: 12 }}>Added: {sr.contributed_fields.join(", ")}</span>}
              </div>
            ))}
          </Section>
        )}

        {/* Notes */}
        <Section title="Notes">
          {d.notes && <pre style={{ color: "#a0b0d0", fontSize: 13, whiteSpace: "pre-wrap", marginBottom: 14, background: "rgba(15,18,25,0.6)", padding: 12, borderRadius: 8 }}>{d.notes}</pre>}
          <div style={{ display: "flex", gap: 8 }}>
            <input value={note} onChange={(e) => setNote(e.target.value)} placeholder="Add a note..." style={{ ...inputStyle, flex: 1 }} className="sidebar-item" onKeyDown={(e) => e.key === "Enter" && addNote()} />
            <button onClick={addNote} className="btn-action" style={{ ...btnSmall, background: "linear-gradient(135deg, #4f6bff, #a855f7)" }}>Add</button>
          </div>
        </Section>

        {/* AI Analysis */}
        {d.ai_analysis && (
          <Section title="AI Analysis">
            <pre style={{ color: "#a0b0d0", fontSize: 12, whiteSpace: "pre-wrap", background: "rgba(15,18,25,0.6)", padding: 12, borderRadius: 8 }}>
              {JSON.stringify(typeof d.ai_analysis === "string" ? JSON.parse(d.ai_analysis) : d.ai_analysis, null, 2)}
            </pre>
          </Section>
        )}

        {/* Branding */}
        <div style={{ textAlign: "center", padding: "24px 0 8px", borderTop: "1px solid rgba(28,43,77,0.4)", marginTop: 16 }}>
          <div style={{ fontSize: 10, color: "#5e739e", letterSpacing: "1px" }}>INCIDENT COMMAND by Donovan Digital Solutions</div>
        </div>
      </div>
    </div>
  );
}

// ============================================================================
// REUSABLE COMPONENTS
// ============================================================================
function EmptyState({ text }) {
  return <p style={{ color: "#5e739e", textAlign: "center", padding: 32, fontSize: 13 }}>{text}</p>;
}

function QualifiedLeadRow({ incident: inc, onSelect }) {
  const persons = inc.persons || [];
  const score = inc.lead_score || 0;
  const scoreColor = score >= 80 ? "#34d399" : score >= 60 ? "#22d3ee" : score >= 40 ? "#fbbf24" : "#ff7b3a";
  return (
    <div onClick={() => onSelect(inc)} className="incident-row" style={{
      display: "grid", gridTemplateColumns: "auto 1fr auto auto auto", gap: 12,
      padding: "14px 10px", borderBottom: "1px solid rgba(28,43,77,0.4)", cursor: "pointer", alignItems: "center"
    }}>
      <div style={{ background: scoreColor, color: "#0b0f1a", padding: "6px 10px", borderRadius: 8, fontWeight: 800, fontSize: 14, minWidth: 40, textAlign: "center" }}>{score}</div>
      <div>
        <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
          <SeverityBadge severity={inc.severity} />
          <TypeBadge type={inc.incident_type} />
          <span style={{ color: "#f4f7ff", fontSize: 13, fontWeight: 600 }}>
            {inc.address || `${inc.city || ''}, ${inc.state || ''}`}
          </span>
          {(inc.occurred_at || inc.discovered_at) && (
            <span style={{ color: "#fbbf24", fontSize: 11, fontWeight: 700, fontFamily: "monospace", background: "rgba(251,191,36,0.1)", padding: "2px 8px", borderRadius: 6 }}>
              ⏱ {formatAccidentDateTime(inc.occurred_at || inc.discovered_at)}
            </span>
          )}
        </div>
        {persons.length > 0 && (
          <div style={{ marginTop: 6, fontSize: 12, color: "#a0b0d0" }}>
            {persons.slice(0, 2).map(p => (
              <div key={p.id || p.full_name}>
                <strong style={{ color: "#f4f7ff" }}>{p.full_name}</strong>
                {p.phone && <span style={{ color: "#34d399", marginLeft: 8 }}>☎ {p.phone}</span>}
                {p.email && <span style={{ color: "#22d3ee", marginLeft: 8 }}>✉ {p.email}</span>}
                {p.address && <span style={{ color: "#fbbf24", marginLeft: 8 }}>⌂ {p.address.substring(0, 40)}</span>}
                {p.has_attorney && <span style={{ color: "#ff4757", marginLeft: 8, fontSize: 10, fontWeight: 700 }}>HAS ATTORNEY</span>}
              </div>
            ))}
            {persons.length > 2 && <span style={{ color: "#5e739e", fontSize: 11 }}>+ {persons.length - 2} more</span>}
          </div>
        )}
        {inc.tags && inc.tags.length > 0 && (
          <div style={{ display: "flex", gap: 4, marginTop: 4, flexWrap: "wrap" }}>
            {inc.tags.slice(0, 5).map(tg => (
              <span key={tg} style={{ background: "rgba(79,107,255,0.12)", color: "#a0b0d0", fontSize: 9, padding: "1px 6px", borderRadius: 8, fontWeight: 700, textTransform: "uppercase" }}>{tg}</span>
            ))}
          </div>
        )}
      </div>
      <div style={{ textAlign: "right" }}>
        <div style={{ color: "#a0b0d0", fontSize: 11 }}>{inc.source_count}× sources</div>
        <div style={{ color: "#a0b0d0", fontSize: 10, marginTop: 2 }}>{inc.confidence_score}% conf</div>
      </div>
      <div style={{ textAlign: "right", color: "#a0b0d0", fontSize: 11 }}>
        {formatTime(inc.qualified_at || inc.discovered_at)}
      </div>
      {inc.fatalities_count > 0 && (
        <div style={{ background: "#ff4757", color: "#fff", padding: "4px 8px", borderRadius: 6, fontSize: 10, fontWeight: 800 }}>FATAL</div>
      )}
    </div>
  );
}

function IncidentRow({ incident: inc, onSelect, compact }) {
  return (
    <div
      onClick={() => onSelect(inc)}
      className="incident-row"
      style={{ display: "flex", alignItems: "center", gap: 12, padding: "14px 10px", borderBottom: "1px solid rgba(28,43,77,0.4)", cursor: "pointer" }}
    >
      <PriorityBadge p={inc.priority} />
      <TypeBadge type={inc.incident_type} />
      <SeverityBadge severity={inc.severity} />
      <div style={{ flex: 1 }}>
        <div style={{ color: "#f4f7ff", fontSize: 14, fontWeight: 500 }}>{inc.address || `${inc.city}, ${inc.state}`}</div>
        {(inc.occurred_at || inc.discovered_at) && (
          <div style={{ color: "#fbbf24", fontSize: 11, fontWeight: 600, marginTop: 2, fontFamily: "monospace" }}>
            ⏱ {formatAccidentDateTime(inc.occurred_at || inc.discovered_at)}
          </div>
        )}
        {!compact && <div style={{ color: "#a0b0d0", fontSize: 12, marginTop: 3 }}>{inc.description?.substring(0, 80)}...</div>}
        {inc.tags && inc.tags.length > 0 && (
          <div style={{ display: "flex", gap: 4, marginTop: 4, flexWrap: "wrap" }}>
            {inc.tags.slice(0, 4).map(tg => (
              <span key={tg} style={{
                background: "rgba(79,107,255,0.12)",
                color: sourceTypeColor("opendata_" + tg) === "#5e739e" ? "#a0b0d0" : sourceTypeColor("opendata_" + tg),
                fontSize: 9,
                padding: "1px 6px",
                borderRadius: 8,
                fontWeight: 700,
                textTransform: "uppercase",
                letterSpacing: "0.3px"
              }}>{tg}</span>
            ))}
          </div>
        )}
      </div>
      <div style={{ textAlign: "right" }}>
        <div style={{ color: "#a0b0d0", fontSize: 12 }}>{formatTime(inc.discovered_at)}</div>
        {inc.injuries_count > 0 && <div style={{ color: "#ff4757", fontSize: 11, fontWeight: 700, marginTop: 2 }}>{inc.injuries_count} injured</div>}
        {inc.source_count > 1 && (
          <div style={{ display: "inline-flex", alignItems: "center", gap: 3, marginTop: 4, background: "rgba(34,211,238,0.15)", padding: "1px 6px", borderRadius: 8 }}>
            <span style={{ color: "#22d3ee", fontSize: 9, fontWeight: 700 }}>{inc.source_count}× SOURCES</span>
          </div>
        )}
        {inc.confidence_score && (
          <div style={{ color: confidenceColor(inc.confidence_score), fontSize: 10, marginTop: 2, fontWeight: 600 }}>
            {Math.round(inc.confidence_score)}% conf
          </div>
        )}
      </div>
    </div>
  );
}

function confidenceColor(s) {
  const v = parseInt(s) || 0;
  if (v >= 90) return "#34d399";
  if (v >= 75) return "#22d3ee";
  if (v >= 60) return "#fbbf24";
  return "#ff7b3a";
}

function Section({ title, children }) {
  return (
    <div style={{ marginBottom: 24 }}>
      <h3 style={{ color: "#4f6bff", fontSize: 11, textTransform: "uppercase", letterSpacing: "1.5px", marginBottom: 12, paddingBottom: 8, borderBottom: "1px solid rgba(79,107,255,0.15)", fontWeight: 700 }}>{title}</h3>
      {children}
    </div>
  );
}

function InfoRow({ label, value }) {
  if (!value) return null;
  return (
    <div style={{ display: "flex", justifyContent: "space-between", padding: "4px 0" }}>
      <span style={{ color: "#a0b0d0", fontSize: 13 }}>{label}</span>
      <span style={{ color: "#f4f7ff", fontSize: 13, textAlign: "right", maxWidth: "60%" }}>{value}</span>
    </div>
  );
}

function QualificationBadge({ state, score }) {
  const map = {
    qualified: { color: "#34d399", label: "QUAL" },
    pending_named: { color: "#22d3ee", label: "NAMED" },
    pending: { color: "#fbbf24", label: "PEND" },
  };
  const cfg = map[state] || { color: "#5e739e", label: "?" };
  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 2 }}>
      <span style={{ background: cfg.color, color: "#0b0f1a", fontSize: 9, fontWeight: 800, padding: "2px 6px", borderRadius: 4, letterSpacing: "0.5px" }}>{cfg.label}</span>
      {score > 0 && <span style={{ color: cfg.color, fontSize: 9, fontWeight: 700 }}>{score}</span>}
    </div>
  );
}

function PriorityBadge({ p }) {
  const colors = { 1: "#ff4757", 2: "#ff7b3a", 3: "#fbbf24", 4: "#22d3ee", 5: "#a0b0d0" };
  const c = colors[p] || "#a0b0d0";
  return <span style={{ ...badgeBase, background: c + "18", color: c, minWidth: 28, textAlign: "center", border: `1px solid ${c}33` }}>P{p || "?"}</span>;
}

function TypeBadge({ type }) {
  const icons = { car_accident: "\uD83D\uDE97", motorcycle_accident: "\uD83C\uDFCD", truck_accident: "\uD83D\uDE9B", work_accident: "\uD83C\uDFD7", pedestrian: "\uD83D\uDEB6", bicycle: "\uD83D\uDEB2", slip_fall: "\u26A0", bus_accident: "\uD83D\uDE8C" };
  return <span style={{ ...badgeBase, background: "rgba(168,85,247,0.15)", color: "#f4f7ff", border: "1px solid rgba(168,85,247,0.3)" }}>{icons[type] || "\uD83D\uDE97"}</span>;
}

function SeverityBadge({ severity }) {
  const map = { fatal: { bg: "rgba(255,71,87,0.3)", fg: "#ff7b9c", border: "rgba(255,71,87,0.3)" }, critical: { bg: "rgba(255,123,58,0.3)", fg: "#ffa366", border: "rgba(255,123,58,0.3)" }, serious: { bg: "rgba(251,191,36,0.3)", fg: "#ffd966", border: "rgba(251,191,36,0.3)" }, moderate: { bg: "rgba(79,107,255,0.3)", fg: "#93c5fd", border: "rgba(79,107,255,0.3)" }, minor: { bg: "rgba(28,43,77,0.5)", fg: "#a0b0d0", border: "rgba(79,107,255,0.2)" } };
  const s = map[severity] || map.minor;
  return <span style={{ ...badgeBase, background: s.bg, color: s.fg, border: `1px solid ${s.border}` }}>{severity || "unknown"}</span>;
}

function StatusBadge({ status }) {
  const map = { new: { bg: "rgba(79,107,255,0.3)", fg: "#7dd3fc", border: "rgba(79,107,255,0.3)" }, verified: { bg: "rgba(52,211,153,0.3)", fg: "#6ee7b7", border: "rgba(52,211,153,0.3)" }, assigned: { bg: "rgba(251,191,36,0.3)", fg: "#ffd966", border: "rgba(251,191,36,0.3)" }, contacted: { bg: "rgba(52,211,153,0.3)", fg: "#6ee7b7", border: "rgba(52,211,153,0.3)" }, in_progress: { bg: "rgba(255,123,58,0.3)", fg: "#ffa366", border: "rgba(255,123,58,0.3)" }, closed: { bg: "rgba(28,43,77,0.5)", fg: "#a0b0d0", border: "rgba(79,107,255,0.2)" } };
  const s = map[status] || map.new;
  return <span style={{ ...badgeBase, background: s.bg, color: s.fg, border: `1px solid ${s.border}` }}>{(status || "new").replace(/_/g, " ")}</span>;
}

function contactStatusColor(status) {
  const map = { not_contacted: { bg: "rgba(28,43,77,0.6)", fg: "#a0b0d0" }, attempted: { bg: "rgba(251,191,36,0.3)", fg: "#ffd966" }, contacted: { bg: "rgba(52,211,153,0.3)", fg: "#6ee7b7" }, interested: { bg: "rgba(52,211,153,0.3)", fg: "#6ee7b7" }, not_interested: { bg: "rgba(28,43,77,0.6)", fg: "#a0b0d0" }, retained: { bg: "rgba(79,107,255,0.3)", fg: "#93c5fd" }, has_attorney: { bg: "rgba(255,71,87,0.3)", fg: "#ff7b9c" } };
  return map[status] || map.not_contacted;
}

// ============================================================================
// HELPERS
// ============================================================================
function sourceTypeColor(t) {
  const map = {
    "tomtom": "#4f6bff",
    "waze": "#34d399",
    "opendata_seattle": "#22d3ee",
    "opendata_sf": "#a855f7",
    "opendata_dallas": "#ff7b3a",
    "opendata_chicago": "#fbbf24",
    "opendata_cincinnati": "#14b8a6",
    "opendata_houston": "#ff4da6",
    "opendata_atlanta": "#e040fb",
    "scanner": "#ff4757",
    "newsapi": "#ff4da6",
    "nhtsa": "#a855f7",
    "state_txdot": "#fbbf24",
    "state_ga511": "#34d399",
    "state_fl511": "#22d3ee",
  };
  return map[t] || "#5e739e";
}
function changelogKindColor(k) {
  const map = {
    "deploy": "#34d399",
    "schema": "#a855f7",
    "pipeline": "#4f6bff",
    "feature": "#22d3ee",
    "fix": "#fbbf24",
    "config": "#ff7b3a"
  };
  return map[k] || "#a0b0d0";
}

function formatType(t) {
  return (t || "").replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatAccidentDate(ts) {
  if (!ts) return "—";
  const d = new Date(ts);
  if (isNaN(d.getTime())) return "—";
  const now = new Date();
  const diffMs = now - d;
  const hours = Math.floor(diffMs / 3600000);
  const days = Math.floor(hours / 24);
  let rel;
  if (hours < 1) rel = `${Math.floor(diffMs/60000)}m ago`;
  else if (hours < 24) rel = `${hours}h ago`;
  else if (days < 7) rel = `${days}d ago`;
  else rel = d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  return rel;
}
function formatAccidentDateTime(ts) {
  if (!ts) return "—";
  const d = new Date(ts);
  if (isNaN(d.getTime())) return "—";
  return d.toLocaleString("en-US", {
    month: "short", day: "numeric", hour: "numeric", minute: "2-digit",
    hour12: true
  });
}

function formatTime(ts) {
  if (!ts) return "";
  const d = new Date(ts);
  const now = new Date();
  const diff = (now - d) / 1000;
  if (diff < 60) return "Just now";
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return d.toLocaleDateString();
}

// ============================================================================
// STYLES
// ============================================================================
const inputStyle = { background: "rgba(13,18,37,0.8)", border: "1px solid rgba(28,43,77,0.6)", borderRadius: 10, padding: "11px 14px", color: "#f4f7ff", fontSize: 14, width: "100%", outline: "none", boxSizing: "border-box", transition: "border-color 0.2s, box-shadow 0.2s" };
const selectStyle = { ...inputStyle, marginBottom: 12, cursor: "pointer" };
const labelStyle = { color: "#a0b0d0", fontSize: 10, textTransform: "uppercase", letterSpacing: "1px", marginBottom: 4, display: "block", fontWeight: 600 };
const btnPrimary = { width: "100%", background: "linear-gradient(135deg, #4f6bff, #a855f7)", color: "#f4f7ff", border: "none", borderRadius: 10, padding: "12px 16px", fontWeight: 700, fontSize: 15, cursor: "pointer", marginTop: 20, letterSpacing: "0.5px" };
const btnSmall = { border: "none", borderRadius: 8, padding: "7px 14px", color: "#fff", fontSize: 12, fontWeight: 600, cursor: "pointer" };
const navBtn = { background: "none", border: "none", cursor: "pointer", fontSize: 12, fontWeight: 600, letterSpacing: "0.5px", padding: "18px 0" };
const enhancedCardStyle = { background: "rgba(21,29,50,0.8)", borderRadius: 14, padding: 22, border: "1px solid rgba(28,43,77,0.5)", backdropFilter: "blur(8px)" };
const enhancedCardTitle = { color: "#f4f7ff", fontSize: 16, fontWeight: 700, margin: "0 0 18px", display: "flex", alignItems: "center" };
const tdStyle = { padding: "12px 8px", fontSize: 13 };
const badgeBase = { fontSize: 11, fontWeight: 600, padding: "3px 10px", borderRadius: 6, display: "inline-block", textTransform: "capitalize" };

// ============================================================================
// CONTACTS VIEW — Full contact management with filters & enrichment
// ============================================================================
function CostView({ costData, onRefresh }) {
  const [time, setTime] = useState(Date.now());
  useEffect(() => {
    const t = setInterval(() => setTime(Date.now()), 60000);
    return () => clearInterval(t);
  }, []);

  if (!costData) return (
    <div style={{ textAlign: "center", padding: 80 }}>
      <div style={{ width: 48, height: 48, border: "3px solid #1c2b4d", borderTopColor: "#34d399", borderRadius: "50%", margin: "0 auto 16px", animation: "gradientShift 1s linear infinite" }} />
      <div style={{ color: "#a0b0d0", fontSize: 14 }}>Loading cost data...</div>
    </div>
  );

  const totals = costData.total_cost_usd || {};
  const byService = costData.by_service_24h || [];
  const byPipeline = costData.by_pipeline_24h || [];
  const monthlyRunRate = costData.monthly_run_rate || 0;

  return (
    <div style={{ animation: "fadeIn 0.3s ease" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <div>
          <h2 style={{ color: "#f4f7ff", margin: 0, fontSize: 22, fontWeight: 800 }}>API Cost & Spend</h2>
          <p style={{ color: "#a0b0d0", margin: "4px 0 0", fontSize: 13 }}>Real-time enrichment + AI spend, auto-tracked per call</p>
        </div>
        <button onClick={onRefresh} style={{ ...btnSmall, background: "rgba(52,211,153,0.15)", color: "#34d399", border: "1px solid rgba(52,211,153,0.3)" }}>↻ Refresh</button>
      </div>

      {/* Big totals */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: 16, marginBottom: 24 }}>
        {[
          { label: "Last 24 Hours", value: totals["24h"] || 0, color: "linear-gradient(135deg, #34d399, #22d3ee)" },
          { label: "Last 7 Days", value: totals["7d"] || 0, color: "linear-gradient(135deg, #4f6bff, #a855f7)" },
          { label: "Last 30 Days", value: totals["30d"] || 0, color: "linear-gradient(135deg, #ff7b3a, #ff4da6)" },
          { label: "Monthly Run Rate", value: monthlyRunRate, color: "linear-gradient(135deg, #fbbf24, #ff7b3a)" },
        ].map(card => (
          <div key={card.label} style={{ background: "#151d32", border: "1px solid #1c2b4d", borderRadius: 14, padding: "22px 20px", position: "relative", overflow: "hidden" }}>
            <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 3, background: card.color }} />
            <div style={{ color: "#a0b0d0", fontSize: 11, textTransform: "uppercase", letterSpacing: "1px", fontWeight: 600 }}>{card.label}</div>
            <div style={{ background: card.color, WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent", fontSize: 36, fontWeight: 800, marginTop: 6, fontFamily: "'Orbitron', monospace" }}>
              \${(parseFloat(card.value) || 0).toFixed(2)}
            </div>
          </div>
        ))}
      </div>

      {/* Service breakdown */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 24 }}>
        <div style={enhancedCardStyle}>
          <h3 style={enhancedCardTitle}>💸 By Service (24h)</h3>
          {byService.length === 0 ? (
            <EmptyState text="No API calls tracked yet — make some enrichment calls to see breakdown" />
          ) : (
            byService.map(s => (
              <div key={s.service} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "10px 0", borderBottom: "1px solid rgba(28,43,77,0.4)" }}>
                <div>
                  <div style={{ color: "#f4f7ff", fontSize: 13, fontWeight: 600 }}>{s.service}</div>
                  <div style={{ color: "#a0b0d0", fontSize: 10 }}>{s.calls} calls</div>
                </div>
                <div style={{ color: "#34d399", fontWeight: 700, fontSize: 14, fontFamily: "monospace" }}>\${parseFloat(s.cost).toFixed(4)}</div>
              </div>
            ))
          )}
        </div>

        <div style={enhancedCardStyle}>
          <h3 style={enhancedCardTitle}>⚙️ By Pipeline (24h)</h3>
          {byPipeline.length === 0 ? (
            <EmptyState text="No pipeline costs tracked yet" />
          ) : (
            byPipeline.map(p => (
              <div key={p.pipeline} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "10px 0", borderBottom: "1px solid rgba(28,43,77,0.4)" }}>
                <div>
                  <div style={{ color: "#f4f7ff", fontSize: 13, fontWeight: 600 }}>{p.pipeline}</div>
                  <div style={{ color: "#a0b0d0", fontSize: 10 }}>{p.calls} calls</div>
                </div>
                <div style={{ color: "#22d3ee", fontWeight: 700, fontSize: 14, fontFamily: "monospace" }}>\${parseFloat(p.cost).toFixed(4)}</div>
              </div>
            ))
          )}
        </div>
      </div>

      {/* Pricing reference */}
      <div style={{ ...enhancedCardStyle, marginTop: 16 }}>
        <h3 style={enhancedCardTitle}>📋 Pricing Reference</h3>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 8, fontSize: 12 }}>
          {Object.entries(costData.pricing_table || {}).map(([k, v]) => (
            <div key={k} style={{ background: "#0d1424", padding: "8px 12px", borderRadius: 8, display: "flex", justifyContent: "space-between" }}>
              <span style={{ color: "#a0b0d0" }}>{k}</span>
              <span style={{ color: "#fbbf24", fontFamily: "monospace" }}>
                {v.flat !== undefined ? `\$${v.flat}/call` : `\$${v.in}/M in, \$${v.out}/M out`}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function ContactsView({ contacts, summary, filters, setFilters, onEnrich, enriching, onRefresh, onSelect }) {
  const [search, setSearch] = useState("");
  const [expandedId, setExpandedId] = useState(null);

  const filteredContacts = contacts.filter(c => {
    if (search) {
      const s = search.toLowerCase();
      const name = (c.display_name || c.first_name + " " + c.last_name || "").toLowerCase();
      if (!name.includes(s) && !(c.phone || "").includes(s) && !(c.email || "").toLowerCase().includes(s)) return false;
    }
    return true;
  });

  const enrichScoreColor = (s) => {
    const score = parseFloat(s) || 0;
    if (score >= 60) return "#34d399";
    if (score >= 30) return "#fbbf24";
    return "#ff4757";
  };

  const contactQualityBar = (score) => {
    const pct = Math.min(parseFloat(score) || 0, 100);
    const color = enrichScoreColor(score);
    return (
      <div style={{ width: 80, height: 6, background: "rgba(28,43,77,0.6)", borderRadius: 3, overflow: "hidden" }}>
        <div style={{ width: `${pct}%`, height: "100%", background: color, borderRadius: 3, transition: "width 0.5s" }} />
      </div>
    );
  };

  return (
    <div style={{ animation: "fadeIn 0.3s ease" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <div>
          <h2 style={{ margin: 0, fontSize: 22, fontWeight: 800, display: "flex", alignItems: "center", gap: 10 }}>
            <span style={{ fontSize: 24 }}>📋</span> Contact Intelligence
          </h2>
          <p style={{ margin: "4px 0 0", color: "#a0b0d0", fontSize: 13 }}>Cross-referenced contact data from all integrated sources</p>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <button onClick={() => onEnrich(null)} disabled={enriching}
            style={{ ...btnSmall, background: enriching ? "rgba(79,107,255,0.3)" : "linear-gradient(135deg, #4f6bff, #a855f7)", color: "#fff", padding: "8px 16px", borderRadius: 8, border: "none" }}
            className="btn-action">
            {enriching ? "⏳ Enriching..." : "🔄 Enrich All"}
          </button>
          <button onClick={onRefresh} style={{ ...btnSmall, background: "rgba(79,107,255,0.15)", color: "#4f6bff", border: "1px solid rgba(79,107,255,0.3)", borderRadius: 8 }} className="btn-action">↻ Refresh</button>
        </div>
      </div>

      {/* Summary Stats */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(8, 1fr)", gap: 12, marginBottom: 20 }}>
        {[
          { label: "Total", value: summary.total || contacts.length, icon: "👥", color: "#4f6bff" },
          { label: "With Phone", value: summary.with_phone || 0, icon: "📱", color: "#34d399" },
          { label: "With Email", value: summary.with_email || 0, icon: "📧", color: "#22d3ee" },
          { label: "With Address", value: summary.with_address || 0, icon: "🏠", color: "#a855f7" },
          { label: "No Attorney", value: summary.no_attorney || 0, icon: "⚡", color: "#fbbf24" },
          { label: "Injured", value: summary.injured || 0, icon: "🩹", color: "#ff7b3a" },
          { label: "Not Contacted", value: summary.not_contacted || 0, icon: "📞", color: "#ff4757" },
          { label: "Avg Enrichment", value: (summary.avg_enrichment || 0) + "%", icon: "📊", color: "#e040fb" },
        ].map((s, i) => (
          <div key={i} style={{ background: "rgba(21,29,50,0.8)", borderRadius: 10, padding: "12px 14px", border: "1px solid rgba(28,43,77,0.5)", textAlign: "center" }}>
            <div style={{ fontSize: 18, marginBottom: 4 }}>{s.icon}</div>
            <div style={{ fontSize: 18, fontWeight: 800, color: s.color }}>{s.value}</div>
            <div style={{ fontSize: 9, color: "#5e739e", textTransform: "uppercase", letterSpacing: "0.5px", fontWeight: 600 }}>{s.label}</div>
          </div>
        ))}
      </div>

      {/* Filter Bar */}
      <div style={{ display: "flex", gap: 8, marginBottom: 16, flexWrap: "wrap", alignItems: "center" }}>
        <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="🔍 Search name, phone, email..."
          style={{ ...inputStyle, width: 280, marginBottom: 0, borderRadius: 8, padding: "8px 14px", fontSize: 13 }} />
        {[
          { key: "has_any_contact", label: "✓ Has Contact Info", val: "true" },
          { key: "has_phone", label: "Has Phone", val: "true" },
          { key: "has_email", label: "Has Email", val: "true" },
          { key: "has_address", label: "Has Address", val: "true" },
          { key: "has_attorney", label: "No Attorney", val: "false" },
          { key: "is_injured", label: "Injured", val: "true" },
          { key: "contact_status", label: "Not Contacted", val: "not_contacted" },
        ].map(f => {
          const active = filters[f.key] === f.val;
          return (
            <button key={f.key} onClick={() => setFilters(prev => ({ ...prev, [f.key]: active ? undefined : f.val }))}
              style={{ ...btnSmall, background: active ? "rgba(79,107,255,0.25)" : "rgba(21,29,50,0.8)", color: active ? "#4f6bff" : "#a0b0d0", border: `1px solid ${active ? "rgba(79,107,255,0.4)" : "rgba(28,43,77,0.5)"}`, borderRadius: 20, fontSize: 11, padding: "5px 12px" }}>
              {f.label}
            </button>
          );
        })}
      </div>

      {/* Contact Table */}
      <div style={{ background: "rgba(21,29,50,0.5)", borderRadius: 12, border: "1px solid rgba(28,43,77,0.5)", overflow: "hidden" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid rgba(28,43,77,0.5)" }}>
              {["Name", "Phone", "Email", "Accident Date", "Location", "Incident", "Injury", "Attorney", "Enrichment", "Status", "Actions"].map(h => (
                <th key={h} style={{ padding: "10px 8px", fontSize: 10, fontWeight: 700, color: "#5e739e", textTransform: "uppercase", letterSpacing: "0.5px", textAlign: "left" }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filteredContacts.map(c => (
              <React.Fragment key={c.id}>
                <tr onClick={() => setExpandedId(expandedId === c.id ? null : c.id)}
                  style={{ borderBottom: "1px solid rgba(28,43,77,0.3)", cursor: "pointer", transition: "background 0.15s" }}
                  className="table-row">
                  <td style={tdStyle}>
                    <div style={{ fontWeight: 600, color: "#f4f7ff" }}>{c.display_name || `${c.first_name || ""} ${c.last_name || ""}`.trim() || "Unknown"}</div>
                    <div style={{ fontSize: 11, color: "#5e739e" }}>{c.role || ""} {c.age ? `• Age ${c.age}` : ""}</div>
                  </td>
                  <td style={tdStyle}>
                    {c.phone ? <span style={{ color: "#34d399" }}>{c.phone}</span> : <span style={{ color: "#5e739e" }}>—</span>}
                    {c.phone_verified && <span title="Verified" style={{ marginLeft: 4 }}>✓</span>}
                  </td>
                  <td style={tdStyle}>
                    {c.email ? <span style={{ color: "#22d3ee", fontSize: 12 }}>{c.email}</span> : <span style={{ color: "#5e739e" }}>—</span>}
                  </td>
                  <td style={tdStyle}>
                    {(c.incident_occurred_at || c.occurred_at || c.incident_discovered_at) ? (
                      <div>
                        <div style={{ color: "#fbbf24", fontSize: 11, fontWeight: 700, fontFamily: "monospace" }}>
                          {formatAccidentDateTime(c.incident_occurred_at || c.occurred_at || c.incident_discovered_at)}
                        </div>
                        <div style={{ color: "#5e739e", fontSize: 9 }}>
                          {formatAccidentDate(c.incident_occurred_at || c.occurred_at || c.incident_discovered_at)}
                        </div>
                      </div>
                    ) : <span style={{ color: "#5e739e" }}>—</span>}
                  </td>
                  <td style={tdStyle}>
                    <span style={{ color: "#a0b0d0", fontSize: 12 }}>{c.incident_city || c.city || ""}{c.incident_state || c.state ? `, ${c.incident_state || c.state}` : ""}</span>
                  </td>
                  <td style={tdStyle}>
                    <span style={{ fontSize: 11, color: "#a0b0d0" }}>{c.incident_number || ""}</span>
                    {c.incident_type && <div><span style={{ ...badgeBase, fontSize: 9, padding: "2px 6px", background: "rgba(168,85,247,0.15)", color: "#c4b5fd" }}>{formatType(c.incident_type)}</span></div>}
                  </td>
                  <td style={tdStyle}>
                    {c.is_injured ? <span style={{ color: "#ff7b3a" }}>{c.injury_description || "Injured"}</span> : <span style={{ color: "#5e739e" }}>None</span>}
                  </td>
                  <td style={tdStyle}>
                    {c.has_attorney ? <span style={{ ...badgeBase, fontSize: 9, background: "rgba(255,71,87,0.2)", color: "#ff7b9c", border: "1px solid rgba(255,71,87,0.3)" }}>Has Atty</span> :
                      <span style={{ ...badgeBase, fontSize: 9, background: "rgba(52,211,153,0.2)", color: "#6ee7b7", border: "1px solid rgba(52,211,153,0.3)" }}>No Atty ⚡</span>}
                  </td>
                  <td style={tdStyle}>
                    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                      {contactQualityBar(c.enrichment_score || c.contact_quality || 0)}
                      <span style={{ fontSize: 11, color: enrichScoreColor(c.enrichment_score), fontWeight: 700 }}>{Math.round(c.enrichment_score || c.contact_quality || 0)}</span>
                    </div>
                  </td>
                  <td style={tdStyle}>
                    {(() => { const cs = contactStatusColor(c.contact_status); return <span style={{ ...badgeBase, fontSize: 9, background: cs.bg, color: cs.fg }}>{(c.contact_status || "not_contacted").replace(/_/g, " ")}</span>; })()}
                  </td>
                  <td style={tdStyle}>
                    <button onClick={(e) => { e.stopPropagation(); onEnrich(c.id); }}
                      style={{ ...btnSmall, background: "rgba(79,107,255,0.15)", color: "#4f6bff", border: "1px solid rgba(79,107,255,0.3)", fontSize: 10, padding: "4px 10px", borderRadius: 6 }}
                      disabled={enriching}>🔍 Enrich</button>
                  </td>
                </tr>
                {expandedId === c.id && (
                  <tr><td colSpan={10} style={{ padding: 0 }}>
                    <div style={{ background: "rgba(13,18,37,0.6)", padding: "16px 24px", borderBottom: "2px solid rgba(79,107,255,0.2)", animation: "slideUp 0.2s ease" }}>
                      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 16 }}>
                        <div>
                          <div style={{ fontSize: 10, color: "#5e739e", fontWeight: 600, textTransform: "uppercase", marginBottom: 4 }}>Full Address</div>
                          <div style={{ color: "#f4f7ff", fontSize: 13 }}>{c.address || "—"}</div>
                          <div style={{ color: "#a0b0d0", fontSize: 12 }}>{c.city || ""} {c.state || ""} {c.zip || ""}</div>
                        </div>
                        <div>
                          <div style={{ fontSize: 10, color: "#5e739e", fontWeight: 600, textTransform: "uppercase", marginBottom: 4 }}>Employment</div>
                          <div style={{ color: "#f4f7ff", fontSize: 13 }}>{c.employer || "—"}</div>
                          <div style={{ color: "#a0b0d0", fontSize: 12 }}>{c.occupation || ""}</div>
                        </div>
                        <div>
                          <div style={{ fontSize: 10, color: "#5e739e", fontWeight: 600, textTransform: "uppercase", marginBottom: 4 }}>Insurance</div>
                          <div style={{ color: "#f4f7ff", fontSize: 13 }}>{c.insurance_company || "—"}</div>
                          <div style={{ color: "#a0b0d0", fontSize: 12 }}>Limits: {c.policy_limits || "—"}</div>
                        </div>
                        <div>
                          <div style={{ fontSize: 10, color: "#5e739e", fontWeight: 600, textTransform: "uppercase", marginBottom: 4 }}>Medical</div>
                          <div style={{ color: "#f4f7ff", fontSize: 13 }}>{c.transported_to || "—"}</div>
                          <div style={{ color: "#a0b0d0", fontSize: 12 }}>{c.injury_description || ""}</div>
                        </div>
                      </div>
                      {c.enrichment_sources && c.enrichment_sources.length > 0 && (
                        <div style={{ marginTop: 12, display: "flex", gap: 6, alignItems: "center" }}>
                          <span style={{ fontSize: 10, color: "#5e739e", fontWeight: 600 }}>SOURCES:</span>
                          {(Array.isArray(c.enrichment_sources) ? c.enrichment_sources : []).map((s, i) => (
                            <span key={i} style={{ ...badgeBase, fontSize: 9, padding: "2px 6px", background: "rgba(79,107,255,0.15)", color: "#93c5fd", border: "1px solid rgba(79,107,255,0.2)" }}>{s}</span>
                          ))}
                        </div>
                      )}
                      {c.attorney_name && (
                        <div style={{ marginTop: 8, color: "#ff7b9c", fontSize: 12 }}>Attorney: {c.attorney_name}</div>
                      )}
                    </div>
                  </td></tr>
                )}
              </React.Fragment>
            ))}
          </tbody>
        </table>
        {filteredContacts.length === 0 && (
          <div style={{ textAlign: "center", padding: 40, color: "#5e739e" }}>
            <div style={{ fontSize: 40, marginBottom: 8 }}>📋</div>
            <div style={{ fontSize: 14, fontWeight: 600 }}>No contacts found</div>
            <div style={{ fontSize: 12, marginTop: 4 }}>Try adjusting filters or run enrichment to populate contact data</div>
          </div>
        )}
      </div>
    </div>
  );
}

// ============================================================================
// INTEGRATIONS VIEW — Full integration management dashboard
// ============================================================================
function IntegrationsView({ integrations, stats, onAction, onRefresh }) {
  const [expandedSlug, setExpandedSlug] = useState(null);
  const [apiKeyInputs, setApiKeyInputs] = useState({});
  const [showKeys, setShowKeys] = useState({});
  const [testResults, setTestResults] = useState({});

  const categoryLabels = {
    crash_data: { label: "🚗 Crash & Accident Data", desc: "Official crash databases — same sources LexisNexis and insurance companies pull from" },
    real_time: { label: "🚨 Real-Time Feeds", desc: "Live 911 dispatch, traffic incidents, and emergency response data" },
    news: { label: "📰 News Sources", desc: "Accident news aggregation from thousands of sources" },
    contact_enrichment: { label: "👤 Contact Enrichment", desc: "Find phones, emails, addresses, employers — same data as skip-tracing services" },
    public_records: { label: "🏛️ Public Records", desc: "Court records, property ownership, business registrations — same sources as LexisNexis" },
    vehicle: { label: "🔎 Vehicle & VIN Data", desc: "VIN decoding, theft/salvage history, safety recalls" },
    conditions: { label: "🌧️ Weather & Conditions", desc: "Weather conditions at time of accident for case building" },
    geocoding: { label: "🗺️ Mapping & Geocoding", desc: "Address resolution and location intelligence" },
    skip_trace: { label: "🕵️ Skip Tracing", desc: "Professional people search — phones, addresses, assets, associates" },
  };

  const grouped = {};
  integrations.forEach(i => {
    if (!grouped[i.category]) grouped[i.category] = [];
    grouped[i.category].push(i);
  });

  const statusColors = {
    active: { bg: "rgba(52,211,153,0.2)", fg: "#6ee7b7", border: "rgba(52,211,153,0.3)" },
    ready: { bg: "rgba(251,191,36,0.2)", fg: "#ffd966", border: "rgba(251,191,36,0.3)" },
    error: { bg: "rgba(255,71,87,0.2)", fg: "#ff7b9c", border: "rgba(255,71,87,0.3)" },
    disconnected: { bg: "rgba(28,43,77,0.5)", fg: "#5e739e", border: "rgba(28,43,77,0.5)" },
    paused: { bg: "rgba(28,43,77,0.5)", fg: "#a0b0d0", border: "rgba(28,43,77,0.5)" },
  };

  const handleTest = async (id) => {
    setTestResults(prev => ({ ...prev, [id]: { testing: true } }));
    const result = await onAction(id, "test", apiKeyInputs[id]);
    setTestResults(prev => ({ ...prev, [id]: result }));
  };

  return (
    <div style={{ animation: "fadeIn 0.3s ease" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <div>
          <h2 style={{ margin: 0, fontSize: 22, fontWeight: 800, display: "flex", alignItems: "center", gap: 10 }}>
            <span style={{ fontSize: 24 }}>⚙</span> Integrations Hub
          </h2>
          <p style={{ margin: "4px 0 0", color: "#a0b0d0", fontSize: 13 }}>Connect data sources, enrich contacts, cross-reference everything</p>
        </div>
        <button onClick={onRefresh} style={{ ...btnSmall, background: "rgba(79,107,255,0.15)", color: "#4f6bff", border: "1px solid rgba(79,107,255,0.3)", borderRadius: 8 }} className="btn-action">↻ Refresh</button>
      </div>

      {/* Stats Banner */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 12, marginBottom: 24 }}>
        {[
          { label: "Total Sources", value: stats.total || integrations.length, icon: "🔌", color: "#4f6bff" },
          { label: "Connected", value: stats.connected || 0, icon: "✅", color: "#34d399" },
          { label: "Active", value: stats.active || 0, icon: "⚡", color: "#fbbf24" },
          { label: "Free Sources", value: stats.free || 0, icon: "🆓", color: "#22d3ee" },
          { label: "Monthly Cost", value: "$" + (stats.total_monthly_cost || 0).toFixed(0), icon: "💰", color: "#e040fb" },
        ].map((s, i) => (
          <div key={i} style={{ background: "rgba(21,29,50,0.8)", borderRadius: 10, padding: "14px 16px", border: "1px solid rgba(28,43,77,0.5)", textAlign: "center" }}>
            <div style={{ fontSize: 20 }}>{s.icon}</div>
            <div style={{ fontSize: 22, fontWeight: 800, color: s.color }}>{s.value}</div>
            <div style={{ fontSize: 9, color: "#5e739e", textTransform: "uppercase", letterSpacing: "0.5px", fontWeight: 600 }}>{s.label}</div>
          </div>
        ))}
      </div>

      {/* Quick Connect Free Sources */}
      <div style={{ background: "linear-gradient(135deg, rgba(52,211,153,0.1), rgba(34,211,238,0.1))", borderRadius: 12, padding: "16px 20px", marginBottom: 24, border: "1px solid rgba(52,211,153,0.2)" }}>
        <h3 style={{ margin: "0 0 10px", fontSize: 14, fontWeight: 700, color: "#34d399" }}>🆓 Quick-Connect Free Sources (No API Key Required)</h3>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          {integrations.filter(i => i.is_free && i.auth_type === "none" && i.status !== "active").map(i => (
            <button key={i.id} onClick={() => onAction(i.id, "connect")}
              style={{ ...btnSmall, background: "rgba(52,211,153,0.15)", color: "#6ee7b7", border: "1px solid rgba(52,211,153,0.3)", borderRadius: 20, fontSize: 11, padding: "5px 14px" }}
              className="btn-action">
              {i.icon} {i.name} →
            </button>
          ))}
          {integrations.filter(i => i.is_free && i.auth_type === "none" && i.status !== "active").length === 0 && (
            <span style={{ color: "#6ee7b7", fontSize: 12 }}>✓ All free no-key sources are connected!</span>
          )}
        </div>
      </div>

      {/* Integration Categories */}
      {Object.entries(categoryLabels).map(([cat, { label, desc }]) => {
        const items = grouped[cat] || [];
        if (items.length === 0) return null;
        return (
          <div key={cat} style={{ marginBottom: 24 }}>
            <h3 style={{ margin: "0 0 4px", fontSize: 16, fontWeight: 700, color: "#f4f7ff" }}>{label}</h3>
            <p style={{ margin: "0 0 12px", fontSize: 12, color: "#5e739e" }}>{desc}</p>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: 12 }}>
              {items.map(intg => {
                const sc = statusColors[intg.status] || statusColors.disconnected;
                const expanded = expandedSlug === intg.slug;
                const testRes = testResults[intg.id];
                return (
                  <div key={intg.id} style={{
                    background: "rgba(21,29,50,0.8)", borderRadius: 12, border: `1px solid ${intg.status === "active" ? "rgba(52,211,153,0.3)" : "rgba(28,43,77,0.5)"}`,
                    overflow: "hidden", transition: "border-color 0.3s"
                  }}>
                    <div style={{ padding: "14px 16px", display: "flex", justifyContent: "space-between", alignItems: "flex-start", cursor: "pointer" }}
                      onClick={() => setExpandedSlug(expanded ? null : intg.slug)}>
                      <div style={{ display: "flex", gap: 12, alignItems: "flex-start", flex: 1 }}>
                        <div style={{ fontSize: 24, minWidth: 32, textAlign: "center" }}>{intg.icon}</div>
                        <div style={{ flex: 1 }}>
                          <div style={{ fontWeight: 700, fontSize: 14, color: "#f4f7ff", marginBottom: 2 }}>{intg.name}</div>
                          <div style={{ fontSize: 11, color: "#a0b0d0", lineHeight: 1.4 }}>{intg.description && intg.description.substring(0, 80)}...</div>
                          <div style={{ display: "flex", gap: 6, marginTop: 6, flexWrap: "wrap" }}>
                            <span style={{ ...badgeBase, fontSize: 9, padding: "2px 8px", background: sc.bg, color: sc.fg, border: `1px solid ${sc.border}` }}>{intg.status}</span>
                            {intg.is_free ? <span style={{ ...badgeBase, fontSize: 9, padding: "2px 8px", background: "rgba(52,211,153,0.15)", color: "#6ee7b7" }}>FREE</span> :
                              <span style={{ ...badgeBase, fontSize: 9, padding: "2px 8px", background: "rgba(251,191,36,0.15)", color: "#ffd966" }}>${parseFloat(intg.monthly_cost || 0)}/mo</span>}
                            {intg.auth_type === "none" && <span style={{ ...badgeBase, fontSize: 9, padding: "2px 8px", background: "rgba(34,211,238,0.15)", color: "#67e8f9" }}>No Key</span>}
                          </div>
                        </div>
                      </div>
                      <span style={{ color: "#5e739e", fontSize: 18, transform: expanded ? "rotate(180deg)" : "rotate(0)", transition: "transform 0.2s" }}>▾</span>
                    </div>

                    {expanded && (
                      <div style={{ padding: "0 16px 16px", borderTop: "1px solid rgba(28,43,77,0.5)", paddingTop: 12, animation: "slideUp 0.2s ease" }}>
                        <div style={{ fontSize: 12, color: "#a0b0d0", marginBottom: 10, lineHeight: 1.5 }}>{intg.description}</div>

                        {intg.api_base_url && (
                          <div style={{ fontSize: 11, color: "#5e739e", marginBottom: 8 }}>
                            <strong>API:</strong> <span style={{ fontFamily: "monospace", color: "#22d3ee" }}>{intg.api_base_url}</span>
                          </div>
                        )}

                        {intg.auth_type !== "none" && (
                          <div style={{ marginBottom: 10 }}>
                            <label style={{ fontSize: 10, color: "#5e739e", fontWeight: 600, textTransform: "uppercase", display: "block", marginBottom: 4 }}>API Key</label>
                            <div style={{ display: "flex", gap: 6 }}>
                              <input type={showKeys[intg.id] ? "text" : "password"} placeholder="Paste your API key here..."
                                value={apiKeyInputs[intg.id] || ""}
                                onChange={(e) => setApiKeyInputs(prev => ({ ...prev, [intg.id]: e.target.value }))}
                                style={{ ...inputStyle, marginBottom: 0, borderRadius: 8, fontSize: 12, flex: 1 }} />
                              <button onClick={() => setShowKeys(prev => ({ ...prev, [intg.id]: !prev[intg.id] }))}
                                style={{ ...btnSmall, background: "rgba(28,43,77,0.5)", color: "#a0b0d0", border: "1px solid rgba(28,43,77,0.5)", borderRadius: 8, fontSize: 10, padding: "6px 10px" }}>
                                {showKeys[intg.id] ? "🙈" : "👁"}
                              </button>
                            </div>
                          </div>
                        )}

                        {/* Stats if active */}
                        {intg.status === "active" && (
                          <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 8, marginBottom: 10 }}>
                            <div style={{ background: "rgba(13,18,37,0.6)", borderRadius: 6, padding: "8px 10px", textAlign: "center" }}>
                              <div style={{ fontSize: 14, fontWeight: 700, color: "#4f6bff" }}>{intg.requests_today || 0}</div>
                              <div style={{ fontSize: 9, color: "#5e739e" }}>REQ TODAY</div>
                            </div>
                            <div style={{ background: "rgba(13,18,37,0.6)", borderRadius: 6, padding: "8px 10px", textAlign: "center" }}>
                              <div style={{ fontSize: 14, fontWeight: 700, color: "#34d399" }}>{intg.total_records_fetched || 0}</div>
                              <div style={{ fontSize: 9, color: "#5e739e" }}>RECORDS</div>
                            </div>
                            <div style={{ background: "rgba(13,18,37,0.6)", borderRadius: 6, padding: "8px 10px", textAlign: "center" }}>
                              <div style={{ fontSize: 14, fontWeight: 700, color: "#fbbf24" }}>{intg.last_success_at ? formatTime(intg.last_success_at) : "Never"}</div>
                              <div style={{ fontSize: 9, color: "#5e739e" }}>LAST SUCCESS</div>
                            </div>
                          </div>
                        )}

                        {intg.last_error && (
                          <div style={{ background: "rgba(255,71,87,0.1)", borderRadius: 6, padding: "8px 12px", marginBottom: 10, fontSize: 11, color: "#ff7b9c" }}>
                            ⚠ Error: {intg.last_error}
                          </div>
                        )}

                        {testRes && !testRes.testing && (
                          <div style={{ background: testRes.success ? "rgba(52,211,153,0.1)" : "rgba(255,71,87,0.1)", borderRadius: 6, padding: "8px 12px", marginBottom: 10, fontSize: 11, color: testRes.success ? "#6ee7b7" : "#ff7b9c" }}>
                            {testRes.success ? "✓ Connection test passed!" : `✗ Test failed: ${testRes.error || testRes.message}`}
                          </div>
                        )}

                        {/* Action Buttons */}
                        <div style={{ display: "flex", gap: 8 }}>
                          {intg.status !== "active" ? (
                            <button onClick={() => onAction(intg.id, "connect", apiKeyInputs[intg.id])}
                              style={{ ...btnSmall, background: "linear-gradient(135deg, #34d399, #22d3ee)", color: "#0b0f1a", border: "none", fontWeight: 700, borderRadius: 8, padding: "8px 16px" }}
                              className="btn-action">⚡ Connect</button>
                          ) : (
                            <button onClick={() => onAction(intg.id, "disconnect")}
                              style={{ ...btnSmall, background: "rgba(255,71,87,0.15)", color: "#ff7b9c", border: "1px solid rgba(255,71,87,0.3)", borderRadius: 8, padding: "8px 16px" }}
                              className="btn-action">Disconnect</button>
                          )}
                          <button onClick={() => handleTest(intg.id)}
                            disabled={testRes?.testing}
                            style={{ ...btnSmall, background: "rgba(79,107,255,0.15)", color: "#4f6bff", border: "1px solid rgba(79,107,255,0.3)", borderRadius: 8, padding: "8px 16px" }}
                            className="btn-action">{testRes?.testing ? "⏳ Testing..." : "🧪 Test"}</button>
                          {intg.auth_type !== "none" && apiKeyInputs[intg.id] && (
                            <button onClick={() => onAction(intg.id, "update_config", apiKeyInputs[intg.id])}
                              style={{ ...btnSmall, background: "rgba(168,85,247,0.15)", color: "#c4b5fd", border: "1px solid rgba(168,85,247,0.3)", borderRadius: 8, padding: "8px 16px" }}
                              className="btn-action">💾 Save Key</button>
                          )}
                        </div>

                        {/* Config JSON */}
                        {intg.config_json && intg.config_json !== "{}" && (() => {
                          try {
                            const cfg = JSON.parse(intg.config_json);
                            return Object.keys(cfg).length > 0 ? (
                              <div style={{ marginTop: 10, fontSize: 11, color: "#5e739e" }}>
                                {Object.entries(cfg).map(([k, v]) => (
                                  <div key={k}><strong>{k}:</strong> {String(v)}</div>
                                ))}
                              </div>
                            ) : null;
                          } catch { return null; }
                        })()}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        );
      })}
    </div>
  );
}
