import { useState, useEffect, useCallback, useRef } from "react";

const API = "/api/v1";

// ============================================================================
// ANIMATED STYLES (injected as <style> tag)
// ============================================================================
const GLOBAL_CSS = `
@keyframes gradientShift {
  0% { background-position: 0% 50%; }
  50% { background-position: 100% 50%; }
  100% { background-position: 0% 50%; }
}
@keyframes pulseGlow {
  0%, 100% { box-shadow: 0 0 15px rgba(245,158,11,0.15); }
  50% { box-shadow: 0 0 30px rgba(245,158,11,0.35); }
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
  0%, 100% { border-color: rgba(245,158,11,0.2); }
  50% { border-color: rgba(245,158,11,0.6); }
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
.incident-row:hover { background: rgba(245,158,11,0.05) !important; transform: translateX(4px); }
.nav-link { transition: all 0.2s ease; position: relative; }
.nav-link:hover { color: #f59e0b !important; }
.nav-link.active::after { content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 2px; background: linear-gradient(90deg, #f59e0b, #f97316); border-radius: 1px; }
.sidebar-item { transition: all 0.2s ease; }
.sidebar-item:focus { border-color: #f59e0b !important; box-shadow: 0 0 0 2px rgba(245,158,11,0.15); }
.btn-action { transition: all 0.2s ease; }
.btn-action:hover { transform: translateY(-1px); filter: brightness(1.15); }
.detail-panel { animation: slideInRight 0.3s ease; }
@keyframes slideInRight {
  from { opacity: 0; transform: translateX(40px); }
  to { opacity: 1; transform: translateX(0); }
}
.table-row { transition: all 0.15s ease; }
.table-row:hover { background: rgba(245,158,11,0.04) !important; }
::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: #0a0e1a; }
::-webkit-scrollbar-thumb { background: #1e293b; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: #334155; }
* { scrollbar-width: thin; scrollbar-color: #1e293b #0a0e1a; }
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
      localStorage.removeItem("aip_token");
      window.location.reload();
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
  const [page, setPage] = useState("dashboard");
  const [incidents, setIncidents] = useState([]);
  const [stats, setStats] = useState(null);
  const [selectedIncident, setSelectedIncident] = useState(null);
  const [filters, setFilters] = useState({});
  const [metros, setMetros] = useState([]);
  const [notifications, setNotifs] = useState([]);
  const [loading, setLoading] = useState(false);

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

  // Load data
  const loadData = useCallback(async () => {
    if (!user) return;
    setLoading(true);
    const params = new URLSearchParams(
      Object.entries(filters).filter(([, v]) => v)
    );
    const [incData, statsData, notifData] = await Promise.all([
      api(`/incidents?${params}&limit=100`),
      api(`/dashboard/stats?period=${filters.period || "today"}&metro=${filters.metro || ""}`),
      api("/alerts/notifications?unreadOnly=true"),
    ]);
    setIncidents(incData.data || []);
    setStats(statsData);
    setNotifs(notifData.data || []);
    setLoading(false);
  }, [user, filters]);

  useEffect(() => { loadData(); }, [loadData]);

  // Auto-refresh every 30s
  useEffect(() => {
    if (!user) return;
    const interval = setInterval(loadData, 30000);
    return () => clearInterval(interval);
  }, [user, loadData]);

  if (!user) return <LoginScreen onLogin={setUser} />;

  return (
    <div style={{ minHeight: "100vh", background: "#060914", color: "#e2e8f0", fontFamily: "'Inter', system-ui, -apple-system, sans-serif" }}>
      <NavBar user={user} page={page} setPage={setPage} notifications={notifications} onLogout={() => { localStorage.removeItem("aip_token"); setUser(null); }} />

      <div style={{ display: "flex" }}>
        <Sidebar filters={filters} setFilters={setFilters} metros={metros} onRefresh={loadData} />
        <main style={{ flex: 1, padding: "24px 28px", overflow: "auto", maxHeight: "calc(100vh - 64px)" }}>
          {page === "dashboard" && <DashboardView stats={stats} incidents={incidents} onSelect={setSelectedIncident} loading={loading} />}
          {page === "incidents" && <IncidentList incidents={incidents} onSelect={setSelectedIncident} filters={filters} setFilters={setFilters} />}
          {page === "my-leads" && <MyLeads user={user} onSelect={setSelectedIncident} />}
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
      background: "linear-gradient(-45deg, #060914, #0c1220, #111827, #0c1a2e, #060914)",
      backgroundSize: "400% 400%", animation: "gradientShift 15s ease infinite"
    }}>
      <div style={{
        background: "rgba(20,24,41,0.85)", backdropFilter: "blur(20px)", borderRadius: 20, padding: 52, width: 440,
        boxShadow: "0 25px 80px rgba(0,0,0,0.6), 0 0 60px rgba(245,158,11,0.08)",
        border: "1px solid rgba(245,158,11,0.1)", animation: "slideUp 0.6s ease"
      }}>
        <div style={{ textAlign: "center", marginBottom: 36 }}>
          {/* Donovan Digital Solutions Brand */}
          <div style={{
            width: 64, height: 64, margin: "0 auto 16px", borderRadius: 16,
            background: "linear-gradient(135deg, #f59e0b, #f97316)", display: "flex", alignItems: "center", justifyContent: "center",
            boxShadow: "0 8px 32px rgba(245,158,11,0.3)", animation: "pulseGlow 3s ease-in-out infinite"
          }}>
            <span style={{ fontSize: 32, filter: "drop-shadow(0 2px 4px rgba(0,0,0,0.3))" }}>&#x1F6A8;</span>
          </div>
          <h1 style={{ fontSize: 26, fontWeight: 800, color: "#fff", margin: 0, letterSpacing: "-0.5px" }}>INCIDENT COMMAND</h1>
          <p style={{ color: "#f59e0b", margin: "6px 0 0", fontSize: 12, fontWeight: 600, letterSpacing: "2px", textTransform: "uppercase" }}>Donovan Digital Solutions</p>
          <p style={{ color: "#64748b", margin: "8px 0 0", fontSize: 13 }}>Accident Intelligence Platform</p>
        </div>
        <form onSubmit={handleLogin}>
          {error && <div style={{ background: "rgba(220,38,38,0.15)", border: "1px solid rgba(220,38,38,0.3)", color: "#fca5a5", padding: "10px 14px", borderRadius: 10, marginBottom: 16, fontSize: 13 }}>{error}</div>}
          <input value={email} onChange={(e) => setEmail(e.target.value)} placeholder="Email" type="email" required style={inputStyle} className="sidebar-item" />
          <input value={pass} onChange={(e) => setPass(e.target.value)} placeholder="Password" type="password" required style={{ ...inputStyle, marginTop: 12 }} className="sidebar-item" />
          <button type="submit" style={{
            ...btnPrimary,
            background: "linear-gradient(135deg, #f59e0b, #f97316)",
            borderRadius: 10, fontSize: 15, fontWeight: 700, letterSpacing: "0.5px"
          }} className="btn-action">Sign In</button>
        </form>
        <div style={{ textAlign: "center", marginTop: 24 }}>
          <a href="https://donovan-ai-growth-platform.vercel.app" target="_blank" rel="noopener noreferrer" style={{ color: "#64748b", fontSize: 12, textDecoration: "none" }}>
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
      borderBottom: "1px solid rgba(245,158,11,0.1)", display: "flex", alignItems: "center",
      justifyContent: "space-between", padding: "0 28px", position: "sticky", top: 0, zIndex: 100
    }}>
      {/* Top gradient accent line */}
      <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 2, background: "linear-gradient(90deg, #f59e0b, #f97316, #ef4444, #f97316, #f59e0b)", backgroundSize: "200% 100%", animation: "shimmer 4s linear infinite" }} />

      <div style={{ display: "flex", alignItems: "center", gap: 32 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{
            width: 32, height: 32, borderRadius: 8, background: "linear-gradient(135deg, #f59e0b, #f97316)",
            display: "flex", alignItems: "center", justifyContent: "center", fontSize: 16
          }}>&#x1F6A8;</div>
          <div>
            <div style={{ fontSize: 16, fontWeight: 800, color: "#fff", letterSpacing: "-0.3px", lineHeight: 1 }}>INCIDENT COMMAND</div>
            <div style={{ fontSize: 9, fontWeight: 600, color: "#f59e0b", letterSpacing: "1.5px", textTransform: "uppercase" }}>Donovan Digital</div>
          </div>
        </div>
        <div style={{ display: "flex", gap: 4 }}>
          {["dashboard", "incidents", "my-leads"].map((p) => (
            <button key={p} onClick={() => setPage(p)}
              className={`nav-link ${page === p ? "active" : ""}`}
              style={{
                background: page === p ? "rgba(245,158,11,0.1)" : "none", border: "none", cursor: "pointer",
                fontSize: 12, fontWeight: 600, letterSpacing: "0.5px", padding: "10px 16px", borderRadius: 8,
                color: page === p ? "#f59e0b" : "#94a3b8"
              }}>
              {p === "dashboard" && "\u25A3 "}
              {p === "incidents" && "\u26A0 "}
              {p === "my-leads" && "\u2605 "}
              {p.replace("-", " ").toUpperCase()}
            </button>
          ))}
        </div>
      </div>

      <div style={{ display: "flex", alignItems: "center", gap: 20 }}>
        {/* Platform switcher */}
        <a href="https://donovan-ai-growth-platform.vercel.app" target="_blank" rel="noopener noreferrer"
          style={{ color: "#64748b", fontSize: 11, textDecoration: "none", padding: "6px 12px", borderRadius: 6, border: "1px solid #1e293b", transition: "all 0.2s" }}
          className="btn-action">
          AI Growth Platform &rarr;
        </a>
        <span style={{ position: "relative", cursor: "pointer" }}>
          <span style={{ fontSize: 18 }}>&#x1F514;</span>
          {notifications.length > 0 && <span style={{
            position: "absolute", top: -6, right: -10, background: "linear-gradient(135deg, #dc2626, #ef4444)",
            color: "#fff", fontSize: 10, borderRadius: 10, padding: "1px 6px", fontWeight: 700, animation: "pulseGlow 2s ease-in-out infinite"
          }}>{notifications.length}</span>}
        </span>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{
            width: 32, height: 32, borderRadius: 8, background: "linear-gradient(135deg, #1e293b, #334155)",
            display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14, color: "#f59e0b", fontWeight: 700
          }}>{(user.firstName || "U")[0]}</div>
          <div>
            <div style={{ color: "#e2e8f0", fontSize: 13, fontWeight: 600 }}>{user.firstName} {user.lastName}</div>
            <div style={{ color: "#64748b", fontSize: 10 }}>{user.role || "Agent"}</div>
          </div>
        </div>
        <button onClick={onLogout} className="btn-action" style={{ background: "rgba(100,116,139,0.1)", border: "1px solid #1e293b", color: "#64748b", cursor: "pointer", fontSize: 12, padding: "6px 12px", borderRadius: 6 }}>Logout</button>
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
      borderRight: "1px solid rgba(30,41,59,0.5)", padding: "24px 18px", minHeight: "calc(100vh - 64px)"
    }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 20 }}>
        <h3 style={{ color: "#f59e0b", fontSize: 11, textTransform: "uppercase", letterSpacing: "2px", margin: 0, fontWeight: 700 }}>Filters</h3>
        <div style={{ width: 6, height: 6, borderRadius: 3, background: "#22c55e", animation: "liveIndicator 2s ease-in-out infinite" }} />
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
        <label style={{ display: "flex", alignItems: "center", gap: 8, color: "#94a3b8", fontSize: 13, cursor: "pointer" }}>
          <input type="checkbox" checked={!!filters.hasAttorney} onChange={(e) => update("hasAttorney", e.target.checked ? "false" : "")} style={{ accentColor: "#f59e0b" }} />
          No Attorney
        </label>
      </div>

      <button onClick={onRefresh} className="btn-action" style={{
        width: "100%", background: "linear-gradient(135deg, #f59e0b, #f97316)", color: "#0a0e1a",
        border: "none", borderRadius: 10, padding: "10px 12px", fontWeight: 700, fontSize: 13,
        cursor: "pointer", marginTop: 20, letterSpacing: "0.5px"
      }}>&#x21BB; Refresh Data</button>

      {/* Branding footer */}
      <div style={{ marginTop: "auto", paddingTop: 32, textAlign: "center" }}>
        <div style={{ width: "100%", height: 1, background: "linear-gradient(90deg, transparent, #1e293b, transparent)", marginBottom: 16 }} />
        <div style={{ fontSize: 9, color: "#475569", letterSpacing: "1px", textTransform: "uppercase" }}>Powered by</div>
        <div style={{ fontSize: 11, color: "#64748b", fontWeight: 600, marginTop: 2 }}>Donovan Digital Solutions</div>
      </div>
    </aside>
  );
}

// ============================================================================
// DASHBOARD VIEW — Color-changing KPIs, animated cards
// ============================================================================
function DashboardView({ stats, incidents, onSelect, loading }) {
  const [time, setTime] = useState(Date.now());
  useEffect(() => {
    const t = setInterval(() => setTime(Date.now()), 3000);
    return () => clearInterval(t);
  }, []);

  if (!stats) return (
    <div style={{ textAlign: "center", padding: 80 }}>
      <div style={{ width: 48, height: 48, border: "3px solid #1e293b", borderTopColor: "#f59e0b", borderRadius: "50%", margin: "0 auto 16px", animation: "gradientShift 1s linear infinite" }} />
      <div style={{ color: "#64748b", fontSize: 14 }}>Loading dashboard...</div>
    </div>
  );
  const t = stats.totals || {};

  // Color cycling for KPI cards
  const hueShift = (time / 50) % 360;
  const kpiConfigs = [
    { label: "Total Incidents", value: t.total_incidents || 0, gradient: "linear-gradient(135deg, #3b82f6, #6366f1)", icon: "\u25A3" },
    { label: "New (Unassigned)", value: t.new_incidents || 0, gradient: "linear-gradient(135deg, #f59e0b, #f97316)", icon: "\u2605" },
    { label: "Total Injuries", value: t.total_injuries || 0, gradient: "linear-gradient(135deg, #ef4444, #dc2626)", icon: "\u2764" },
    { label: "Fatalities", value: t.total_fatalities || 0, gradient: "linear-gradient(135deg, #dc2626, #991b1b)", icon: "\u26A0" },
    { label: "High Severity", value: t.high_severity_count || 0, gradient: "linear-gradient(135deg, #f97316, #ea580c)", icon: "\u26A1" },
    { label: "Active Reps", value: t.active_reps || 0, gradient: "linear-gradient(135deg, #22c55e, #16a34a)", icon: "\u263A" },
  ];

  return (
    <div style={{ animation: "fadeIn 0.4s ease" }}>
      {/* Dashboard Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24 }}>
        <div>
          <h1 style={{ color: "#f8fafc", margin: 0, fontSize: 24, fontWeight: 800, letterSpacing: "-0.5px" }}>
            Command Dashboard
          </h1>
          <p style={{ color: "#64748b", margin: "4px 0 0", fontSize: 13 }}>
            Real-time accident intelligence overview
          </p>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <div style={{ width: 8, height: 8, borderRadius: 4, background: "#22c55e", animation: "liveIndicator 2s ease-in-out infinite" }} />
          <span style={{ color: "#22c55e", fontSize: 12, fontWeight: 600 }}>LIVE</span>
          {loading && <span style={{ color: "#f59e0b", fontSize: 12 }}>&middot; Syncing...</span>}
        </div>
      </div>

      {/* KPI Cards — Animated gradient borders */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 16, marginBottom: 28 }}>
        {kpiConfigs.map((kpi, idx) => (
          <div key={kpi.label} className="kpi-card" style={{
            background: "#0c1220", borderRadius: 14, padding: "22px 20px",
            border: "1px solid rgba(30,41,59,0.6)", position: "relative", overflow: "hidden",
            animation: `slideUp ${0.3 + idx * 0.1}s ease`
          }}>
            {/* Subtle gradient top accent */}
            <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 3, background: kpi.gradient, opacity: 0.8 }} />
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
              <div>
                <div style={{ color: "#64748b", fontSize: 11, textTransform: "uppercase", letterSpacing: "1px", fontWeight: 600 }}>{kpi.label}</div>
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
              padding: "10px 0", borderBottom: "1px solid rgba(30,41,59,0.4)"
            }}>
              <span style={{ color: "#e2e8f0", fontSize: 13 }}>{m.metro || "Unknown"}</span>
              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <div style={{ width: 60, height: 4, background: "#1e293b", borderRadius: 2, overflow: "hidden" }}>
                  <div style={{
                    width: `${Math.min(100, (m.count / Math.max(...(stats.byMetro || []).map(x => x.count), 1)) * 100)}%`,
                    height: "100%", background: "linear-gradient(90deg, #f59e0b, #f97316)", borderRadius: 2
                  }} />
                </div>
                <span style={{ color: "#f59e0b", fontWeight: 700, fontSize: 14, minWidth: 24, textAlign: "right" }}>{m.count}</span>
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
              padding: "10px 0", borderBottom: "1px solid rgba(30,41,59,0.4)"
            }}>
              <span style={{ color: "#e2e8f0", fontSize: 13 }}>{formatType(t.incident_type)}</span>
              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <div style={{ width: 60, height: 4, background: "#1e293b", borderRadius: 2, overflow: "hidden" }}>
                  <div style={{
                    width: `${Math.min(100, (t.count / Math.max(...(stats.byType || []).map(x => x.count), 1)) * 100)}%`,
                    height: "100%", background: "linear-gradient(90deg, #3b82f6, #6366f1)", borderRadius: 2
                  }} />
                </div>
                <span style={{ color: "#3b82f6", fontWeight: 700, fontSize: 14, minWidth: 24, textAlign: "right" }}>{t.count}</span>
              </div>
            </div>
          ))}
          {(!stats.byType || stats.byType.length === 0) && <EmptyState text="No type data" />}
        </div>
      </div>

      {/* High Priority Feed */}
      <div style={{ ...enhancedCardStyle, borderColor: "rgba(239,68,68,0.15)" }}>
        <h3 style={enhancedCardTitle}>
          <span style={{ marginRight: 8, animation: "liveIndicator 1.5s ease-in-out infinite" }}>&#x1F525;</span>
          High Priority Incidents
          {loading && <span style={{ fontSize: 11, color: "#64748b", fontWeight: 400, marginLeft: 8 }}>(syncing...)</span>}
        </h3>
        <div>
          {(stats.recentHighPriority || []).map((inc) => (
            <IncidentRow key={inc.id} incident={inc} onSelect={onSelect} compact />
          ))}
          {(!stats.recentHighPriority || stats.recentHighPriority.length === 0) && <EmptyState text="No high-priority incidents in this period" />}
        </div>
      </div>

      {/* Recent Feed */}
      <div style={{ ...enhancedCardStyle, marginTop: 16 }}>
        <h3 style={enhancedCardTitle}>
          <span style={{ marginRight: 8 }}>&#x23F1;&#xFE0F;</span>Live Feed &mdash; Most Recent
        </h3>
        {incidents.slice(0, 20).map((inc) => (
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
function IncidentList({ incidents, onSelect, filters, setFilters }) {
  return (
    <div style={{ animation: "fadeIn 0.3s ease" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <div>
          <h2 style={{ color: "#f8fafc", margin: 0, fontSize: 22, fontWeight: 800 }}>All Incidents</h2>
          <p style={{ color: "#64748b", margin: "4px 0 0", fontSize: 13 }}>{incidents.length} total results</p>
        </div>
        <div style={{ position: "relative" }}>
          <input
            placeholder="Search address, report #, description..."
            value={filters.search || ""}
            onChange={(e) => setFilters((f) => ({ ...f, search: e.target.value }))}
            style={{ ...inputStyle, width: 340, paddingLeft: 36 }}
            className="sidebar-item"
          />
          <span style={{ position: "absolute", left: 12, top: "50%", transform: "translateY(-50%)", color: "#64748b", fontSize: 14 }}>&#x1F50D;</span>
        </div>
      </div>
      <div style={enhancedCardStyle}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "2px solid rgba(245,158,11,0.15)" }}>
              {["Priority", "Type", "Severity", "Location", "Time", "Injuries", "Sources", "Status", "Persons"].map((h) => (
                <th key={h} style={{ textAlign: "left", padding: "12px 8px", color: "#f59e0b", fontSize: 10, textTransform: "uppercase", letterSpacing: "1px", fontWeight: 700 }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {incidents.map((inc) => (
              <tr key={inc.id} onClick={() => onSelect(inc)} className="table-row"
                style={{ borderBottom: "1px solid rgba(30,41,59,0.4)", cursor: "pointer" }}>
                <td style={tdStyle}><PriorityBadge p={inc.priority} /></td>
                <td style={tdStyle}><TypeBadge type={inc.incident_type} /></td>
                <td style={tdStyle}><SeverityBadge severity={inc.severity} /></td>
                <td style={tdStyle}><span style={{ color: "#e2e8f0", fontSize: 13 }}>{inc.city}, {inc.state}</span><br /><span style={{ color: "#64748b", fontSize: 11 }}>{inc.address?.substring(0, 40)}</span></td>
                <td style={{ ...tdStyle, color: "#94a3b8", fontSize: 12 }}>{formatTime(inc.discovered_at)}</td>
                <td style={tdStyle}><span style={{ color: inc.injuries_count > 0 ? "#ef4444" : "#64748b", fontWeight: inc.injuries_count > 0 ? 700 : 400 }}>{inc.injuries_count || 0}</span></td>
                <td style={tdStyle}><span style={{ color: "#3b82f6" }}>{inc.source_count || 1}</span></td>
                <td style={tdStyle}><StatusBadge status={inc.status} /></td>
                <td style={tdStyle}><span style={{ color: "#94a3b8", fontSize: 12 }}>{(inc.persons || []).length}</span></td>
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
        <h2 style={{ color: "#f8fafc", margin: 0, fontSize: 22, fontWeight: 800 }}>My Assigned Leads</h2>
        <p style={{ color: "#64748b", margin: "4px 0 0", fontSize: 13 }}>{leads.length} leads assigned to you</p>
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
        width: 660, background: "#0c1220", height: "100vh", overflow: "auto",
        borderLeft: "1px solid rgba(245,158,11,0.15)", padding: "28px 24px"
      }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 24 }}>
          <h2 style={{ color: "#f8fafc", margin: 0, fontSize: 20, fontWeight: 800 }}>Incident Detail</h2>
          <button onClick={onClose} className="btn-action" style={{
            background: "rgba(100,116,139,0.1)", border: "1px solid #1e293b", color: "#94a3b8",
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
          {d.confidence_score && <span style={{ ...badgeBase, background: "rgba(59,130,246,0.15)", color: "#60a5fa", border: "1px solid rgba(59,130,246,0.2)" }}>{Math.round(d.confidence_score)}% confidence</span>}
          {d.source_count > 1 && <span style={{ ...badgeBase, background: "rgba(59,130,246,0.15)", color: "#60a5fa", border: "1px solid rgba(59,130,246,0.2)" }}>{d.source_count} sources</span>}
        </div>

        {/* Actions */}
        <div style={{ display: "flex", gap: 8, marginBottom: 24, flexWrap: "wrap" }}>
          <button onClick={assignToMe} className="btn-action" style={{ ...btnSmall, background: "linear-gradient(135deg, #2563eb, #3b82f6)" }}>&#x1F464; Assign to Me</button>
          <button onClick={() => updateStatus("contacted")} className="btn-action" style={{ ...btnSmall, background: "linear-gradient(135deg, #16a34a, #22c55e)" }}>&#x2714; Mark Contacted</button>
          <button onClick={() => updateStatus("in_progress")} className="btn-action" style={{ ...btnSmall, background: "linear-gradient(135deg, #f59e0b, #f97316)" }}>&#x23F3; In Progress</button>
          <button onClick={() => updateStatus("closed")} className="btn-action" style={{ ...btnSmall, background: "linear-gradient(135deg, #475569, #64748b)" }}>&#x2716; Close</button>
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
          <p style={{ color: "#e2e8f0", fontSize: 14, lineHeight: 1.6, whiteSpace: "pre-wrap" }}>{d.description || "No description available"}</p>
        </Section>

        {/* Persons Involved */}
        <Section title={`Persons Involved (${(detail?.persons || d.persons || []).length})`}>
          {(detail?.persons || d.persons || []).map((p, i) => (
            <div key={p.id || i} style={{
              background: "rgba(15,18,25,0.8)", borderRadius: 10, padding: 16, marginBottom: 12,
              border: "1px solid rgba(30,41,59,0.5)", transition: "border-color 0.2s"
            }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
                <span style={{ color: "#f8fafc", fontWeight: 700, fontSize: 14 }}>{p.full_name || "Unknown"}</span>
                <span style={{ ...badgeBase, background: p.is_injured ? "rgba(127,29,29,0.6)" : "rgba(30,41,59,0.6)", color: p.is_injured ? "#fca5a5" : "#94a3b8" }}>
                  {p.is_injured ? `Injured (${p.injury_severity || "unknown"})` : p.role}
                </span>
              </div>
              {p.phone && <InfoRow label="Phone" value={p.phone} />}
              {p.email && <InfoRow label="Email" value={p.email} />}
              {p.insurance_company && <InfoRow label="Insurance" value={`${p.insurance_company} - ${p.policy_limits || "limits unknown"}`} />}
              {p.transported_to && <InfoRow label="Hospital" value={p.transported_to} />}
              {p.has_attorney && <InfoRow label="Attorney" value={`${p.attorney_name || "Yes"} ${p.attorney_firm ? `(${p.attorney_firm})` : ""}`} />}
              {!p.has_attorney && p.is_injured && (
                <div style={{ marginTop: 8, padding: "6px 12px", background: "rgba(34,197,94,0.1)", border: "1px solid rgba(34,197,94,0.2)", borderRadius: 6 }}>
                  <span style={{ color: "#22c55e", fontSize: 12, fontWeight: 700 }}>&#x2714; NO ATTORNEY - POTENTIAL LEAD</span>
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
              <div key={v.id || i} style={{ background: "rgba(15,18,25,0.8)", borderRadius: 10, padding: 14, marginBottom: 10, border: "1px solid rgba(30,41,59,0.5)" }}>
                <span style={{ color: "#f8fafc", fontWeight: 700 }}>{v.year} {v.make} {v.model}</span>
                {v.color && <span style={{ color: "#94a3b8" }}> ({v.color})</span>}
                {v.is_commercial && <span style={{ ...badgeBase, background: "rgba(124,58,237,0.15)", color: "#a78bfa", marginLeft: 8, fontSize: 11 }}>COMMERCIAL</span>}
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
              <div key={sr.id} style={{ padding: "10px 0", borderBottom: "1px solid rgba(30,41,59,0.4)" }}>
                <div style={{ display: "flex", justifyContent: "space-between" }}>
                  <span style={{ color: "#60a5fa", fontWeight: 600, fontSize: 13 }}>{sr.source_name || sr.source_type}</span>
                  <span style={{ color: "#64748b", fontSize: 11 }}>{formatTime(sr.fetched_at)}</span>
                </div>
                {sr.contributed_fields && <span style={{ color: "#94a3b8", fontSize: 12 }}>Added: {sr.contributed_fields.join(", ")}</span>}
              </div>
            ))}
          </Section>
        )}

        {/* Notes */}
        <Section title="Notes">
          {d.notes && <pre style={{ color: "#94a3b8", fontSize: 13, whiteSpace: "pre-wrap", marginBottom: 14, background: "rgba(15,18,25,0.6)", padding: 12, borderRadius: 8 }}>{d.notes}</pre>}
          <div style={{ display: "flex", gap: 8 }}>
            <input value={note} onChange={(e) => setNote(e.target.value)} placeholder="Add a note..." style={{ ...inputStyle, flex: 1 }} className="sidebar-item" onKeyDown={(e) => e.key === "Enter" && addNote()} />
            <button onClick={addNote} className="btn-action" style={{ ...btnSmall, background: "linear-gradient(135deg, #2563eb, #3b82f6)" }}>Add</button>
          </div>
        </Section>

        {/* AI Analysis */}
        {d.ai_analysis && (
          <Section title="AI Analysis">
            <pre style={{ color: "#94a3b8", fontSize: 12, whiteSpace: "pre-wrap", background: "rgba(15,18,25,0.6)", padding: 12, borderRadius: 8 }}>
              {JSON.stringify(typeof d.ai_analysis === "string" ? JSON.parse(d.ai_analysis) : d.ai_analysis, null, 2)}
            </pre>
          </Section>
        )}

        {/* Branding */}
        <div style={{ textAlign: "center", padding: "24px 0 8px", borderTop: "1px solid rgba(30,41,59,0.4)", marginTop: 16 }}>
          <div style={{ fontSize: 10, color: "#475569", letterSpacing: "1px" }}>INCIDENT COMMAND by Donovan Digital Solutions</div>
        </div>
      </div>
    </div>
  );
}

// ============================================================================
// REUSABLE COMPONENTS
// ============================================================================
function EmptyState({ text }) {
  return <p style={{ color: "#475569", textAlign: "center", padding: 32, fontSize: 13 }}>{text}</p>;
}

function IncidentRow({ incident: inc, onSelect, compact }) {
  return (
    <div
      onClick={() => onSelect(inc)}
      className="incident-row"
      style={{ display: "flex", alignItems: "center", gap: 12, padding: "14px 10px", borderBottom: "1px solid rgba(30,41,59,0.4)", cursor: "pointer" }}
    >
      <PriorityBadge p={inc.priority} />
      <TypeBadge type={inc.incident_type} />
      <SeverityBadge severity={inc.severity} />
      <div style={{ flex: 1 }}>
        <div style={{ color: "#e2e8f0", fontSize: 14, fontWeight: 500 }}>{inc.address || `${inc.city}, ${inc.state}`}</div>
        {!compact && <div style={{ color: "#64748b", fontSize: 12, marginTop: 3 }}>{inc.description?.substring(0, 80)}...</div>}
      </div>
      <div style={{ textAlign: "right" }}>
        <div style={{ color: "#94a3b8", fontSize: 12 }}>{formatTime(inc.discovered_at)}</div>
        {inc.injuries_count > 0 && <div style={{ color: "#ef4444", fontSize: 11, fontWeight: 700, marginTop: 2 }}>{inc.injuries_count} injured</div>}
      </div>
    </div>
  );
}

function Section({ title, children }) {
  return (
    <div style={{ marginBottom: 24 }}>
      <h3 style={{ color: "#f59e0b", fontSize: 11, textTransform: "uppercase", letterSpacing: "1.5px", marginBottom: 12, paddingBottom: 8, borderBottom: "1px solid rgba(245,158,11,0.15)", fontWeight: 700 }}>{title}</h3>
      {children}
    </div>
  );
}

function InfoRow({ label, value }) {
  if (!value) return null;
  return (
    <div style={{ display: "flex", justifyContent: "space-between", padding: "4px 0" }}>
      <span style={{ color: "#64748b", fontSize: 13 }}>{label}</span>
      <span style={{ color: "#e2e8f0", fontSize: 13, textAlign: "right", maxWidth: "60%" }}>{value}</span>
    </div>
  );
}

function PriorityBadge({ p }) {
  const colors = { 1: "#dc2626", 2: "#f97316", 3: "#f59e0b", 4: "#eab308", 5: "#64748b" };
  const c = colors[p] || "#64748b";
  return <span style={{ ...badgeBase, background: c + "18", color: c, minWidth: 28, textAlign: "center", border: `1px solid ${c}33` }}>P{p || "?"}</span>;
}

function TypeBadge({ type }) {
  const icons = { car_accident: "\uD83D\uDE97", motorcycle_accident: "\uD83C\uDFCD", truck_accident: "\uD83D\uDE9B", work_accident: "\uD83C\uDFD7", pedestrian: "\uD83D\uDEB6", bicycle: "\uD83D\uDEB2", slip_fall: "\u26A0", bus_accident: "\uD83D\uDE8C" };
  return <span style={{ ...badgeBase, background: "rgba(30,41,59,0.6)", color: "#e2e8f0", border: "1px solid rgba(30,41,59,0.8)" }}>{icons[type] || "\uD83D\uDE97"}</span>;
}

function SeverityBadge({ severity }) {
  const map = { fatal: { bg: "rgba(127,29,29,0.5)", fg: "#fca5a5", border: "rgba(239,68,68,0.3)" }, critical: { bg: "rgba(124,45,18,0.5)", fg: "#fdba74", border: "rgba(249,115,22,0.3)" }, serious: { bg: "rgba(113,63,18,0.5)", fg: "#fde68a", border: "rgba(234,179,8,0.3)" }, moderate: { bg: "rgba(30,58,95,0.5)", fg: "#93c5fd", border: "rgba(59,130,246,0.3)" }, minor: { bg: "rgba(30,41,59,0.5)", fg: "#94a3b8", border: "rgba(100,116,139,0.3)" } };
  const s = map[severity] || map.minor;
  return <span style={{ ...badgeBase, background: s.bg, color: s.fg, border: `1px solid ${s.border}` }}>{severity || "unknown"}</span>;
}

function StatusBadge({ status }) {
  const map = { new: { bg: "rgba(30,58,95,0.5)", fg: "#60a5fa", border: "rgba(59,130,246,0.3)" }, verified: { bg: "rgba(20,83,45,0.5)", fg: "#86efac", border: "rgba(34,197,94,0.3)" }, assigned: { bg: "rgba(113,63,18,0.5)", fg: "#fde68a", border: "rgba(234,179,8,0.3)" }, contacted: { bg: "rgba(20,83,45,0.5)", fg: "#86efac", border: "rgba(34,197,94,0.3)" }, in_progress: { bg: "rgba(124,45,18,0.5)", fg: "#fdba74", border: "rgba(249,115,22,0.3)" }, closed: { bg: "rgba(30,41,59,0.5)", fg: "#64748b", border: "rgba(100,116,139,0.3)" } };
  const s = map[status] || map.new;
  return <span style={{ ...badgeBase, background: s.bg, color: s.fg, border: `1px solid ${s.border}` }}>{(status || "new").replace(/_/g, " ")}</span>;
}

function contactStatusColor(status) {
  const map = { not_contacted: { bg: "rgba(30,41,59,0.6)", fg: "#94a3b8" }, attempted: { bg: "rgba(113,63,18,0.6)", fg: "#fde68a" }, contacted: { bg: "rgba(20,83,45,0.6)", fg: "#86efac" }, interested: { bg: "rgba(20,83,45,0.6)", fg: "#4ade80" }, not_interested: { bg: "rgba(30,41,59,0.6)", fg: "#64748b" }, retained: { bg: "rgba(30,58,95,0.6)", fg: "#60a5fa" }, has_attorney: { bg: "rgba(127,29,29,0.6)", fg: "#fca5a5" } };
  return map[status] || map.not_contacted;
}

// ============================================================================
// HELPERS
// ============================================================================
function formatType(t) {
  return (t || "").replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
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
const inputStyle = { background: "rgba(15,18,25,0.8)", border: "1px solid rgba(30,41,59,0.6)", borderRadius: 10, padding: "11px 14px", color: "#e2e8f0", fontSize: 14, width: "100%", outline: "none", boxSizing: "border-box", transition: "border-color 0.2s, box-shadow 0.2s" };
const selectStyle = { ...inputStyle, marginBottom: 12, cursor: "pointer" };
const labelStyle = { color: "#64748b", fontSize: 10, textTransform: "uppercase", letterSpacing: "1px", marginBottom: 4, display: "block", fontWeight: 600 };
const btnPrimary = { width: "100%", background: "linear-gradient(135deg, #f59e0b, #f97316)", color: "#0a0e1a", border: "none", borderRadius: 10, padding: "12px 16px", fontWeight: 700, fontSize: 15, cursor: "pointer", marginTop: 20, letterSpacing: "0.5px" };
const btnSmall = { border: "none", borderRadius: 8, padding: "7px 14px", color: "#fff", fontSize: 12, fontWeight: 600, cursor: "pointer" };
const navBtn = { background: "none", border: "none", cursor: "pointer", fontSize: 12, fontWeight: 600, letterSpacing: "0.5px", padding: "18px 0" };
const enhancedCardStyle = { background: "rgba(12,18,32,0.8)", borderRadius: 14, padding: 22, border: "1px solid rgba(30,41,59,0.5)", backdropFilter: "blur(8px)" };
const enhancedCardTitle = { color: "#f8fafc", fontSize: 16, fontWeight: 700, margin: "0 0 18px", display: "flex", alignItems: "center" };
const tdStyle = { padding: "12px 8px", fontSize: 13 };
const badgeBase = { fontSize: 11, fontWeight: 600, padding: "3px 10px", borderRadius: 6, display: "inline-block", textTransform: "capitalize" };
