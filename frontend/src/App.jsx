import { useState, useEffect, useCallback, useRef } from "react";

const API = "/api/v1";

// ============================================================================
// API HELPER
import { useState, useEffect, useCallback, useRef } from "react";

const API = "/api/v1";

// ============================================================================
// API HELPER
// ============================================================================
async function api(path, opts = {}) {
  const tok = localStorage.getItem("aip_token");
  try {
    const res = await fetch(`${API}${path}`, {
      ...opts,
      headers: {
        "Content-Type": "application/json",
        ...(tok ? { Authorization: `Bearer ${tok}` } : {}),
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
    <div style={{ minHeight: "100vh", background: "#0a0e1a", color: "#e2e8f0", fontFamily: "system-ui,-apple-system,sans-serif" }}>
      <NavBar user={user} page={page} setPage={setPage} notifications={notifications} onLogout={() => { localStorage.removeItem("aip_token"); setUser(null); }} />

      <div style={{ display: "flex" }}>
        <Sidebar filters={filters} setFilters={setFilters} metros={metros} onRefresh={loadData} />
        <main style={{ flex: 1, padding: "20px", overflow: "auto", maxHeight: "calc(100vh - 60px)" }}>
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
// LOGIN SCREEN
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
    <div style={{ minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center", background: "linear-gradient(135deg, #0a0e1a 0%, #1a1f3a 100%)" }}>
      <div style={{ background: "#141829", borderRadius: 16, padding: 48, width: 420, boxShadow: "0 20px 60px rgba(0,0,0,0.5)" }}>
        <div style={{ textAlign: "center", marginBottom: 32 }}>
          <div style={{ fontSize: 40, marginBottom: 8 }}>&#x1F6A8;</div>
          <h1 style={{ fontSize: 28, fontWeight: 700, color: "#fff", margin: 0 }}>INCIDENT COMMAND</h1>
          <p style={{ color: "#64748b", margin: "8px 0 0", fontSize: 14 }}>Accident Intelligence Platform</p>
        </div>
        <form onSubmit={handleLogin}>
          {error && <div style={{ background: "#dc2626", color: "#fff", padding: "10px 14px", borderRadius: 8, marginBottom: 16, fontSize: 14 }}>{error}</div>}
          <input value={email} onChange={(e) => setEmail(e.target.value)} placeholder="Email" type="email" required style={inputStyle} />
          <input value={pass} onChange={(e) => setPass(e.target.value)} placeholder="Password" type="password" required style={{ ...inputStyle, marginTop: 12 }} />
          <button type="submit" style={btnPrimary}>Sign In</button>
        </form>
      </div>
    </div>
  );
}

// ============================================================================
// NAVIGATION BAR
// ============================================================================
function NavBar({ user, page, setPage, notifications, onLogout }) {
  return (
    <nav style={{ height: 60, background: "#141829", borderBottom: "1px solid #1e293b", display: "flex", alignItems: "center", justifyContent: "space-between", padding: "0 24px", position: "sticky", top: 0, zIndex: 100 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 24 }}>
        <span style={{ fontSize: 22, fontWeight: 700, color: "#f59e0b" }}>&#x1F6A8; INCIDENT COMMAND</span>
        {["dashboard", "incidents", "my-leads"].map((p) => (
          <button key={p} onClick={() => setPage(p)} style={{ ...navBtn, color: page === p ? "#f59e0b" : "#94a3b8", borderBottom: page === p ? "2px solid #f59e0b" : "none" }}>
            {p.replace("-", " ").toUpperCase()}
          </button>
        ))}
      </div>
      <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
        <span style={{ position: "relative" }}>
          &#x1F514;
          {notifications.length > 0 && <span style={{ position: "absolute", top: -6, right: -8, background: "#dc2626", color: "#fff", fontSize: 10, borderRadius: 10, padding: "1px 5px", fontWeight: 700 }}>{notifications.length}</span>}
        </span>
        <span style={{ color: "#94a3b8", fontSize: 14 }}>{user.firstName} {user.lastName}</span>
        <button onClick={onLogout} style={{ background: "none", border: "none", color: "#64748b", cursor: "pointer", fontSize: 13 }}>Logout</button>
      </div>
    </nav>
  );
}

// ============================================================================
// SIDEBAR FILTERS
// ============================================================================
function Sidebar({ filters, setFilters, metros, onRefresh }) {
  const update = (key, val) => setFilters((f) => ({ ...f, [key]: val }));

  return (
    <aside style={{ width: 240, background: "#141829", borderRight: "1px solid #1e293b", padding: "20px 16px", minHeight: "calc(100vh - 60px)" }}>
      <h3 style={{ color: "#94a3b8", fontSize: 11, textTransform: "uppercase", letterSpacing: 1, margin: "0 0 12px" }}>Filters</h3>
      <label style={labelStyle}>Time Period</label>
      <select value={filters.period || "today"} onChange={(e) => update("period", e.target.value)} style={selectStyle}>
        <option value="today">Today</option>
        <option value="week">Last 7 Days</option>
        <option value="month">Last 30 Days</option>
      </select>

      <label style={labelStyle}>Metro Area</label>
      <select value={filters.metro || ""} onChange={(e) => update("metro", e.target.value)} style={selectStyle}>
        <option value="">All Metros</option>
        {metros.map((m) => <option key={m.id} value={m.id}>{m.name}, {m.state}</option>)}
      </select>

      <label style={labelStyle}>Incident Type</label>
      <select value={filters.type || ""} onChange={(e) => update("type", e.target.value)} style={selectStyle}>
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
      <select value={filters.severity || ""} onChange={(e) => update("severity", e.target.value)} style={selectStyle}>
        <option value="">All</option>
        <option value="fatal">Fatal</option>
        <option value="critical">Critical</option>
        <option value="serious">Serious</option>
        <option value="moderate">Moderate</option>
        <option value="minor">Minor</option>
      </select>

      <label style={labelStyle}>Status</label>
      <select value={filters.status || ""} onChange={(e) => update("status", e.target.value)} style={selectStyle}>
        <option value="">All</option>
        <option value="new">New</option>
        <option value="verified">Verified</option>
        <option value="assigned">Assigned</option>
        <option value="contacted">Contacted</option>
      </select>

      <div style={{ marginTop: 16, display: "flex", flexDirection: "column", gap: 8 }}>
        <label style={{ display: "flex", alignItems: "center", gap: 8, color: "#94a3b8", fontSize: 13 }}>
          <input type="checkbox" checked={!!filters.hasAttorney} onChange={(e) => update("hasAttorney", e.target.checked ? "false" : "")} />
          No Attorney
        </label>
      </div>

      <button onClick={onRefresh} style={{ ...btnPrimary, marginTop: 20, fontSize: 13, padding: "8px 12px" }}>&#x21BB; Refresh</button>
    </aside>
  );
}

// ============================================================================
// DASHBOARD VIEW
// ============================================================================
function DashboardView({ stats, incidents, onSelect, loading }) {
  if (!stats) return <div style={{ textAlign: "center", padding: 60, color: "#64748b" }}>Loading dashboard...</div>;
  const t = stats.totals || {};

  return (
    <div>
      {/* KPI Cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 16, marginBottom: 24 }}>
        <KPICard label="Total Incidents" value={t.total_incidents || 0} color="#3b82f6" />
        <KPICard label="New (Unassigned)" value={t.new_incidents || 0} color="#f59e0b" />
        <KPICard label="Total Injuries" value={t.total_injuries || 0} color="#ef4444" />
        <KPICard label="Fatalities" value={t.total_fatalities || 0} color="#dc2626" />
        <KPICard label="High Severity" value={t.high_severity_count || 0} color="#f97316" />
        <KPICard label="Active Reps" value={t.active_reps || 0} color="#22c55e" />
      </div>

      {/* Metro breakdown */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 24 }}>
        <div style={cardStyle}>
          <h3 style={cardTitle}>Incidents by Metro</h3>
          {(stats.byMetro || []).map((m) => (
            <div key={m.metro} style={{ display: "flex", justifyContent: "space-between", padding: "6px 0", borderBottom: "1px solid #1e293b" }}>
              <span style={{ color: "#e2e8f0" }}>{m.metro || "Unknown"}</span>
              <span style={{ color: "#f59e0b", fontWeight: 600 }}>{m.count}</span>
            </div>
          ))}
        </div>
        <div style={cardStyle}>
          <h3 style={cardTitle}>By Type</h3>
          {(stats.byType || []).map((t) => (
            <div key={t.incident_type} style={{ display: "flex", justifyContent: "space-between", padding: "6px 0", borderBottom: "1px solid #1e293b" }}>
              <span style={{ color: "#e2e8f0" }}>{formatType(t.incident_type)}</span>
              <span style={{ color: "#3b82f6", fontWeight: 600 }}>{t.count}</span>
            </div>
          ))}
        </div>
      </div>

      {/* High Priority Feed */}
      <div style={cardStyle}>
        <h3 style={cardTitle}>&#x1F525; High Priority Incidents {loading && <span style={{ fontSize: 12, color: "#64748b" }}>(refreshing...)</span>}</h3>
        <div>
          {(stats.recentHighPriority || []).map((inc) => (
            <IncidentRow key={inc.id} incident={inc} onSelect={onSelect} compact />
          ))}
          {(!stats.recentHighPriority || stats.recentHighPriority.length === 0) && (
            <p style={{ color: "#64748b", textAlign: "center", padding: 20 }}>No high-priority incidents in this period</p>
          )}
        </div>
      </div>

      {/* Recent Feed */}
      <div style={{ ...cardStyle, marginTop: 16 }}>
        <h3 style={cardTitle}>&#x23F1;&#xFE0F; Live Feed â Most Recent</h3>
        {incidents.slice(0, 20).map((inc) => (
          <IncidentRow key={inc.id} incident={inc} onSelect={onSelect} />
        ))}
      </div>
    </div>
  );
}

// ============================================================================
// INCIDENT LIST VIEW
// ============================================================================
function IncidentList({ incidents, onSelect, filters, setFilters }) {
  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
        <h2 style={{ color: "#f8fafc", margin: 0 }}>All Incidents ({incidents.length})</h2>
        <input
          placeholder="Search address, report #, description..."
          value={filters.search || ""}
          onChange={(e) => setFilters((f) => ({ ...f, search: e.target.value }))}
          style={{ ...inputStyle, width: 320 }}
        />
      </div>
      <div style={cardStyle}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "2px solid #1e293b" }}>
              {["Priority", "Type", "Severity", "Location", "Time", "Injuries", "Sources", "Status", "Persons"].map((h) => (
                <th key={h} style={{ textAlign: "left", padding: "10px 8px", color: "#94a3b8", fontSize: 11, textTransform: "uppercase", letterSpacing: 0.5 }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {incidents.map((inc) => (
              <tr key={inc.id} onClick={() => onSelect(inc)} style={{ borderBottom: "1px solid #1e293b", cursor: "pointer", transition: "background 0.15s" }}
                onMouseEnter={(e) => (e.currentTarget.style.background = "#1e293b")}
                onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}>
                <td style={tdStyle}><PriorityBadge p={inc.priority} /></td>
                <td style={tdStyle}><TypeBadge type={inc.incident_type} /></td>
                <td style={tdStyle}><SeverityBadge severity={inc.severity} /></td>
                <td style={tdStyle}><span style={{ color: "#e2e8f0" }}>{inc.city}, {inc.state}</span><br /><span style={{ color: "#64748b", fontSize: 12 }}>{inc.address?.substring(0, 40)}</span></td>
                <td style={{ ...tdStyle, color: "#94a3b8", fontSize: 13 }}>{formatTime(inc.discovered_at)}</td>
                <td style={tdStyle}><span style={{ color: inc.injuries_count > 0 ? "#ef4444" : "#64748b", fontWeight: inc.injuries_count > 0 ? 700 : 400 }}>{inc.injuries_count || 0}</span></td>
                <td style={tdStyle}><span style={{ color: "#3b82f6" }}>{inc.source_count || 1}</span></td>
                <td style={tdStyle}><StatusBadge status={inc.status} /></td>
                <td style={tdStyle}><span style={{ color: "#94a3b8", fontSize: 13 }}>{(inc.persons || []).length}</span></td>
              </tr>
            ))}
          </tbody>
        </table>
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
    <div>
      <h2 style={{ color: "#f8fafc", marginBottom: 16 }}>My Assigned Leads ({leads.length})</h2>
      <div style={cardStyle}>
        {leads.map((inc) => <IncidentRow key={inc.id} incident={inc} onSelect={onSelect} />)}
        {leads.length === 0 && <p style={{ color: "#64748b", textAlign: "center", padding: 40 }}>No leads assigned to you</p>}
      </div>
    </div>
  );
}

// ============================================================================
// INCIDENT DETAIL MODAL
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
    <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.7)", zIndex: 200, display: "flex", justifyContent: "flex-end" }}>
      <div style={{ width: 640, background: "#141829", height: "100vh", overflow: "auto", borderLeft: "1px solid #1e293b", padding: 24 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
          <h2 style={{ color: "#f8fafc", margin: 0, fontSize: 20 }}>Incident Detail</h2>
          <button onClick={onClose} style={{ background: "none", border: "none", color: "#94a3b8", fontSize: 24, cursor: "pointer" }}>&times;</button>
        </div>

        {/* Header info */}
        <div style={{ display: "flex", gap: 8, marginBottom: 16, flexWrap: "wrap" }}>
          <TypeBadge type={d.incident_type} />
          <SeverityBadge severity={d.severity} />
          <PriorityBadge p={d.priority} />
          <StatusBadge status={d.status} />
          {d.confidence_score && <span style={{ ...badgeBase, background: "#1e3a5f", color: "#60a5fa" }}>{Math.round(d.confidence_score)}% confidence</span>}
          {d.source_count > 1 && <span style={{ ...badgeBase, background: "#1e3a5f", color: "#60a5fa" }}>{d.source_count} sources</span>}
        </div>

        {/* Actions */}
        <div style={{ display: "flex", gap: 8, marginBottom: 20 }}>
          <button onClick={assignToMe} style={{ ...btnSmall, background: "#2563eb" }}>Assign to Me</button>
          <button onClick={() => updateStatus("contacted")} style={{ ...btnSmall, background: "#16a34a" }}>Mark Contacted</button>
          <button onClick={() => updateStatus("in_progress")} style={{ ...btnSmall, background: "#f59e0b" }}>In Progress</button>
          <button onClick={() => updateStatus("closed")} style={{ ...btnSmall, background: "#64748b" }}>Close</button>
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
            <div key={p.id || i} style={{ background: "#0f1219", borderRadius: 8, padding: 14, marginBottom: 10, border: "1px solid #1e293b" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
                <span style={{ color: "#f8fafc", fontWeight: 600 }}>{p.full_name || "Unknown"}</span>
                <span style={{ ...badgeBase, background: p.is_injured ? "#7f1d1d" : "#1e293b", color: p.is_injured ? "#fca5a5" : "#94a3b8" }}>
                  {p.is_injured ? `Injured (${p.injury_severity || "unknown"})` : p.role}
                </span>
              </div>
              {p.phone && <InfoRow label="Phone" value={p.phone} />}
              {p.email && <InfoRow label="Email" value={p.email} />}
              {p.insurance_company && <InfoRow label="Insurance" value={`${p.insurance_company} - ${p.policy_limits || "limits unknown"}`} />}
              {p.transported_to && <InfoRow label="Hospital" value={p.transported_to} />}
              {p.has_attorney && <InfoRow label="Attorney" value={`${p.attorney_name || "Yes"} ${p.attorney_firm ? `(${p.attorney_firm})` : ""}`} />}
              {!p.has_attorney && p.is_injured && <span style={{ color: "#22c55e", fontSize: 12, fontWeight: 600 }}>&#x2714; NO ATTORNEY - POTENTIAL LEAD</span>}
              <div style={{ marginTop: 6 }}>
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
              <div key={v.id || i} style={{ background: "#0f1219", borderRadius: 8, padding: 12, marginBottom: 8, border: "1px solid #1e293b" }}>
                <span style={{ color: "#f8fafc", fontWeight: 600 }}>{v.year} {v.make} {v.model}</span>
                {v.color && <span style={{ color: "#94a3b8" }}> ({v.color})</span>}
                {v.is_commercial && <span style={{ ...badgeBase, background: "#7c3aed33", color: "#a78bfa", marginLeft: 8, fontSize: 11 }}>COMMERCIAL</span>}
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
              <div key={sr.id} style={{ padding: "8px 0", borderBottom: "1px solid #1e293b" }}>
                <div style={{ display: "flex", justifyContent: "space-between" }}>
                  <span style={{ color: "#60a5fa", fontWeight: 600, fontSize: 13 }}>{sr.source_name || sr.source_type}</span>
                  <span style={{ color: "#64748b", fontSize: 12 }}>{formatTime(sr.fetched_at)}</span>
                </div>
                {sr.contributed_fields && <span style={{ color: "#94a3b8", fontSize: 12 }}>Added: {sr.contributed_fields.join(", ")}</span>}
              </div>
            ))}
          </Section>
        )}

        {/* Notes */}
        <Section title="Notes">
          {d.notes && <pre style={{ color: "#94a3b8", fontSize: 13, whiteSpace: "pre-wrap", marginBottom: 12 }}>{d.notes}</pre>}
          <div style={{ display: "flex", gap: 8 }}>
            <input value={note} onChange={(e) => setNote(e.target.value)} placeholder="Add a note..." style={{ ...inputStyle, flex: 1 }} onKeyDown={(e) => e.key === "Enter" && addNote()} />
            <button onClick={addNote} style={{ ...btnSmall, background: "#2563eb" }}>Add</button>
          </div>
        </Section>

        {/* AI Analysis */}
        {d.ai_analysis && (
          <Section title="AI Analysis">
            <pre style={{ color: "#94a3b8", fontSize: 12, whiteSpace: "pre-wrap" }}>
              {JSON.stringify(typeof d.ai_analysis === "string" ? JSON.parse(d.ai_analysis) : d.ai_analysis, null, 2)}
            </pre>
          </Section>
        )}
      </div>
    </div>
  );
}

// ============================================================================
// REUSABLE COMPONENTS
// ============================================================================
function KPICard({ label, value, color }) {
  return (
    <div style={{ background: "#141829", borderRadius: 12, padding: "20px 18px", border: "1px solid #1e293b" }}>
      <div style={{ color: "#94a3b8", fontSize: 12, textTransform: "uppercase", letterSpacing: 0.5 }}>{label}</div>
      <div style={{ color, fontSize: 32, fontWeight: 700, marginTop: 4 }}>{value}</div>
    </div>
  );
}

function IncidentRow({ incident: inc, onSelect, compact }) {
  return (
    <div
      onClick={() => onSelect(inc)}
      style={{ display: "flex", alignItems: "center", gap: 12, padding: "12px 8px", borderBottom: "1px solid #1e293b", cursor: "pointer", transition: "background 0.15s" }}
      onMouseEnter={(e) => (e.currentTarget.style.background = "#1e293b")}
      onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
    >
      <PriorityBadge p={inc.priority} />
      <TypeBadge type={inc.incident_type} />
      <SeverityBadge severity={inc.severity} />
      <div style={{ flex: 1 }}>
        <div style={{ color: "#e2e8f0", fontSize: 14 }}>{inc.address || `${inc.city}, ${inc.state}`}</div>
        {!compact && <div style={{ color: "#64748b", fontSize: 12, marginTop: 2 }}>{inc.description?.substring(0, 80)}...</div>}
      </div>
      <div style={{ textAlign: "right" }}>
        <div style={{ color: "#94a3b8", fontSize: 12 }}>{formatTime(inc.discovered_at)}</div>
        {inc.injuries_count > 0 && <div style={{ color: "#ef4444", fontSize: 12, fontWeight: 600 }}>{inc.injuries_count} injured</div>}
      </div>
    </div>
  );
}

function Section({ title, children }) {
  return (
    <div style={{ marginBottom: 20 }}>
      <h3 style={{ color: "#94a3b8", fontSize: 12, textTransform: "uppercase", letterSpacing: 1, marginBottom: 10, paddingBottom: 6, borderBottom: "1px solid #1e293b" }}>{title}</h3>
      {children}
    </div>
  );
}

function InfoRow({ label, value }) {
  if (!value) return null;
  return (
    <div style={{ display: "flex", justifyContent: "space-between", padding: "3px 0" }}>
      <span style={{ color: "#64748b", fontSize: 13 }}>{label}</span>
      <span style={{ color: "#e2e8f0", fontSize: 13, textAlign: "right", maxWidth: "60%" }}>{value}</span>
    </div>
  );
}

function PriorityBadge({ p }) {
  const colors = { 1: "#dc2626", 2: "#f97316", 3: "#f59e0b", 4: "#eab308", 5: "#64748b" };
  return <span style={{ ...badgeBase, background: (colors[p] || "#64748b") + "22", color: colors[p] || "#64748b", minWidth: 24, textAlign: "center" }}>P{p || "?"}</span>;
}

function TypeBadge({ type }) {
  const icons = { car_accident: "&#x1F697;", motorcycle_accident: "&#x1F3CD;", truck_accident: "&#x1F69B;", work_accident: "&#x1F3D7;", pedestrian: "&#x1F6B6;", bicycle: "&#x1F6B2;", slip_fall: "&#x26A0;", bus_accident: "&#x1F68C;" };
  return <span style={{ ...badgeBase, background: "#1e293b", color: "#e2e8f0" }} dangerouslySetInnerHTML={{ __html: icons[type] || "&#x1F697;" }} />;
}

function SeverityBadge({ severity }) {
  const map = { fatal: { bg: "#7f1d1d", fg: "#fca5a5" }, critical: { bg: "#7c2d12", fg: "#fdba74" }, serious: { bg: "#713f12", fg: "#fde68a" }, moderate: { bg: "#1e3a5f", fg: "#93c5fd" }, minor: { bg: "#1e293b", fg: "#94a3b8" } };
  const s = map[severity] || map.minor;
  return <span style={{ ...badgeBase, background: s.bg, color: s.fg }}>{severity || "unknown"}</span>;
}

function StatusBadge({ status }) {
  const map = { new: { bg: "#1e3a5f", fg: "#60a5fa" }, verified: { bg: "#14532d", fg: "#86efac" }, assigned: { bg: "#713f12", fg: "#fde68a" }, contacted: { bg: "#14532d", fg: "#86efac" }, in_progress: { bg: "#7c2d12", fg: "#fdba74" }, closed: { bg: "#1e293b", fg: "#64748b" } };
  const s = map[status] || map.new;
  return <span style={{ ...badgeBase, background: s.bg, color: s.fg }}>{(status || "new").replace(/_/g, " ")}</span>;
}

function contactStatusColor(status) {
  const map = { not_contacted: { bg: "#1e293b", fg: "#94a3b8" }, attempted: { bg: "#713f12", fg: "#fde68a" }, contacted: { bg: "#14532d", fg: "#86efac" }, interested: { bg: "#14532d", fg: "#4ade80" }, not_interested: { bg: "#1e293b", fg: "#64748b" }, retained: { bg: "#1e3a5f", fg: "#60a5fa" }, has_attorney: { bg: "#7f1d1d", fg: "#fca5a5" } };
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
const inputStyle = { background: "#0f1219", border: "1px solid #1e293b", borderRadius: 8, padding: "10px 14px", color: "#e2e8f0", fontSize: 14, width: "100%", outline: "none", boxSizing: "border-box" };
const selectStyle = { ...inputStyle, marginBottom: 12 };
const labelStyle = { color: "#64748b", fontSize: 11, textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 4, display: "block" };
const btnPrimary = { width: "100%", background: "#f59e0b", color: "#0a0e1a", border: "none", borderRadius: 8, padding: "12px 16px", fontWeight: 700, fontSize: 15, cursor: "pointer", marginTop: 20 };
const btnSmall = { border: "none", borderRadius: 6, padding: "6px 12px", color: "#fff", fontSize: 12, fontWeight: 600, cursor: "pointer" };
const navBtn = { background: "none", border: "none", cursor: "pointer", fontSize: 12, fontWeight: 600, letterSpacing: 0.5, padding: "18px 0" };
const cardStyle = { background: "#141829", borderRadius: 12, padding: 20, border: "1px solid #1e293b" };
const cardTitle = { color: "#f8fafc", fontSize: 16, fontWeight: 600, margin: "0 0 16px" };
const tdStyle = { padding: "10px 8px", fontSize: 13 };
const badgeBase = { fontSize: 11, fontWeight: 600, padding: "3px 8px", borderRadius: 6, display: "inline-block", textTransform: "capitalize" };
// ============================================================================
async function api(path, opts = {}) {
  const token = localStorage.getItem("aip_token");
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
  return res.json();
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
    <div style={{ minHeight: "100vh", background: "#0a0e1a", color: "#e2e8f0", fontFamily: "system-ui,-apple-system,sans-serif" }}>
      <NavBar user={user} page={page} setPage={setPage} notifications={notifications} onLogout={() => { localStorage.removeItem("aip_token"); setUser(null); }} />

      <div style={{ display: "flex" }}>
        <Sidebar filters={filters} setFilters={setFilters} metros={metros} onRefresh={loadData} />
        <main style={{ flex: 1, padding: "20px", overflow: "auto", maxHeight: "calc(100vh - 60px)" }}>
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
// LOGIN SCREEN
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
    <div style={{ minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center", background: "linear-gradient(135deg, #0a0e1a 0%, #1a1f3a 100%)" }}>
      <div style={{ background: "#141829", borderRadius: 16, padding: 48, width: 420, boxShadow: "0 20px 60px rgba(0,0,0,0.5)" }}>
        <div style={{ textAlign: "center", marginBottom: 32 }}>
          <div style={{ fontSize: 40, marginBottom: 8 }}>&#x1F6A8;</div>
          <h1 style={{ fontSize: 28, fontWeight: 700, color: "#fff", margin: 0 }}>INCIDENT COMMAND</h1>
          <p style={{ color: "#64748b", margin: "8px 0 0", fontSize: 14 }}>Accident Intelligence Platform</p>
        </div>
        <form onSubmit={handleLogin}>
          {error && <div style={{ background: "#dc2626", color: "#fff", padding: "10px 14px", borderRadius: 8, marginBottom: 16, fontSize: 14 }}>{error}</div>}
          <input value={email} onChange={(e) => setEmail(e.target.value)} placeholder="Email" type="email" required style={inputStyle} />
          <input value={pass} onChange={(e) => setPass(e.target.value)} placeholder="Password" type="password" required style={{ ...inputStyle, marginTop: 12 }} />
          <button type="submit" style={btnPrimary}>Sign In</button>
        </form>
      </div>
    </div>
  );
}

// ============================================================================
// NAVIGATION BAR
// ============================================================================
function NavBar({ user, page, setPage, notifications, onLogout }) {
  return (
    <nav style={{ height: 60, background: "#141829", borderBottom: "1px solid #1e293b", display: "flex", alignItems: "center", justifyContent: "space-between", padding: "0 24px", position: "sticky", top: 0, zIndex: 100 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 24 }}>
        <span style={{ fontSize: 22, fontWeight: 700, color: "#f59e0b" }}>&#x1F6A8; INCIDENT COMMAND</span>
        {["dashboard", "incidents", "my-leads"].map((p) => (
          <button key={p} onClick={() => setPage(p)} style={{ ...navBtn, color: page === p ? "#f59e0b" : "#94a3b8", borderBottom: page === p ? "2px solid #f59e0b" : "none" }}>
            {p.replace("-", " ").toUpperCase()}
          </button>
        ))}
      </div>
      <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
        <span style={{ position: "relative" }}>
          &#x1F514;
          {notifications.length > 0 && <span style={{ position: "absolute", top: -6, right: -8, background: "#dc2626", color: "#fff", fontSize: 10, borderRadius: 10, padding: "1px 5px", fontWeight: 700 }}>{notifications.length}</span>}
        </span>
        <span style={{ color: "#94a3b8", fontSize: 14 }}>{user.firstName} {user.lastName}</span>
        <button onClick={onLogout} style={{ background: "none", border: "none", color: "#64748b", cursor: "pointer", fontSize: 13 }}>Logout</button>
      </div>
    </nav>
  );
}

// ============================================================================
// SIDEBAR FILTERS
// ============================================================================
function Sidebar({ filters, setFilters, metros, onRefresh }) {
  const update = (key, val) => setFilters((f) => ({ ...f, [key]: val }));

  return (
    <aside style={{ width: 240, background: "#141829", borderRight: "1px solid #1e293b", padding: "20px 16px", minHeight: "calc(100vh - 60px)" }}>
      <h3 style={{ color: "#94a3b8", fontSize: 11, textTransform: "uppercase", letterSpacing: 1, margin: "0 0 12px" }}>Filters</h3>
      <label style={labelStyle}>Time Period</label>
      <select value={filters.period || "today"} onChange={(e) => update("period", e.target.value)} style={selectStyle}>
        <option value="today">Today</option>
        <option value="week">Last 7 Days</option>
        <option value="month">Last 30 Days</option>
      </select>

      <label style={labelStyle}>Metro Area</label>
      <select value={filters.metro || ""} onChange={(e) => update("metro", e.target.value)} style={selectStyle}>
        <option value="">All Metros</option>
        {metros.map((m) => <option key={m.id} value={m.id}>{m.name}, {m.state}</option>)}
      </select>

      <label style={labelStyle}>Incident Type</label>
      <select value={filters.type || ""} onChange={(e) => update("type", e.target.value)} style={selectStyle}>
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
      <select value={filters.severity || ""} onChange={(e) => update("severity", e.target.value)} style={selectStyle}>
        <option value="">All</option>
        <option value="fatal">Fatal</option>
        <option value="critical">Critical</option>
        <option value="serious">Serious</option>
        <option value="moderate">Moderate</option>
        <option value="minor">Minor</option>
      </select>

      <label style={labelStyle}>Status</label>
      <select value={filters.status || ""} onChange={(e) => update("status", e.target.value)} style={selectStyle}>
        <option value="">All</option>
        <option value="new">New</option>
        <option value="verified">Verified</option>
        <option value="assigned">Assigned</option>
        <option value="contacted">Contacted</option>
      </select>

      <div style={{ marginTop: 16, display: "flex", flexDirection: "column", gap: 8 }}>
        <label style={{ display: "flex", alignItems: "center", gap: 8, color: "#94a3b8", fontSize: 13 }}>
          <input type="checkbox" checked={!!filters.hasAttorney} onChange={(e) => update("hasAttorney", e.target.checked ? "false" : "")} />
          No Attorney
        </label>
      </div>

      <button onClick={onRefresh} style={{ ...btnPrimary, marginTop: 20, fontSize: 13, padding: "8px 12px" }}>&#x21BB; Refresh</button>
    </aside>
  );
}

// ============================================================================
// DASHBOARD VIEW
// ============================================================================
function DashboardView({ stats, incidents, onSelect, loading }) {
  if (!stats) return <div style={{ textAlign: "center", padding: 60, color: "#64748b" }}>Loading dashboard...</div>;
  const t = stats.totals || {};

  return (
    <div>
      {/* KPI Cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 16, marginBottom: 24 }}>
        <KPICard label="Total Incidents" value={t.total_incidents || 0} color="#3b82f6" />
        <KPICard label="New (Unassigned)" value={t.new_incidents || 0} color="#f59e0b" />
        <KPICard label="Total Injuries" value={t.total_injuries || 0} color="#ef4444" />
        <KPICard label="Fatalities" value={t.total_fatalities || 0} color="#dc2626" />
        <KPICard label="High Severity" value={t.high_severity_count || 0} color="#f97316" />
        <KPICard label="Active Reps" value={t.active_reps || 0} color="#22c55e" />
      </div>

      {/* Metro breakdown */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 24 }}>
        <div style={cardStyle}>
          <h3 style={cardTitle}>Incidents by Metro</h3>
          {(stats.byMetro || []).map((m) => (
            <div key={m.metro} style={{ display: "flex", justifyContent: "space-between", padding: "6px 0", borderBottom: "1px solid #1e293b" }}>
              <span style={{ color: "#e2e8f0" }}>{m.metro || "Unknown"}</span>
              <span style={{ color: "#f59e0b", fontWeight: 600 }}>{m.count}</span>
            </div>
          ))}
        </div>
        <div style={cardStyle}>
          <h3 style={cardTitle}>By Type</h3>
          {(stats.byType || []).map((t) => (
            <div key={t.incident_type} style={{ display: "flex", justifyContent: "space-between", padding: "6px 0", borderBottom: "1px solid #1e293b" }}>
              <span style={{ color: "#e2e8f0" }}>{formatType(t.incident_type)}</span>
              <span style={{ color: "#3b82f6", fontWeight: 600 }}>{t.count}</span>
            </div>
          ))}
        </div>
      </div>

      {/* High Priority Feed */}
      <div style={cardStyle}>
        <h3 style={cardTitle}>&#x1F525; High Priority Incidents {loading && <span style={{ fontSize: 12, color: "#64748b" }}>(refreshing...)</span>}</h3>
        <div>
          {(stats.recentHighPriority || []).map((inc) => (
            <IncidentRow key={inc.id} incident={inc} onSelect={onSelect} compact />
          ))}
          {(!stats.recentHighPriority || stats.recentHighPriority.length === 0) && (
            <p style={{ color: "#64748b", textAlign: "center", padding: 20 }}>No high-priority incidents in this period</p>
          )}
        </div>
      </div>

      {/* Recent Feed */}
      <div style={{ ...cardStyle, marginTop: 16 }}>
        <h3 style={cardTitle}>&#x23F1;&#xFE0F; Live Feed — Most Recent</h3>
        {incidents.slice(0, 20).map((inc) => (
          <IncidentRow key={inc.id} incident={inc} onSelect={onSelect} />
        ))}
      </div>
    </div>
  );
}

// ============================================================================
// INCIDENT LIST VIEW
// ============================================================================
function IncidentList({ incidents, onSelect, filters, setFilters }) {
  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
        <h2 style={{ color: "#f8fafc", margin: 0 }}>All Incidents ({incidents.length})</h2>
        <input
          placeholder="Search address, report #, description..."
          value={filters.search || ""}
          onChange={(e) => setFilters((f) => ({ ...f, search: e.target.value }))}
          style={{ ...inputStyle, width: 320 }}
        />
      </div>
      <div style={cardStyle}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "2px solid #1e293b" }}>
              {["Priority", "Type", "Severity", "Location", "Time", "Injuries", "Sources", "Status", "Persons"].map((h) => (
                <th key={h} style={{ textAlign: "left", padding: "10px 8px", color: "#94a3b8", fontSize: 11, textTransform: "uppercase", letterSpacing: 0.5 }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {incidents.map((inc) => (
              <tr key={inc.id} onClick={() => onSelect(inc)} style={{ borderBottom: "1px solid #1e293b", cursor: "pointer", transition: "background 0.15s" }}
                onMouseEnter={(e) => (e.currentTarget.style.background = "#1e293b")}
                onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}>
                <td style={tdStyle}><PriorityBadge p={inc.priority} /></td>
                <td style={tdStyle}><TypeBadge type={inc.incident_type} /></td>
                <td style={tdStyle}><SeverityBadge severity={inc.severity} /></td>
                <td style={tdStyle}><span style={{ color: "#e2e8f0" }}>{inc.city}, {inc.state}</span><br /><span style={{ color: "#64748b", fontSize: 12 }}>{inc.address?.substring(0, 40)}</span></td>
                <td style={{ ...tdStyle, color: "#94a3b8", fontSize: 13 }}>{formatTime(inc.discovered_at)}</td>
                <td style={tdStyle}><span style={{ color: inc.injuries_count > 0 ? "#ef4444" : "#64748b", fontWeight: inc.injuries_count > 0 ? 700 : 400 }}>{inc.injuries_count || 0}</span></td>
                <td style={tdStyle}><span style={{ color: "#3b82f6" }}>{inc.source_count || 1}</span></td>
                <td style={tdStyle}><StatusBadge status={inc.status} /></td>
                <td style={tdStyle}><span style={{ color: "#94a3b8", fontSize: 13 }}>{(inc.persons || []).length}</span></td>
              </tr>
            ))}
          </tbody>
        </table>
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
    <div>
      <h2 style={{ color: "#f8fafc", marginBottom: 16 }}>My Assigned Leads ({leads.length})</h2>
      <div style={cardStyle}>
        {leads.map((inc) => <IncidentRow key={inc.id} incident={inc} onSelect={onSelect} />)}
        {leads.length === 0 && <p style={{ color: "#64748b", textAlign: "center", padding: 40 }}>No leads assigned to you</p>}
      </div>
    </div>
  );
}

// ============================================================================
// INCIDENT DETAIL MODAL
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
  };

  const d = detail || incident;

  return (
    <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.7)", zIndex: 200, display: "flex", justifyContent: "flex-end" }}>
      <div style={{ width: 640, background: "#141829", height: "100vh", overflow: "auto", borderLeft: "1px solid #1e293b", padding: 24 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
          <h2 style={{ color: "#f8fafc", margin: 0, fontSize: 20 }}>Incident Detail</h2>
          <button onClick={onClose} style={{ background: "none", border: "none", color: "#94a3b8", fontSize: 24, cursor: "pointer" }}>&times;</button>
        </div>

        {/* Header info */}
        <div style={{ display: "flex", gap: 8, marginBottom: 16, flexWrap: "wrap" }}>
          <TypeBadge type={d.incident_type} />
          <SeverityBadge severity={d.severity} />
          <PriorityBadge p={d.priority} />
          <StatusBadge status={d.status} />
          {d.confidence_score && <span style={{ ...badgeBase, background: "#1e3a5f", color: "#60a5fa" }}>{Math.round(d.confidence_score)}% confidence</span>}
          {d.source_count > 1 && <span style={{ ...badgeBase, background: "#1e3a5f", color: "#60a5fa" }}>{d.source_count} sources</span>}
        </div>

        {/* Actions */}
        <div style={{ display: "flex", gap: 8, marginBottom: 20 }}>
          <button onClick={assignToMe} style={{ ...btnSmall, background: "#2563eb" }}>Assign to Me</button>
          <button onClick={() => updateStatus("contacted")} style={{ ...btnSmall, background: "#16a34a" }}>Mark Contacted</button>
          <button onClick={() => updateStatus("in_progress")} style={{ ...btnSmall, background: "#f59e0b" }}>In Progress</button>
          <button onClick={() => updateStatus("closed")} style={{ ...btnSmall, background: "#64748b" }}>Close</button>
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
            <div key={p.id || i} style={{ background: "#0f1219", borderRadius: 8, padding: 14, marginBottom: 10, border: "1px solid #1e293b" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
                <span style={{ color: "#f8fafc", fontWeight: 600 }}>{p.full_name || "Unknown"}</span>
                <span style={{ ...badgeBase, background: p.is_injured ? "#7f1d1d" : "#1e293b", color: p.is_injured ? "#fca5a5" : "#94a3b8" }}>
                  {p.is_injured ? `Injured (${p.injury_severity || "unknown"})` : p.role}
                </span>
              </div>
              {p.phone && <InfoRow label="Phone" value={p.phone} />}
              {p.email && <InfoRow label="Email" value={p.email} />}
              {p.insurance_company && <InfoRow label="Insurance" value={`${p.insurance_company} - ${p.policy_limits || "limits unknown"}`} />}
              {p.transported_to && <InfoRow label="Hospital" value={p.transported_to} />}
              {p.has_attorney && <InfoRow label="Attorney" value={`${p.attorney_name || "Yes"} ${p.attorney_firm ? `(${p.attorney_firm})` : ""}`} />}
              {!p.has_attorney && p.is_injured && <span style={{ color: "#22c55e", fontSize: 12, fontWeight: 600 }}>&#x2714; NO ATTORNEY - POTENTIAL LEAD</span>}
              <div style={{ marginTop: 6 }}>
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
              <div key={v.id || i} style={{ background: "#0f1219", borderRadius: 8, padding: 12, marginBottom: 8, border: "1px solid #1e293b" }}>
                <span style={{ color: "#f8fafc", fontWeight: 600 }}>{v.year} {v.make} {v.model}</span>
                {v.color && <span style={{ color: "#94a3b8" }}> ({v.color})</span>}
                {v.is_commercial && <span style={{ ...badgeBase, background: "#7c3aed33", color: "#a78bfa", marginLeft: 8, fontSize: 11 }}>COMMERCIAL</span>}
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
              <div key={sr.id} style={{ padding: "8px 0", borderBottom: "1px solid #1e293b" }}>
                <div style={{ display: "flex", justifyContent: "space-between" }}>
                  <span style={{ color: "#60a5fa", fontWeight: 600, fontSize: 13 }}>{sr.source_name || sr.source_type}</span>
                  <span style={{ color: "#64748b", fontSize: 12 }}>{formatTime(sr.fetched_at)}</span>
                </div>
                {sr.contributed_fields && <span style={{ color: "#94a3b8", fontSize: 12 }}>Added: {sr.contributed_fields.join(", ")}</span>}
              </div>
            ))}
          </Section>
        )}

        {/* Notes */}
        <Section title="Notes">
          {d.notes && <pre style={{ color: "#94a3b8", fontSize: 13, whiteSpace: "pre-wrap", marginBottom: 12 }}>{d.notes}</pre>}
          <div style={{ display: "flex", gap: 8 }}>
            <input value={note} onChange={(e) => setNote(e.target.value)} placeholder="Add a note..." style={{ ...inputStyle, flex: 1 }} onKeyDown={(e) => e.key === "Enter" && addNote()} />
            <button onClick={addNote} style={{ ...btnSmall, background: "#2563eb" }}>Add</button>
          </div>
        </Section>

        {/* AI Analysis */}
        {d.ai_analysis && (
          <Section title="AI Analysis">
            <pre style={{ color: "#94a3b8", fontSize: 12, whiteSpace: "pre-wrap" }}>
              {JSON.stringify(typeof d.ai_analysis === "string" ? JSON.parse(d.ai_analysis) : d.ai_analysis, null, 2)}
            </pre>
          </Section>
        )}
      </div>
    </div>
  );
}

// ============================================================================
// REUSABLE COMPONENTS
// ============================================================================
function KPICard({ label, value, color }) {
  return (
    <div style={{ background: "#141829", borderRadius: 12, padding: "20px 18px", border: "1px solid #1e293b" }}>
      <div style={{ color: "#94a3b8", fontSize: 12, textTransform: "uppercase", letterSpacing: 0.5 }}>{label}</div>
      <div style={{ color, fontSize: 32, fontWeight: 700, marginTop: 4 }}>{value}</div>
    </div>
  );
}

function IncidentRow({ incident: inc, onSelect, compact }) {
  return (
    <div
      onClick={() => onSelect(inc)}
      style={{ display: "flex", alignItems: "center", gap: 12, padding: "12px 8px", borderBottom: "1px solid #1e293b", cursor: "pointer", transition: "background 0.15s" }}
      onMouseEnter={(e) => (e.currentTarget.style.background = "#1e293b")}
      onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
    >
      <PriorityBadge p={inc.priority} />
      <TypeBadge type={inc.incident_type} />
      <SeverityBadge severity={inc.severity} />
      <div style={{ flex: 1 }}>
        <div style={{ color: "#e2e8f0", fontSize: 14 }}>{inc.address || `${inc.city}, ${inc.state}`}</div>
        {!compact && <div style={{ color: "#64748b", fontSize: 12, marginTop: 2 }}>{inc.description?.substring(0, 80)}...</div>}
      </div>
      <div style={{ textAlign: "right" }}>
        <div style={{ color: "#94a3b8", fontSize: 12 }}>{formatTime(inc.discovered_at)}</div>
        {inc.injuries_count > 0 && <div style={{ color: "#ef4444", fontSize: 12, fontWeight: 600 }}>{inc.injuries_count} injured</div>}
      </div>
    </div>
  );
}

function Section({ title, children }) {
  return (
    <div style={{ marginBottom: 20 }}>
      <h3 style={{ color: "#94a3b8", fontSize: 12, textTransform: "uppercase", letterSpacing: 1, marginBottom: 10, paddingBottom: 6, borderBottom: "1px solid #1e293b" }}>{title}</h3>
      {children}
    </div>
  );
}

function InfoRow({ label, value }) {
  if (!value) return null;
  return (
    <div style={{ display: "flex", justifyContent: "space-between", padding: "3px 0" }}>
      <span style={{ color: "#64748b", fontSize: 13 }}>{label}</span>
      <span style={{ color: "#e2e8f0", fontSize: 13, textAlign: "right", maxWidth: "60%" }}>{value}</span>
    </div>
  );
}

function PriorityBadge({ p }) {
  const colors = { 1: "#dc2626", 2: "#f97316", 3: "#f59e0b", 4: "#eab308", 5: "#64748b" };
  return <span style={{ ...badgeBase, background: (colors[p] || "#64748b") + "22", color: colors[p] || "#64748b", minWidth: 24, textAlign: "center" }}>P{p || "?"}</span>;
}

function TypeBadge({ type }) {
  const icons = { car_accident: "&#x1F697;", motorcycle_accident: "&#x1F3CD;", truck_accident: "&#x1F69B;", work_accident: "&#x1F3D7;", pedestrian: "&#x1F6B6;", bicycle: "&#x1F6B2;", slip_fall: "&#x26A0;", bus_accident: "&#x1F68C;" };
  return <span style={{ ...badgeBase, background: "#1e293b", color: "#e2e8f0" }} dangerouslySetInnerHTML={{ __html: icons[type] || "&#x1F697;" }} />;
}

function SeverityBadge({ severity }) {
  const map = { fatal: { bg: "#7f1d1d", fg: "#fca5a5" }, critical: { bg: "#7c2d12", fg: "#fdba74" }, serious: { bg: "#713f12", fg: "#fde68a" }, moderate: { bg: "#1e3a5f", fg: "#93c5fd" }, minor: { bg: "#1e293b", fg: "#94a3b8" } };
  const s = map[severity] || map.minor;
  return <span style={{ ...badgeBase, background: s.bg, color: s.fg }}>{severity || "unknown"}</span>;
}

function StatusBadge({ status }) {
  const map = { new: { bg: "#1e3a5f", fg: "#60a5fa" }, verified: { bg: "#14532d", fg: "#86efac" }, assigned: { bg: "#713f12", fg: "#fde68a" }, contacted: { bg: "#14532d", fg: "#86efac" }, in_progress: { bg: "#7c2d12", fg: "#fdba74" }, closed: { bg: "#1e293b", fg: "#64748b" } };
  const s = map[status] || map.new;
  return <span style={{ ...badgeBase, background: s.bg, color: s.fg }}>{(status || "new").replace(/_/g, " ")}</span>;
}

function contactStatusColor(status) {
  const map = { not_contacted: { bg: "#1e293b", fg: "#94a3b8" }, attempted: { bg: "#713f12", fg: "#fde68a" }, contacted: { bg: "#14532d", fg: "#86efac" }, interested: { bg: "#14532d", fg: "#4ade80" }, not_interested: { bg: "#1e293b", fg: "#64748b" }, retained: { bg: "#1e3a5f", fg: "#60a5fa" }, has_attorney: { bg: "#7f1d1d", fg: "#fca5a5" } };
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
const inputStyle = { background: "#0f1219", border: "1px solid #1e293b", borderRadius: 8, padding: "10px 14px", color: "#e2e8f0", fontSize: 14, width: "100%", outline: "none", boxSizing: "border-box" };
const selectStyle = { ...inputStyle, marginBottom: 12 };
const labelStyle = { color: "#64748b", fontSize: 11, textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 4, display: "block" };
const btnPrimary = { width: "100%", background: "#f59e0b", color: "#0a0e1a", border: "none", borderRadius: 8, padding: "12px 16px", fontWeight: 700, fontSize: 15, cursor: "pointer", marginTop: 20 };
const btnSmall = { border: "none", borderRadius: 6, padding: "6px 12px", color: "#fff", fontSize: 12, fontWeight: 600, cursor: "pointer" };
const navBtn = { background: "none", border: "none", cursor: "pointer", fontSize: 12, fontWeight: 600, letterSpacing: 0.5, padding: "18px 0" };
const cardStyle = { background: "#141829", borderRadius: 12, padding: 20, border: "1px solid #1e293b" };
const cardTitle = { color: "#f8fafc", fontSize: 16, fontWeight: 600, margin: "0 0 16px" };
const tdStyle = { padding: "10px 8px", fontSize: 13 };
const badgeBase = { fontSize: 11, fontWeight: 600, padding: "3px 8px", borderRadius: 6, display: "inline-block", textTransform: "capitalize" };
