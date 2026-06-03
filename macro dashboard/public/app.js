const state = {
  monitor: null,
  selectedId: null,
  selectedGroup: "All",
  selectedSeverity: "All",
  sort: "alert",
  query: "",
  window: "5Y",
  view: window.location.hash.startsWith("#case") ? "cases" : "monitor",
  selectedCaseId: window.location.hash.startsWith("#case=")
    ? decodeURIComponent(window.location.hash.slice(6).split("&m=")[0])
    : null,
  selectedCaseMetricId: window.location.hash.includes("&m=")
    ? decodeURIComponent(window.location.hash.split("&m=")[1])
    : null,
  seriesCache: new Map(),
  loading: false,
  demo: new URLSearchParams(window.location.search).get("demo") === "1",
  liveRefreshInFlight: false
};

const els = {
  groupNav: document.getElementById("groupNav"),
  severityNav: document.getElementById("severityNav"),
  metricRows: document.getElementById("metricRows"),
  scenarioGrid: document.getElementById("scenarioGrid"),
  assetImpactGrid: document.getElementById("assetImpactGrid"),
  countryGrid: document.getElementById("countryGrid"),
  monitorView: document.getElementById("monitorView"),
  caseView: document.getElementById("caseView"),
  monitorTopbar: document.getElementById("monitorTopbar"),
  caseTopbar: document.getElementById("caseTopbar"),
  viewNav: document.getElementById("viewNav"),
  backToMonitorBtn: document.getElementById("backToMonitorBtn"),
  caseTabs: document.getElementById("caseTabs"),
  caseDetail: document.getElementById("caseDetail"),
  casePillars: document.getElementById("casePillars"),
  caseRows: document.getElementById("caseRows"),
  inspector: document.getElementById("inspector"),
  searchInput: document.getElementById("searchInput"),
  refreshBtn: document.getElementById("refreshBtn"),
  demoBtn: document.getElementById("demoBtn"),
  sortTabs: document.getElementById("sortTabs"),
  runMeta: document.getElementById("runMeta"),
  coverageText: document.getElementById("coverageText"),
  topScenarioText: document.getElementById("topScenarioText"),
  extremeText: document.getElementById("extremeText"),
  cacheText: document.getElementById("cacheText"),
  sourceHealth: document.getElementById("sourceHealth"),
  sourceLinks: document.getElementById("sourceLinks"),
  toast: document.getElementById("toast")
};

const severityOrder = ["All", "extreme", "shock", "elevated", "watch", "normal", "unavailable"];
const windowOptions = ["1Y", "5Y", "10Y", "ALL"];

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function cssSeverity(severity) {
  return `severity-${severity || "unknown"}`;
}

function titleCase(value) {
  return String(value || "").replace(/_/g, " ").replace(/\b\w/g, (m) => m.toUpperCase());
}

function clamp(value, low, high) {
  return Math.max(low, Math.min(high, value));
}

function severityLabel(value) {
  if (!value) return "unknown";
  return titleCase(value);
}

function formatDate(value) {
  if (!value) return "--";
  return String(value).slice(0, 10);
}

function formatGeneratedAt(value) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString([], { month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit" });
}

function compactNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
  const num = Number(value);
  const abs = Math.abs(num);
  if (abs >= 1_000_000_000) return `${(num / 1_000_000_000).toFixed(2)}b`;
  if (abs >= 1_000_000) return `${(num / 1_000_000).toFixed(2)}m`;
  if (abs >= 100_000) return `${(num / 1000).toFixed(0)}k`;
  if (abs >= 1000) return num.toLocaleString(undefined, { maximumFractionDigits: 0 });
  if (abs >= 100) return num.toLocaleString(undefined, { maximumFractionDigits: 1 });
  if (abs >= 10) return num.toLocaleString(undefined, { maximumFractionDigits: digits });
  return num.toLocaleString(undefined, { maximumFractionDigits: 3 });
}

function formatValue(value, unit) {
  if (value === null || value === undefined) return "--";
  const text = compactNumber(value, unit === "%" || unit === "pp" ? 2 : 2);
  if (!unit || unit === "index" || unit === "people") return text;
  if (unit === "%") return `${text}%`;
  if (unit === "% GDP") return `${text}% GDP`;
  if (unit === "pp") return `${text} pp`;
  if (unit === "% ann.") return `${text}% ann.`;
  return `${text} ${unit}`;
}

function formatMove(metric) {
  const move = metric.change1m;
  if (move === null || move === undefined) return "--";
  const suffix = metric.changeMode === "pct" ? "%" : (metric.unit === "%" || metric.unit === "% GDP") ? " pp" : "";
  const sign = move > 0 ? "+" : "";
  return `${sign}${compactNumber(move, 2)}${suffix}`;
}

function formatPercentile(value) {
  if (value === null || value === undefined) return "--";
  return `${Math.round(Number(value) * 100)}%`;
}

function formatCache(seconds) {
  if (!seconds) return "--";
  const hours = seconds / 3600;
  if (hours >= 1) return `${Math.round(hours)}h TTL`;
  return `${Math.round(seconds / 60)}m TTL`;
}

function showToast(message) {
  els.toast.textContent = message;
  els.toast.classList.add("visible");
  window.clearTimeout(showToast._timer);
  showToast._timer = window.setTimeout(() => els.toast.classList.remove("visible"), 3200);
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${response.status} ${text.slice(0, 140)}`);
  }
  return response.json();
}

async function fetchJsonWithTimeout(url, timeoutMs = 45000) {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { signal: controller.signal });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`${response.status} ${text.slice(0, 140)}`);
    }
    return response.json();
  } finally {
    window.clearTimeout(timer);
  }
}

function applyMonitorPayload(payload, options = {}) {
  state.monitor = payload;
  state.loading = false;
  if (!state.selectedId) {
    const first = payload.metrics.find((metric) => metric.ok);
    state.selectedId = first ? first.id : null;
  }
  if (!state.selectedCaseId) {
    const firstCase = (payload.caseStudies || [])[0];
    state.selectedCaseId = firstCase ? firstCase.id : null;
  }
  renderAll();
  if (state.selectedId) selectMetric(state.selectedId, { quiet: true });
  if (!options.quiet) {
    const mode = payload.snapshot ? "bootstrap snapshot" : payload.demo ? "demo data" : "public data";
    showToast(`Monitor loaded from ${mode}.`);
  }
}

async function loadMonitor(refresh = false) {
  state.loading = true;
  renderLoading();
  const params = new URLSearchParams();
  if (refresh) params.set("refresh", "1");
  if (state.demo) params.set("demo", "1");
  try {
    if (!refresh && !state.demo) {
      try {
        const snapshot = await fetchJsonWithTimeout("/api/snapshot", 8000);
        applyMonitorPayload(snapshot);
        refreshMonitorInBackground();
        return;
      } catch (snapshotError) {
        showToast("Bootstrap snapshot unavailable; pulling live data.");
      }
    }
    const payload = await fetchJsonWithTimeout(`/api/monitor?${params.toString()}`, refresh ? 90000 : 45000);
    applyMonitorPayload(payload);
  } catch (error) {
    state.loading = false;
    renderError(error);
    showToast("Live pull is slow or failed. Demo mode is available.");
  }
}

async function refreshMonitorInBackground() {
  if (state.liveRefreshInFlight || state.demo) return;
  state.liveRefreshInFlight = true;
  try {
    const payload = await fetchJsonWithTimeout("/api/monitor", 120000);
    applyMonitorPayload(payload, { quiet: true });
    showToast("Live public data finished refreshing.");
  } catch (error) {
    showToast("Live refresh is still warming up; snapshot remains loaded.");
  } finally {
    state.liveRefreshInFlight = false;
  }
}

function renderLoading() {
  els.metricRows.innerHTML = `<div class="loading-block">Pulling public data and scoring anomalies...</div>`;
  if (els.caseRows) {
    els.caseRows.innerHTML = `<div class="loading-block">Replaying historical framework...</div>`;
  }
  if (els.casePillars) {
    els.casePillars.innerHTML = Array.from({ length: 5 }).map(() => `
      <div class="case-pillar loading"><span class="score-ring small" style="--score:0">--</span><strong>Loading pillar</strong></div>
    `).join("");
  }
  if (els.caseTabs) {
    els.caseTabs.innerHTML = Array.from({ length: 4 }).map(() => `
      <div class="case-tab loading"><span class="score-ring small" style="--score:0">--</span><strong>Loading case</strong><em>waiting</em></div>
    `).join("");
  }
  if (els.countryGrid) {
    els.countryGrid.innerHTML = `
      <div class="country-header">
        <span>Score</span><span>Country</span><span>Debt/GDP</span><span>10Y</span><span>Inflation</span><span>GDP</span><span>C/A</span><span>Driver</span>
      </div>
      ${Array.from({ length: 6 }).map(() => `
        <div class="country-row loading">
          <span class="score-ring small" style="--score:0">--</span>
          <span class="country-name"><strong>Loading</strong><em>waiting</em></span>
          <span>--</span><span>--</span><span>--</span><span>--</span><span>--</span><span>--</span>
        </div>
      `).join("")}
    `;
  }
  els.scenarioGrid.innerHTML = Array.from({ length: 6 }).map(() => `
    <div class="scenario-tile severity-unknown">
      <div class="scenario-top"><span class="score-ring" style="--score:0">--</span><span class="severity-pill">loading</span></div>
      <h4>Loading scenario</h4>
      <ul class="driver-list"><li><span>waiting</span><strong>--</strong></li></ul>
    </div>
  `).join("");
}

function renderError(error) {
  els.runMeta.textContent = `Live pull failed: ${error.message}`;
  els.metricRows.innerHTML = `
    <div class="loading-block">
      <div>
        <strong>No live data returned.</strong><br />
        Start demo mode to inspect the interface, or refresh after network access is available.
      </div>
    </div>
  `;
}

function renderAll() {
  if (!state.monitor) return;
  renderView();
  renderTopStatus();
  renderSources();
  renderNavs();
  renderCountries();
  renderScenarios();
  renderAssetImpacts();
  renderRows();
  renderInspector();
  renderCaseStudies();
}

function setView(view, options = {}) {
  state.view = view === "cases" ? "cases" : "monitor";
  if (!options.quiet) {
    if (state.view === "cases") {
      window.location.hash = state.selectedCaseId ? `case=${state.selectedCaseId}` : "case-studies";
    } else {
      window.location.hash = "";
    }
  }
  renderView();
}

function parseLocationHash() {
  const hash = window.location.hash || "";
  if (hash.startsWith("#case=")) {
    const body = hash.slice(6);
    const [caseId, metricId] = body.split("&m=");
    return {
      view: "cases",
      caseId: decodeURIComponent(caseId),
      metricId: metricId ? decodeURIComponent(metricId) : null
    };
  }
  if (hash === "#case-studies") {
    return { view: "cases", caseId: null, metricId: null };
  }
  return { view: "monitor", caseId: null, metricId: null };
}

function renderView() {
  const showCases = state.view === "cases";
  els.monitorView.hidden = showCases;
  els.monitorTopbar.hidden = showCases;
  els.caseView.hidden = !showCases;
  els.caseTopbar.hidden = !showCases;
  els.viewNav?.querySelectorAll("button[data-view]").forEach((button) => {
    let active = button.dataset.view === state.view;
    if (active && state.view === "cases") {
      // Multiple case buttons share data-view="cases"; only the selected case is active.
      active = button.dataset.case
        ? button.dataset.case === state.selectedCaseId
        : false;
    }
    button.classList.toggle("active", active);
  });
}

function renderTopStatus() {
  const monitor = state.monitor;
  const coverage = monitor.coverage || {};
  const topScenario = (monitor.scenarios || [])[0];
  const extremes = monitor.extremes || [];
  const mode = monitor.snapshot ? "Snapshot mode" : monitor.demo ? "Demo mode" : "Live mode";
  els.runMeta.textContent = `${mode} - generated ${formatGeneratedAt(monitor.generatedAt)} from ${coverage.ok || 0}/${coverage.total || 0} available series.`;
  els.coverageText.textContent = `${coverage.ok || 0}/${coverage.total || 0} live`;
  els.topScenarioText.textContent = topScenario ? `${topScenario.name} ${topScenario.score}` : "--";
  els.extremeText.textContent = `${extremes.length} highlighted`;
  els.cacheText.textContent = formatCache(monitor.cacheTtlSeconds);
  if (els.demoBtn) {
    els.demoBtn.textContent = state.demo ? "Live" : "Demo";
  }
}

function renderSources() {
  const counts = state.monitor.sourceCounts || {};
  const parts = Object.keys(counts).sort().map((key) => `${key}: ${counts[key]}`);
  els.sourceHealth.textContent = parts.length ? parts.join(" / ") : "No sources yet.";
  els.sourceLinks.innerHTML = (state.monitor.sources || []).map((source) => `
    <a href="${escapeHtml(source.url)}" target="_blank" rel="noreferrer">${escapeHtml(source.label || source.name)}</a>
  `).join("");
}

function renderNavs() {
  const metrics = state.monitor.metrics || [];
  const groups = ["All", ...Array.from(new Set(metrics.map((metric) => metric.group).filter(Boolean))).sort()];
  els.groupNav.innerHTML = groups.map((group) => {
    const count = group === "All" ? metrics.length : metrics.filter((metric) => metric.group === group).length;
    return `
      <button type="button" class="${state.selectedGroup === group ? "active" : ""}" data-group="${escapeHtml(group)}">
        <span>${escapeHtml(group)}</span>
        <span class="nav-count">${count}</span>
      </button>
    `;
  }).join("");

  els.severityNav.innerHTML = severityOrder.map((severity) => {
    const count = severity === "All" ? metrics.length : metrics.filter((metric) => metric.severity === severity).length;
    return `
      <button type="button" class="${state.selectedSeverity === severity ? "active" : ""}" data-severity="${escapeHtml(severity)}">
        <span>${severityLabel(severity)}</span>
        <span class="nav-count">${count}</span>
      </button>
    `;
  }).join("");
}

function sortedFilteredMetrics() {
  const query = state.query.trim().toLowerCase();
  const metrics = (state.monitor?.metrics || []).filter((metric) => {
    if (state.selectedGroup !== "All" && metric.group !== state.selectedGroup) return false;
    if (state.selectedSeverity !== "All" && metric.severity !== state.selectedSeverity) return false;
    if (query) {
      const haystack = [
        metric.short,
        metric.name,
        metric.group,
        metric.region,
        metric.country,
        metric.countryName,
        metric.countryShort,
        metric.sourceId,
        ...(metric.tags || [])
      ].join(" ").toLowerCase();
      if (!haystack.includes(query)) return false;
    }
    return true;
  });
  const sorters = {
    alert: (metric) => metric.alertScore || 0,
    risk: (metric) => metric.riskScore || 0,
    tail: (metric) => Math.abs((metric.tailPercentile || 0.5) - 0.5),
    move: (metric) => Math.abs(metric.momentumZ || 0)
  };
  metrics.sort((a, b) => {
    const primary = (sorters[state.sort] || sorters.alert)(b) - (sorters[state.sort] || sorters.alert)(a);
    if (primary !== 0) return primary;
    return (b.alertScore || 0) - (a.alertScore || 0);
  });
  return metrics;
}

function renderScenarios() {
  const scenarios = state.monitor.scenarios || [];
  els.scenarioGrid.innerHTML = scenarios.map((scenario) => {
    const drivers = (scenario.drivers || []).slice(0, 3).map((driver) => `
      <li>
        <span>${escapeHtml(driver.short || driver.name)}</span>
        <strong>${driver.riskScore ?? "--"}</strong>
      </li>
    `).join("");
    return `
      <button type="button" class="scenario-tile ${cssSeverity(scenario.severity)}" data-scenario="${escapeHtml(scenario.id)}">
        <div class="scenario-top">
          <span class="score-ring" style="--score:${scenario.score || 0}">${scenario.score || 0}</span>
          <span class="severity-pill">${severityLabel(scenario.severity)}</span>
        </div>
        <h4>${escapeHtml(scenario.name)}</h4>
        <ul class="driver-list">${drivers || `<li><span>No driver</span><strong>--</strong></li>`}</ul>
      </button>
    `;
  }).join("");
}

function assetDirectionMeta(direction) {
  return {
    tailwind: { label: "Tailwind", arrow: "▲", cls: "asset-up" },
    headwind: { label: "Headwind", arrow: "▼", cls: "asset-down" },
    neutral: { label: "Neutral", arrow: "▬", cls: "asset-flat" }
  }[direction] || { label: "Neutral", arrow: "▬", cls: "asset-flat" };
}

function renderAssetImpacts() {
  if (!els.assetImpactGrid) return;
  const assets = state.monitor.assetImpacts || [];
  if (!assets.length) {
    els.assetImpactGrid.innerHTML = `<div class="loading-block">Asset impact read becomes available after a live scenario pull.</div>`;
    return;
  }
  els.assetImpactGrid.innerHTML = assets.map((asset) => {
    const meta = assetDirectionMeta(asset.direction);
    const bias = Number(asset.bias) || 0;
    const width = clamp(Math.abs(bias), 0, 100);
    const topDriver = (asset.drivers || [])[0];
    const drivers = (asset.drivers || []).slice(0, 3).map((driver) => `
      <li class="${driver.supports ? "supports" : "opposes"}">
        <span class="driver-dir">${driver.supports ? "+" : "−"}</span>
        <span>${escapeHtml(driver.name)}</span>
        <strong>${driver.scenarioScore ?? "--"}</strong>
      </li>
    `).join("");
    return `
      <button type="button" class="asset-tile ${meta.cls}" data-scenario="${escapeHtml(topDriver?.scenarioId || "")}">
        <div class="asset-top">
          <div class="asset-name">
            <strong>${escapeHtml(asset.short || asset.name)}</strong>
            <em>${escapeHtml(asset.name)}</em>
          </div>
          <span class="asset-bias">${meta.arrow} ${bias > 0 ? "+" : ""}${bias}</span>
        </div>
        <div class="asset-readout">
          <span class="asset-pill">${escapeHtml(titleCase(asset.strength))} ${meta.label}</span>
        </div>
        <div class="asset-bar"><span class="asset-bar-fill" style="--w:${width};--side:${bias >= 0 ? 1 : -1}"></span></div>
        <ul class="asset-drivers">${drivers || `<li><span>No active driver</span></li>`}</ul>
      </button>
    `;
  }).join("");
}

function metricChip(metric, fallback = "--") {
  if (!metric) return `<span class="country-missing">${fallback}</span>`;
  return `
    <span class="country-value ${cssSeverity(metric.severity)}">
      <strong>${formatValue(metric.latest, metric.unit)}</strong>
      <em>${formatDate(metric.asOf)}</em>
    </span>
  `;
}

function renderCountries() {
  if (!els.countryGrid) return;
  const countries = state.monitor.countries || [];
  if (!countries.length) {
    els.countryGrid.innerHTML = `<div class="loading-block">No country stress data available.</div>`;
    return;
  }

  els.countryGrid.innerHTML = `
    <div class="country-header">
      <span>Score</span>
      <span>Country</span>
      <span>Debt/GDP</span>
      <span>10Y</span>
      <span>Inflation</span>
      <span>GDP</span>
      <span>C/A</span>
      <span>Driver</span>
    </div>
    ${countries.map((country) => {
      const summary = country.summary || {};
      const driver = (country.drivers || [])[0];
      const targetId = driver?.id || summary.debt?.id || summary.inflation?.id || "";
      return `
        <button type="button" class="country-row ${cssSeverity(country.severity)}" data-id="${escapeHtml(targetId)}">
          <span class="score-ring small" style="--score:${country.score || 0}">${country.score || 0}</span>
          <span class="country-name">
            <strong>${escapeHtml(country.short || country.code)}</strong>
            <em>${escapeHtml(country.name || "")}</em>
          </span>
          ${metricChip(summary.debt)}
          ${metricChip(summary.yield10y || summary.spread)}
          ${metricChip(summary.inflation)}
          ${metricChip(summary.growth)}
          ${metricChip(summary.currentAccount)}
          <span class="country-driver">
            <strong>${escapeHtml(driver?.short || driver?.name || "No driver")}</strong>
            <em>${driver?.riskScore ?? "--"} risk / ${country.available || 0} live</em>
          </span>
        </button>
      `;
    }).join("")}
  `;
}

function caseStatusLabel(status) {
  return {
    matched: "Matched",
    not_yet: "Missing now",
    now_only: "Now-only",
    quiet: "Quiet"
  }[status] || "Unknown";
}

function caseStatusNote(status) {
  return {
    matched: "Then fired and now fires",
    not_yet: "Then fired; now below threshold",
    now_only: "Quiet then; firing now",
    quiet: "No alert in either window"
  }[status] || "";
}

function caseValueBlock(side) {
  if (!side?.ok) return `<span class="case-value muted">Unavailable</span>`;
  return `
    <span class="case-value ${cssSeverity(side.severity)}">
      <strong>${formatValue(side.latest, side.unit)}</strong>
      <em>${side.riskScore ?? "--"} risk / ${formatPercentile(side.percentile)} / ${formatDate(side.asOf)}</em>
    </span>
  `;
}

function signedNumber(value, digits = 0) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return "--";
  const numeric = Number(value);
  const sign = numeric > 0 ? "+" : "";
  return `${sign}${numeric.toFixed(digits)}`;
}

function pointDelta(nowSide, thenSide) {
  if (!nowSide?.ok || !thenSide?.ok) return { level: "--", relative: "--" };
  const now = Number(nowSide.latest);
  const then = Number(thenSide.latest);
  if (!Number.isFinite(now) || !Number.isFinite(then)) return { level: "--", relative: "--" };
  const unit = nowSide.unit || thenSide.unit;
  const level = formatValue(now - then, unit);
  const relative = then === 0 ? "--" : `${signedNumber((now / then - 1) * 100, 1)}%`;
  return { level: now > then ? `+${level}` : level, relative };
}

function caseSideDetail(label, side) {
  if (!side?.ok) {
    return `
      <div class="case-side-card muted">
        <h4>${label}</h4>
        <p>Unavailable in this window.</p>
      </div>
    `;
  }
  return `
    <div class="case-side-card ${cssSeverity(side.severity)}">
      <h4>${label}</h4>
      <strong>${formatValue(side.latest, side.unit)}</strong>
      <div class="case-side-metrics">
        <span><em>Risk</em><b>${side.riskScore ?? "--"}</b></span>
        <span><em>Percentile</em><b>${formatPercentile(side.percentile)}</b></span>
        <span><em>As of</em><b>${formatDate(side.asOf)}</b></span>
      </div>
    </div>
  `;
}

function caseExpandedDetail(row, study) {
  const delta = pointDelta(row.now, row.case);
  const riskDelta = (row.now?.riskScore ?? null) !== null && (row.case?.riskScore ?? null) !== null
    ? signedNumber((row.now.riskScore || 0) - (row.case.riskScore || 0), 0)
    : "--";
  const pctDelta = (row.now?.percentile ?? null) !== null && (row.case?.percentile ?? null) !== null
    ? signedNumber(((row.now.percentile || 0) - (row.case.percentile || 0)) * 100, 0) + " pts"
    : "--";
  return `
    <div class="case-expanded ${row.status}">
      <div class="case-expanded-head">
        <div>
          <p class="eyebrow">${escapeHtml(row.pillar?.name || row.group || "Public signal")}</p>
          <h4>${escapeHtml(row.short || row.name)}</h4>
          <p>${escapeHtml(row.name || "")}</p>
        </div>
        <span class="severity-pill">${escapeHtml(caseStatusLabel(row.status))}</span>
      </div>
      <div class="case-chart-block">
        <div class="case-chart-head">
          <span class="case-chart-title">${study.shockDate ? "Historical path into the shock" : "Trend vs baseline"}</span>
          <span class="case-chart-legend">
            ${study.shockDate ? `<i class="lg-shock"></i>${escapeHtml(formatDate(study.shockDate))} shock` : ""}
            <i class="lg-base"></i>${study.monitorMode ? "baseline" : "as-of"} ${escapeHtml(formatDate(study.asOf))}
          </span>
        </div>
        <svg id="caseChart" class="case-chart" role="img" aria-label="${escapeHtml((row.short || row.name) + " historical trend")}"></svg>
        <div class="case-chart-loading" id="caseChartLoading">Loading historical series…</div>
      </div>
      <div class="case-compare-grid">
        ${caseSideDetail(`Then · ${formatDate(study.asOf)}`, row.case)}
        ${caseSideDetail("Now", row.now)}
      </div>
      <div class="case-delta-grid">
        <span><em>Risk delta</em><strong>${riskDelta}</strong></span>
        <span><em>Percentile delta</em><strong>${pctDelta}</strong></span>
        <span><em>Level delta</em><strong>${escapeHtml(delta.level)}</strong></span>
        <span><em>Relative delta</em><strong>${escapeHtml(delta.relative)}</strong></span>
      </div>
      <div class="case-expanded-read">
        <p><strong>Read:</strong> ${escapeHtml(caseStatusNote(row.status))}. Threshold is ${study.threshold || 45}; then risk was ${row.case?.riskScore ?? "--"}, now risk is ${row.now?.riskScore ?? "--"}.</p>
        <p><strong>Why it matters:</strong> ${escapeHtml(row.why || "This indicator is part of the public-data checklist for the historical setup.")}</p>
      </div>
      <button type="button" class="case-open-monitor" data-open-monitor="${escapeHtml(row.id)}">Open full history chart</button>
    </div>
  `;
}

function summarizePillarRows(rows) {
  const active = rows.filter((row) => row.status !== "quiet");
  if (!active.length) return `<span class="case-mini-signal muted">No active public-data signal</span>`;
  return active.slice(0, 4).map((row) => `
    <span class="case-mini-signal ${row.status}">
      <strong>${escapeHtml(row.short || row.name)}</strong>
      <em>${caseStatusLabel(row.status)}</em>
    </span>
  `).join("");
}

function renderCaseStudies() {
  if (!els.caseTabs || !els.caseRows || !els.caseDetail || !els.casePillars) return;
  const studies = state.monitor.caseStudies || [];
  if (!studies.length) {
    els.caseTabs.innerHTML = "";
    els.casePillars.innerHTML = "";
    els.caseDetail.innerHTML = `<div class="empty-state"><span class="empty-dot"></span><h3>No case study</h3><p>The research framework is not available in the current payload.</p></div>`;
    els.caseRows.innerHTML = "";
    state.selectedCaseMetricId = null;
    return;
  }
  // Only default when nothing is selected. If a case is selected (e.g. from a
  // #case= deep link) but missing from THIS payload (a stale bootstrap snapshot
  // that predates it), keep it pending so the live refresh can honor it instead
  // of clobbering the choice to studies[0].
  if (!state.selectedCaseId) {
    state.selectedCaseId = studies[0].id;
  }
  const selected = studies.find((study) => study.id === state.selectedCaseId) || studies[0];

  els.caseTabs.innerHTML = studies.map((study) => `
    <button type="button" class="case-tab ${study.id === selected.id ? "active" : ""}" data-case="${escapeHtml(study.id)}">
      <span class="score-ring small" style="--score:${study.matchScore || 0}">${study.matchScore || 0}</span>
      <span>
        <strong>${escapeHtml(study.name)}</strong>
        <em>${study.matchedCount || 0}/${study.caseAlertCount || 0} public signals match · as-of ${formatDate(study.asOf)}</em>
      </span>
    </button>
  `).join("");

  const rows = selected.metrics || [];
  const matched = rows.filter((row) => row.status === "matched");
  const notYet = rows.filter((row) => row.status === "not_yet");
  const nowOnly = rows.filter((row) => row.status === "now_only");
  if (!state.selectedCaseMetricId || !rows.some((row) => row.id === state.selectedCaseMetricId)) {
    state.selectedCaseMetricId = (matched[0] || nowOnly[0] || notYet[0] || rows[0] || {}).id || null;
  }
  els.caseDetail.innerHTML = `
    <div class="case-detail-head">
      <div>
        <p class="eyebrow">${selected.monitorMode
          ? `baseline ${escapeHtml(formatDate(selected.asOf))} / live monitor`
          : `${escapeHtml(formatDate(selected.asOf))} / shock ${escapeHtml(formatDate(selected.shockDate))}`}</p>
        <h3>${escapeHtml(selected.name)}</h3>
        <p>${escapeHtml(selected.market || "")}</p>
      </div>
      <div class="big-score">${selected.matchScore || 0}</div>
    </div>
    <div class="case-stats">
      <div><span class="metric-label">Framework match</span><strong>${selected.matchScore || 0}</strong></div>
      <div><span class="metric-label">Matched</span><strong>${matched.length}</strong></div>
      <div><span class="metric-label">Now-only</span><strong>${nowOnly.length}</strong></div>
    </div>
    <div class="case-read-grid">
      <div>
        <span class="metric-label">Framework basis</span>
        <p>${escapeHtml(selected.frameworkBasis || "Candidate indicators are converted into public ratios and standardized against their own history before they affect the score.")}</p>
      </div>
      <div>
        <span class="metric-label">Current verification</span>
        <p>${escapeHtml(selected.currentVerification || "Current public readings are checked independently against the historical framework.")}</p>
      </div>
    </div>
    <ul class="analysis-list">
      <li>${escapeHtml(selected.summary || "")}</li>
      <li>${escapeHtml(selected.lesson || "")}</li>
      <li>Historical scores use only data dated on or before ${escapeHtml(formatDate(selected.asOf))}; current scores use the latest public observations.</li>
    </ul>
  `;

  els.casePillars.innerHTML = (selected.pillars || []).map((pillar) => {
    const pillarRows = rows.filter((row) => (pillar.metricIds || []).includes(row.id));
    return `
      <article class="case-pillar">
        <div class="case-pillar-head">
          <span class="score-ring small" style="--score:${pillar.matchScore || 0}">${pillar.matchScore || 0}</span>
          <div>
            <strong>${escapeHtml(pillar.name || "Pillar")}</strong>
            <em>${pillar.matchedCount || 0}/${pillar.caseAlertCount || 0} matched · ${pillar.nowOnlyCount || 0} now-only</em>
          </div>
        </div>
        <div class="case-pillar-read">
          <span class="metric-label">Indicator logic</span>
          <p>${escapeHtml(pillar.historicalRead || "")}</p>
        </div>
        <div class="case-pillar-read">
          <span class="metric-label">Public-data check</span>
          <p>${escapeHtml(pillar.todayRead || "")}</p>
        </div>
        <div class="case-mini-signals">${summarizePillarRows(pillarRows)}</div>
      </article>
    `;
  }).join("");

  els.caseRows.innerHTML = rows.map((row) => {
    const active = row.id === state.selectedCaseMetricId;
    return `
      <button type="button" class="case-row ${row.status} ${active ? "active" : ""}" data-id="${escapeHtml(row.id)}" aria-expanded="${active ? "true" : "false"}">
        <span class="case-status">
          <strong>${caseStatusLabel(row.status)}</strong>
          <em>${caseStatusNote(row.status)}</em>
        </span>
        <span class="indicator-cell">
          <strong>${escapeHtml(row.short || row.name)}</strong>
          <span>${escapeHtml(row.name || "")}</span>
          <small>${escapeHtml(row.pillar?.name || row.group || "")}</small>
        </span>
        ${caseValueBlock(row.case)}
        ${caseValueBlock(row.now)}
      </button>
      ${active ? caseExpandedDetail(row, selected) : ""}
    `;
  }).join("");

  if (state.view === "cases" && state.selectedCaseMetricId) {
    drawSelectedCaseChart(selected);
  }
}

function caseChartWindow(points, study) {
  if (!points || !points.length) return points || [];
  const latest = new Date(points[points.length - 1].date).getTime();
  const year = 365 * 86400000;
  const anchor = study.shockDate
    ? new Date(study.shockDate).getTime()
    : study.asOf
    ? new Date(study.asOf).getTime()
    : latest;
  // Start a few years before the shock/baseline so the run-up is clearly visible.
  const start = Math.min(anchor - 8 * year, latest - 6 * year);
  const windowed = points.filter((point) => new Date(point.date).getTime() >= start);
  return windowed.length >= 2 ? windowed : points;
}

async function drawSelectedCaseChart(study) {
  const metricId = state.selectedCaseMetricId;
  if (!metricId) return;
  const cached = state.seriesCache.get(metricId);
  if (cached?.points) {
    const svg = document.getElementById("caseChart");
    const loading = document.getElementById("caseChartLoading");
    if (loading) loading.style.display = "none";
    if (svg) drawCaseChart(svg, cached, study);
    return;
  }
  try {
    const params = new URLSearchParams({ id: metricId });
    if (state.demo) params.set("demo", "1");
    const series = await fetchJson(`/api/series?${params.toString()}`);
    state.seriesCache.set(metricId, series);
    if (state.view !== "cases" || state.selectedCaseMetricId !== metricId) return;
    const svg = document.getElementById("caseChart");
    const loading = document.getElementById("caseChartLoading");
    if (loading) loading.style.display = "none";
    if (svg && series?.points) drawCaseChart(svg, series, study);
  } catch (error) {
    const loading = document.getElementById("caseChartLoading");
    if (loading) loading.textContent = "Historical series unavailable for this signal.";
  }
}

function drawCaseChart(svg, series, study) {
  if (!svg || !series?.points?.length) return;
  const wrap = svg.parentElement;
  const width = Math.max(360, (wrap && wrap.clientWidth) || 560);
  const height = 230;
  const margin = { top: 20, right: 16, bottom: 30, left: 48 };
  const raw = caseChartWindow(series.points, study).filter((point) => Number.isFinite(Number(point.value)));
  const points = downsample(raw, 700);
  if (points.length < 2) return;
  const dates = points.map((point) => new Date(point.date).getTime());
  const values = points.map((point) => Number(point.value));
  let min = Math.min(...values);
  let max = Math.max(...values);
  const pad = (max - min || 1) * 0.12;
  min -= pad;
  max += pad;
  const xMin = Math.min(...dates);
  const xMax = Math.max(...dates);
  const plotW = width - margin.left - margin.right;
  const plotH = height - margin.top - margin.bottom;
  const x = (ms) => margin.left + ((ms - xMin) / (xMax - xMin || 1)) * plotW;
  const y = (value) => margin.top + (1 - (value - min) / (max - min || 1)) * plotH;
  const path = points
    .map((point, index) => `${index ? "L" : "M"} ${x(new Date(point.date).getTime()).toFixed(1)} ${y(Number(point.value)).toFixed(1)}`)
    .join(" ");
  const grid = [0, 0.5, 1]
    .map((step) => {
      const gy = margin.top + step * plotH;
      const value = max - step * (max - min);
      return `<line x1="${margin.left}" y1="${gy.toFixed(1)}" x2="${width - margin.right}" y2="${gy.toFixed(1)}" stroke="rgba(23,21,15,0.08)" /><text x="6" y="${(gy + 3).toFixed(1)}" fill="#736f64" font-size="9" font-family="monospace">${escapeHtml(compactNumber(value, 2))}</text>`;
    })
    .join("");
  const band = series.p05 != null && series.p95 != null && series.p95 >= series.p05
    ? `<rect x="${margin.left}" y="${y(series.p95).toFixed(1)}" width="${plotW}" height="${Math.max(1, y(series.p05) - y(series.p95)).toFixed(1)}" fill="rgba(44,122,107,0.08)" />`
    : "";
  const vline = (ms, color, label, dash) => {
    if (!Number.isFinite(ms) || ms < xMin || ms > xMax) return "";
    const vx = x(ms);
    return `<line x1="${vx.toFixed(1)}" y1="${margin.top}" x2="${vx.toFixed(1)}" y2="${height - margin.bottom}" stroke="${color}" stroke-width="1.6"${dash ? ` stroke-dasharray="${dash}"` : ""} /><text x="${vx.toFixed(1)}" y="${(margin.top - 6).toFixed(1)}" text-anchor="middle" fill="${color}" font-size="9.5" font-family="monospace">${escapeHtml(label)}</text>`;
  };
  const baseMs = study.asOf ? new Date(study.asOf).getTime() : null;
  const shockMs = study.shockDate ? new Date(study.shockDate).getTime() : null;
  const baseLine = baseMs ? vline(baseMs, "#736f64", study.monitorMode ? "baseline" : "as-of", "4 4") : "";
  const shockLine = shockMs ? vline(shockMs, "#bd3e34", "war", null) : "";
  const last = points[points.length - 1];
  const lastX = x(new Date(last.date).getTime());
  const lastY = y(Number(last.value));
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.innerHTML = `
    ${band}
    ${grid}
    ${baseLine}
    ${shockLine}
    <path d="${path}" fill="none" stroke="#2c7a6b" stroke-width="2" vector-effect="non-scaling-stroke" />
    <circle cx="${lastX.toFixed(1)}" cy="${lastY.toFixed(1)}" r="3.5" fill="#2c7a6b" stroke="#fffaf0" stroke-width="1.5" />
    <text x="${margin.left}" y="${(height - 8).toFixed(1)}" fill="#736f64" font-size="9" font-family="monospace">${formatDate(points[0].date)}</text>
    <text x="${(width - margin.right).toFixed(1)}" y="${(height - 8).toFixed(1)}" text-anchor="end" fill="#736f64" font-size="9" font-family="monospace">${formatDate(last.date)}</text>
  `;
}

function renderRows() {
  const metrics = sortedFilteredMetrics();
  if (!metrics.length) {
    els.metricRows.innerHTML = `<div class="loading-block">No signals match the current filters.</div>`;
    return;
  }
  els.metricRows.innerHTML = metrics.map((metric) => `
    <button type="button" class="metric-row ${cssSeverity(metric.severity)} ${state.selectedId === metric.id ? "active" : ""}" data-id="${escapeHtml(metric.id)}">
      <span class="score-stack">
        <span class="score-value">${metric.alertScore ?? 0}</span>
        <span class="score-kind">${severityLabel(metric.severity)}</span>
      </span>
      <span class="indicator-cell">
        <strong>${escapeHtml(metric.short || metric.name)}</strong>
        <span>${escapeHtml(metric.name || "")}</span>
      </span>
      <span class="num-cell latest-cell">${formatValue(metric.latest, metric.unit)}</span>
      <span class="percentile-bar">
        <span class="num-cell">${formatPercentile(metric.percentile)}</span>
        <span class="bar-track"><span class="bar-fill" style="--pct:${Math.round((metric.percentile || 0) * 100)}"></span></span>
      </span>
      <span class="num-cell move-cell">${formatMove(metric)}</span>
      <span class="spark-cell">${sparkline(metric.spark || [], metric.severity)}</span>
      <span class="asof">${formatDate(metric.asOf)}</span>
    </button>
  `).join("");
}

function sparkline(points, severity) {
  if (!points.length) return `<svg class="spark" viewBox="0 0 120 36" aria-hidden="true"></svg>`;
  const values = points.map((point) => Number(point.value)).filter((value) => Number.isFinite(value));
  if (!values.length) return `<svg class="spark" viewBox="0 0 120 36" aria-hidden="true"></svg>`;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const coords = points.map((point, index) => {
    const x = points.length === 1 ? 0 : (index / (points.length - 1)) * 118 + 1;
    const y = 34 - ((Number(point.value) - min) / span) * 30;
    return `${x.toFixed(2)},${y.toFixed(2)}`;
  }).join(" ");
  return `
    <svg class="spark ${cssSeverity(severity)}" viewBox="0 0 120 36" preserveAspectRatio="none" aria-hidden="true">
      <polyline points="${coords}" fill="none" stroke="var(--tile-color)" stroke-width="2" vector-effect="non-scaling-stroke" />
    </svg>
  `;
}

function selectedMetricBase() {
  if (!state.selectedId) return null;
  return (state.monitor?.metrics || []).find((metric) => metric.id === state.selectedId) || null;
}

async function selectMetric(id, options = {}) {
  state.selectedId = id;
  renderRows();
  renderInspector();
  if (state.monitor?.snapshot && !state.demo) {
    if (!options.quiet) showToast("Full history loads after live refresh finishes.");
    return;
  }
  const cached = state.seriesCache.get(id);
  if (cached?.points) return;
  const params = new URLSearchParams({ id });
  if (state.demo) params.set("demo", "1");
  try {
    const payload = await fetchJson(`/api/series?${params.toString()}`);
    state.seriesCache.set(id, payload);
    renderInspector();
  } catch (error) {
    if (!options.quiet) showToast(`Series load failed: ${error.message}`);
  }
}

function renderInspector() {
  const base = selectedMetricBase();
  if (!base) {
    els.inspector.innerHTML = `
      <div class="empty-state">
        <span class="empty-dot"></span>
        <h3>Select a signal</h3>
        <p>Click any row to inspect the history, distribution band, event markers, and rule-based anomaly read.</p>
      </div>
    `;
    return;
  }
  const series = state.seriesCache.get(base.id);
  const metric = series || base;
  const notes = metric.notes || base.notes || [];
  els.inspector.className = `inspector ${cssSeverity(base.severity)}`;
  els.inspector.innerHTML = `
    <div class="inspector-inner">
      <div class="inspector-head">
        <div>
          <p class="eyebrow">${escapeHtml(base.group || "Signal")} / ${escapeHtml(base.region || "")}</p>
          <h3>${escapeHtml(base.short || base.name)}</h3>
          <p>${escapeHtml(base.why || base.name || "")}</p>
        </div>
        <div class="big-score">${base.alertScore ?? 0}</div>
      </div>

      <div class="inspector-stats">
        <div><span class="metric-label">Latest</span><strong>${formatValue(base.latest, base.unit)}</strong></div>
        <div><span class="metric-label">Percentile</span><strong>${formatPercentile(base.percentile)}</strong></div>
        <div><span class="metric-label">Recent move</span><strong>${formatMove(base)}</strong></div>
      </div>

      <div class="window-tabs">
        ${windowOptions.map((option) => `<button type="button" data-window="${option}" class="${state.window === option ? "active" : ""}">${option}</button>`).join("")}
      </div>

      <div class="chart-wrap">
        ${series?.points ? `<svg id="detailChart" class="detail-chart" role="img" aria-label="${escapeHtml(base.name)} history chart"></svg><div id="chartTip" class="chart-tip"></div>` : `<div class="loading-block">Loading full history...</div>`}
      </div>

      <ul class="analysis-list">
        ${notes.map((note) => `<li>${escapeHtml(note)}</li>`).join("")}
        <li>${escapeHtml(extremaSentence(base))}</li>
      </ul>

      <div class="source-line">
        <span>${escapeHtml(base.provider || "").toUpperCase()} ${escapeHtml(base.sourceId || "")}</span>
        <span>${escapeHtml(base.sourceStatus || "")}</span>
        ${base.sourceUrl ? `<a href="${escapeHtml(base.sourceUrl)}" target="_blank" rel="noreferrer">source</a>` : ""}
        <span>as of ${formatDate(base.asOf)}</span>
      </div>
    </div>
  `;
  if (series?.points) {
    drawDetailChart(series);
  }
}

function extremaSentence(metric) {
  const min = metric.historicalMin;
  const max = metric.historicalMax;
  if (!min || !max) return "Historical extrema are unavailable for this series.";
  return `Historical low ${formatValue(min.value, metric.unit)} on ${formatDate(min.date)}; high ${formatValue(max.value, metric.unit)} on ${formatDate(max.date)}.`;
}

function filteredChartPoints(points) {
  if (!points?.length) return [];
  if (state.window === "ALL") return points;
  const days = { "1Y": 365, "5Y": 365 * 5, "10Y": 365 * 10 }[state.window] || 365 * 5;
  const last = new Date(points[points.length - 1].date);
  const cutoff = new Date(last.getTime() - days * 86400000);
  return points.filter((point) => new Date(point.date) >= cutoff);
}

function downsample(points, maxPoints = 900) {
  if (points.length <= maxPoints) return points;
  const stride = Math.ceil(points.length / maxPoints);
  const sampled = points.filter((_, index) => index % stride === 0);
  if (sampled[sampled.length - 1] !== points[points.length - 1]) sampled.push(points[points.length - 1]);
  return sampled;
}

function eventLabel(label) {
  return String(label || "")
    .replace("Lehman / GFC", "Lehman")
    .replace("U.S. downgrade / euro stress", "US downgrade")
    .replace("China devaluation shock", "China deval")
    .replace("COVID liquidation", "COVID")
    .replace("Inflation / Fed repricing", "Fed repricing")
    .replace("U.S. regional banks", "Regional banks")
    .replace("Japan carry unwind", "JPY carry");
}

function buildEventLayer(metric, x, xMin, xMax, width, height, margin) {
  const visible = (metric.events || [])
    .map((event) => ({ event, ms: new Date(event.date).getTime() }))
    .filter((item) => item.ms >= xMin && item.ms <= xMax)
    .map((item) => ({ ...item, ex: x(item.ms) }))
    .sort((a, b) => a.ex - b.ex);

  if (!visible.length) return "";

  const lines = visible.map(({ event, ex }) => `
    <g class="event-marker">
      <title>${escapeHtml(`${event.label} - ${event.date}`)}</title>
      <line x1="${ex.toFixed(2)}" y1="${margin.top}" x2="${ex.toFixed(2)}" y2="${height - margin.bottom}" stroke="rgba(169,85,45,0.26)" stroke-dasharray="4 6" />
      <circle cx="${ex.toFixed(2)}" cy="${height - margin.bottom + 7}" r="3" fill="#a9552d" opacity="0.72" />
    </g>
  `).join("");

  const labels = [];
  const maxLabels = width < 520 ? 2 : 4;
  let lastRight = -Infinity;
  for (const item of visible) {
    if (labels.length >= maxLabels) break;
    const text = eventLabel(item.event.label);
    const estimated = Math.min(96, Math.max(46, text.length * 6));
    const anchor = item.ex > width - margin.right - estimated - 12 ? "end" : "start";
    const tx = anchor === "end"
      ? clamp(item.ex - 5, margin.left + estimated, width - margin.right - 2)
      : clamp(item.ex + 5, margin.left + 2, width - margin.right - estimated);
    const left = anchor === "end" ? tx - estimated : tx;
    const right = anchor === "end" ? tx : tx + estimated;
    if (left - lastRight < 22) continue;
    const ty = margin.top + 12 + (labels.length % 2) * 15;
    labels.push(`
      <text x="${tx.toFixed(2)}" y="${ty}" text-anchor="${anchor}" fill="#a9552d" font-size="10" font-family="monospace">${escapeHtml(text)}</text>
    `);
    lastRight = right;
  }

  return lines + labels.join("");
}

function drawDetailChart(metric) {
  const svg = document.getElementById("detailChart");
  const tip = document.getElementById("chartTip");
  if (!svg || !metric.points?.length) return;
  const wrap = svg.parentElement;
  const width = Math.max(360, wrap.clientWidth || 640);
  const height = Math.max(260, wrap.clientHeight || 300);
  const margin = { top: 22, right: 18, bottom: 34, left: 48 };
  const raw = filteredChartPoints(metric.points).filter((point) => Number.isFinite(Number(point.value)));
  const points = downsample(raw);
  if (!points.length) return;

  const dates = points.map((point) => new Date(point.date).getTime());
  const values = points.map((point) => Number(point.value));
  let min = Math.min(...values, Number(metric.p05 ?? Infinity), Number(metric.historicalMin?.value ?? Infinity));
  let max = Math.max(...values, Number(metric.p95 ?? -Infinity), Number(metric.historicalMax?.value ?? -Infinity));
  if (!Number.isFinite(min)) min = Math.min(...values);
  if (!Number.isFinite(max)) max = Math.max(...values);
  const pad = (max - min || 1) * 0.12;
  min -= pad;
  max += pad;
  const xMin = Math.min(...dates);
  const xMax = Math.max(...dates);
  const plotW = width - margin.left - margin.right;
  const plotH = height - margin.top - margin.bottom;
  const x = (dateMs) => margin.left + ((dateMs - xMin) / (xMax - xMin || 1)) * plotW;
  const y = (value) => margin.top + (1 - (value - min) / (max - min || 1)) * plotH;
  const path = points.map((point, index) => `${index ? "L" : "M"} ${x(new Date(point.date).getTime()).toFixed(2)} ${y(Number(point.value)).toFixed(2)}`).join(" ");
  const grid = [0, 0.25, 0.5, 0.75, 1].map((step) => {
    const gy = margin.top + step * plotH;
    const value = max - step * (max - min);
    return `<line x1="${margin.left}" y1="${gy}" x2="${width - margin.right}" y2="${gy}" stroke="rgba(23,21,15,0.10)" /><text x="8" y="${gy + 4}" fill="#736f64" font-size="10" font-family="monospace">${escapeHtml(compactNumber(value, 2))}</text>`;
  }).join("");
  const band = metric.p05 !== null && metric.p95 !== null
    ? `<rect x="${margin.left}" y="${y(metric.p95)}" width="${plotW}" height="${Math.max(1, y(metric.p05) - y(metric.p95))}" fill="rgba(44,122,107,0.10)" />`
    : "";
  const events = buildEventLayer(metric, x, xMin, xMax, width, height, margin);
  const last = points[points.length - 1];
  const lastX = x(new Date(last.date).getTime());
  const lastY = y(Number(last.value));
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.innerHTML = `
    ${band}
    ${grid}
    ${events}
    <path d="${path}" fill="none" stroke="var(--tile-color)" stroke-width="2.4" vector-effect="non-scaling-stroke" />
    <circle cx="${lastX}" cy="${lastY}" r="4.5" fill="var(--tile-color)" stroke="#fffaf0" stroke-width="2" />
    <line id="crossX" x1="${lastX}" y1="${margin.top}" x2="${lastX}" y2="${height - margin.bottom}" stroke="rgba(23,21,15,0.22)" opacity="0" />
    <circle id="crossDot" cx="${lastX}" cy="${lastY}" r="4" fill="#17150f" opacity="0" />
    <text x="${margin.left}" y="${height - 10}" fill="#736f64" font-size="10" font-family="monospace">${formatDate(points[0].date)}</text>
    <text x="${width - margin.right}" y="${height - 10}" text-anchor="end" fill="#736f64" font-size="10" font-family="monospace">${formatDate(last.date)}</text>
  `;

  const crossX = svg.querySelector("#crossX");
  const crossDot = svg.querySelector("#crossDot");
  svg.onmousemove = (event) => {
    const rect = svg.getBoundingClientRect();
    const px = ((event.clientX - rect.left) / rect.width) * width;
    const ratio = clamp((px - margin.left) / plotW, 0, 1);
    const target = xMin + ratio * (xMax - xMin);
    let nearest = points[0];
    let best = Infinity;
    for (const point of points) {
      const diff = Math.abs(new Date(point.date).getTime() - target);
      if (diff < best) {
        best = diff;
        nearest = point;
      }
    }
    const nx = x(new Date(nearest.date).getTime());
    const ny = y(Number(nearest.value));
    crossX.setAttribute("x1", nx);
    crossX.setAttribute("x2", nx);
    crossX.setAttribute("opacity", "1");
    crossDot.setAttribute("cx", nx);
    crossDot.setAttribute("cy", ny);
    crossDot.setAttribute("opacity", "1");
    tip.innerHTML = `${formatDate(nearest.date)}<br>${formatValue(nearest.value, metric.unit)}`;
    tip.style.left = `${(nx / width) * rect.width}px`;
    tip.style.top = `${(ny / height) * rect.height}px`;
    tip.classList.add("visible");
  };
  svg.onmouseleave = () => {
    crossX.setAttribute("opacity", "0");
    crossDot.setAttribute("opacity", "0");
    tip.classList.remove("visible");
  };
}

function wireEvents() {
  els.viewNav?.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-view]");
    if (!button) return;
    if (button.dataset.case) {
      state.selectedCaseId = button.dataset.case;
      state.selectedCaseMetricId = null;
    }
    setView(button.dataset.view);
    if (state.view === "cases") renderCaseStudies();
  });

  els.backToMonitorBtn?.addEventListener("click", () => setView("monitor"));

  els.caseTabs?.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-case]");
    if (!button) return;
    state.selectedCaseId = button.dataset.case;
    state.selectedCaseMetricId = null;
    renderCaseStudies();
    renderView();
  });

  els.caseRows?.addEventListener("click", (event) => {
    const openButton = event.target.closest("button[data-open-monitor]");
    if (openButton) {
      selectMetric(openButton.dataset.openMonitor);
      setView("monitor");
      return;
    }
    const row = event.target.closest(".case-row[data-id]");
    if (!row) return;
    const scrollTop = els.caseRows.scrollTop;
    state.selectedCaseMetricId = row.dataset.id;
    renderCaseStudies();
    els.caseRows.scrollTop = scrollTop;
  });

  els.groupNav.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-group]");
    if (!button) return;
    state.selectedGroup = button.dataset.group;
    renderNavs();
    renderRows();
  });

  els.severityNav.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-severity]");
    if (!button) return;
    state.selectedSeverity = button.dataset.severity;
    renderNavs();
    renderRows();
  });

  els.metricRows.addEventListener("click", (event) => {
    const row = event.target.closest(".metric-row[data-id]");
    if (!row) return;
    selectMetric(row.dataset.id);
  });

  els.scenarioGrid.addEventListener("click", (event) => {
    const tile = event.target.closest(".scenario-tile[data-scenario]");
    if (!tile) return;
    const scenario = (state.monitor?.scenarios || []).find((item) => item.id === tile.dataset.scenario);
    const driver = scenario?.drivers?.[0];
    if (driver) selectMetric(driver.id);
  });

  els.assetImpactGrid?.addEventListener("click", (event) => {
    const tile = event.target.closest(".asset-tile[data-scenario]");
    if (!tile || !tile.dataset.scenario) return;
    const scenario = (state.monitor?.scenarios || []).find((item) => item.id === tile.dataset.scenario);
    const driver = scenario?.drivers?.[0];
    if (driver) selectMetric(driver.id);
  });

  els.countryGrid?.addEventListener("click", (event) => {
    const row = event.target.closest(".country-row[data-id]");
    if (!row || !row.dataset.id) return;
    selectMetric(row.dataset.id);
  });

  if (els.searchInput) {
    els.searchInput.addEventListener("input", (event) => {
      state.query = event.target.value;
      renderRows();
    });
  }

  els.sortTabs.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-sort]");
    if (!button) return;
    state.sort = button.dataset.sort;
    els.sortTabs.querySelectorAll("button").forEach((item) => item.classList.toggle("active", item === button));
    renderRows();
  });

  if (els.refreshBtn) {
    els.refreshBtn.addEventListener("click", () => loadMonitor(true));
  }
  if (els.demoBtn) {
    els.demoBtn.addEventListener("click", () => {
      const url = new URL(window.location.href);
      if (state.demo) {
        url.searchParams.delete("demo");
      } else {
        url.searchParams.set("demo", "1");
      }
      window.location.href = url.toString();
    });
  }

  els.inspector.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-window]");
    if (!button) return;
    state.window = button.dataset.window;
    renderInspector();
  });

  window.addEventListener("resize", () => {
    if (state.view === "cases") {
      const studies = state.monitor?.caseStudies || [];
      const selected = studies.find((study) => study.id === state.selectedCaseId);
      if (selected) drawSelectedCaseChart(selected);
      return;
    }
    const series = state.seriesCache.get(state.selectedId);
    if (series?.points) drawDetailChart(series);
  });

  window.addEventListener("hashchange", () => {
    const route = parseLocationHash();
    if (route.caseId && route.caseId !== state.selectedCaseId) {
      state.selectedCaseId = route.caseId;
      state.selectedCaseMetricId = null;
    }
    if (route.metricId) state.selectedCaseMetricId = route.metricId;
    if (route.view !== state.view) {
      setView(route.view, { quiet: true });
    } else {
      renderView();
    }
    if (route.view === "cases") renderCaseStudies();
  });
}

wireEvents();
renderView();
loadMonitor(false);
window.setInterval(() => loadMonitor(false), 15 * 60 * 1000);
