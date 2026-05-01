const state = {
  monitor: null,
  selectedId: null,
  selectedGroup: "All",
  selectedSeverity: "All",
  sort: "alert",
  query: "",
  window: "5Y",
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
  countryGrid: document.getElementById("countryGrid"),
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
  renderTopStatus();
  renderSources();
  renderNavs();
  renderCountries();
  renderScenarios();
  renderRows();
  renderInspector();
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
  els.demoBtn.textContent = state.demo ? "Live" : "Demo";
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

  els.countryGrid?.addEventListener("click", (event) => {
    const row = event.target.closest(".country-row[data-id]");
    if (!row || !row.dataset.id) return;
    selectMetric(row.dataset.id);
  });

  els.searchInput.addEventListener("input", (event) => {
    state.query = event.target.value;
    renderRows();
  });

  els.sortTabs.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-sort]");
    if (!button) return;
    state.sort = button.dataset.sort;
    els.sortTabs.querySelectorAll("button").forEach((item) => item.classList.toggle("active", item === button));
    renderRows();
  });

  els.refreshBtn.addEventListener("click", () => loadMonitor(true));
  els.demoBtn.addEventListener("click", () => {
    const url = new URL(window.location.href);
    if (state.demo) {
      url.searchParams.delete("demo");
    } else {
      url.searchParams.set("demo", "1");
    }
    window.location.href = url.toString();
  });

  els.inspector.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-window]");
    if (!button) return;
    state.window = button.dataset.window;
    renderInspector();
  });

  window.addEventListener("resize", () => {
    const series = state.seriesCache.get(state.selectedId);
    if (series?.points) drawDetailChart(series);
  });
}

wireEvents();
loadMonitor(false);
window.setInterval(() => loadMonitor(false), 15 * 60 * 1000);
