/**
 * app.js — Notion Forge Dashboard
 *
 * Fetches Notion database rows via the dashboard_server.py JSON API and
 * renders charts using Observable Plot (CDN ESM).
 *
 * Chart auto-detection:
 *   select / status / multi_select  → horizontal bar (frequency)
 *   date / created_time             → time-series dots + area
 *   number                          → histogram bin chart
 *   checkbox                        → donut-style ratio bar
 *
 * Inspired by Observable Plot (ISC) and Redash's data provider pattern (BSD-2-Clause).
 */

import * as Plot from "https://cdn.jsdelivr.net/npm/@observablehq/plot@0.6/+esm";

// ── DOM refs ───────────────────────────────────────────────────────────────────

const dbNav        = document.getElementById("db-nav");
const dbTitle      = document.getElementById("db-title");
const rowCount     = document.getElementById("row-count");
const kpiRow       = document.getElementById("kpi-row");
const chartsGrid   = document.getElementById("charts-grid");
const emptyState   = document.getElementById("empty-state");
const errorState   = document.getElementById("error-state");
const errorMsg     = document.getElementById("error-msg");
const loadingState = document.getElementById("loading-state");
const refreshBtn   = document.getElementById("refresh-btn");
const aggregateBtn = document.getElementById("aggregate-btn");
const limitInput   = document.getElementById("limit-input");
const sidebarFooter = document.getElementById("sidebar-footer");

// ── State ──────────────────────────────────────────────────────────────────────

let currentDbId   = null;
let currentDbLabel = "";
let lastFetchedAt = null;

// ── API helpers ────────────────────────────────────────────────────────────────

async function apiFetch(path) {
  const resp = await fetch(path);
  const data = await resp.json();
  if (data.error) throw new Error(data.error);
  return data;
}

// ── Sidebar ────────────────────────────────────────────────────────────────────

async function loadSidebar() {
  let data;
  try {
    data = await apiFetch("/api/databases");
  } catch (e) {
    dbNav.innerHTML = `<li class="sidebar-error">Failed to load databases</li>`;
    return;
  }
  dbNav.innerHTML = "";
  const DB_ICONS = { work_items: "📋", lab_projects: "🧪", audit_log: "📊" };
  for (const db of data.databases) {
    const li = document.createElement("li");
    const btn = document.createElement("button");
    const icon = DB_ICONS[db.key] || "🗄";
    btn.innerHTML = `<span class="db-icon">${icon}</span>${db.label}`;
    btn.dataset.dbId = db.id;
    btn.dataset.label = db.label;
    btn.addEventListener("click", () => selectDatabase(db.id, db.label, btn));
    li.appendChild(btn);
    dbNav.appendChild(li);
  }
}

function setActiveBtn(btn) {
  for (const b of dbNav.querySelectorAll("button")) b.classList.remove("active");
  btn.classList.add("active");
}

// ── View transitions ───────────────────────────────────────────────────────────

function showLoading() {
  emptyState.style.display   = "none";
  errorState.style.display   = "none";
  chartsGrid.style.display   = "none";
  kpiRow.style.display       = "none";
  loadingState.style.display = "flex";
}

function showEmpty() {
  loadingState.style.display = "none";
  errorState.style.display   = "none";
  chartsGrid.style.display   = "none";
  kpiRow.style.display       = "none";
  emptyState.style.display   = "flex";
}

function showError(msg) {
  loadingState.style.display = "none";
  emptyState.style.display   = "none";
  chartsGrid.style.display   = "none";
  kpiRow.style.display       = "none";
  errorState.style.display   = "flex";
  errorMsg.textContent       = msg;
}

function showCharts() {
  loadingState.style.display = "none";
  emptyState.style.display   = "none";
  errorState.style.display   = "none";
  kpiRow.style.display       = "grid";
  chartsGrid.style.display   = "grid";
}

// ── Select a database ──────────────────────────────────────────────────────────

async function selectDatabase(dbId, label, btn) {
  currentDbId    = dbId;
  currentDbLabel = label;
  setActiveBtn(btn);
  dbTitle.textContent = label;
  rowCount.textContent = "";
  refreshBtn.disabled   = false;
  aggregateBtn.disabled = false;
  await fetchAndRender(dbId);
}

async function fetchAndRender(dbId) {
  showLoading();
  const limit = parseInt(limitInput.value) || 100;
  try {
    const data = await apiFetch(`/api/query/${dbId}?limit=${limit}`);
    lastFetchedAt = new Date();
    renderDashboard(data.schema, data.rows, data.total, currentDbLabel);
    updateFooter();
  } catch (e) {
    showError(e.message);
  }
}

async function aggregateAndRender(dbId) {
  showLoading();
  dbTitle.textContent = currentDbLabel + " — Full scan";
  try {
    const data = await apiFetch(`/api/aggregate/${dbId}`);
    lastFetchedAt = new Date();
    renderAggregated(data, currentDbLabel);
    updateFooter();
  } catch (e) {
    showError(e.message);
  }
}

function updateFooter() {
  if (!lastFetchedAt) return;
  sidebarFooter.textContent = `Updated ${lastFetchedAt.toLocaleTimeString()}`;
}

// ── Render full row data ───────────────────────────────────────────────────────

function renderDashboard(schema, rows, total, label) {
  rowCount.textContent = `${total} rows`;

  // ── KPI row ────────────────────────────────────────────────────────────────
  kpiRow.innerHTML = "";
  kpiRow.appendChild(kpiCard("Total rows", total));

  // Status / select column with most entries → top KPI
  const statusCol = Object.entries(schema).find(([, t]) => t === "status")?.[0]
    || Object.entries(schema).find(([, t]) => t === "select")?.[0];
  if (statusCol) {
    const dist = countValues(rows, statusCol);
    const topEntry = Object.entries(dist).sort((a, b) => b[1] - a[1])[0];
    if (topEntry) {
      const pct = Math.round(topEntry[1] / total * 100);
      kpiRow.appendChild(kpiCard(statusCol, `${topEntry[0]}`, `${topEntry[1]} (${pct}%)`));
    }
    kpiRow.appendChild(kpiCard("Statuses", Object.keys(dist).length, "distinct values"));
  }

  // Most recent date
  const dateCol = Object.entries(schema).find(([, t]) => t === "created_time"
    || t === "date" || t === "last_edited_time")?.[0];
  if (dateCol) {
    const dates = rows.map(r => r[dateCol]).filter(Boolean).sort();
    if (dates.length) {
      const latest = dates[dates.length - 1].slice(0, 10);
      kpiRow.appendChild(kpiCard("Latest", latest));
    }
  }

  // ── Charts grid ────────────────────────────────────────────────────────────
  chartsGrid.innerHTML = "";
  for (const [col, type] of Object.entries(schema)) {
    const card = buildChartCard(col, type, rows);
    if (card) chartsGrid.appendChild(card);
  }

  showCharts();
}

// ── Render pre-aggregated data (full scan) ─────────────────────────────────────

function renderAggregated(data, label) {
  rowCount.textContent = `${data.total} rows (full scan)`;

  kpiRow.innerHTML = "";
  kpiRow.appendChild(kpiCard("Total pages", data.total));

  chartsGrid.innerHTML = "";
  for (const [col, stat] of Object.entries(data.columns)) {
    const card = buildAggregatedCard(col, stat);
    if (card) chartsGrid.appendChild(card);
  }

  showCharts();
}

// ── KPI card ───────────────────────────────────────────────────────────────────

function kpiCard(label, value, sub = "") {
  const div = document.createElement("div");
  div.className = "kpi-card";
  div.innerHTML = `
    <div class="kpi-label">${esc(String(label))}</div>
    <div class="kpi-value">${esc(String(value))}</div>
    ${sub ? `<div class="kpi-sub">${esc(String(sub))}</div>` : ""}
  `;
  return div;
}

// ── Chart card builder (from row data) ────────────────────────────────────────

function buildChartCard(col, type, rows) {
  const SKIP = new Set(["_id", "_url", "rich_text", "url", "email",
                         "phone_number", "people", "files", "relation",
                         "formula", "rollup", "verification"]);
  if (SKIP.has(col) || SKIP.has(type)) return null;
  if (col.startsWith("_")) return null;

  let chart = null;
  let chartType = "";

  if (type === "select" || type === "status" || type === "multi_select") {
    chart = buildFreqBar(col, type, rows);
    chartType = "frequency";
  } else if (type === "date" || type === "created_time" || type === "last_edited_time") {
    chart = buildTimeSeries(col, rows);
    chartType = "time series";
  } else if (type === "number") {
    chart = buildHistogram(col, rows);
    chartType = "histogram";
  } else if (type === "checkbox") {
    chart = buildCheckboxBar(col, rows);
    chartType = "ratio";
  }

  if (!chart) return null;
  return chartCard(col, chartType, chart);
}

function buildAggregatedCard(col, stat) {
  let chart = null;
  let chartType = "";

  if (stat.distribution) {
    chart = buildFreqBarFromDist(col, stat.type, stat.distribution);
    chartType = "frequency";
  } else if (stat.by_date) {
    chart = buildTimeSeriesFromDist(col, stat.by_date);
    chartType = "time series";
  } else if (stat.type === "number" && stat.count) {
    chart = statSummaryCard(col, stat);
    chartType = "stats";
  } else if (stat.type === "checkbox") {
    chart = buildCheckboxBarFromStats(col, stat);
    chartType = "ratio";
  }

  if (!chart) return null;
  return chartCard(col, chartType, chart);
}

function chartCard(col, type, chartEl) {
  const div = document.createElement("div");
  div.className = "chart-card";
  div.innerHTML = `
    <div class="chart-card-header">
      <span class="chart-card-title">${esc(col)}</span>
      <span class="chart-type-badge">${esc(type)}</span>
    </div>
    <div class="chart-card-body"></div>
  `;
  div.querySelector(".chart-card-body").appendChild(chartEl);
  return div;
}

// ── Observable Plot chart builders ────────────────────────────────────────────

const PLOT_COLORS = ["#4e6ef2","#7c8ff7","#a5b4fc","#c7d2fe","#e0e7ff",
                     "#f472b6","#fb923c","#34d399","#f59e0b","#60a5fa"];

const DARK_THEME = {
  background: "transparent",
  color: "#9b9b9b",
  fontSize: 11,
  style: "font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;",
};

function plotWidth() { return 440; }

// Horizontal bar chart for select/status distributions (Inspired by Observable Plot ISC)
function buildFreqBar(col, type, rows) {
  let data;
  if (type === "multi_select") {
    const flat = [];
    for (const row of rows) {
      for (const tag of (row[col] || [])) flat.push({ tag });
    }
    data = flat;
  } else {
    data = rows.filter(r => r[col] != null).map(r => ({ tag: String(r[col]) }));
  }
  if (!data.length) return emptyPlaceholder();
  return buildFreqBarFromDist(col, type, Object.fromEntries(
    Object.entries(countOccurrences(data.map(d => d.tag))).sort((a, b) => b[1] - a[1])
  ));
}

function buildFreqBarFromDist(col, type, dist) {
  const entries = Object.entries(dist)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15);
  if (!entries.length) return emptyPlaceholder();
  const bars = entries.map(([name, count]) => ({ name, count }));

  return Plot.plot({
    ...DARK_THEME,
    width: plotWidth(),
    height: Math.max(120, bars.length * 28 + 40),
    marginLeft: 120,
    marginRight: 40,
    x: { grid: true, tickFormat: "~s", style: { fill: "#666" } },
    y: { label: null },
    color: { range: PLOT_COLORS },
    marks: [
      Plot.barX(bars, {
        x: "count",
        y: "name",
        fill: "name",
        sort: { y: "-x" },
        rx: 3,
      }),
      Plot.text(bars, {
        x: "count",
        y: "name",
        text: d => String(d.count),
        dx: 6,
        fill: "#666",
        fontSize: 10,
        textAnchor: "start",
      }),
    ],
  });
}

// Time series: items per day/week  (Inspired by Observable Plot ISC)
function buildTimeSeries(col, rows) {
  const dated = rows
    .map(r => ({ day: (r[col] || "").slice(0, 10) }))
    .filter(d => d.day && d.day.length === 10);
  if (!dated.length) return emptyPlaceholder();
  const byDate = countOccurrences(dated.map(d => d.day));
  return buildTimeSeriesFromDist(col, byDate);
}

function buildTimeSeriesFromDist(col, byDate) {
  const entries = Object.entries(byDate).sort((a, b) => a[0].localeCompare(b[0]));
  if (!entries.length) return emptyPlaceholder();
  const data = entries.map(([day, count]) => ({ day: new Date(day), count }));

  return Plot.plot({
    ...DARK_THEME,
    width: plotWidth(),
    height: 180,
    marginLeft: 40,
    x: { type: "utc", label: null, ticks: 5 },
    y: { label: "count", grid: true, tickFormat: "~s" },
    marks: [
      Plot.areaY(data, {
        x: "day",
        y: "count",
        fill: PLOT_COLORS[0],
        fillOpacity: 0.15,
        curve: "monotone-x",
      }),
      Plot.lineY(data, {
        x: "day",
        y: "count",
        stroke: PLOT_COLORS[0],
        strokeWidth: 1.5,
        curve: "monotone-x",
      }),
      Plot.dot(data, {
        x: "day",
        y: "count",
        fill: PLOT_COLORS[0],
        r: 2.5,
        tip: true,
      }),
    ],
  });
}

// Histogram for numeric columns (Inspired by Observable Plot ISC)
function buildHistogram(col, rows) {
  const nums = rows.map(r => r[col]).filter(v => v != null);
  if (!nums.length) return emptyPlaceholder();
  const data = nums.map(v => ({ v }));

  return Plot.plot({
    ...DARK_THEME,
    width: plotWidth(),
    height: 160,
    marginLeft: 48,
    x: { label: col },
    y: { label: "count", grid: true },
    marks: [
      Plot.rectY(data, Plot.binX({ y: "count" }, {
        x: "v",
        fill: PLOT_COLORS[0],
        fillOpacity: 0.8,
        thresholds: 20,
      })),
      Plot.ruleY([0]),
    ],
  });
}

// Checkbox ratio bar
function buildCheckboxBar(col, rows) {
  const trueCount  = rows.filter(r => r[col] === true).length;
  const falseCount = rows.filter(r => r[col] === false).length;
  return buildCheckboxBarFromStats(col, { true: trueCount, false: falseCount });
}

function buildCheckboxBarFromStats(col, stat) {
  const total = (stat.true || 0) + (stat.false || 0);
  if (!total) return emptyPlaceholder();
  const data = [
    { label: "Yes", count: stat.true || 0 },
    { label: "No",  count: stat.false || 0 },
  ];

  return Plot.plot({
    ...DARK_THEME,
    width: plotWidth(),
    height: 100,
    marginLeft: 40,
    marginRight: 40,
    x: { label: null, domain: [0, total], axis: null },
    y: { label: null, axis: null },
    marks: [
      Plot.barX(data, {
        x: "count",
        y: "label",
        fill: d => d.label === "Yes" ? PLOT_COLORS[0] : "#3a3a3a",
        rx: 3,
        sort: { y: "-x" },
      }),
      Plot.text(data, {
        x: d => d.count / 2,
        y: "label",
        text: d => `${d.label}: ${d.count} (${Math.round(d.count / total * 100)}%)`,
        fill: "white",
        fontSize: 11,
        fontWeight: "600",
      }),
    ],
  });
}

// Number stats summary (used in aggregate mode when no histogram available)
function statSummaryCard(col, stat) {
  const div = document.createElement("div");
  div.style.cssText = "padding:20px 16px;color:#9b9b9b;font-size:12px;line-height:2;";
  div.innerHTML = `
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;">
      <div><span style="color:#666">Min</span><br><strong style="color:#e3e3e3;font-size:18px">${stat.min}</strong></div>
      <div><span style="color:#666">Max</span><br><strong style="color:#e3e3e3;font-size:18px">${stat.max}</strong></div>
      <div><span style="color:#666">Mean</span><br><strong style="color:#e3e3e3;font-size:18px">${stat.mean}</strong></div>
      <div><span style="color:#666">Count</span><br><strong style="color:#e3e3e3;font-size:18px">${stat.count}</strong></div>
    </div>
  `;
  return div;
}

function emptyPlaceholder() {
  const div = document.createElement("div");
  div.style.cssText = "padding:20px 16px;color:#444;font-size:12px;text-align:center;";
  div.textContent = "No data";
  return div;
}

// ── Utility ────────────────────────────────────────────────────────────────────

function countValues(rows, col) {
  const counts = {};
  for (const row of rows) {
    const v = String(row[col] ?? "(empty)");
    counts[v] = (counts[v] || 0) + 1;
  }
  return counts;
}

function countOccurrences(arr) {
  const counts = {};
  for (const v of arr) counts[v] = (counts[v] || 0) + 1;
  return counts;
}

function esc(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

// ── Event handlers ─────────────────────────────────────────────────────────────

refreshBtn.addEventListener("click", () => {
  if (currentDbId) fetchAndRender(currentDbId);
});

aggregateBtn.addEventListener("click", () => {
  if (currentDbId) aggregateAndRender(currentDbId);
});

// ── Init ───────────────────────────────────────────────────────────────────────

await loadSidebar();
