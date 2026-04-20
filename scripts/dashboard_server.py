#!/usr/bin/env python3
"""
Interactive research dashboard server for V4 Ukraine-war media analysis.

Reads the original + supplement CSVs and serves an HTML dashboard with
Plotly charts that update on each refresh.

Usage:
    python3 dashboard_server.py              # starts on port 8050
    python3 dashboard_server.py --port 9000  # custom port

Then open http://localhost:8050 in your browser.
"""

import argparse
import csv
import json
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

csv.field_size_limit(10 * 1024 * 1024)

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"

# ── Portal config ──
PORTAL_CONFIG = {
    # original CSV portals
    "MF Dnes":          {"country": "CZ", "illiberal": 1, "color": "#c0675a"},
    "Novinky":          {"country": "CZ", "illiberal": 0, "color": "#6a9fbd"},
    "Magyar Nemzet":    {"country": "HU", "illiberal": 1, "color": "#c0675a"},
    "Telex":            {"country": "HU", "illiberal": 0, "color": "#6a9fbd"},
    "wPolityce":        {"country": "PL", "illiberal": 1, "color": "#c0675a"},
    "Pravda":           {"country": "SK", "illiberal": 1, "color": "#c0675a"},
    "Aktuality":        {"country": "SK", "illiberal": 0, "color": "#6a9fbd"},
    # supplement portals
    "Onet":             {"country": "PL", "illiberal": 0, "color": "#6a9fbd"},
}

SUPPLEMENT_PORTAL_MAP = {
    "novinky": "Novinky", "idnes": "MF Dnes", "pravda": "Pravda",
    "aktuality": "Aktuality", "telex": "Telex",
    "magyarnemzet": "Magyar Nemzet", "wpolityce": "wPolityce",
    "onet": "Onet",
}

ORIGINAL_FILES = {
    "CZ_M_novinky_document_level_with_preds.csv": "Novinky",
    "CZ_M_mfdnes_document_level_with_preds.csv": "MF Dnes",
    "SK_M_pravda_document_level_with_preds.csv": "Pravda",
    "SK_M_aktuality_document_level_with_preds.csv": "Aktuality",
    "HU_M_indextelex_document_level_with_preds.csv": None,  # mixed Index+Telex
    "HU_M_magyarnemzet_document_level_with_preds.csv": "Magyar Nemzet",
    "PL_M_wpolityce_document_level_with_preds.csv": "wPolityce",
}

UKRAINE_KEYWORDS = [
    "Rusko", "Putin", "Moskva", "Ukrajina", "Zelenskyj", "Kyjev",
    "Oroszország", "Putyin", "Moszkva", "Ukrajna", "Zelenszkij", "Kijev",
    "Rosja", "Moskwa", "Ukraina", "Zełenski", "Kijów",
]
_UKRAINE_RE = re.compile("|".join(UKRAINE_KEYWORDS), re.IGNORECASE)

DATE_START = "2022-01-01"
DATE_END = "2026-02-23"

EFI_CATS = {"macroeconomics", "energy"}
HFI_CATS = {"civil rights", "immigration", "social welfare"}

COUNTRIES = ["CZ", "HU", "PL", "SK"]
PORTAL_ORDER = [
    "MF Dnes", "Novinky", "Magyar Nemzet", "Telex",
    "wPolityce", "Onet", "Pravda", "Aktuality",
]


def load_all_data():
    """Load original CSVs + supplement CSVs, deduplicate, return list of dicts."""
    rows = []
    seen_ids = set()

    print(f"  ROOT_DIR: {ROOT_DIR}")
    print(f"  DATA_DIR: {DATA_DIR}")

    def _in_date_range(date_str):
        """Quick date check without parsing datetime."""
        if not date_str or len(date_str) < 10:
            return False
        return "2022-01-01" <= date_str[:10] <= "2026-02-23"

    # 1) Original files
    for fname, portal_name in ORIGINAL_FILES.items():
        path = ROOT_DIR / fname
        if not path.exists():
            print(f"  [SKIP] not found: {path}")
            continue
        count = 0
        skipped = 0
        with open(path, "r", encoding="utf-8") as f:
            rd = csv.DictReader(f)
            for row in rd:
                if not _in_date_range(row.get("date", "")):
                    skipped += 1
                    continue
                did = row.get("document_id", "")
                if did in seen_ids:
                    continue
                seen_ids.add(did)
                p = portal_name or row.get("portal", "")
                if p not in PORTAL_CONFIG:
                    continue
                row["_portal"] = p
                rows.append(row)
                count += 1
        print(f"  [OK] {fname}: {count:,} rows in range, "
              f"{skipped:,} out of range (portal={portal_name})")

    # 2) Supplement files
    for sfile in sorted(DATA_DIR.glob("*_supplement.csv")):
        stem = sfile.stem.replace("_supplement", "")
        portal_name = SUPPLEMENT_PORTAL_MAP.get(stem)
        if not portal_name:
            continue
        count = 0
        with open(sfile, "r", encoding="utf-8") as f:
            rd = csv.DictReader(f)
            for row in rd:
                if not _in_date_range(row.get("date", "")):
                    continue
                did = row.get("document_id", "")
                if did in seen_ids:
                    continue
                seen_ids.add(did)
                row["_portal"] = portal_name
                rows.append(row)
                count += 1
        print(f"  [OK] {sfile.name}: {count:,} rows (portal={portal_name})")

    print(f"  Total loaded (in date range): {len(rows):,} rows")
    return rows


def compute_dashboard_data():
    """Compute all chart data in a single pass — no datetime parsing."""
    t0 = time.time()
    rows = load_all_data()
    print(f"  [COMPUTE] processing {len(rows):,} rows...")

    # Single-pass accumulators
    total_by_portal = Counter()
    ukr_by_portal = Counter()
    monthly_all = Counter()
    monthly_ukr = Counter()
    monthly_portal_ukr = Counter()       # (portal, ym) → count
    sent_portal = defaultdict(lambda: [0, 0, 0])  # portal → [neg, neu, pos]
    cap_counts = Counter()
    idx_data = defaultdict(lambda: [0, 0, 0])  # (portal, ym) → [total, gfi, hfi]
    portal_set = set()
    n_total = 0
    n_ukraine = 0
    sent_map = {"Negative": 0, "Neutral": 1, "Positive": 2}

    for row in rows:
        date_str = row.get("date", "")
        if not date_str or len(date_str) < 7 or date_str == "NA":
            continue
        portal = row.get("_portal", "")
        if portal not in PORTAL_CONFIG:
            continue

        ym = date_str[:7]
        n_total += 1
        total_by_portal[portal] += 1
        monthly_all[ym] += 1
        portal_set.add(portal)

        nerw = row.get("document_nerw", "") or ""
        is_ukr = bool(_UKRAINE_RE.search(nerw)) if nerw else False

        if is_ukr:
            n_ukraine += 1
            ukr_by_portal[portal] += 1
            monthly_ukr[ym] += 1
            monthly_portal_ukr[(portal, ym)] += 1

            cap = (row.get("document_cap_major_label", "") or "").strip().lower()
            sent = (row.get("document_sentiment3", "") or "").strip()

            if cap and cap != "na":
                cap_counts[cap] += 1
                key = (portal, ym)
                idx_data[key][0] += 1
                if cap in EFI_CATS:
                    idx_data[key][1] += 1
                if cap in HFI_CATS:
                    idx_data[key][2] += 1

            si = sent_map.get(sent)
            if si is not None:
                sent_portal[portal][si] += 1

    print(f"  [COMPUTE] {n_total:,} articles, {n_ukraine:,} ukraine")

    # ── Build chart JSON from accumulators ──
    chart1 = {"portals": [], "total": [], "ukraine": [], "colors": [], "pct": []}
    for p in PORTAL_ORDER:
        t = total_by_portal[p]
        if t == 0:
            continue
        u = ukr_by_portal[p]
        chart1["portals"].append(p)
        chart1["total"].append(t)
        chart1["ukraine"].append(u)
        chart1["colors"].append(PORTAL_CONFIG[p]["color"])
        chart1["pct"].append(round(100 * u / t, 1))

    months = sorted(monthly_all.keys())
    chart2 = {
        "months": months,
        "total": [monthly_all[m] for m in months],
        "ukraine": [monthly_ukr[m] for m in months],
        "share": [round(100 * monthly_ukr[m] / monthly_all[m], 1)
                  if monthly_all[m] else 0 for m in months],
    }

    chart3 = {}
    for c in COUNTRIES:
        chart3[c] = {}
        for p in PORTAL_ORDER:
            if PORTAL_CONFIG.get(p, {}).get("country") != c:
                continue
            chart3[c][p] = {
                "months": months,
                "values": [monthly_portal_ukr.get((p, m), 0) for m in months],
                "color": PORTAL_CONFIG[p]["color"],
                "dash": "solid" if PORTAL_CONFIG[p]["illiberal"] else "dash",
            }

    chart4 = {"portals": [], "negative": [], "neutral": [], "positive": [], "colors": []}
    for p in PORTAL_ORDER:
        sc = sent_portal[p]
        ts = sum(sc)
        if ts == 0:
            continue
        chart4["portals"].append(p)
        chart4["negative"].append(round(100 * sc[0] / ts, 1))
        chart4["neutral"].append(round(100 * sc[1] / ts, 1))
        chart4["positive"].append(round(100 * sc[2] / ts, 1))
        chart4["colors"].append(PORTAL_CONFIG[p]["color"])

    top_cats = cap_counts.most_common(12)
    chart5 = {
        "categories": [c[0].title() for c in top_cats],
        "counts": [c[1] for c in top_cats],
    }

    chart6, chart7 = {}, {}
    for c in COUNTRIES:
        chart6[c], chart7[c] = {}, {}
        for p in PORTAL_ORDER:
            if PORTAL_CONFIG.get(p, {}).get("country") != c:
                continue
            gv, hv, vm = [], [], []
            for m in months:
                d = idx_data.get((p, m))
                if d and d[0] >= 5:
                    vm.append(m)
                    gv.append(round(d[1] / d[0], 4))
                    hv.append(round(d[2] / d[0], 4))
            style = {"color": PORTAL_CONFIG[p]["color"],
                     "dash": "solid" if PORTAL_CONFIG[p]["illiberal"] else "dash"}
            chart6[c][p] = {"months": vm, "values": gv, **style}
            chart7[c][p] = {"months": vm, "values": hv, **style}

    chart8 = {"portals": [], "gfi": [], "hfi": [], "colors": []}
    for p in PORTAL_ORDER:
        gs, hs, ts2 = 0, 0, 0
        for m in months:
            d = idx_data.get((p, m))
            if d and d[0] >= 5:
                ts2 += d[0]; gs += d[1]; hs += d[2]
        if ts2 > 0:
            chart8["portals"].append(p)
            chart8["gfi"].append(round(gs / ts2, 4))
            chart8["hfi"].append(round(hs / ts2, 4))
            chart8["colors"].append(PORTAL_CONFIG[p]["color"])

    load_time = round(time.time() - t0, 1)
    print(f"  [COMPUTE] done in {load_time}s")

    return {
        "summary": {
            "total_articles": n_total,
            "ukraine_articles": n_ukraine,
            "ukraine_pct": round(100 * n_ukraine / n_total, 1) if n_total else 0,
            "portals": len(portal_set),
            "date_range": f"{DATE_START} — {DATE_END}",
            "load_time": load_time,
        },
        "chart1": chart1, "chart2": chart2, "chart3": chart3,
        "chart4": chart4, "chart5": chart5, "chart6": chart6,
        "chart7": chart7, "chart8": chart8,
    }


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>V4 Ukraine-War Media Analysis Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: #0f1419; color: #e7e9ea; }
  .header { background: linear-gradient(135deg, #1a1f2e 0%, #0d1117 100%);
            padding: 28px 40px; border-bottom: 1px solid #30363d; }
  .header h1 { font-size: 22px; font-weight: 600; margin-bottom: 6px; }
  .header .subtitle { color: #8b949e; font-size: 14px; }
  .stats-bar { display: flex; gap: 32px; padding: 20px 40px;
               background: #161b22; border-bottom: 1px solid #30363d; }
  .stat { text-align: center; }
  .stat .num { font-size: 28px; font-weight: 700; color: #7db8d4; }
  .stat .label { font-size: 12px; color: #8b949e; margin-top: 2px; }
  .controls { padding: 16px 40px; background: #161b22;
              border-bottom: 1px solid #30363d; display: flex; gap: 16px; align-items: center; }
  .controls button { background: #21262d; color: #c9d1d9; border: 1px solid #30363d;
                     padding: 8px 16px; border-radius: 6px; cursor: pointer; font-size: 13px; }
  .controls button:hover { background: #30363d; border-color: #7db8d4; }
  .controls .status { font-size: 12px; color: #8b949e; margin-left: auto; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px;
          padding: 24px 40px; }
  .chart-card { background: #161b22; border: 1px solid #30363d;
                border-radius: 8px; padding: 20px; }
  .chart-card.full { grid-column: 1 / -1; }
  .chart-card h3 { font-size: 14px; font-weight: 600; margin-bottom: 12px;
                   color: #c9d1d9; }
  .chart-card .chart-desc { font-size: 12px; color: #8b949e; margin-bottom: 10px; }
  .legend-note { text-align: center; padding: 8px; font-size: 12px; color: #8b949e; }
  .legend-note span.ill { color: #c0675a; font-weight: bold; }
  .legend-note span.lib { color: #6a9fbd; font-weight: bold; }
</style>
</head>
<body>
<div class="header">
  <h1>V4 Ukraine-War Media Coverage — Research Dashboard</h1>
</div>

<div class="stats-bar">
  <div class="stat"><div class="num" id="s-total">—</div><div class="label">Total Articles</div></div>
  <div class="stat"><div class="num" id="s-ukraine">—</div><div class="label">Ukraine-War Articles</div></div>
  <div class="stat"><div class="num" id="s-pct">—</div><div class="label">Ukraine Share</div></div>
  <div class="stat"><div class="num" id="s-portals">—</div><div class="label">Portals</div></div>
  <div class="stat"><div class="num" id="s-range">—</div><div class="label">Date Range</div></div>
</div>

<div class="controls">
  <button onclick="refreshData()">Refresh Data</button>
  <div class="status" id="status">Loading...</div>
</div>

<div class="legend-note">
  <span class="ill">■ Illiberal portals</span> &nbsp;|&nbsp;
  <span class="lib">■ Liberal portals</span>
</div>

<div class="grid">
  <div class="chart-card full">
    <h3>1. Total vs Ukraine-War Articles per Portal</h3>
    <div class="chart-desc">Stacked bar showing overall corpus size and Ukraine-war subset (dictionary-based keyword filter on NER entities)</div>
    <div id="chart1" style="height:400px"></div>
  </div>

  <div class="chart-card full">
    <h3>2. Monthly Article Volume: Full Corpus vs Ukraine-War Subset</h3>
    <div class="chart-desc">Area chart with dual axis — article counts (left) and Ukraine share % (right)</div>
    <div id="chart2" style="height:380px"></div>
  </div>

  <div class="chart-card" id="chart3-cz-card">
    <h3>3a. Monthly Ukraine Volume — Czech Republic</h3>
    <div id="chart3-CZ" style="height:300px"></div>
  </div>
  <div class="chart-card" id="chart3-hu-card">
    <h3>3b. Monthly Ukraine Volume — Hungary</h3>
    <div id="chart3-HU" style="height:300px"></div>
  </div>
  <div class="chart-card" id="chart3-pl-card">
    <h3>3c. Monthly Ukraine Volume — Poland</h3>
    <div id="chart3-PL" style="height:300px"></div>
  </div>
  <div class="chart-card" id="chart3-sk-card">
    <h3>3d. Monthly Ukraine Volume — Slovakia</h3>
    <div id="chart3-SK" style="height:300px"></div>
  </div>

  <div class="chart-card full">
    <h3>4. Sentiment Distribution — Ukraine-War Articles by Portal</h3>
    <div class="chart-desc">Grouped bar: Negative / Neutral / Positive share per portal</div>
    <div id="chart4" style="height:400px"></div>
  </div>

  <div class="chart-card">
    <h3>5. CAP Topic Distribution — Ukraine-War Articles</h3>
    <div class="chart-desc">Top 12 policy topics (CAP Major classification)</div>
    <div id="chart5" style="height:400px"></div>
  </div>

  <div class="chart-card">
    <h3>6. Mean EFI & HFI by Portal</h3>
    <div class="chart-desc">Economic Focus (EFI) and Humanitarian Focus (HFI) indices</div>
    <div id="chart8" style="height:400px"></div>
  </div>

  <div class="chart-card" id="chart6-cz-card">
    <h3>7a. EFI Over Time — Czech Republic</h3>
    <div id="chart6-CZ" style="height:280px"></div>
  </div>
  <div class="chart-card" id="chart6-hu-card">
    <h3>7b. EFI Over Time — Hungary</h3>
    <div id="chart6-HU" style="height:280px"></div>
  </div>
  <div class="chart-card" id="chart6-pl-card">
    <h3>7c. EFI Over Time — Poland</h3>
    <div id="chart6-PL" style="height:280px"></div>
  </div>
  <div class="chart-card" id="chart6-sk-card">
    <h3>7d. EFI Over Time — Slovakia</h3>
    <div id="chart6-SK" style="height:280px"></div>
  </div>

  <div class="chart-card" id="chart7-cz-card">
    <h3>8a. HFI Over Time — Czech Republic</h3>
    <div id="chart7-CZ" style="height:280px"></div>
  </div>
  <div class="chart-card" id="chart7-hu-card">
    <h3>8b. HFI Over Time — Hungary</h3>
    <div id="chart7-HU" style="height:280px"></div>
  </div>
  <div class="chart-card" id="chart7-pl-card">
    <h3>8c. HFI Over Time — Poland</h3>
    <div id="chart7-PL" style="height:280px"></div>
  </div>
  <div class="chart-card" id="chart7-sk-card">
    <h3>8d. HFI Over Time — Slovakia</h3>
    <div id="chart7-SK" style="height:280px"></div>
  </div>
</div>

<script>
const layout_defaults = {
  paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
  font: { color: '#c9d1d9', size: 12 },
  margin: { t: 30, b: 50, l: 60, r: 40 },
  xaxis: { gridcolor: '#30363d', zerolinecolor: '#30363d' },
  yaxis: { gridcolor: '#30363d', zerolinecolor: '#30363d' },
  legend: { bgcolor: 'rgba(0,0,0,0)', font: { size: 10 } },
};
const config = { responsive: true, displayModeBar: false };

function refreshData() {
  document.getElementById('status').textContent = 'Loading...';
  fetch('/api/data')
    .then(r => r.json())
    .then(data => {
      renderAll(data);
      document.getElementById('status').textContent =
        'Updated ' + new Date().toLocaleTimeString() + ' (loaded in ' + data.summary.load_time + 's)';
    })
    .catch(e => {
      document.getElementById('status').textContent = 'Error: ' + e.message;
    });
}

function renderAll(data) {
  const s = data.summary;
  document.getElementById('s-total').textContent = s.total_articles.toLocaleString();
  document.getElementById('s-ukraine').textContent = s.ukraine_articles.toLocaleString();
  document.getElementById('s-pct').textContent = s.ukraine_pct + '%';
  document.getElementById('s-portals').textContent = s.portals;
  document.getElementById('s-range').textContent = s.date_range;

  // Chart 1: Stacked bar
  const c1 = data.chart1;
  const non_ukr = c1.total.map((t,i) => t - c1.ukraine[i]);
  const c1_colors_light = c1.colors.map(c => c === '#c0675a' ? '#dbbcb7' : '#b5c9d9');
  Plotly.newPlot('chart1', [
    { x: c1.portals, y: non_ukr, type: 'bar', name: 'Other', marker: { color: c1_colors_light } },
    { x: c1.portals, y: c1.ukraine, type: 'bar', name: 'Ukraine-war',
      marker: { color: c1.colors },
      text: c1.pct.map(p => p + '%'), textposition: 'outside', textfont: { size: 11 } },
  ], { ...layout_defaults, barmode: 'stack', margin: { ...layout_defaults.margin, t: 40 } }, config);

  // Chart 2: Monthly volume
  const c2 = data.chart2;
  Plotly.newPlot('chart2', [
    { x: c2.months, y: c2.total, type: 'scatter', mode: 'lines', fill: 'tozeroy',
      name: 'Total', line: { color: '#7f8c8d' }, fillcolor: 'rgba(127,140,141,0.15)' },
    { x: c2.months, y: c2.ukraine, type: 'scatter', mode: 'lines', fill: 'tozeroy',
      name: 'Ukraine', line: { color: '#c0675a' }, fillcolor: 'rgba(192,103,90,0.15)' },
    { x: c2.months, y: c2.share, type: 'scatter', mode: 'lines',
      name: 'Ukraine %', line: { color: '#c9a560', dash: 'dash' }, yaxis: 'y2' },
  ], { ...layout_defaults,
    yaxis: { ...layout_defaults.yaxis, title: 'Articles' },
    yaxis2: { overlaying: 'y', side: 'right', title: 'Ukraine %',
              titlefont: { color: '#c9a560' }, tickfont: { color: '#c9a560' },
              gridcolor: 'rgba(0,0,0,0)' },
  }, config);

  // Chart 3: Monthly Ukraine by country
  ['CZ','HU','PL','SK'].forEach(c => {
    const traces = [];
    const cdata = data.chart3[c] || {};
    Object.entries(cdata).forEach(([p, d]) => {
      traces.push({
        x: d.months, y: d.values, type: 'scatter', mode: 'lines',
        name: p, line: { color: d.color, dash: d.dash, width: 2 },
      });
    });
    Plotly.newPlot('chart3-' + c, traces, { ...layout_defaults }, config);
  });

  // Chart 4: Sentiment
  const c4 = data.chart4;
  Plotly.newPlot('chart4', [
    { x: c4.portals, y: c4.negative, type: 'bar', name: 'Negative', marker: { color: '#c0675a' } },
    { x: c4.portals, y: c4.neutral, type: 'bar', name: 'Neutral', marker: { color: '#a3b1b2' } },
    { x: c4.portals, y: c4.positive, type: 'bar', name: 'Positive', marker: { color: '#6aae8e' } },
  ], { ...layout_defaults, barmode: 'group',
       yaxis: { ...layout_defaults.yaxis, title: 'Share (%)' } }, config);

  // Chart 5: CAP topics
  const c5 = data.chart5;
  Plotly.newPlot('chart5', [{
    y: c5.categories.slice().reverse(), x: c5.counts.slice().reverse(),
    type: 'bar', orientation: 'h', marker: { color: '#7db8d4' },
  }], { ...layout_defaults, margin: { ...layout_defaults.margin, l: 160 } }, config);

  // Chart 8: EFI/HFI bar
  const c8 = data.chart8;
  Plotly.newPlot('chart8', [
    { x: c8.portals, y: c8.gfi, type: 'bar', name: 'EFI (Economic)',
      marker: { color: '#c0675a', opacity: 0.8 } },
    { x: c8.portals, y: c8.hfi, type: 'bar', name: 'HFI (Humanitarian)',
      marker: { color: '#3498db', opacity: 0.8 } },
  ], { ...layout_defaults, barmode: 'group',
       yaxis: { ...layout_defaults.yaxis, title: 'Index value' } }, config);

  // Charts 6+7: EFI/HFI time series by country
  ['CZ','HU','PL','SK'].forEach(c => {
    // GFI
    const gfi_traces = [];
    const gdata = data.chart6[c] || {};
    Object.entries(gdata).forEach(([p, d]) => {
      if (d.months.length > 0) {
        gfi_traces.push({
          x: d.months, y: d.values, type: 'scatter', mode: 'lines',
          name: p, line: { color: d.color, dash: d.dash, width: 2 },
        });
      }
    });
    Plotly.newPlot('chart6-' + c, gfi_traces,
      { ...layout_defaults, yaxis: { ...layout_defaults.yaxis, title: 'EFI' } }, config);

    // HFI
    const hfi_traces = [];
    const hdata = data.chart7[c] || {};
    Object.entries(hdata).forEach(([p, d]) => {
      if (d.months.length > 0) {
        hfi_traces.push({
          x: d.months, y: d.values, type: 'scatter', mode: 'lines',
          name: p, line: { color: d.color, dash: d.dash, width: 2 },
        });
      }
    });
    Plotly.newPlot('chart7-' + c, hfi_traces,
      { ...layout_defaults, yaxis: { ...layout_defaults.yaxis, title: 'HFI' } }, config);
  });
}

refreshData();
</script>
</body>
</html>"""


class DashboardHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(DASHBOARD_HTML.encode("utf-8"))
        elif self.path == "/api/data":
            try:
                data = compute_dashboard_data()
                payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(payload)
                print(f"  [API] OK — {len(payload):,} bytes, "
                      f"{data['summary']['total_articles']:,} articles")
            except Exception as e:
                import traceback
                traceback.print_exc()
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # quiet


def main():
    ap = argparse.ArgumentParser(description="V4 Research Dashboard")
    ap.add_argument("--port", type=int, default=8050)
    args = ap.parse_args()

    print(f"\n  V4 Ukraine-War Media Research Dashboard")
    print(f"  ========================================")
    print(f"  Open in browser: http://localhost:{args.port}")
    print(f"  Press Ctrl+C to stop\n")

    server = HTTPServer(("0.0.0.0", args.port), DashboardHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Stopped.")
        server.server_close()


if __name__ == "__main__":
    main()
