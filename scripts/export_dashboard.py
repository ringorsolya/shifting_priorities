#!/usr/bin/env python3
"""
Export dashboard data as JSON for GitHub Pages.

Reads original + supplement CSVs and writes docs/data.json.
Run this whenever you want to update the dashboard with new data,
then git push to update GitHub Pages.

Usage:
    python3 export_dashboard.py
"""

import csv
import json
import re
import time
from collections import Counter, defaultdict
from pathlib import Path

csv.field_size_limit(10 * 1024 * 1024)

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
DOCS_DIR = ROOT_DIR / "docs"

PORTAL_CONFIG = {
    "MF Dnes":          {"country": "CZ", "illiberal": 1, "color": "#c0675a"},
    "Novinky":          {"country": "CZ", "illiberal": 0, "color": "#6a9fbd"},
    "Magyar Nemzet":    {"country": "HU", "illiberal": 1, "color": "#c0675a"},
    "Telex":            {"country": "HU", "illiberal": 0, "color": "#6a9fbd"},
    "wPolityce":        {"country": "PL", "illiberal": 1, "color": "#c0675a"},
    "Pravda":           {"country": "SK", "illiberal": 1, "color": "#c0675a"},
    "Aktuality":        {"country": "SK", "illiberal": 0, "color": "#6a9fbd"},
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
    "HU_M_indextelex_document_level_with_preds.csv": None,
    "HU_M_magyarnemzet_document_level_with_preds.csv": "Magyar Nemzet",
    "PL_M_wpolityce_document_level_with_preds.csv": "wPolityce",
}

UKRAINE_KEYWORDS = [
    "Rusko", "Putin", "Moskva", "Ukrajina", "Zelenskyj", "Kyjev",
    "Oroszország", "Putyin", "Moszkva", "Ukrajna", "Zelenszkij", "Kijev",
    "Rosja", "Moskwa", "Ukraina", "Zełenski", "Kijów",
]
_UKRAINE_RE = re.compile("|".join(UKRAINE_KEYWORDS), re.IGNORECASE)

EFI_CATS = {"macroeconomics", "energy"}
HFI_CATS = {"civil rights", "immigration", "social welfare"}
COUNTRIES = ["CZ", "HU", "PL", "SK"]
PORTAL_ORDER = [
    "MF Dnes", "Novinky", "Magyar Nemzet", "Telex",
    "wPolityce", "Onet", "Pravda", "Aktuality",
]


def _in_date_range(date_str):
    if not date_str or len(date_str) < 10:
        return False
    return "2022-01-01" <= date_str[:10] <= "2026-02-23"


def load_and_compute():
    t0 = time.time()
    seen_ids = set()

    # Accumulators
    total_by_portal = Counter()
    ukr_by_portal = Counter()
    monthly_all = Counter()
    monthly_ukr = Counter()
    monthly_portal_ukr = Counter()
    monthly_portal_total = Counter()          # NEW: total articles per portal-month
    sent_portal = defaultdict(lambda: [0, 0, 0])
    cap_counts = Counter()
    cap_by_portal = defaultdict(Counter)      # NEW: CAP per portal
    cap_by_portal_month = defaultdict(Counter) # NEW: CAP per portal-month
    idx_data = defaultdict(lambda: [0, 0, 0])
    portal_set = set()
    n_total = 0
    n_ukraine = 0
    sent_map = {"Negative": 0, "Neutral": 1, "Positive": 2}

    def process_row(row, portal):
        nonlocal n_total, n_ukraine
        date_str = row.get("date", "")
        if not date_str or len(date_str) < 7 or date_str == "NA":
            return
        did = row.get("document_id", "")
        if did in seen_ids:
            return
        seen_ids.add(did)
        if portal not in PORTAL_CONFIG:
            return

        ym = date_str[:7]
        n_total += 1
        total_by_portal[portal] += 1
        monthly_all[ym] += 1
        monthly_portal_total[(portal, ym)] += 1  # NEW
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
                cap_by_portal[portal][cap] += 1          # NEW
                cap_by_portal_month[(portal, ym)][cap] += 1  # NEW
                key = (portal, ym)
                idx_data[key][0] += 1
                if cap in EFI_CATS:
                    idx_data[key][1] += 1
                if cap in HFI_CATS:
                    idx_data[key][2] += 1

            si = sent_map.get(sent)
            if si is not None:
                sent_portal[portal][si] += 1

    # ── Phase 1: Count supplement articles per portal-month ──
    supp_month_counts = Counter()  # (portal, YYYY-MM) → count
    for sfile in sorted(DATA_DIR.glob("*_supplement.csv")):
        stem = sfile.stem.replace("_supplement", "")
        portal_name = SUPPLEMENT_PORTAL_MAP.get(stem)
        if not portal_name:
            continue
        with open(sfile, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                date_str = row.get("date", "")
                if date_str and len(date_str) >= 7:
                    supp_month_counts[(portal_name, date_str[:7])] += 1

    # ── Phase 2: Load supplements (primary source) ──
    for sfile in sorted(DATA_DIR.glob("*_supplement.csv")):
        stem = sfile.stem.replace("_supplement", "")
        portal_name = SUPPLEMENT_PORTAL_MAP.get(stem)
        if not portal_name:
            continue
        count = 0
        with open(sfile, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if not _in_date_range(row.get("date", "")):
                    continue
                process_row(row, portal_name)
                count += 1
        print(f"  [OK] {sfile.name}: {count:,}")

    # ── Phase 3: Load originals as FALLBACK for gap months only ──
    # Only use original corpus rows for months where supplement has < 100 articles
    MIN_SUPP_THRESHOLD = 100
    orig_used = 0
    orig_skipped = 0
    for fname, portal_name in ORIGINAL_FILES.items():
        path = ROOT_DIR / fname
        if not path.exists():
            print(f"  [SKIP] {fname}")
            continue
        count = 0
        with open(path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if not _in_date_range(row.get("date", "")):
                    continue
                p = portal_name or row.get("portal", "")
                # Skip MF Dnes NA-title artifacts
                title = (row.get("document_title", "") or "").strip()
                if (not title or title == "NA") and p == "MF Dnes":
                    continue
                # Only use original if supplement is thin for this month
                date_str = row.get("date", "")
                ym = date_str[:7] if date_str and len(date_str) >= 7 else ""
                supp_count = supp_month_counts.get((p, ym), 0)
                if supp_count >= MIN_SUPP_THRESHOLD:
                    orig_skipped += 1
                    continue
                process_row(row, p)
                count += 1
        if count > 0:
            print(f"  [OK] {fname}: {count:,} (gap-fill)")
        else:
            print(f"  [OK] {fname}: 0 (supplement covers all months)")
    print(f"  Original fallback: {orig_used + sum(1 for _ in [])} rows used, "
          f"{orig_skipped:,} skipped (supplement adequate)")

    print(f"  Total: {n_total:,} articles, {n_ukraine:,} ukraine")

    # Build charts
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
            # Share (%) instead of absolute count
            shares = []
            for m in months:
                ukr = monthly_portal_ukr.get((p, m), 0)
                tot = monthly_portal_total.get((p, m), 0)
                shares.append(round(100 * ukr / tot, 1) if tot >= 10 else None)
            chart3[c][p] = {
                "months": months,
                "values": shares,
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

    # ── NEW: chart3b — monthly total + Ukraine per portal, grouped by country ──
    chart3b = {}
    for c in COUNTRIES:
        chart3b[c] = {}
        for p in PORTAL_ORDER:
            if PORTAL_CONFIG.get(p, {}).get("country") != c:
                continue
            chart3b[c][p] = {
                "months": months,
                "total": [monthly_portal_total.get((p, m), 0) for m in months],
                "ukraine": [monthly_portal_ukr.get((p, m), 0) for m in months],
                "color": PORTAL_CONFIG[p]["color"],
                "dash": "solid" if PORTAL_CONFIG[p]["illiberal"] else "dash",
            }

    # ── NEW: chart5b — CAP distribution per portal (no "no policy content") ──
    # Get top CAP categories across all portals (excl. no policy content)
    filtered_caps = {k: v for k, v in cap_counts.items()
                     if k != "no policy content"}
    top_cap_names = [c[0] for c in sorted(filtered_caps.items(),
                     key=lambda x: -x[1])[:12]]

    chart5b = {}
    for p in PORTAL_ORDER:
        pcap = cap_by_portal.get(p, {})
        ptotal = sum(v for k, v in pcap.items() if k != "no policy content")
        if ptotal == 0:
            continue
        chart5b[p] = {
            "categories": [c.title() for c in top_cap_names],
            "shares": [round(100 * pcap.get(c, 0) / ptotal, 1)
                       for c in top_cap_names],
            "counts": [pcap.get(c, 0) for c in top_cap_names],
            "color": PORTAL_CONFIG[p]["color"],
        }

    # ── NEW: chart5c — CAP stacked area per portal ──
    # Top categories + "Other" bucket, aligned months, for 100% stacked area
    chart5c = {}
    for c in COUNTRIES:
        chart5c[c] = {}
        for p in PORTAL_ORDER:
            if PORTAL_CONFIG.get(p, {}).get("country") != c:
                continue
            pcap = cap_by_portal.get(p, {})
            ptop = [k for k, v in sorted(pcap.items(), key=lambda x: -x[1])
                    if k != "no policy content"][:8]
            if not ptop:
                continue
            # Find months with enough data
            valid_months = []
            for m in months:
                mc = cap_by_portal_month.get((p, m), {})
                mtotal = sum(v2 for k2, v2 in mc.items()
                             if k2 != "no policy content")
                if mtotal >= 10:
                    valid_months.append(m)
            if not valid_months:
                continue
            series = []
            for cat in ptop:
                vals = []
                for m in valid_months:
                    mc = cap_by_portal_month.get((p, m), {})
                    mtotal = sum(v2 for k2, v2 in mc.items()
                                 if k2 != "no policy content")
                    vals.append(round(100 * mc.get(cat, 0) / mtotal, 1)
                                if mtotal > 0 else 0)
                series.append({"cat": cat.title(), "values": vals})
            # Add "Other" = 100% - sum(top cats)
            other_vals = []
            for mi in range(len(valid_months)):
                top_sum = sum(s["values"][mi] for s in series)
                other_vals.append(round(max(0, 100 - top_sum), 1))
            series.append({"cat": "Other", "values": other_vals})
            chart5c[c][p] = {"months": valid_months, "series": series}

    # ── Supplement transition points (where original corpus ends) ──
    # These mark where scraping methodology changed (all sections → news only)
    supp_transitions = {
        "MF Dnes": "2024-04",
        "Novinky": "2024-03",
        "Magyar Nemzet": "2023-03",
        "Telex": "2024-03",
        "wPolityce": "2023-08",
        "Onet": None,  # supplement-only portal
        "Pravda": "2024-04",
        "Aktuality": "2024-04",
    }

    load_time = round(time.time() - t0, 1)

    return {
        "summary": {
            "total_articles": n_total,
            "ukraine_articles": n_ukraine,
            "ukraine_pct": round(100 * n_ukraine / n_total, 1) if n_total else 0,
            "portals": len(portal_set),
            "date_range": "2022-01-01 — 2026-02-23",
            "exported_at": time.strftime("%Y-%m-%d %H:%M"),
        },
        "chart1": chart1, "chart2": chart2, "chart3": chart3,
        "chart3b": chart3b, "chart4": chart4, "chart5": chart5,
        "chart5b": chart5b, "chart5c": chart5c,
        "chart6": chart6, "chart7": chart7, "chart8": chart8,
        "supp_transitions": supp_transitions,
    }


def main():
    print("Exporting dashboard data...")
    data = load_and_compute()
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    out = DOCS_DIR / "data.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    size_mb = out.stat().st_size / 1024 / 1024
    print(f"\nSaved: {out} ({size_mb:.1f} MB)")
    print(f"  {data['summary']['total_articles']:,} articles, "
          f"{data['summary']['ukraine_articles']:,} Ukraine-war")


if __name__ == "__main__":
    main()
