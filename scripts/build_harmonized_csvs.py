#!/usr/bin/env python3
"""
Build per-portal harmonised CSV files for the article repository.

For each (portal, month) pair, picks the source (supplement or original) with
more CAP-classified Ukraine-war articles — same coverage adjudication as
export_dashboard.py — and writes one unified CSV per portal under harmonized/.

Streaming I/O: rows are written to per-portal files as they are read, so memory
usage stays bounded regardless of corpus size. The final per-portal CSVs are
then sorted in place by date using an external streaming merge.

Usage:
    python3 scripts/build_harmonized_csvs.py
"""

import csv
import json
import re
import time
from collections import Counter
from pathlib import Path

csv.field_size_limit(10 * 1024 * 1024)

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
OUT_DIR = ROOT_DIR / "harmonized"
USE_ORIG_CACHE = OUT_DIR / ".use_orig_cache.json"

PORTAL_CFG = {"MF Dnes", "Novinky", "Magyar Nemzet", "Telex",
              "wPolityce", "Onet", "Pravda", "Aktuality"}

SUPP_MAP = {"novinky": "Novinky", "idnes": "MF Dnes", "pravda": "Pravda",
            "aktuality": "Aktuality", "telex": "Telex",
            "magyarnemzet": "Magyar Nemzet", "wpolityce": "wPolityce",
            "onet": "Onet"}

ORIG_FILES = {
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

OUTPUT_COLUMNS = [
    "document_id", "date", "portal", "country", "illiberal",
    "document_title", "first_sentence", "first_sentence_english",
    "document_text", "document_text_english",
    "document_ner", "document_nerw",
    "document_cap_major_code", "document_cap_major_label",
    "document_cap_media2_code", "document_cap_media2_label",
    "document_sentiment3",
    "is_ukraine_war",
    "source",
    "url", "scraped_at", "electoral_cycle",
]

PORTAL_META = {
    "MF Dnes":       {"country": "CZ", "illiberal": 1},
    "Novinky":       {"country": "CZ", "illiberal": 0},
    "Magyar Nemzet": {"country": "HU", "illiberal": 1},
    "Telex":         {"country": "HU", "illiberal": 0},
    "wPolityce":     {"country": "PL", "illiberal": 1},
    "Onet":          {"country": "PL", "illiberal": 0},
    "Pravda":        {"country": "SK", "illiberal": 1},
    "Aktuality":     {"country": "SK", "illiberal": 0},
}


def _in_date_range(date_str):
    if not date_str or len(date_str) < 10:
        return False
    return "2022-01-01" <= date_str[:10] <= "2026-02-23"


def _is_cap_ukraine(row):
    nerw = row.get("document_nerw", "") or ""
    if not nerw or not _UKRAINE_RE.search(nerw):
        return False
    cap = (row.get("document_cap_major_label", "") or "").strip().lower()
    return bool(cap) and cap != "na"


def harmonise(row, portal, source):
    did = row.get("document_id", "")
    if not did:
        return None
    meta = PORTAL_META[portal]
    nerw = row.get("document_nerw", "") or ""
    is_ukr = bool(nerw and _UKRAINE_RE.search(nerw))
    return {
        "document_id": did,
        "date": (row.get("date", "") or "")[:10],
        "portal": portal,
        "country": meta["country"],
        "illiberal": meta["illiberal"],
        "document_title": row.get("document_title", "") or "",
        "first_sentence": row.get("first_sentence", "") or "",
        "first_sentence_english": row.get("first_sentence_english", "") or "",
        "document_text": row.get("document_text", "") or "",
        "document_text_english": row.get("document_text_english", "") or "",
        "document_ner": row.get("document_ner", "") or "",
        "document_nerw": nerw,
        "document_cap_major_code": row.get("document_cap_major_code", "") or "",
        "document_cap_major_label": row.get("document_cap_major_label", "") or "",
        "document_cap_media2_code": row.get("document_cap_media2_code", "") or "",
        "document_cap_media2_label": row.get("document_cap_media2_label", "") or "",
        "document_sentiment3": row.get("document_sentiment3", "") or "",
        "is_ukraine_war": "1" if is_ukr else "0",
        "source": source,
        "url": row.get("url", "") or "",
        "scraped_at": row.get("scraped_at", "") or "",
        "electoral_cycle": row.get("electoral_cycle", "") or "",
    }


def _portal_has_originals(out_path):
    """Quickly check if an existing output file already contains rows with source='original'.
    Used to skip portals already finished in a previous run."""
    if not out_path.exists():
        return False
    with open(out_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("source") == "original":
                return True
    return False


def main():
    t0 = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    use_orig = None
    if USE_ORIG_CACHE.exists():
        print(f"[cache] Loading use_orig from {USE_ORIG_CACHE.name}", flush=True)
        with open(USE_ORIG_CACHE, "r") as f:
            cache = json.load(f)
        use_orig = set(tuple(k) for k in cache)

    if use_orig is None:
        # ── Phase 1: count CAP-Ukraine articles per (portal, ym) per source ──
        print("[1/3] Counting CAP-Ukraine articles per source...", flush=True)
        supp_cnt = Counter()
        orig_cnt = Counter()

        for sfile in sorted(DATA_DIR.glob("*_supplement.csv")):
            portal = SUPP_MAP.get(sfile.stem.replace("_supplement", ""))
            if not portal:
                continue
            with open(sfile, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    d = row.get("date", "")
                    if _in_date_range(d) and _is_cap_ukraine(row):
                        supp_cnt[(portal, d[:7])] += 1

        for fname, p_name in ORIG_FILES.items():
            path = ROOT_DIR / fname
            if not path.exists():
                continue
            with open(path, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    d = row.get("date", "")
                    if not _in_date_range(d):
                        continue
                    p = p_name or row.get("portal", "")
                    if p not in PORTAL_CFG:
                        continue
                    t = (row.get("document_title", "") or "").strip()
                    if (not t or t == "NA") and p == "MF Dnes":
                        continue
                    if _is_cap_ukraine(row):
                        orig_cnt[(p, d[:7])] += 1

        # ── Phase 2: decide source winner per (portal, ym) ──
        print("[2/3] Deciding source per (portal, ym)...", flush=True)
        use_orig = set()
        for k in set(supp_cnt) | set(orig_cnt):
            if orig_cnt.get(k, 0) > supp_cnt.get(k, 0):
                use_orig.add(k)
        with open(USE_ORIG_CACHE, "w") as f:
            json.dump(sorted(list(use_orig)), f)
        print(f"  Cached use_orig to {USE_ORIG_CACHE.name}", flush=True)

    # ── Phase 3: stream-write directly to per-portal output files ──
    # If a portal output already has rows with source='original' (or has supplement-only
    # rows and there is no original source for it), skip processing for that portal.
    print("[3/3] Streaming rows to per-portal harmonised CSVs...", flush=True)
    out_files = {}
    out_writers = {}
    counters = Counter()
    counters_ukr = Counter()
    seen_ids = set()  # global dedup within this run

    portals_done = set()
    portals_supp_done = set()  # portals where supplement was already written
    for portal in sorted(PORTAL_CFG):
        slug = portal.lower().replace(" ", "_")
        path = OUT_DIR / f"{slug}_harmonized.csv"
        # Onet has no original source — supplement-only is the final state
        has_orig_source = any(
            (p_name or "Telex" if "indextelex" in fn else p_name) == portal
            for fn, p_name in ORIG_FILES.items()
            if (ROOT_DIR / fn).exists()
        ) or portal == "Telex"
        if path.exists() and (_portal_has_originals(path) or not has_orig_source):
            print(f"  [SKIP] {portal:>16} — already complete", flush=True)
            portals_done.add(portal)
            continue
        # If file exists with supplement-only rows: append originals
        if path.exists():
            f = open(path, "a", encoding="utf-8", newline="")
            out_files[portal] = f
            out_writers[portal] = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS,
                                                  extrasaction="ignore")
            portals_supp_done.add(portal)
            # Also pre-load existing IDs into seen_ids so we don't re-write them
            with open(path, "r", encoding="utf-8") as rf:
                for row in csv.DictReader(rf):
                    seen_ids.add(row["document_id"])
            print(f"  [APPEND] {portal:>16} — has supplement, adding originals "
                  f"({len(seen_ids):,} ids loaded)", flush=True)
        else:
            f = open(path, "w", encoding="utf-8", newline="")
            out_files[portal] = f
            out_writers[portal] = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS,
                                                  extrasaction="ignore")
            out_writers[portal].writeheader()

    # Read supplements: write rows where (portal, ym) NOT in use_orig
    # (skip portals where supplement was already written in a prior run)
    for sfile in sorted(DATA_DIR.glob("*_supplement.csv")):
        portal = SUPP_MAP.get(sfile.stem.replace("_supplement", ""))
        if not portal or portal in portals_done or portal in portals_supp_done:
            continue
        n = 0
        with open(sfile, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                d = row.get("date", "")
                if not _in_date_range(d):
                    continue
                if (portal, d[:7]) in use_orig:
                    continue
                did = row.get("document_id", "")
                if not did or did in seen_ids:
                    continue
                seen_ids.add(did)
                hrow = harmonise(row, portal, "supplement")
                if hrow is None:
                    continue
                out_writers[portal].writerow(hrow)
                counters[portal] += 1
                if hrow["is_ukraine_war"] == "1":
                    counters_ukr[portal] += 1
                n += 1
        print(f"  {sfile.name}: +{n:,}", flush=True)

    # Read originals: write rows where (portal, ym) IS in use_orig
    # (skip files whose target portal is already done)
    for fname, p_name in ORIG_FILES.items():
        path = ROOT_DIR / fname
        if not path.exists():
            continue
        # Quick skip if all relevant portals for this file are already done
        if p_name and p_name in portals_done:
            print(f"  [SKIP] {fname} (target {p_name} already done)", flush=True)
            continue
        n = 0
        with open(path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                d = row.get("date", "")
                if not _in_date_range(d):
                    continue
                p = p_name or row.get("portal", "")
                if p not in PORTAL_CFG or p in portals_done:
                    continue
                t = (row.get("document_title", "") or "").strip()
                if (not t or t == "NA") and p == "MF Dnes":
                    continue
                if (p, d[:7]) not in use_orig:
                    continue
                did = row.get("document_id", "")
                if not did or did in seen_ids:
                    continue
                seen_ids.add(did)
                hrow = harmonise(row, p, "original")
                if hrow is None:
                    continue
                out_writers[p].writerow(hrow)
                counters[p] += 1
                if hrow["is_ukraine_war"] == "1":
                    counters_ukr[p] += 1
                n += 1
        print(f"  {fname}: +{n:,}", flush=True)

    for f in out_files.values():
        f.close()

    # ── Summary ──
    print()
    print("Output (rows are written in source-stream order; sort by date if needed):")
    grand_total = 0
    grand_ukr = 0
    for portal in sorted(PORTAL_CFG):
        slug = portal.lower().replace(" ", "_")
        path = OUT_DIR / f"{slug}_harmonized.csv"
        size_mb = path.stat().st_size / 1024 / 1024
        n = counters[portal]
        nu = counters_ukr[portal]
        grand_total += n
        grand_ukr += nu
        print(f"  {portal:>16}: {n:>7,} rows ({nu:>6,} Ukraine-war) → "
              f"{path.name} ({size_mb:,.1f} MB)")
    print(f"  {'TOTAL':>16}: {grand_total:>7,} rows ({grand_ukr:>6,} Ukraine-war)")
    print(f"\nDone in {round(time.time() - t0, 1)}s.")


if __name__ == "__main__":
    main()
