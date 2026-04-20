#!/usr/bin/env python3
"""
Enrichment pipeline dashboard — shows progress of NER, keyword filter,
CAP Major, and Sentiment3 across all portals.

Usage:
    python3 pipeline_status.py              # one-shot
    python3 pipeline_status.py --watch      # auto-refresh every 30s
    python3 pipeline_status.py --watch 10   # auto-refresh every 10s
"""

import csv
import os
import re
import sys
import time
from pathlib import Path

csv.field_size_limit(10 * 1024 * 1024)

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"

PORTALS = [
    "novinky", "idnes", "pravda", "aktuality",
    "telex", "magyarnemzet", "wpolityce", "onet",
]

UKRAINE_KEYWORDS = [
    "Rusko", "Putin", "Moskva", "Ukrajina", "Zelenskyj", "Kyjev",
    "Oroszország", "Putyin", "Moszkva", "Ukrajna", "Zelenszkij", "Kijev",
    "Rosja", "Moskwa", "Ukraina", "Zełenski", "Kijów",
]
_UKRAINE_RE = re.compile("|".join(UKRAINE_KEYWORDS), re.IGNORECASE)

# ANSI colors
BOLD = "\033[1m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
DIM = "\033[2m"
RESET = "\033[0m"


def bar(done, total, width=20):
    if total == 0:
        return f"{DIM}{'░' * width}{RESET}"
    pct = done / total
    filled = int(pct * width)
    color = GREEN if pct >= 1.0 else YELLOW if pct >= 0.5 else RED
    return f"{color}{'█' * filled}{'░' * (width - filled)}{RESET}"


def pct_str(done, total):
    if total == 0:
        return f"{DIM}  —{RESET}"
    p = 100 * done / total
    if p >= 100:
        return f"{GREEN}{p:5.1f}%{RESET}"
    elif p >= 50:
        return f"{YELLOW}{p:5.1f}%{RESET}"
    else:
        return f"{RED}{p:5.1f}%{RESET}"


def count_portal(portal):
    path = DATA_DIR / f"{portal}_supplement.csv"
    if not path.exists():
        return None

    total = 0
    ner_done = 0
    ukraine = 0
    cap_done = 0
    sent_done = 0

    with open(path, "r", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for row in rd:
            total += 1
            has_ner = bool(row.get("document_ner", "").strip())
            if has_ner:
                ner_done += 1
            nerw = row.get("document_nerw", "")
            is_ukr = bool(_UKRAINE_RE.search(nerw)) if nerw else False
            if is_ukr:
                ukraine += 1
                if row.get("document_cap_major_code", "").strip():
                    cap_done += 1
                if row.get("document_sentiment3", "").strip():
                    sent_done += 1

    return {
        "total": total,
        "ner": ner_done,
        "ukraine": ukraine,
        "cap": cap_done,
        "sent": sent_done,
    }


def print_dashboard():
    now = time.strftime("%H:%M:%S")
    width = 88

    print(f"\n{BOLD}{'═' * width}{RESET}")
    print(f"{BOLD}  📊 Enrichment Pipeline Dashboard{RESET}"
          f"                              {DIM}{now}{RESET}")
    print(f"{BOLD}{'═' * width}{RESET}")

    # Header
    print(f"\n  {BOLD}{'Portal':<15} {'Total':>7}  "
          f"{'NER':>7}  {'🇺🇦 War':>7}  "
          f"{'CAP':>7}  {'Sent':>7}{RESET}")
    print(f"  {'─' * 72}")

    totals = {"total": 0, "ner": 0, "ukraine": 0, "cap": 0, "sent": 0}

    for portal in PORTALS:
        stats = count_portal(portal)
        if stats is None:
            print(f"  {portal:<15} {DIM}(no data){RESET}")
            continue

        for k in totals:
            totals[k] += stats[k]

        ner_pct = pct_str(stats["ner"], stats["total"])
        cap_pct = pct_str(stats["cap"], stats["ukraine"])
        sent_pct = pct_str(stats["sent"], stats["ukraine"])

        print(f"  {portal:<15} {stats['total']:>7,}  "
              f"{stats['ner']:>6,} {ner_pct}  "
              f"{stats['ukraine']:>6,}  "
              f"{stats['cap']:>5,} {cap_pct}  "
              f"{stats['sent']:>5,} {sent_pct}")

    print(f"  {'─' * 72}")
    ner_pct = pct_str(totals["ner"], totals["total"])
    cap_pct = pct_str(totals["cap"], totals["ukraine"])
    sent_pct = pct_str(totals["sent"], totals["ukraine"])
    print(f"  {BOLD}{'TOTAL':<15}{RESET} {totals['total']:>7,}  "
          f"{totals['ner']:>6,} {ner_pct}  "
          f"{totals['ukraine']:>6,}  "
          f"{totals['cap']:>5,} {cap_pct}  "
          f"{totals['sent']:>5,} {sent_pct}")

    # Progress bars
    print(f"\n  {BOLD}Overall progress:{RESET}")
    print(f"    NER        {bar(totals['ner'], totals['total'])} "
          f" {totals['ner']:,}/{totals['total']:,}")
    print(f"    CAP Major  {bar(totals['cap'], totals['ukraine'])} "
          f" {totals['cap']:,}/{totals['ukraine']:,} (war articles)")
    print(f"    Sentiment  {bar(totals['sent'], totals['ukraine'])} "
          f" {totals['sent']:,}/{totals['ukraine']:,} (war articles)")
    print()


def main():
    watch = False
    interval = 30

    if "--watch" in sys.argv:
        watch = True
        idx = sys.argv.index("--watch")
        if idx + 1 < len(sys.argv):
            try:
                interval = int(sys.argv[idx + 1])
            except ValueError:
                pass

    if watch:
        try:
            while True:
                os.system("clear" if os.name != "nt" else "cls")
                print_dashboard()
                print(f"  {DIM}Auto-refresh every {interval}s  "
                      f"(Ctrl+C to stop){RESET}\n")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n  Stopped.\n")
    else:
        print_dashboard()


if __name__ == "__main__":
    main()
