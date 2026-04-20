#!/bin/bash
# Restart all scrapers that are not currently running.
# Safe to run anytime — checkpoint ensures no duplicates.
cd "$(dirname "$0")"

start() {
  local name="$1"; shift
  if pgrep -f "$name" >/dev/null 2>&1; then
    echo "  SKIP  $name (already running)"
  else
    echo "  START $name"
    nohup python3 "$@" > "../../data/${name%.py}.log" 2>&1 &
  fi
}

echo "=== Restarting scrapers ($(date)) ==="

start scrape_wpolityce.py scrape_wpolityce.py \
    --start 2023-08-13 --end 2026-02-23 \
    --out ../../data/wpolityce_supplement.csv

start scrape_aktuality.py scrape_aktuality.py \
    --start 2024-04-04 --end 2026-02-23 \
    --out ../../data/aktuality_supplement.csv

start scrape_novinky_cdx.py scrape_novinky_cdx.py \
    --start 2024-03-23 --end 2026-02-23 \
    --out ../../data/novinky_supplement.csv \
    --cdx-cache ../../data/novinky_cdx_urls.jsonl

start scrape_idnes_cdx.py scrape_idnes_cdx.py \
    --start 2024-04-04 --end 2026-02-23 \
    --out ../../data/idnes_supplement.csv \
    --cdx-cache ../../data/idnes_cdx_urls.jsonl

start scrape_pravda_cdx.py scrape_pravda_cdx.py \
    --start 2024-03-23 --end 2026-02-23 \
    --out ../../data/pravda_supplement.csv \
    --cdx-cache ../../data/pravda_cdx_urls.jsonl

start scrape_onet.py scrape_onet.py \
    --start 2022-02-01 --end 2026-02-23 \
    --out ../../data/onet_supplement.csv \
    --cdx-cache ../../data/onet_cdx_urls.jsonl \
    --skip-cdx

start scrape_telex_cdx.py scrape_telex_cdx.py \
    --start 2024-03-28 --end 2026-02-23 \
    --out ../../data/telex_supplement.csv \
    --cdx-cache ../../data/telex_cdx_urls.jsonl \
    --skip-cdx

start scrape_magyarnemzet.py scrape_magyarnemzet.py \
    --start 2023-02-28 --end 2026-02-23 \
    --out ../../data/magyarnemzet_supplement.csv

echo "=== Done. Run ./status.sh to check progress ==="
