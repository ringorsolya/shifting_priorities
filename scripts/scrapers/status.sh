#!/bin/bash
# Quick status dashboard for all running scrapers.
# Usage: ./status.sh   (or: watch -n 30 ./status.sh)
#
# Shows for each *_supplement.csv:
#   - row count
#   - date range covered in the CSV
#   - modification time of the file
#   - whether the corresponding scrape_*.py process is running

DATA=~/Documents/Shifting_priorities/data
cd "$DATA" 2>/dev/null || { echo "data dir not found: $DATA"; exit 1; }

echo "=================================================================="
echo "  V4 scrapers — status dashboard  ($(date +%H:%M:%S))"
echo "=================================================================="
printf "%-12s  %-10s  %-22s  %-8s  %s\n" PORTAL ROWS "DATE RANGE" "MOD" RUNNING
printf "%-12s  %-10s  %-22s  %-8s  %s\n" "------" "----" "----------" "---" "-------"

for portal in novinky idnes pravda aktuality onet wpolityce telex magyarnemzet; do
  f="${portal}_supplement.csv"
  if [ -f "$f" ]; then
    total=$(wc -l < "$f")
    if [ "$total" -eq 0 ]; then
      rows=0
    else
      rows=$((total - 1))
    fi
    mod=$(stat -f "%Sm" -t "%H:%M:%S" "$f" 2>/dev/null || stat -c "%y" "$f" 2>/dev/null | cut -c12-19)
    range=$(python3 -c "
import csv, sys
try:
    with open('$f') as fh:
        ds = sorted({r['date'][:10] for r in csv.DictReader(fh) if r.get('date')})
    print(f'{ds[0]} → {ds[-1]}' if ds else '--')
except Exception as e:
    print('ERR')
" 2>/dev/null)
  else
    rows=0
    mod="--"
    range="--"
  fi

  # Is the scraper process alive?
  # Check both sitemap-based and CDX-based scraper variants
  if pgrep -f "scrape_${portal}(_cdx)?.py" >/dev/null 2>&1 || \
     pgrep -f "scrape_${portal}_cdx.py" >/dev/null 2>&1 || \
     pgrep -f "scrape_${portal}.py" >/dev/null 2>&1; then
    running="YES"
  else
    running="-"
  fi

  printf "%-12s  %-10s  %-22s  %-8s  %s\n" "$portal" "$rows" "$range" "$mod" "$running"
done
echo "=================================================================="
