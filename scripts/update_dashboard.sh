#!/bin/bash
# One-command dashboard update: export data → commit → push
set -e
cd "$(dirname "$0")/.."

echo "Exporting dashboard data..."
python3 scripts/export_dashboard.py

echo "Committing and pushing..."
git add docs/data.json
git commit -m "Update dashboard data $(date +%Y-%m-%d)" || echo "No changes to commit"
git push

echo "Done! Dashboard will update in ~1 minute."
