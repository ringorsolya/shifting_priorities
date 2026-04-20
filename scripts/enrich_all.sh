#!/bin/bash
# =================================================================
# Master enrichment pipeline for supplement CSVs
# Follows the MediaText CEE methodology (manuscript Section 2.2)
#
# Steps:
#   1. NER (spaCy EntityRecognizer) → document_ner, document_nerw
#   2. CAP Major (xlm-roberta-large-pooled-cap-v4) → document_cap_major_*
#   3. CAP Media2 (xlm-roberta-large-english-cap-media2-v17)
#      → requires Helsinki-NLP translation → document_cap_media2_*
#   4. Sentiment3 (xlm-roberta-large-pooled-sentiment-v2)
#      → sentence-level → neutrality-weighted aggregation
#      → document_sentiment3
#
# Usage:
#   ./enrich_all.sh                # all portals, all tasks
#   ./enrich_all.sh novinky        # one portal only
#   ./enrich_all.sh novinky 50     # one portal, 50 rows (test)
# =================================================================

set -e
cd "$(dirname "$0")"

PORTAL="${1:-}"
LIMIT="${2:-0}"
LOGDIR="../data"

echo "============================================"
echo "  MediaText CEE Enrichment Pipeline"
echo "  $(date)"
echo "============================================"

# ── Step 1: NER ──
echo ""
echo ">>> Step 1/2: NER (spaCy EntityRecognizer)"
echo ""

NER_CMD="python3 enrich_ner.py"
[ -n "$PORTAL" ] && NER_CMD="$NER_CMD --portal $PORTAL"
[ "$LIMIT" -gt 0 ] 2>/dev/null && NER_CMD="$NER_CMD --limit $LIMIT"

echo "  Running: $NER_CMD"
$NER_CMD 2>&1 | tee "$LOGDIR/enrich_ner.log"

# ── Step 2: CAP + Sentiment ──
echo ""
echo ">>> Step 2/2: CAP Major + CAP Media2 + Sentiment3"
echo ""

CAP_CMD="python3 enrich_cap_sentiment.py"
[ -n "$PORTAL" ] && CAP_CMD="$CAP_CMD --portal $PORTAL"
[ "$LIMIT" -gt 0 ] 2>/dev/null && CAP_CMD="$CAP_CMD --limit $LIMIT"

echo "  Running: $CAP_CMD"
$CAP_CMD 2>&1 | tee "$LOGDIR/enrich_cap_sentiment.log"

echo ""
echo "============================================"
echo "  Pipeline complete — $(date)"
echo "============================================"
