#!/usr/bin/env python3
"""
NER enrichment for supplement CSVs.

Reads each *_supplement.csv, runs spaCy NER on the original-language text,
and writes document_ner + document_nerw columns.

Follows the MediaText CEE methodology: spaCy EntityRecognizer at sentence level,
then aggregated to document level.

Usage:
    python3 enrich_ner.py                       # process all supplement CSVs
    python3 enrich_ner.py --portal novinky       # process only one
    python3 enrich_ner.py --limit 100            # first 100 rows (for testing)
    python3 enrich_ner.py --batch-size 500       # adjust batch size

Requirements:
    pip install spacy --break-system-packages
    python3 -m spacy download xx_ent_wiki_sm     # multilingual NER (CZ, SK)
    python3 -m spacy download hu_core_news_lg    # Hungarian NER
    python3 -m spacy download pl_core_news_lg    # Polish NER
"""

import argparse
import csv
import logging
import sys
import time
from pathlib import Path

csv.field_size_limit(10 * 1024 * 1024)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Portal → language code mapping ──
PORTAL_LANG = {
    "novinky":       "xx",   # Czech  → multilingual
    "idnes":         "xx",   # Czech  → multilingual
    "pravda":        "xx",   # Slovak → multilingual
    "aktuality":     "xx",   # Slovak → multilingual
    "telex":         "hu",   # Hungarian → dedicated model
    "magyarnemzet":  "hu",   # Hungarian → dedicated model
    "onet":          "pl",   # Polish → dedicated model
    "wpolityce":     "pl",   # Polish → dedicated model
}

# ── Language code → spaCy model name ──
SPACY_MODELS = {
    "xx": "xx_ent_wiki_sm",
    "hu": "hu_core_news_lg",
    "pl": "pl_core_news_lg",
}

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# Cache loaded models so we don't reload per-portal
_model_cache: dict[str, object] = {}


def load_spacy_model(lang_code: str):
    """Load (and download if needed) a spaCy model. Cached per lang_code."""
    if lang_code in _model_cache:
        return _model_cache[lang_code]

    import spacy
    model_name = SPACY_MODELS[lang_code]
    try:
        nlp = spacy.load(model_name)
    except OSError:
        log.info(f"Model {model_name} not found — downloading...")
        from spacy.cli import download
        download(model_name)
        nlp = spacy.load(model_name)

    nlp.max_length = 2_000_000
    _model_cache[lang_code] = nlp
    log.info(f"  loaded spaCy model: {model_name}")
    return nlp


def format_ner(doc) -> tuple[str, str]:
    """
    Format spaCy NER output into the two column formats used by MediaText:
      document_ner:  "LOC: x, y; PER: a, b; ORG: c; MISC: d"
      document_nerw: "x, y, a, b, c, d"  (all entities, no type prefix)
    """
    by_label: dict[str, list[str]] = {}
    all_ents: list[str] = []

    for ent in doc.ents:
        text = ent.text.strip()
        if not text:
            continue
        # Map spaCy labels to the 4 standard types used in the original data
        label = ent.label_
        if label in ("GPE", "LOC", "FAC"):
            cat = "LOC"
        elif label in ("PERSON", "PER"):
            cat = "PER"
        elif label in ("ORG", "NORP"):
            cat = "ORG"
        else:
            cat = "MISC"

        if cat not in by_label:
            by_label[cat] = []
        by_label[cat].append(text)
        all_ents.append(text)

    # Build document_ner string
    parts = []
    for cat in ["LOC", "PER", "ORG", "MISC"]:
        if cat in by_label:
            parts.append(f"{cat}: {', '.join(by_label[cat])}")
    ner_str = "; ".join(parts)

    # Build document_nerw string (all entities without type)
    nerw_str = ", ".join(all_ents)

    return ner_str, nerw_str


def process_csv(csv_path: Path, lang_code: str, limit: int = 0,
                batch_size: int = 200):
    """
    Read a supplement CSV, run NER on rows that need it, write results back.
    Uses a temporary output file for safety.
    """
    nlp = load_spacy_model(lang_code)

    # Read all rows
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    total = len(rows)
    needs_ner = sum(1 for r in rows if not r.get("document_ner"))
    log.info(f"  {csv_path.name}: {total} rows, {needs_ner} need NER")

    if needs_ner == 0:
        log.info("  nothing to do — skipping")
        return

    if limit:
        log.info(f"  --limit {limit}: processing at most {limit} rows")

    # Separate rows: already done vs need processing
    tmp_path = csv_path.with_suffix(".tmp")
    enriched = 0
    t0 = time.time()

    to_process = []
    pass_through = []

    for i, row in enumerate(rows):
        if row.get("document_ner"):
            pass_through.append((i, row))
        else:
            if limit and len(to_process) >= limit:
                pass_through.append((i, row))
            else:
                to_process.append((i, row))

    # Build text list for nlp.pipe
    texts = []
    for idx, row in to_process:
        text = row.get("document_text", "") or ""
        title = row.get("document_title", "") or ""
        combined = f"{title}. {text}" if text else title
        texts.append(combined)

    log.info(f"  running spaCy NER ({SPACY_MODELS[lang_code]}) on "
             f"{len(texts)} articles (batch_size={batch_size})...")

    # Process with nlp.pipe
    ner_results = {}
    for i, doc in enumerate(nlp.pipe(texts, batch_size=batch_size)):
        idx = to_process[i][0]
        ner_str, nerw_str = format_ner(doc)
        ner_results[idx] = (ner_str, nerw_str)
        enriched += 1
        if enriched <= 3 or enriched % 500 == 0:
            elapsed = time.time() - t0
            rate = enriched / elapsed if elapsed > 0 else 0
            log.info(f"  enriched={enriched}/{len(texts)}  "
                     f"rate={rate:.1f} art/s  "
                     f"elapsed={elapsed:.0f}s")

    # Write all rows in original order
    with open(tmp_path, "w", encoding="utf-8", newline="") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=fieldnames,
                                quoting=csv.QUOTE_ALL)
        writer.writeheader()

        all_indexed = pass_through + to_process
        all_indexed.sort(key=lambda x: x[0])

        for idx, row in all_indexed:
            if idx in ner_results:
                row["document_ner"] = ner_results[idx][0]
                row["document_nerw"] = ner_results[idx][1]
            writer.writerow(row)

    # Replace original with enriched version
    backup_path = csv_path.with_suffix(".csv.bak")
    if backup_path.exists():
        backup_path.unlink()
    csv_path.rename(backup_path)
    tmp_path.rename(csv_path)
    log.info(f"  done: {enriched} rows enriched in {time.time()-t0:.0f}s")
    log.info(f"  backup: {backup_path.name}")


def main():
    ap = argparse.ArgumentParser(description="NER enrichment for supplement CSVs")
    ap.add_argument("--portal", type=str, default="",
                    help="process only this portal (e.g. 'novinky')")
    ap.add_argument("--limit", type=int, default=0,
                    help="max rows to process per file (0 = all)")
    ap.add_argument("--batch-size", type=int, default=200,
                    help="spaCy nlp.pipe batch size")
    ap.add_argument("--data-dir", type=str, default="",
                    help="override data directory")
    args = ap.parse_args()

    data_dir = Path(args.data_dir) if args.data_dir else DATA_DIR

    portals = [args.portal] if args.portal else list(PORTAL_LANG.keys())

    for portal in portals:
        csv_path = data_dir / f"{portal}_supplement.csv"
        if not csv_path.exists():
            log.warning(f"  {csv_path} not found — skipping")
            continue

        lang = PORTAL_LANG.get(portal, "xx")
        log.info(f"Processing {portal} ({csv_path.name}, "
                 f"model={SPACY_MODELS[lang]})")
        try:
            process_csv(csv_path, lang, limit=args.limit,
                        batch_size=args.batch_size)
        except Exception as e:
            log.error(f"  ERROR processing {portal}: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()
