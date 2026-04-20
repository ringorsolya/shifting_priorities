#!/usr/bin/env python3
"""
Full enrichment + merge pipeline for supplement CSVs.

Pipeline steps per portal:
  1. NER  (spaCy EntityRecognizer)  → document_ner, document_nerw
  2. Ukraine keyword filter on document_nerw (same dictionary as 01_load_and_filter.py)
  3. CAP Major + Sentiment3 — ONLY on Ukraine-war articles (saves time)
  4. Merge enriched supplement rows into original *_with_preds.csv

Usage:
    python3 enrich_and_merge.py                   # auto: finished scrapers only
    python3 enrich_and_merge.py --portal novinky   # one portal
    python3 enrich_and_merge.py --limit 10         # test on 10 rows
    python3 enrich_and_merge.py --step ner         # NER only (first pass)
    python3 enrich_and_merge.py --step classify     # CAP+Sentiment only (second pass)
    python3 enrich_and_merge.py --merge-only        # skip enrichment, just merge
    python3 enrich_and_merge.py --force             # run even if scraper still active

Requirements:
    pip install spacy transformers torch sentencepiece --break-system-packages
    python3 -m spacy download hu_core_news_lg
    python3 -m spacy download pl_core_news_lg
    python3 -m spacy download xx_ent_wiki_sm
"""

import argparse
import csv
import logging
import re
import shutil
import subprocess
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

# ── Paths ──
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"

# ── Ukraine-war keyword dictionary (from config.py) ──
UKRAINE_KEYWORDS = [
    # Czech / Slovak
    "Rusko", "Putin", "Moskva", "Ukrajina", "Zelenskyj", "Kyjev",
    # Hungarian
    "Oroszország", "Putyin", "Moszkva", "Ukrajna", "Zelenszkij", "Kijev",
    # Polish
    "Rosja", "Moskwa", "Ukraina", "Zełenski", "Kijów",
]
_UKRAINE_RE = re.compile("|".join(UKRAINE_KEYWORDS), re.IGNORECASE)

# ── Portal configuration ──
PORTALS = {
    "novinky": {
        "spacy_lang": "xx",
        "original_csv": "CZ_M_novinky_document_level_with_preds.csv",
    },
    "idnes": {
        "spacy_lang": "xx",
        "original_csv": "CZ_M_mfdnes_document_level_with_preds.csv",
    },
    "pravda": {
        "spacy_lang": "xx",
        "original_csv": "SK_M_pravda_document_level_with_preds.csv",
    },
    "aktuality": {
        "spacy_lang": "xx",
        "original_csv": "SK_M_aktuality_document_level_with_preds.csv",
    },
    "telex": {
        "spacy_lang": "hu",
        "original_csv": "HU_M_indextelex_document_level_with_preds.csv",
    },
    "magyarnemzet": {
        "spacy_lang": "hu",
        "original_csv": "HU_M_magyarnemzet_document_level_with_preds.csv",
    },
    "onet": {
        "spacy_lang": "pl",
        "original_csv": None,  # new portal, no original CSV
    },
    "wpolityce": {
        "spacy_lang": "pl",
        "original_csv": "PL_M_wpolityce_document_level_with_preds.csv",
    },
}

SPACY_MODELS = {
    "xx": "xx_ent_wiki_sm",
    "hu": "hu_core_news_lg",
    "pl": "pl_core_news_lg",
}

# ── CAP Major codebook ──
CAP_MAJOR_CODES = {
    1: "Macroeconomics", 2: "Civil Rights", 3: "Health", 4: "Agriculture",
    5: "Labor", 6: "Education", 7: "Environment", 8: "Energy",
    9: "Immigration", 10: "Transportation", 12: "Law and Crime",
    13: "Social Welfare", 14: "Housing",
    15: "Banking, Finance, and Domestic Commerce", 16: "Defense",
    17: "Technology", 18: "Foreign Trade", 19: "International Affairs",
    20: "Government Operations", 21: "Public Lands", 23: "Culture",
    999: "No Policy Content",
}

# Original CSV columns (supplement has extra: url, scraped_at)
ORIGINAL_COLUMNS = [
    "document_id", "document_title", "first_sentence",
    "first_sentence_english", "document_text", "document_text_english",
    "date", "electoral_cycle", "portal", "illiberal",
    "document_cap_media2_code", "document_cap_media2_label",
    "document_cap_major_code", "document_cap_major_label",
    "document_sentiment3", "document_ner", "document_nerw",
]

_model_cache = {}


# =====================================================================
# Utilities
# =====================================================================
def is_scraper_running(portal: str) -> bool:
    try:
        result = subprocess.run(
            ["pgrep", "-f", f"scrape_{portal}"],
            capture_output=True, text=True)
        return result.returncode == 0
    except FileNotFoundError:
        return False


def get_device():
    import torch
    if torch.cuda.is_available():
        return torch.device("cuda")
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


def truncate_text(text: str, max_tokens: int = 500) -> str:
    words = text.split()
    return " ".join(words[:max_tokens]) if len(words) > max_tokens else text


def is_ukraine_article(nerw: str) -> bool:
    """Check if document_nerw matches any Ukraine-war keyword."""
    return bool(_UKRAINE_RE.search(nerw)) if nerw else False


def read_csv(path: Path) -> tuple[list[str], list[dict]]:
    """Read CSV, return (fieldnames, rows)."""
    with open(path, "r", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        return list(rd.fieldnames), list(rd)


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]):
    """Write rows to CSV via temp file + backup."""
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames,
                                quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    backup = path.with_suffix(".csv.bak")
    if backup.exists():
        backup.unlink()
    path.rename(backup)
    tmp.rename(path)


# =====================================================================
# Step 1: NER (spaCy)
# =====================================================================
def load_spacy_model(lang_code: str):
    key = f"spacy_{lang_code}"
    if key in _model_cache:
        return _model_cache[key]
    import spacy
    model_name = SPACY_MODELS[lang_code]
    try:
        nlp = spacy.load(model_name)
    except OSError:
        log.info(f"  downloading spaCy model: {model_name}")
        from spacy.cli import download
        download(model_name)
        nlp = spacy.load(model_name)
    nlp.max_length = 2_000_000
    _model_cache[key] = nlp
    log.info(f"  loaded spaCy: {model_name}")
    return nlp


def format_ner(doc) -> tuple[str, str]:
    by_label: dict[str, list[str]] = {}
    all_ents: list[str] = []
    for ent in doc.ents:
        text = ent.text.strip()
        if not text:
            continue
        label = ent.label_
        if label in ("GPE", "LOC", "FAC"):
            cat = "LOC"
        elif label in ("PERSON", "PER"):
            cat = "PER"
        elif label in ("ORG", "NORP"):
            cat = "ORG"
        else:
            cat = "MISC"
        by_label.setdefault(cat, []).append(text)
        all_ents.append(text)

    parts = []
    for cat in ["LOC", "PER", "ORG", "MISC"]:
        if cat in by_label:
            parts.append(f"{cat}: {', '.join(by_label[cat])}")
    return "; ".join(parts), ", ".join(all_ents)


def run_ner(rows: list[dict], indices: list[int], lang_code: str,
            batch_size: int = 200, save_fn=None, save_every: int = 500):
    nlp = load_spacy_model(lang_code)
    texts = []
    for i in indices:
        text = rows[i].get("document_text", "") or ""
        title = rows[i].get("document_title", "") or ""
        texts.append(f"{title}. {text}" if text else title)

    log.info(f"    NER ({SPACY_MODELS[lang_code]}): "
             f"{len(texts)} articles...")
    t0 = time.time()
    for j, doc in enumerate(nlp.pipe(texts, batch_size=batch_size)):
        ner_str, nerw_str = format_ner(doc)
        rows[indices[j]]["document_ner"] = ner_str
        rows[indices[j]]["document_nerw"] = nerw_str
        if (j + 1) <= 3 or (j + 1) % 1000 == 0:
            log.info(f"    NER: {j+1}/{len(texts)}  "
                     f"({(j+1)/(time.time()-t0):.1f} art/s)")
        if save_fn and (j + 1) % save_every == 0:
            save_fn()
            log.info(f"    checkpoint saved at {j+1}/{len(texts)}")
    log.info(f"    NER done: {len(texts)} articles in {time.time()-t0:.0f}s")


# =====================================================================
# Step 2: Ukraine keyword filter (on document_nerw)
# =====================================================================
def find_ukraine_indices(rows: list[dict]) -> list[int]:
    """Return indices of rows whose document_nerw matches Ukraine keywords."""
    ukraine_idx = []
    for i, row in enumerate(rows):
        if is_ukraine_article(row.get("document_nerw", "")):
            ukraine_idx.append(i)
    return ukraine_idx


# =====================================================================
# Step 3a: CAP Major (poltextlab/xlm-roberta-large-pooled-cap-v4)
# =====================================================================
def load_cap_major():
    if "cap_major" in _model_cache:
        return _model_cache["cap_major"]
    import torch
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    name = "poltextlab/xlm-roberta-large-pooled-cap-v4"
    log.info(f"  loading CAP Major: {name}")
    tok = AutoTokenizer.from_pretrained(name)
    model = AutoModelForSequenceClassification.from_pretrained(name)
    device = get_device()
    model.to(device).eval()
    _model_cache["cap_major"] = (tok, model, device)
    return tok, model, device


def run_cap_major(rows: list[dict], indices: list[int], batch_size: int = 8,
                  save_fn=None, save_every: int = 200):
    import torch
    tok, model, device = load_cap_major()
    id2label = model.config.id2label

    texts = []
    for i in indices:
        text = rows[i].get("document_text", "") or ""
        title = rows[i].get("document_title", "") or ""
        texts.append(truncate_text(f"{title}. {text}" if text else title))

    log.info(f"    CAP Major: {len(texts)} articles...")
    t0 = time.time()

    for start in range(0, len(texts), batch_size):
        batch = texts[start:start + batch_size]
        inputs = tok(batch, padding=True, truncation=True, max_length=512,
                     return_tensors="pt").to(device)
        with torch.no_grad():
            logits = model(**inputs).logits
        preds = logits.argmax(dim=-1).cpu().tolist()

        for j, p in enumerate(preds):
            idx = indices[start + j]
            label_str = id2label.get(p, str(p))
            try:
                code = int(label_str)
                label = CAP_MAJOR_CODES.get(code, label_str)
            except ValueError:
                label = label_str
                code = next((k for k, v in CAP_MAJOR_CODES.items()
                            if v == label_str), 999)
            rows[idx]["document_cap_major_code"] = str(code)
            rows[idx]["document_cap_major_label"] = label

        done = min(start + batch_size, len(texts))
        if done <= batch_size or done % 500 < batch_size:
            log.info(f"    CAP Major: {done}/{len(texts)}")
        if save_fn and done % save_every < batch_size:
            save_fn()
            log.info(f"    checkpoint saved at {done}/{len(texts)}")

    log.info(f"    CAP Major done in {time.time()-t0:.0f}s")


# =====================================================================
# Step 3b: Sentiment3 (poltextlab/xlm-roberta-large-pooled-sentiment-v2)
# =====================================================================
def load_sentiment():
    if "sentiment" in _model_cache:
        return _model_cache["sentiment"]
    import torch
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    name = "poltextlab/xlm-roberta-large-pooled-sentiment-v2"
    log.info(f"  loading Sentiment3: {name}")
    tok = AutoTokenizer.from_pretrained(name)
    model = AutoModelForSequenceClassification.from_pretrained(name)
    device = get_device()
    model.to(device).eval()
    _model_cache["sentiment"] = (tok, model, device)
    return tok, model, device


def split_sentences(text: str) -> list[str]:
    if "sentencizer" not in _model_cache:
        import spacy
        nlp = spacy.blank("xx")
        nlp.add_pipe("sentencizer")
        nlp.max_length = 2_000_000
        _model_cache["sentencizer"] = nlp
    doc = _model_cache["sentencizer"](text)
    return [s.text.strip() for s in doc.sents if s.text.strip()]


def predict_sentiment_document(text: str, batch_size: int = 16) -> str:
    """
    Sentence-level → document-level sentiment via neutrality-weighted polarity.
    polarity = P_pos - P_neg, weight = 1 - P_neu
    doc_polarity = weighted_mean(polarity, weight)
    Thresholds: < -0.1 → Negative, > 0.1 → Positive, else Neutral
    """
    import torch
    tok, model, device = load_sentiment()
    sentences = split_sentences(text)
    if not sentences:
        return "Neutral"

    all_probs = []
    for i in range(0, len(sentences), batch_size):
        batch = sentences[i:i + batch_size]
        inputs = tok(batch, padding=True, truncation=True, max_length=512,
                     return_tensors="pt").to(device)
        with torch.no_grad():
            logits = model(**inputs).logits
            probs = torch.softmax(logits, dim=-1).cpu()
        all_probs.append(probs)

    all_probs = torch.cat(all_probs, dim=0)
    p_neg, p_neu, p_pos = all_probs[:, 0], all_probs[:, 1], all_probs[:, 2]

    polarity = p_pos - p_neg
    weights = 1.0 - p_neu
    weight_sum = weights.sum()

    if weight_sum > 0:
        doc_polarity = (polarity * weights).sum() / weight_sum
    else:
        doc_polarity = polarity.mean()

    val = doc_polarity.item()
    if val < -0.1:
        return "Negative"
    elif val > 0.1:
        return "Positive"
    return "Neutral"


def run_sentiment(rows: list[dict], indices: list[int],
                  save_fn=None, save_every: int = 200):
    log.info(f"    Sentiment3: {len(indices)} articles...")
    t0 = time.time()
    for k, i in enumerate(indices):
        text = rows[i].get("document_text", "") or ""
        title = rows[i].get("document_title", "") or ""
        full = f"{title}. {text}" if text else title
        rows[i]["document_sentiment3"] = predict_sentiment_document(full)
        if (k + 1) <= 3 or (k + 1) % 100 == 0:
            elapsed = time.time() - t0
            rate = (k + 1) / elapsed if elapsed > 0 else 0
            log.info(f"    Sentiment3: {k+1}/{len(indices)}  "
                     f"({rate:.1f} art/s)")
        if save_fn and (k + 1) % save_every == 0:
            save_fn()
            log.info(f"    checkpoint saved at {k+1}/{len(indices)}")
    log.info(f"    Sentiment3 done in {time.time()-t0:.0f}s")


# =====================================================================
# Step 4: Merge into original CSV
# =====================================================================
def merge_into_original(portal: str, supplement_path: Path):
    config = PORTALS[portal]
    if config["original_csv"] is None:
        standalone = ROOT_DIR / f"{portal}_document_level_with_preds.csv"
        _write_original_columns(supplement_path, standalone)
        log.info(f"  saved standalone: {standalone.name}")
        return

    original_path = ROOT_DIR / config["original_csv"]
    if not original_path.exists():
        log.warning(f"  {original_path.name} not found — skipping merge")
        return

    log.info(f"  merging → {original_path.name}")

    # Existing document_ids
    existing_ids = set()
    original_rows = 0
    with open(original_path, "r", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        original_fields = rd.fieldnames
        for row in rd:
            existing_ids.add(row.get("document_id", ""))
            original_rows += 1

    # Read supplement, filter dupes and unenriched
    new_rows = []
    skipped_dupes = 0
    skipped_empty = 0
    with open(supplement_path, "r", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for row in rd:
            if row.get("document_id", "") in existing_ids:
                skipped_dupes += 1
                continue
            if not row.get("document_ner"):
                skipped_empty += 1
                continue
            new_rows.append(row)

    if not new_rows:
        log.info(f"  no new rows (dupes={skipped_dupes}, "
                 f"unenriched={skipped_empty})")
        return

    # Backup
    backup = original_path.with_suffix(".csv.pre_merge_bak")
    if not backup.exists():
        shutil.copy2(original_path, backup)
        log.info(f"  backup: {backup.name}")

    # Append
    with open(original_path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=original_fields,
                                quoting=csv.QUOTE_ALL,
                                extrasaction="ignore")
        for row in new_rows:
            writer.writerow(row)

    log.info(f"  merged +{len(new_rows)} rows "
             f"({original_rows} → {original_rows + len(new_rows)})")
    if skipped_dupes:
        log.info(f"  skipped {skipped_dupes} duplicates")


def _write_original_columns(src: Path, dst: Path):
    with open(src, "r", encoding="utf-8") as f_in:
        rd = csv.DictReader(f_in)
        with open(dst, "w", encoding="utf-8", newline="") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=ORIGINAL_COLUMNS,
                                    quoting=csv.QUOTE_ALL,
                                    extrasaction="ignore")
            writer.writeheader()
            for row in rd:
                if row.get("document_ner"):
                    writer.writerow(row)


# =====================================================================
# Main pipeline
# =====================================================================
def enrich_portal(portal: str, step: str, limit: int = 0,
                  batch_size: int = 8):
    """
    Enrich one portal's supplement CSV.

    step="ner"      → only NER (fast, first pass)
    step="classify"  → only CAP+Sentiment on Ukraine articles (second pass)
    step="all"       → NER → filter → CAP+Sentiment → all in one go
    """
    config = PORTALS[portal]
    supplement_path = DATA_DIR / f"{portal}_supplement.csv"

    if not supplement_path.exists():
        log.warning(f"  {supplement_path.name} not found")
        return False

    fieldnames, rows = read_csv(supplement_path)
    total = len(rows)
    if total == 0:
        log.info(f"  {supplement_path.name}: empty")
        return False

    changed = False

    # Checkpoint save function — periodically writes progress to disk
    def save_checkpoint():
        nonlocal changed
        write_csv(supplement_path, fieldnames, rows)
        changed = False  # already saved

    # ── Step 1: NER ──
    if step in ("ner", "all"):
        needs_ner = [i for i, r in enumerate(rows)
                     if not r.get("document_ner")]
        if limit:
            needs_ner = needs_ner[:limit]

        if needs_ner:
            log.info(f"  Step 1: NER on {len(needs_ner)}/{total} rows")
            run_ner(rows, needs_ner, config["spacy_lang"],
                    save_fn=save_checkpoint)
            changed = True
        else:
            log.info(f"  Step 1: NER — all {total} rows already done")

    # Save after NER before moving on
    if changed:
        write_csv(supplement_path, fieldnames, rows)
        log.info(f"  saved after NER: {supplement_path.name}")
        changed = False

    # ── Step 2: Ukraine keyword filter ──
    ukraine_idx = find_ukraine_indices(rows)
    n_ukraine = len(ukraine_idx)
    log.info(f"  Step 2: Ukraine filter → {n_ukraine}/{total} articles "
             f"({n_ukraine/total*100:.1f}%) match keywords")

    # ── Step 3: CAP + Sentiment (only Ukraine articles) ──
    if step in ("classify", "all"):
        # Filter to Ukraine articles that still need classification
        needs_cap = [i for i in ukraine_idx
                     if not rows[i].get("document_cap_major_label")]
        needs_sent = [i for i in ukraine_idx
                      if not rows[i].get("document_sentiment3")]

        if limit:
            needs_cap = needs_cap[:limit]
            needs_sent = needs_sent[:limit]

        if needs_cap:
            log.info(f"  Step 3a: CAP Major on {len(needs_cap)} "
                     f"Ukraine articles")
            run_cap_major(rows, needs_cap, batch_size=batch_size,
                          save_fn=save_checkpoint)
            changed = True
        else:
            log.info(f"  Step 3a: CAP Major — all Ukraine articles done")

        # Save after CAP before Sentiment
        if changed:
            write_csv(supplement_path, fieldnames, rows)
            log.info(f"  saved after CAP: {supplement_path.name}")
            changed = False

        if needs_sent:
            log.info(f"  Step 3b: Sentiment3 on {len(needs_sent)} "
                     f"Ukraine articles")
            run_sentiment(rows, needs_sent, save_fn=save_checkpoint)
            changed = True
        else:
            log.info(f"  Step 3b: Sentiment3 — all Ukraine articles done")

    # ── Final save ──
    if changed:
        write_csv(supplement_path, fieldnames, rows)
        log.info(f"  saved {supplement_path.name}")

    return True


def main():
    ap = argparse.ArgumentParser(
        description="Enrich supplement CSVs and merge into originals")
    ap.add_argument("--portal", type=str, default="",
                    help="process only this portal")
    ap.add_argument("--step", type=str, default="all",
                    choices=["all", "ner", "classify"],
                    help="ner=NER only, classify=CAP+Sentiment only, "
                         "all=full pipeline")
    ap.add_argument("--limit", type=int, default=0,
                    help="max rows to process per portal (0 = all)")
    ap.add_argument("--batch-size", type=int, default=8,
                    help="batch size for transformer models")
    ap.add_argument("--skip-merge", action="store_true",
                    help="run enrichment but skip merging")
    ap.add_argument("--merge-only", action="store_true",
                    help="skip enrichment, only merge")
    ap.add_argument("--force", action="store_true",
                    help="run even if scraper still active")
    args = ap.parse_args()

    portals = [args.portal] if args.portal else list(PORTALS.keys())

    log.info("=" * 60)
    log.info("  MediaText CEE — Enrich & Merge Pipeline")
    log.info("=" * 60)

    for portal in portals:
        supplement = DATA_DIR / f"{portal}_supplement.csv"
        if not supplement.exists():
            continue

        # Check if scraper is still running
        if not args.force and not args.merge_only:
            if is_scraper_running(portal):
                log.info(f"  {portal}: scraper still running — skipping "
                         "(use --force to override)")
                continue

        log.info(f"\n{'='*40}")
        log.info(f"  Portal: {portal}")
        log.info(f"{'='*40}")

        # Enrichment
        if not args.merge_only:
            enrich_portal(portal, step=args.step, limit=args.limit,
                          batch_size=args.batch_size)

        # Merge
        if not args.skip_merge and args.step == "all":
            merge_into_original(portal, supplement)

    log.info("\n" + "=" * 60)
    log.info("  Pipeline complete")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
