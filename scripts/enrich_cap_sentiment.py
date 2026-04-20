#!/usr/bin/env python3
"""
CAP topic classification + Sentiment enrichment for supplement CSVs.

Follows the MediaText CEE methodology exactly:

  CAP Major:  poltextlab/xlm-roberta-large-pooled-cap-v4
              → applied to original-language document_text
              → outputs: document_cap_major_code, document_cap_major_label

  CAP Media2: poltextlab/xlm-roberta-large-english-cap-media2-v17
              → applied to English translation of document_text
              → requires Helsinki-NLP/opus-mt-{src}-en translation first
              → outputs: document_cap_media2_code, document_cap_media2_label

  Sentiment3: poltextlab/xlm-roberta-large-pooled-sentiment-v2
              → applied to original-language text at sentence level
              → aggregated to document level using neutrality-weighted polarity
              → thresholds: < -0.1 = Negative, -0.1..0.1 = Neutral, > 0.1 = Positive
              → outputs: document_sentiment3

Usage:
    python3 enrich_cap_sentiment.py                        # all portals, all tasks
    python3 enrich_cap_sentiment.py --portal novinky       # one portal
    python3 enrich_cap_sentiment.py --task cap_major       # only CAP Major
    python3 enrich_cap_sentiment.py --task sentiment       # only Sentiment
    python3 enrich_cap_sentiment.py --task cap_media2      # only CAP Media2
    python3 enrich_cap_sentiment.py --limit 50             # test on 50 rows
    python3 enrich_cap_sentiment.py --batch-size 8         # smaller batches (less RAM)

Requirements:
    pip install transformers torch sentencepiece spacy --break-system-packages
"""

import argparse
import csv
import logging
import time
from pathlib import Path

import torch

csv.field_size_limit(10 * 1024 * 1024)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# ── Portal metadata ──
PORTAL_LANG = {
    "novinky":       "cs",
    "idnes":         "cs",
    "pravda":        "sk",
    "aktuality":     "sk",
    "telex":         "hu",
    "magyarnemzet":  "hu",
    "onet":          "pl",
    "wpolityce":     "pl",
}

# ── CAP Major codebook (code → label) ──
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

# ── CAP Media2 codebook (code → label) ──
CAP_MEDIA2_CODES = {
    1: "Macroeconomics", 2: "Civil Rights", 3: "Health", 4: "Agriculture",
    5: "Labor", 6: "Education", 7: "Environment", 8: "Energy",
    9: "Immigration", 10: "Transportation", 12: "Law and Crime",
    13: "Social Welfare", 14: "Housing",
    15: "Banking, Finance, and Domestic Commerce", 16: "Defense",
    17: "Technology", 18: "Foreign Trade", 19: "International Affairs",
    20: "Government Operations", 21: "Public Lands", 23: "Culture",
    24: "State and Local Government Administration", 25: "Weather",
    26: "Fires, emergencies and natural disasters", 27: "Crime and trials",
    28: "Arts, culture, entertainment and history", 29: "Style and fashion",
    30: "Food", 31: "Travel", 32: "Wellbeing and learning",
    33: "Personal finance and real estate",
    34: "Personal technology and popular science",
    35: "Churches and Religion", 36: "Celebrities and human interest",
    37: "Obituaries and death notices", 38: "Sports",
    39: "Crosswords, puzzles, comics",
    40: "Media production/internal, letters", 41: "Advertisements",
    998: "No Policy and No Media Content",
}

# Sentiment label mapping
SENTIMENT_LABELS = {0: "Negative", 1: "Neutral", 2: "Positive"}

# Helsinki-NLP translation model names per source language
TRANSLATION_MODELS = {
    "cs": "Helsinki-NLP/opus-mt-cs-en",
    "sk": "Helsinki-NLP/opus-mt-sk-en",  # May not exist; fallback to tc-big-en
    "hu": "Helsinki-NLP/opus-mt-hu-en",
    "pl": "Helsinki-NLP/opus-mt-pl-en",
}

# ── Global model cache ──
_cache = {}


def get_device():
    """Pick best available device."""
    if torch.cuda.is_available():
        return torch.device("cuda")
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


def truncate_text(text: str, max_tokens: int = 500) -> str:
    """Rough truncation by whitespace tokens to stay within model limits."""
    words = text.split()
    if len(words) <= max_tokens:
        return text
    return " ".join(words[:max_tokens])


# ─────────────────────────────────────────────────────────────────
# CAP Major: poltextlab/xlm-roberta-large-pooled-cap-v4
# ─────────────────────────────────────────────────────────────────
def load_cap_major():
    if "cap_major" in _cache:
        return _cache["cap_major"]
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    name = "poltextlab/xlm-roberta-large-pooled-cap-v4"
    log.info(f"  loading CAP Major model: {name}")
    tok = AutoTokenizer.from_pretrained(name)
    model = AutoModelForSequenceClassification.from_pretrained(name)
    device = get_device()
    model.to(device).eval()
    _cache["cap_major"] = (tok, model, device)
    return tok, model, device


def predict_cap_major(texts: list[str], batch_size: int = 8) -> list[tuple[str, str]]:
    """Return list of (code_str, label) for each text."""
    tok, model, device = load_cap_major()
    results = []
    id2label = model.config.id2label  # model's own label mapping

    for i in range(0, len(texts), batch_size):
        batch = [truncate_text(t) for t in texts[i:i+batch_size]]
        inputs = tok(batch, padding=True, truncation=True, max_length=512,
                     return_tensors="pt").to(device)
        with torch.no_grad():
            logits = model(**inputs).logits
        preds = logits.argmax(dim=-1).cpu().tolist()

        for p in preds:
            # The model's id2label maps internal index → label string
            label_str = id2label.get(p, str(p))
            # Try to parse as CAP code
            try:
                code = int(label_str)
                label = CAP_MAJOR_CODES.get(code, label_str)
            except ValueError:
                # label_str is already a text label
                label = label_str
                # Reverse-lookup code
                code = next((k for k, v in CAP_MAJOR_CODES.items()
                            if v == label_str), 999)
            results.append((str(code), label))

    return results


# ─────────────────────────────────────────────────────────────────
# CAP Media2: poltextlab/xlm-roberta-large-english-cap-media2-v17
# (requires English translation)
# ─────────────────────────────────────────────────────────────────
def load_cap_media2():
    if "cap_media2" in _cache:
        return _cache["cap_media2"]
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    name = "poltextlab/xlm-roberta-large-english-cap-media2-v17"
    log.info(f"  loading CAP Media2 model: {name}")
    tok = AutoTokenizer.from_pretrained(name)
    model = AutoModelForSequenceClassification.from_pretrained(name)
    device = get_device()
    model.to(device).eval()
    _cache["cap_media2"] = (tok, model, device)
    return tok, model, device


def load_translator(src_lang: str):
    key = f"translator_{src_lang}"
    if key in _cache:
        return _cache[key]
    from transformers import MarianMTModel, MarianTokenizer
    model_name = TRANSLATION_MODELS.get(src_lang)
    if not model_name:
        log.warning(f"  no translation model for {src_lang}")
        return None, None
    log.info(f"  loading translation model: {model_name}")
    try:
        tok = MarianTokenizer.from_pretrained(model_name)
        model = MarianMTModel.from_pretrained(model_name)
        device = get_device()
        # MarianMT doesn't always work well on MPS, keep on CPU
        if device.type == "mps":
            device = torch.device("cpu")
        model.to(device).eval()
        _cache[key] = (tok, model, device)
        return tok, model, device
    except Exception as e:
        log.warning(f"  could not load {model_name}: {e}")
        _cache[key] = (None, None, None)
        return None, None, None


def translate_batch(texts: list[str], src_lang: str,
                    batch_size: int = 8) -> list[str]:
    """Translate a batch of texts to English using Helsinki-NLP opus-mt."""
    result = load_translator(src_lang)
    if result is None or result[0] is None:
        log.warning(f"  translation not available for {src_lang} — "
                    "using original text")
        return texts

    tok, model, device = result
    translated = []
    for i in range(0, len(texts), batch_size):
        batch = [truncate_text(t, 400) for t in texts[i:i+batch_size]]
        inputs = tok(batch, padding=True, truncation=True, max_length=512,
                     return_tensors="pt").to(device)
        with torch.no_grad():
            gen = model.generate(**inputs, max_new_tokens=512)
        decoded = tok.batch_decode(gen, skip_special_tokens=True)
        translated.extend(decoded)
    return translated


def predict_cap_media2(texts_en: list[str],
                       batch_size: int = 8) -> list[tuple[str, str]]:
    """Return list of (code_str, label) for each English text."""
    tok, model, device = load_cap_media2()
    results = []
    id2label = model.config.id2label

    for i in range(0, len(texts_en), batch_size):
        batch = [truncate_text(t) for t in texts_en[i:i+batch_size]]
        inputs = tok(batch, padding=True, truncation=True, max_length=512,
                     return_tensors="pt").to(device)
        with torch.no_grad():
            logits = model(**inputs).logits
        preds = logits.argmax(dim=-1).cpu().tolist()

        for p in preds:
            label_str = id2label.get(p, str(p))
            try:
                code = int(label_str)
                label = CAP_MEDIA2_CODES.get(code, label_str)
            except ValueError:
                label = label_str
                code = next((k for k, v in CAP_MEDIA2_CODES.items()
                            if v == label_str), 998)
            results.append((str(code), label))

    return results


# ─────────────────────────────────────────────────────────────────
# Sentiment3: poltextlab/xlm-roberta-large-pooled-sentiment-v2
# Applied at sentence level, aggregated to document via
# neutrality-weighted polarity.
# ─────────────────────────────────────────────────────────────────
def load_sentiment():
    if "sentiment" in _cache:
        return _cache["sentiment"]
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    name = "poltextlab/xlm-roberta-large-pooled-sentiment-v2"
    log.info(f"  loading Sentiment3 model: {name}")
    tok = AutoTokenizer.from_pretrained(name)
    model = AutoModelForSequenceClassification.from_pretrained(name)
    device = get_device()
    model.to(device).eval()
    _cache["sentiment"] = (tok, model, device)
    return tok, model, device


def split_sentences(text: str) -> list[str]:
    """Split text into sentences using spaCy Sentencizer (language-agnostic)."""
    if "sentencizer" not in _cache:
        import spacy
        nlp = spacy.blank("xx")
        nlp.add_pipe("sentencizer")
        nlp.max_length = 2_000_000
        _cache["sentencizer"] = nlp
    nlp = _cache["sentencizer"]
    doc = nlp(text)
    return [s.text.strip() for s in doc.sents if s.text.strip()]


def predict_sentiment_document(text: str, batch_size: int = 16) -> str:
    """
    Predict document-level sentiment following MediaText methodology:
    1. Split into sentences
    2. Predict sentence-level sentiment probabilities
    3. Compute neutrality-weighted polarity:
       polarity_i = (P_pos - P_neg)
       weight_i = (1 - P_neu)
       doc_polarity = weighted_mean(polarity_i, weight_i)
    4. Classify: < -0.1 → Negative, > 0.1 → Positive, else Neutral
    """
    tok, model, device = load_sentiment()
    sentences = split_sentences(text)

    if not sentences:
        return "Neutral"

    # Predict probabilities for each sentence
    all_probs = []
    for i in range(0, len(sentences), batch_size):
        batch = sentences[i:i+batch_size]
        inputs = tok(batch, padding=True, truncation=True, max_length=512,
                     return_tensors="pt").to(device)
        with torch.no_grad():
            logits = model(**inputs).logits
            probs = torch.softmax(logits, dim=-1).cpu()
        all_probs.append(probs)

    all_probs = torch.cat(all_probs, dim=0)  # shape: (n_sentences, 3)

    # Model output order: 0=Negative, 1=Neutral, 2=Positive
    p_neg = all_probs[:, 0]
    p_neu = all_probs[:, 1]
    p_pos = all_probs[:, 2]

    # Neutrality-weighted polarity
    polarity = p_pos - p_neg         # per-sentence polarity
    weights = 1.0 - p_neu            # weight: affective sentences count more
    weight_sum = weights.sum()

    if weight_sum > 0:
        doc_polarity = (polarity * weights).sum() / weight_sum
    else:
        doc_polarity = polarity.mean()

    doc_polarity = doc_polarity.item()

    if doc_polarity < -0.1:
        return "Negative"
    elif doc_polarity > 0.1:
        return "Positive"
    else:
        return "Neutral"


# ─────────────────────────────────────────────────────────────────
# Main processing loop
# ─────────────────────────────────────────────────────────────────
def process_csv(csv_path: Path, portal: str, tasks: list[str],
                limit: int = 0, batch_size: int = 8):
    """Process a supplement CSV for the specified tasks."""
    lang = PORTAL_LANG[portal]

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    total = len(rows)
    log.info(f"  {csv_path.name}: {total} rows")

    # Determine which rows need which task
    def needs(row, task):
        if task == "cap_major":
            return not row.get("document_cap_major_label")
        elif task == "cap_media2":
            return not row.get("document_cap_media2_label")
        elif task == "sentiment":
            return not row.get("document_sentiment3")
        return False

    # Collect rows that need processing
    work_indices = []
    for i, row in enumerate(rows):
        if any(needs(row, t) for t in tasks):
            work_indices.append(i)
            if limit and len(work_indices) >= limit:
                break

    if not work_indices:
        log.info("  nothing to do — all fields already filled")
        return

    log.info(f"  {len(work_indices)} rows to process for tasks: {tasks}")

    # Extract texts for these rows
    texts = []
    for i in work_indices:
        text = rows[i].get("document_text", "") or ""
        title = rows[i].get("document_title", "") or ""
        texts.append(f"{title}. {text}" if text else title)

    t0 = time.time()

    # ── CAP Major ──
    if "cap_major" in tasks:
        log.info("  → running CAP Major classification...")
        cap_results = predict_cap_major(texts, batch_size=batch_size)
        for j, i in enumerate(work_indices):
            rows[i]["document_cap_major_code"] = cap_results[j][0]
            rows[i]["document_cap_major_label"] = cap_results[j][1]
        log.info(f"    done ({time.time()-t0:.0f}s)")

    # ── Sentiment3 ──
    if "sentiment" in tasks:
        t2 = time.time()
        log.info("  → running Sentiment3 (sentence-level → document)...")
        for k, i in enumerate(work_indices):
            rows[i]["document_sentiment3"] = predict_sentiment_document(texts[k])
            if (k + 1) <= 3 or (k + 1) % 100 == 0:
                elapsed = time.time() - t2
                rate = (k + 1) / elapsed if elapsed > 0 else 0
                log.info(f"    sentiment: {k+1}/{len(work_indices)}  "
                         f"rate={rate:.1f} art/s")
        log.info(f"    done ({time.time()-t2:.0f}s)")

    # ── Write results ──
    tmp_path = csv_path.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8", newline="") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=fieldnames,
                                quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    backup_path = csv_path.with_suffix(".csv.bak")
    if backup_path.exists():
        backup_path.unlink()
    csv_path.rename(backup_path)
    tmp_path.rename(csv_path)

    elapsed = time.time() - t0
    log.info(f"  all tasks done in {elapsed:.0f}s  "
             f"({len(work_indices)} rows enriched)")
    log.info(f"  backup: {backup_path.name}")


def main():
    ap = argparse.ArgumentParser(
        description="CAP + Sentiment enrichment for supplement CSVs")
    ap.add_argument("--portal", type=str, default="",
                    help="process only this portal")
    ap.add_argument("--task", type=str, default="all",
                    choices=["all", "cap_major", "sentiment"],
                    help="which task(s) to run")
    ap.add_argument("--limit", type=int, default=0,
                    help="max rows to process per file (0 = all)")
    ap.add_argument("--batch-size", type=int, default=8,
                    help="batch size for transformer models")
    ap.add_argument("--data-dir", type=str, default="")
    args = ap.parse_args()

    data_dir = Path(args.data_dir) if args.data_dir else DATA_DIR

    if args.task == "all":
        tasks = ["cap_major", "sentiment"]
    else:
        tasks = [args.task]

    portals = [args.portal] if args.portal else list(PORTAL_LANG.keys())

    for portal in portals:
        csv_path = data_dir / f"{portal}_supplement.csv"
        if not csv_path.exists():
            log.warning(f"  {csv_path} not found — skipping")
            continue

        log.info(f"Processing {portal} ({csv_path.name})")
        try:
            process_csv(csv_path, portal, tasks, limit=args.limit,
                        batch_size=args.batch_size)
        except Exception as e:
            log.error(f"  ERROR processing {portal}: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()
