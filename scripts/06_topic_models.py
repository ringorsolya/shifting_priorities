#!/usr/bin/env python3
"""
Step 6: Dynamic Topic Modelling on EFI / HFI articles split by sentiment.

For each portal × index (EFI/HFI) × sentiment (Positive/Negative):
  1. BERTopic clusters articles
  2. Each topic gets: keywords, representative article titles, auto-label
  3. Heatmap (topic × quarter) shows temporal dynamics
  4. Export JSON with full topic details for dashboard

Requirements:
    pip install bertopic sentence-transformers umap-learn hdbscan \
                scikit-learn plotly kaleido --break-system-packages

Usage:
    python3 06_topic_models.py                     # all portals
    python3 06_topic_models.py --portal novinky     # one portal
    python3 06_topic_models.py --top-n 8            # top N topics
    python3 06_topic_models.py --export-json         # export for dashboard
"""

import argparse
import csv
import json
import logging
import re
import datetime
from collections import defaultdict, Counter
from pathlib import Path

csv.field_size_limit(10 * 1024 * 1024)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ── Paths ──
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
DATA_DIR = ROOT_DIR / "data"
PLOTS_DIR = ROOT_DIR / "output" / "topic_plots"
DOCS_DIR = ROOT_DIR / "docs"

# ── Index definitions ──
EFI_CATS = {"macroeconomics", "energy"}
HFI_CATS = {"civil rights", "immigration", "social welfare"}

# ── Cleaning ──
HTML_ENT_RE = re.compile(r"&(?:amp|nbsp|quot|lt|gt|#\d+);?", re.IGNORECASE)
HTML_TAG_RE = re.compile(r"<[^>]+>")

# ── Portal config ──
PORTAL_CONFIG = {
    "mfdnes":       {"files": ["CZ_M_mfdnes_document_level_with_preds.csv"],
                     "suppl": "data/idnes_supplement.csv",
                     "label": "MF Dnes", "country": "CZ"},
    "novinky":      {"files": ["CZ_M_novinky_document_level_with_preds.csv"],
                     "suppl": "data/novinky_supplement.csv",
                     "label": "Novinky", "country": "CZ"},
    "magyarnemzet": {"files": ["HU_M_magyarnemzet_document_level_with_preds.csv"],
                     "suppl": "data/magyarnemzet_supplement.csv",
                     "label": "Magyar Nemzet", "country": "HU"},
    "telex":        {"files": [],
                     "suppl": "data/telex_supplement.csv",
                     "label": "Telex", "country": "HU"},
    "wpolityce":    {"files": ["PL_M_wpolityce_document_level_with_preds.csv"],
                     "suppl": "data/wpolityce_supplement.csv",
                     "label": "wPolityce", "country": "PL"},
    "onet":         {"files": [],
                     "suppl": "data/onet_supplement.csv",
                     "label": "Onet", "country": "PL"},
    "pravda":       {"files": ["SK_M_pravda_document_level_with_preds.csv"],
                     "suppl": "data/pravda_supplement.csv",
                     "label": "Pravda", "country": "SK"},
    "aktuality":    {"files": ["SK_M_aktuality_document_level_with_preds.csv"],
                     "suppl": "data/aktuality_supplement.csv",
                     "label": "Aktuality", "country": "SK"},
}


def clean_text(text: str) -> str:
    text = HTML_TAG_RE.sub(" ", text)
    text = HTML_ENT_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def load_portal_articles(portal_key: str) -> list[dict]:
    config = PORTAL_CONFIG[portal_key]
    rows = []
    for fname in config["files"]:
        fpath = ROOT_DIR / fname
        if not fpath.exists():
            continue
        with open(fpath) as f:
            for row in csv.DictReader(f):
                rows.append(row)
        log.info(f"  loaded {fpath.name}: {len(rows)} rows")
    suppl_path = ROOT_DIR / config["suppl"]
    if suppl_path.exists():
        before = len(rows)
        with open(suppl_path) as f:
            for row in csv.DictReader(f):
                rows.append(row)
        log.info(f"  loaded {suppl_path.name}: +{len(rows)-before} rows")
    return rows


def parse_date(s: str):
    if not s:
        return None
    try:
        return datetime.datetime.strptime(s[:10], "%Y-%m-%d")
    except (ValueError, IndexError):
        return None


def to_quarter(dt):
    return f"{dt.year}-Q{(dt.month-1)//3+1}"


def filter_articles(rows, index_cats, sentiment):
    """Return list of dicts with text, title, date for matching articles."""
    articles = []
    for row in rows:
        cap = (row.get("document_cap_major_label", "") or "").strip().lower()
        sent = (row.get("document_sentiment3", "") or "").strip()
        if cap in index_cats and sent == sentiment:
            text = clean_text((row.get("document_text", "") or ""))
            title = clean_text((row.get("document_title", "") or ""))
            dt = parse_date(row.get("date", ""))
            full = f"{title}. {text}" if text else title
            if len(full) > 30 and dt:
                articles.append({
                    "text": full,
                    "title": title,
                    "date": dt,
                    "quarter": to_quarter(dt),
                })
    return articles


def auto_label_from_titles(titles: list[str], keywords: list[str]) -> str:
    """Create a short human-readable label from representative titles."""
    # Take first 2 most informative keywords and combine with common
    # themes from titles
    if not titles:
        return " / ".join(keywords[:3])

    # Find common content words across titles (>4 chars, not stopwords)
    word_freq = Counter()
    stopwords = {"that", "this", "with", "from", "will", "have", "been",
                 "were", "they", "their", "which", "about", "could",
                 "would", "also", "than", "more", "after", "before"}
    for t in titles[:5]:
        words = set(w.lower() for w in re.findall(r'\b\w{4,}\b', t)
                    if w.lower() not in stopwords)
        for w in words:
            word_freq[w] += 1

    # Combine: top keywords + most shared title words
    common = [w for w, c in word_freq.most_common(5) if c >= 2]
    label_parts = keywords[:2]
    for w in common[:2]:
        if w.lower() not in [k.lower() for k in label_parts]:
            label_parts.append(w)

    return " / ".join(label_parts[:4])


def run_topic_model(articles, portal_label, index_name, sentiment,
                    top_n=8, min_topic_size=5):
    """Fit BERTopic, extract rich topic info with titles and heatmap data."""
    from bertopic import BERTopic
    from sentence_transformers import SentenceTransformer
    from sklearn.feature_extraction.text import CountVectorizer
    from umap import UMAP
    from hdbscan import HDBSCAN
    import numpy as np

    texts = [a["text"] for a in articles]
    titles = [a["title"] for a in articles]
    quarters = [a["quarter"] for a in articles]

    log.info(f"    BERTopic on {len(texts)} docs")

    embedding_model = SentenceTransformer(
        "paraphrase-multilingual-MiniLM-L12-v2")

    noise_words = ["amp", "nbsp", "quot", "http", "https", "www",
                   "html", "com", "cz", "hu", "pl", "sk"]
    vectorizer = CountVectorizer(
        max_features=5000, min_df=2, max_df=0.85,
        ngram_range=(1, 2), stop_words=noise_words)

    adaptive_min = max(min_topic_size, len(texts) // 80)

    umap_model = UMAP(n_neighbors=15, n_components=5,
                      min_dist=0.0, metric="cosine", random_state=42)
    hdbscan_model = HDBSCAN(min_cluster_size=adaptive_min,
                            min_samples=3, prediction_data=True)

    topic_model = BERTopic(
        embedding_model=embedding_model,
        vectorizer_model=vectorizer,
        umap_model=umap_model,
        hdbscan_model=hdbscan_model,
        nr_topics=max(top_n + 5, 15),
        verbose=False,
    )

    topic_assignments, probs = topic_model.fit_transform(texts)

    # ── Get topic info ──
    topic_info = topic_model.get_topic_info()
    topic_info = topic_info[topic_info["Topic"] != -1].head(top_n)
    top_ids = set(topic_info["Topic"].tolist())

    # ── Build per-topic details ──
    # Group articles by topic
    topic_articles = defaultdict(list)
    for i, tid in enumerate(topic_assignments):
        if tid in top_ids:
            topic_articles[tid].append(i)

    all_quarters = sorted(set(quarters))
    topic_results = []

    for _, row in topic_info.iterrows():
        tid = int(row["Topic"])
        topic_words_raw = topic_model.get_topic(tid)
        keywords = [w for w, _ in topic_words_raw[:8]]

        # Get representative titles (most central docs)
        indices = topic_articles[tid]
        topic_titles = [titles[i] for i in indices]

        # Pick diverse representative titles (first 5 unique-ish)
        seen = set()
        rep_titles = []
        for t in topic_titles:
            t_lower = t.lower()[:50]
            if t_lower not in seen and len(t) > 10:
                seen.add(t_lower)
                rep_titles.append(t)
                if len(rep_titles) >= 5:
                    break

        # Auto-generate label
        label = auto_label_from_titles(rep_titles, keywords)

        # Quarter distribution for this topic
        q_counts = Counter(quarters[i] for i in indices)
        heatmap_row = [q_counts.get(q, 0) for q in all_quarters]

        topic_results.append({
            "id": tid,
            "count": int(row["Count"]),
            "keywords": keywords,
            "label": label,
            "representative_titles": rep_titles,
            "quarter_counts": heatmap_row,
        })

    result = {
        "portal": portal_label,
        "index": index_name,
        "sentiment": sentiment,
        "n_docs": len(texts),
        "quarters": all_quarters,
        "topics": topic_results,
    }

    return result


def save_heatmap(result: dict, output_dir: Path):
    """Save a heatmap: topics (y) × quarters (x), color = article count."""
    import plotly.graph_objects as go
    import numpy as np

    topics = result["topics"]
    if not topics:
        return

    quarters = result["quarters"]
    # Labels: auto-label (truncated for readability)
    labels = [t["label"][:50] for t in topics]
    z = [t["quarter_counts"] for t in topics]

    # Normalize rows to % for comparable colors across topics
    z_pct = []
    for row in z:
        total = sum(row)
        z_pct.append([round(v/total*100, 1) if total > 0 else 0 for v in row])

    color = "Reds" if result["sentiment"] == "Negative" else "Greens"

    fig = go.Figure(go.Heatmap(
        z=z_pct, x=quarters, y=labels,
        colorscale=color,
        text=[[str(v) if v > 0 else "" for v in row] for row in z],
        texttemplate="%{text}",
        textfont=dict(size=10),
        hovertemplate="Topic: %{y}<br>Quarter: %{x}<br>"
                      "Articles: %{text}<br>Share: %{z:.1f}%<extra></extra>",
        colorbar=dict(title="% of topic"),
    ))

    portal = result["portal"]
    idx = result["index"]
    sent = result["sentiment"]

    fig.update_layout(
        title=dict(text=f"{portal} — {idx} — {sent}  (n={result['n_docs']})",
                   font=dict(size=14)),
        xaxis=dict(title="Quarter", tickangle=-45, side="bottom",
                   tickfont=dict(size=10)),
        yaxis=dict(automargin=True, tickfont=dict(size=11)),
        margin=dict(l=20, r=80, t=50, b=80),
        height=max(350, len(topics) * 45 + 150),
        width=max(600, len(quarters) * 55 + 250),
        plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
        font=dict(color="#c9d1d9"),
    )

    safe = (f"heatmap_{portal}_{idx}_{sent}".lower()
            .replace(" ", "_").replace("/", "_"))
    fig.write_image(str(output_dir / f"{safe}.png"), scale=2)
    log.info(f"    saved {safe}.png")


def save_topic_detail_text(result: dict, output_dir: Path):
    """Save a readable text summary of topics with titles."""
    portal = result["portal"]
    idx = result["index"]
    sent = result["sentiment"]

    lines = [f"{'='*60}",
             f"  {portal} — {idx} — {sent}  (n={result['n_docs']})",
             f"{'='*60}", ""]

    for i, t in enumerate(result["topics"], 1):
        lines.append(f"  Topic {i}: {t['label']}")
        lines.append(f"  Articles: {t['count']}  "
                      f"({t['count']/result['n_docs']*100:.1f}%)")
        lines.append(f"  Keywords: {', '.join(t['keywords'][:6])}")
        lines.append(f"  Representative articles:")
        for title in t["representative_titles"]:
            lines.append(f"    • {title}")
        lines.append("")

    safe = (f"topics_{portal}_{idx}_{sent}".lower()
            .replace(" ", "_").replace("/", "_"))
    out_path = output_dir / f"{safe}.txt"
    out_path.write_text("\n".join(lines), encoding="utf-8")
    log.info(f"    saved {safe}.txt")


def main():
    ap = argparse.ArgumentParser(
        description="Topic modelling: EFI/HFI × Positive/Negative per portal")
    ap.add_argument("--portal", type=str, default="")
    ap.add_argument("--min-docs", type=int, default=30)
    ap.add_argument("--top-n", type=int, default=8)
    ap.add_argument("--export-json", action="store_true")
    args = ap.parse_args()

    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    portals = [args.portal] if args.portal else list(PORTAL_CONFIG.keys())
    all_results = []

    for portal_key in portals:
        config = PORTAL_CONFIG[portal_key]
        label = config["label"]

        log.info(f"\n{'='*50}")
        log.info(f"  Portal: {label}")
        log.info(f"{'='*50}")

        rows = load_portal_articles(portal_key)
        if not rows:
            log.warning(f"  no data for {label}")
            continue

        for index_name, index_cats in [("EFI", EFI_CATS), ("HFI", HFI_CATS)]:
            for sentiment in ["Positive", "Negative"]:
                articles = filter_articles(rows, index_cats, sentiment)
                log.info(f"  {index_name}/{sentiment}: {len(articles)} docs")

                if len(articles) < args.min_docs:
                    log.info(f"    skipping (< {args.min_docs} docs)")
                    continue

                quarters = sorted(set(a["quarter"] for a in articles))
                log.info(f"    quarters: {quarters[0]} → {quarters[-1]}")

                try:
                    result = run_topic_model(
                        articles, label, index_name, sentiment,
                        top_n=args.top_n)
                    all_results.append(result)

                    save_heatmap(result, PLOTS_DIR)
                    save_topic_detail_text(result, PLOTS_DIR)

                except Exception as e:
                    log.error(f"    failed: {e}")
                    import traceback
                    traceback.print_exc()
                    continue

    # ── Export JSON (merge with existing) ──
    if args.export_json and all_results:
        out_path = DOCS_DIR / "topics.json"
        existing = []
        if out_path.exists():
            try:
                with open(out_path) as f:
                    existing = json.load(f)
            except (json.JSONDecodeError, ValueError):
                existing = []

        processed_keys = {(r["portal"], r["index"], r["sentiment"])
                          for r in all_results}
        merged = [e for e in existing
                  if (e["portal"], e["index"], e["sentiment"])
                  not in processed_keys]
        merged.extend(all_results)
        merged.sort(key=lambda r: (r["portal"], r["index"], r["sentiment"]))

        with open(out_path, "w") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)
        log.info(f"\nExported {len(merged)} topic models → {out_path}")

    # ── Summary ──
    log.info(f"\n{'='*50}")
    log.info(f"  Done: {len(all_results)} subsets")
    log.info(f"  Plots: {PLOTS_DIR}")
    log.info(f"{'='*50}")

    for r in all_results:
        print(f"\n{r['portal']} — {r['index']} — {r['sentiment']} "
              f"(n={r['n_docs']})")
        for t in r["topics"][:3]:
            print(f"  [{t['count']}] {t['label']}")
            if t["representative_titles"]:
                print(f"       e.g. \"{t['representative_titles'][0][:70]}\"")


if __name__ == "__main__":
    main()
