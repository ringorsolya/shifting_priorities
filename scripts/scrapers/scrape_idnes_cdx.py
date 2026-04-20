"""
iDnes.cz / MF Dnes scraper — CDX discovery + Wayback snapshot extraction.

iDnes serves a GDPR consent wall that blocks all article content for
automated fetchers (no cookies → no body). However:
  1. The Wayback Machine captured full article HTML before/without the wall.
  2. CDX API yields ~500 article URLs per section per year with collapse=urlkey.
  3. Wayback snapshots (web.archive.org/web/<ts>id_/<url>) contain the full
     article text inside <div class="art-full"> (and sometimes "bbtext").

Strategy:
  - Discover article URLs via CDX (section-by-section, chunk_days=14).
  - For each URL, fetch the Wayback snapshot HTML (NOT the live site).
  - Extract title from <h1> or og:title, date from article:published_time,
    body from <div class="art-full"> <p> tags.

Covers the gap 2024-04-04 → 2026-02-23 (or any date range).
portal="MF Dnes", illiberal="illiberal".

USAGE
-----
    python scrape_idnes_cdx.py --start 2024-04-04 --end 2026-02-23 \\
        --out ../../data/idnes_supplement.csv \\
        --cdx-cache ../../data/idnes_cdx_urls.jsonl
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from bs4 import BeautifulSoup

from scraper_utils import (
    Article, Checkpoint, DEFAULT_UA,
    clean_text, first_sentence, log,
    make_document_id, make_session, parse_date_str,
    polite_get, wayback_cdx_urls,
)

PORTAL_NAME = "MF Dnes"
PORTAL_CODE = "CZ_idnes"
ILLIBERAL = "illiberal"
DEFAULT_HOST = "www.idnes.cz"

# CDX sections — news & politics where Ukraine-war coverage lives.
DEFAULT_SECTIONS = [
    "zpravy/domaci",
    "zpravy/zahranicni",
    "ekonomika",
]

# ─────────────────────────────────────────────────────────────────
# URL filtering
# ─────────────────────────────────────────────────────────────────
# iDnes article URLs: /zpravy/<section>/<slug>.A<YYMMDD>_<HHMMSS>_<section>_<initials>
_ARTICLE_RE = re.compile(r"\.A\d{6}_\d{6}_", re.IGNORECASE)

_BAD_SUFFIXES = ("/diskuse", "/tisk", "/foto", "/video", "/galerie",
                 "/komentare", "/anketa", "/nazory")
_BAD_PARTS = (
    "/autor/", "/galerie/", "/video/", "/hry/", "/bydleni/",
    "/xman/", "/hobby/", "/horoskopy/", "/revue/", "/cestovani/",
    "/technet/", "/sport/", "/fotbal/", "/hokej/",
)


def looks_like_article_url(url: str) -> bool:
    lower = url.lower().split("?", 1)[0].split("#", 1)[0]
    if "idnes.cz" not in lower:
        return False
    if any(lower.endswith(s) for s in _BAD_SUFFIXES):
        return False
    if any(b in lower for b in _BAD_PARTS):
        return False
    return bool(_ARTICLE_RE.search(lower))


def normalize_url(url: str) -> str:
    url = url.strip()
    url = url.split("?", 1)[0].split("#", 1)[0]
    # Strip /diskuse, /tisk suffixes
    for suffix in _BAD_SUFFIXES:
        if url.lower().endswith(suffix):
            url = url[:len(url) - len(suffix)]
    if url.startswith("http://"):
        url = "https://" + url[len("http://"):]
    return url.rstrip("/")


# ─────────────────────────────────────────────────────────────────
# iDnes article extractor (for Wayback HTML)
# ─────────────────────────────────────────────────────────────────
def extract_idnes_article(html: str) -> dict:
    """
    Parse iDnes article from Wayback snapshot HTML.

    Body location hierarchy:
      1. <div class="art-full"> (most common)
      2. <div class="bbtext"> (alternative layout)
      3. <div id="art-text"> (older layout)
    """
    soup = BeautifulSoup(html, "lxml")

    def og(prop: str) -> str:
        tag = soup.find("meta", property=prop)
        return clean_text(tag["content"]) if tag and tag.get("content") else ""

    def meta(name: str) -> str:
        tag = (soup.find("meta", attrs={"property": name})
               or soup.find("meta", attrs={"name": name}))
        return tag["content"].strip() if tag and tag.get("content") else ""

    headline = og("og:title")
    # Strip " - iDNES.cz" suffix from title
    if headline and " - iDNES.cz" in headline:
        headline = headline.rsplit(" - iDNES.cz", 1)[0].strip()

    description = og("og:description")
    date_published = meta("article:published_time")

    # Body extraction — try multiple selectors
    body = ""
    # Paywall/premium markers to filter out
    paywall_markers = [
        "Získejte všechny článk",  # "Get all articles"
        "Premium bez reklam",
        "jen za",                  # "only for XX CZK"
        "Přihlaste se",           # "Log in"
        "Už máte účet",           # "Already have account"
    ]

    def is_paywall_paragraph(text: str) -> bool:
        return any(m in text for m in paywall_markers)

    for selector in [".art-full", ".bbtext", "#art-text", ".opener"]:
        el = soup.select_one(selector)
        if not el:
            continue
        paragraphs = []
        for p in el.find_all("p"):
            text = p.get_text(strip=True)
            if text and not is_paywall_paragraph(text):
                paragraphs.append(text)
        candidate = clean_text("\n".join(paragraphs))
        if len(candidate) > len(body):
            body = candidate

    # Fallback: if no named container, try <article>
    if len(body) < 100:
        article_el = soup.find("article")
        if article_el:
            paragraphs = []
            for p in article_el.find_all("p"):
                text = p.get_text(strip=True)
                if text and not is_paywall_paragraph(text):
                    paragraphs.append(text)
            candidate = clean_text("\n".join(paragraphs))
            if len(candidate) > len(body):
                body = candidate

    return {
        "headline": headline,
        "articleBody": body,
        "description": description,
        "datePublished": date_published,
        "isAccessibleForFree": True,
    }


# ─────────────────────────────────────────────────────────────────
# CDX cache (resumable)
# ─────────────────────────────────────────────────────────────────
def load_cdx_cache(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    log.info(f"  loaded CDX cache: {len(out)} URLs from {path}")
    return out


def save_cdx_entry(path: Path, entry: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


# ─────────────────────────────────────────────────────────────────
# Article build
# ─────────────────────────────────────────────────────────────────
def build_article(url: str, parsed: dict) -> Article | None:
    title = parsed.get("headline") or ""
    body = parsed.get("articleBody") or ""
    date_str = parsed.get("datePublished") or ""
    d = parse_date_str(date_str)
    iso_date = d.isoformat() if d else ""

    if not (title and body and iso_date):
        return None

    return Article(
        document_id=make_document_id(PORTAL_CODE, iso_date, url),
        document_title=title,
        first_sentence=first_sentence(body),
        document_text=body,
        date=iso_date,
        portal=PORTAL_NAME,
        illiberal=ILLIBERAL,
        url=url,
        scraped_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )


# ─────────────────────────────────────────────────────────────────
# Wayback snapshot fetcher
# ─────────────────────────────────────────────────────────────────
def fetch_wayback_snapshot(session, url: str, timestamp: str) -> str | None:
    """Fetch the raw archived HTML from the Wayback Machine.
    Uses the `id_` flag to get the original page without Wayback toolbar.
    """
    wb_url = f"https://web.archive.org/web/{timestamp}id_/{url}"
    try:
        resp = session.get(wb_url, timeout=30)
        if resp.status_code == 200:
            return resp.text
        log.debug(f"  Wayback {resp.status_code} for {wb_url}")
    except Exception as e:
        log.debug(f"  Wayback error: {e}")
    return None


# ─────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        description="Scrape iDnes.cz via Wayback Machine CDX + snapshots."
    )
    ap.add_argument("--start", required=True, help="start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="end date YYYY-MM-DD (inclusive)")
    ap.add_argument("--out", required=True, help="output CSV path")
    ap.add_argument("--host", default=DEFAULT_HOST,
                    help=f"iDnes host (default: {DEFAULT_HOST})")
    ap.add_argument("--cdx-cache", default="",
                    help="JSONL path to cache CDX results (resumable)")
    ap.add_argument("--limit", type=int, default=0,
                    help="max articles to scrape (0 = no limit)")
    ap.add_argument("--skip-cdx", action="store_true",
                    help="skip CDX query, use only --cdx-cache for URLs")
    ap.add_argument("--sections", default=",".join(DEFAULT_SECTIONS),
                    help="comma-separated section prefixes")
    ap.add_argument("--chunk-days", type=int, default=14,
                    help="CDX time-window size in days (default: 14)")
    args = ap.parse_args()

    date_from = datetime.strptime(args.start, "%Y-%m-%d").date()
    date_to = datetime.strptime(args.end, "%Y-%m-%d").date()
    out_path = Path(args.out)
    cdx_cache_path = Path(args.cdx_cache) if args.cdx_cache else None

    log.info(f"  iDnes CDX+Wayback scraper — {date_from} → {date_to}")
    log.info(f"  host: {args.host}")
    log.info(f"  output: {out_path}")
    if cdx_cache_path:
        log.info(f"  CDX cache: {cdx_cache_path}")

    session = make_session(DEFAULT_UA)

    # ── 1. URL discovery (CDX + optional cache) ──
    all_entries: list[dict] = []
    seen_cdx_urls: set[str] = set()

    if cdx_cache_path and cdx_cache_path.exists():
        for e in load_cdx_cache(cdx_cache_path):
            u = normalize_url(e.get("url", ""))
            if u and u not in seen_cdx_urls:
                seen_cdx_urls.add(u)
                all_entries.append({"url": u, "timestamp": e.get("timestamp", "")})

    if not args.skip_cdx:
        log.info("  starting CDX URL discovery...")
        sections = [s.strip() for s in args.sections.split(",") if s.strip()]
        if sections:
            patterns = [f"{args.host}/{s}/*" for s in sections]
        else:
            patterns = [f"{args.host}/*"]
        log.info(f"  querying {len(patterns)} CDX pattern(s), "
                 f"chunk_days={args.chunk_days}")

        new_count = 0
        try:
            for pattern in patterns:
                log.info(f"  pattern: {pattern}")
                for entry in wayback_cdx_urls(
                    session, pattern, date_from, date_to,
                    month_chunks=False,
                    chunk_days=args.chunk_days,
                ):
                    u = normalize_url(entry.get("url", ""))
                    if not u or u in seen_cdx_urls:
                        continue
                    seen_cdx_urls.add(u)
                    rec = {"url": u, "timestamp": entry.get("timestamp", "")}
                    all_entries.append(rec)
                    new_count += 1
                    if cdx_cache_path:
                        save_cdx_entry(cdx_cache_path, rec)
        except KeyboardInterrupt:
            log.warning("  CDX discovery interrupted — proceeding with what we have")
        log.info(f"  CDX discovery finished: {new_count} new URLs, "
                 f"{len(all_entries)} total")

    if not all_entries:
        log.error("  no URLs discovered — aborting")
        return

    # ── 2. Filter to article-looking URLs, dedupe ──
    article_entries: list[dict] = []
    seen_norm: set[str] = set()
    for entry in all_entries:
        url = normalize_url(entry["url"])
        if not looks_like_article_url(url):
            continue
        if url in seen_norm:
            continue
        seen_norm.add(url)
        article_entries.append({"url": url, "timestamp": entry["timestamp"]})
    log.info(f"  candidate article URLs after filtering: {len(article_entries)}")

    # ── 3. Checkpoint & scrape loop (via Wayback snapshots) ──
    checkpoint = Checkpoint(out_path)
    checkpoint.open()

    scraped = 0
    skipped = 0
    rejected = 0
    no_snapshot = 0

    try:
        for entry in article_entries:
            url = entry["url"]
            timestamp = entry["timestamp"]

            if url in checkpoint.seen_urls:
                skipped += 1
                continue

            # Fetch Wayback snapshot (NOT live iDnes — consent wall)
            html = fetch_wayback_snapshot(session, url, timestamp)
            if html is None:
                no_snapshot += 1
                continue

            parsed = extract_idnes_article(html)
            if not parsed.get("headline"):
                rejected += 1
                continue

            article = build_article(url, parsed)
            if article is None:
                rejected += 1
                continue

            # Verify date lies inside the requested window
            d = parse_date_str(article.date)
            if d and not (date_from <= d <= date_to):
                skipped += 1
                continue

            checkpoint.write(article)
            scraped += 1
            if scraped % 25 == 0:
                log.info(
                    f"  scraped={scraped}  skipped={skipped}  "
                    f"no_snapshot={no_snapshot}  rejected={rejected}"
                )

            if args.limit and scraped >= args.limit:
                log.info(f"  reached --limit {args.limit}, stopping")
                break

            # Be polite to the Wayback Machine
            time.sleep(1.5)
    finally:
        checkpoint.close()

    log.info(
        f"  done.  scraped={scraped}  skipped={skipped}  "
        f"no_snapshot={no_snapshot}  rejected={rejected}"
    )
    log.info(f"  CSV written to: {out_path}")


if __name__ == "__main__":
    main()
