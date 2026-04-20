"""
Novinky.cz scraper — CDX discovery + Wayback snapshot extraction.

Novinky.cz serves a GDPR consent wall ("Nastavení souhlasu s personalizací")
that blocks all article content for automated fetchers. The sitemap has dates
(100% lastmod) but the live fetch returns consent pages.

Strategy (same as iDnes):
  - Discover article URLs via Wayback CDX API (section patterns).
  - For each URL, fetch the Wayback snapshot HTML (NOT the live site).
  - Extract title from og:title, date from article:published_time or og:article:published_time,
    body from HTML article containers.

Covers the gap 2024-03-23 → 2026-02-23 (or any date range).
portal="Novinky", illiberal="liberal".

USAGE
-----
    python scrape_novinky_cdx.py --start 2024-03-23 --end 2026-02-23 \\
        --out ../../data/novinky_supplement.csv \\
        --cdx-cache ../../data/novinky_cdx_urls.jsonl
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

PORTAL_NAME = "Novinky"
PORTAL_CODE = "CZ_novinky"
ILLIBERAL = "liberal"
DEFAULT_HOST = "www.novinky.cz"

# Novinky sections where war/politics coverage lives.
DEFAULT_SECTIONS = [
    "clanek/domaci",
    "clanek/zahranicni",
    "clanek/krimi",
    "clanek/ekonomika",
    "clanek/koktejl",
    "clanek/valka-na-ukrajine",
]

# ─────────────────────────────────────────────────────────────────
# URL filtering
# ─────────────────────────────────────────────────────────────────
# Novinky article URLs: /clanek/<section>-<slug>-<numeric-id>
# e.g. /clanek/domaci-macinka-si-predvolal-ruskeho-velvyslance-40573821
_ARTICLE_RE = re.compile(r"/clanek/[a-z].*-\d{5,}", re.IGNORECASE)

_BAD_PARTS = (
    "/autor/", "/tema/", "/tag/", "/galerie/", "/video/",
    "/hry/", "/tv/", "/sport/", "/koktejl/celebrity",
    "/reklama", "/podminky", "/kontakt", "/rss",
)


def looks_like_article_url(url: str) -> bool:
    lower = url.lower().split("?", 1)[0].split("#", 1)[0]
    if "novinky.cz" not in lower:
        return False
    if any(b in lower for b in _BAD_PARTS):
        return False
    return bool(_ARTICLE_RE.search(lower))


def normalize_url(url: str) -> str:
    url = url.strip()
    url = url.split("?", 1)[0].split("#", 1)[0]
    if url.startswith("http://"):
        url = "https://" + url[len("http://"):]
    return url.rstrip("/")


# ─────────────────────────────────────────────────────────────────
# Novinky article extractor (for Wayback HTML)
# ─────────────────────────────────────────────────────────────────
def extract_novinky_article(html: str) -> dict:
    """
    Parse Novinky article from Wayback snapshot HTML.

    Novinky may use:
      - og:title, og:description, article:published_time (meta)
      - JSON-LD NewsArticle (newer articles)
      - <article> or various container divs for body
    """
    soup = BeautifulSoup(html, "lxml")

    def og(prop: str) -> str:
        tag = soup.find("meta", property=prop)
        return clean_text(tag["content"]) if tag and tag.get("content") else ""

    def meta(name: str) -> str:
        tag = (soup.find("meta", attrs={"property": name})
               or soup.find("meta", attrs={"name": name}))
        return tag["content"].strip() if tag and tag.get("content") else ""

    # Try JSON-LD first
    headline = ""
    body = ""
    date_published = ""
    description = ""

    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            payload = json.loads(script.string or "{}")
        except (ValueError, TypeError):
            continue
        # Handle @graph arrays
        items = [payload]
        if isinstance(payload, list):
            items = payload
        elif "@graph" in payload:
            items = payload["@graph"]
        for item in items:
            if not isinstance(item, dict):
                continue
            t = item.get("@type", "")
            if isinstance(t, list):
                types = t
            else:
                types = [t]
            if "NewsArticle" in types or "Article" in types:
                headline = clean_text(item.get("headline", ""))
                body = clean_text(item.get("articleBody", ""))
                date_published = item.get("datePublished", "")
                description = clean_text(item.get("description", ""))
                break

    # Fallback to OG/meta
    if not headline:
        headline = og("og:title")
    if not description:
        description = og("og:description")
    if not date_published:
        date_published = meta("article:published_time")

    # Consent wall markers
    consent_markers = [
        "Nastavení souhlasu",
        "personalizací",
        "Získejte všechny",
        "Přihlaste se",
        "Už máte účet",
    ]

    def is_consent(text: str) -> bool:
        return any(m in text for m in consent_markers)

    # Body extraction from HTML if JSON-LD body is empty or truncated
    if len(body) < 200:
        for selector in ["article", ".g_art-content", ".g_art-text",
                         ".d_cnt", "#articleBody", ".o_article_text",
                         ".b_article_text", "[itemprop='articleBody']"]:
            el = soup.select_one(selector)
            if not el:
                continue
            paragraphs = []
            for p in el.find_all("p"):
                text = p.get_text(strip=True)
                if text and not is_consent(text):
                    paragraphs.append(text)
            candidate = clean_text("\n".join(paragraphs))
            if len(candidate) > len(body):
                body = candidate

    # Strip consent page titles
    if headline and is_consent(headline):
        headline = ""

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
        description="Scrape Novinky.cz via Wayback Machine CDX + snapshots."
    )
    ap.add_argument("--start", required=True, help="start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="end date YYYY-MM-DD (inclusive)")
    ap.add_argument("--out", required=True, help="output CSV path")
    ap.add_argument("--host", default=DEFAULT_HOST,
                    help=f"Novinky host (default: {DEFAULT_HOST})")
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

    log.info(f"  Novinky CDX+Wayback scraper — {date_from} → {date_to}")
    log.info(f"  host: {args.host}")
    log.info(f"  output: {out_path}")
    if cdx_cache_path:
        log.info(f"  CDX cache: {cdx_cache_path}")

    session = make_session(DEFAULT_UA)

    # ── 1. URL discovery ──
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
            patterns = [f"{args.host}/{s}*" for s in sections]
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

    # ── 2. Filter ──
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

    # ── 3. Scrape via Wayback ──
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

            html = fetch_wayback_snapshot(session, url, timestamp)
            if html is None:
                no_snapshot += 1
                continue

            parsed = extract_novinky_article(html)
            if not parsed.get("headline"):
                rejected += 1
                continue

            article = build_article(url, parsed)
            if article is None:
                rejected += 1
                continue

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
