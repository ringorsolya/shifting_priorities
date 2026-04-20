"""
MagyarNemzet.hu scraper — sitemap-based + CDX fallback.

Magyar Nemzet is an illiberal Hungarian news portal (government-aligned).
It has a sitemapindex.xml with monthly sub-sitemaps. No consent wall —
articles are accessible directly.

If sitemaps lack dates or are incomplete, falls back to CDX discovery.

Article URL structure (typical):
    https://magyarnemzet.hu/{section}/{YYYY}/{MM}/{slug}
    e.g. https://magyarnemzet.hu/kulfold/2024/06/zelenszkij-orban-viktor

Covers the gap 2023-02-28 → 2026-02-23 (or any date range).
portal="MagyarNemzet", illiberal="illiberal".

USAGE
-----
    # Sitemap mode (default):
    python scrape_magyarnemzet.py --start 2023-02-28 --end 2026-02-23 \
        --out ../../data/magyarnemzet_supplement.csv

    # CDX fallback mode:
    python scrape_magyarnemzet.py --start 2023-02-28 --end 2026-02-23 \
        --out ../../data/magyarnemzet_supplement.csv \
        --mode cdx --cdx-cache ../../data/magyarnemzet_cdx_urls.jsonl
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone, date as Date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from bs4 import BeautifulSoup

from scraper_utils import (
    Article, Checkpoint, DEFAULT_UA, PortalConfig,
    clean_text, first_sentence, log,
    make_document_id, make_session, parse_date_str,
    polite_get, wayback_cdx_urls, walk_sitemaps,
    load_robots, robots_allows,
)

PORTAL_NAME = "MagyarNemzet"
PORTAL_CODE = "HU_magyarnemzet"
ILLIBERAL = "illiberal"
DEFAULT_HOST = "magyarnemzet.hu"

DEFAULT_SITEMAP = "https://magyarnemzet.hu/sitemapindex.xml"

# CDX fallback sections
DEFAULT_SECTIONS = [
    "kulfold",
    "belfold",
    "gazdasag",
    "lugas",
    "sport",
    "velemeny",
]

# ─────────────────────────────────────────────────────────────────
# URL filtering
# ─────────────────────────────────────────────────────────────────
# Magyar Nemzet articles typically: /section/YYYY/MM/slug
_ARTICLE_RE = re.compile(
    r"magyarnemzet\.hu/[a-z][a-z0-9-]*/\d{4}/\d{2}/[a-z0-9]",
    re.IGNORECASE,
)

_BAD_PARTS = (
    "/tag/", "/szerzo/", "/autor/", "/cimke/", "/tema/",
    "/category/", "/page/", "/feed/", "/wp-",
    "/impresszum", "/kapcsolat", "/adatvedelem", "/felhasznalasi",
    "/rss", "/search", "/archivum",
    "/wp-content/", "/wp-admin/", "/wp-includes/",
    "/english/",  # We want Hungarian articles only
)


def looks_like_article_url(url: str) -> bool:
    lower = url.lower().split("?", 1)[0].split("#", 1)[0]
    if "magyarnemzet.hu" not in lower:
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
# Magyar Nemzet article extractor
# ─────────────────────────────────────────────────────────────────
def extract_magyarnemzet_article(html: str) -> dict:
    """
    Parse Magyar Nemzet article from live HTML.

    MN is a WordPress-based site. Typical structure:
      - JSON-LD (may or may not have NewsArticle)
      - og:title, og:description (usually present)
      - article:published_time (usually present)
      - <article> tag with <p> paragraphs for body
      - Various WordPress content classes
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
        items = [payload]
        if isinstance(payload, list):
            items = payload
        elif "@graph" in payload:
            items = payload["@graph"]
        for item in items:
            if not isinstance(item, dict):
                continue
            t = item.get("@type", "")
            types = t if isinstance(t, list) else [t]
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
    if not date_published:
        date_published = meta("datePublished")

    # Body from HTML containers
    if len(body) < 200:
        # WordPress / Magyar Nemzet selectors
        for selector in [
            "article",
            ".entry-content",
            ".article-content",
            ".article__body",
            ".post-content",
            ".content-area",
            ".article-body",
            ".single-content",
            "[itemprop='articleBody']",
            ".td-post-content",
            ".article_container",
        ]:
            el = soup.select_one(selector)
            if not el:
                continue
            paragraphs = []
            for p in el.find_all("p"):
                text = p.get_text(strip=True)
                if text and len(text) > 20:
                    # Filter out common non-article text
                    lower_text = text.lower()
                    if any(skip in lower_text for skip in [
                        "cookie", "süti", "hozzájárul", "adatvédelmi",
                        "feliratkoz", "hírlevél", "kövess",
                    ]):
                        continue
                    paragraphs.append(text)
            candidate = clean_text("\n".join(paragraphs))
            if len(candidate) > len(body):
                body = candidate

    # If still no body, try all <p> inside <main>
    if len(body) < 200:
        main_el = soup.find("main")
        if main_el:
            paragraphs = []
            for p in main_el.find_all("p"):
                text = p.get_text(strip=True)
                if text and len(text) > 30:
                    paragraphs.append(text)
            candidate = clean_text("\n".join(paragraphs))
            if len(candidate) > len(body):
                body = candidate

    # Date from <time> tag as last resort
    if not date_published:
        time_tag = soup.find("time", attrs={"datetime": True})
        if time_tag:
            date_published = time_tag["datetime"]

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
# Sitemap mode
# ─────────────────────────────────────────────────────────────────
def run_sitemap_mode(args, session, date_from, date_to, out_path):
    """Walk sitemapindex.xml → sub-sitemaps → article URLs → fetch live."""
    log.info(f"  mode: sitemap ({args.sitemap})")

    checkpoint = Checkpoint(out_path)
    checkpoint.open()

    scraped = 0
    skipped = 0
    rejected = 0
    fetch_fail = 0

    try:
        for entry in walk_sitemaps(session, args.sitemap, date_from, date_to):
            url = normalize_url(entry.get("loc", ""))
            if not url:
                continue
            if not looks_like_article_url(url):
                continue
            if url in checkpoint.seen_urls:
                skipped += 1
                continue

            resp = polite_get(session, url)
            if resp is None:
                fetch_fail += 1
                continue

            parsed = extract_magyarnemzet_article(resp.text)
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
                    f"fetch_fail={fetch_fail}  rejected={rejected}"
                )

            if args.limit and scraped >= args.limit:
                log.info(f"  reached --limit {args.limit}, stopping")
                break

            time.sleep(1.5)
    finally:
        checkpoint.close()

    log.info(
        f"  done.  scraped={scraped}  skipped={skipped}  "
        f"fetch_fail={fetch_fail}  rejected={rejected}"
    )
    log.info(f"  CSV written to: {out_path}")


# ─────────────────────────────────────────────────────────────────
# CDX mode (fallback)
# ─────────────────────────────────────────────────────────────────
def run_cdx_mode(args, session, date_from, date_to, out_path):
    """CDX URL discovery + live fetch. Used when sitemaps are incomplete."""
    cdx_cache_path = Path(args.cdx_cache) if args.cdx_cache else None
    log.info(f"  mode: CDX ({args.host})")
    if cdx_cache_path:
        log.info(f"  CDX cache: {cdx_cache_path}")

    # URL discovery
    all_entries: list[dict] = []
    seen_urls: set[str] = set()

    if cdx_cache_path and cdx_cache_path.exists():
        for e in load_cdx_cache(cdx_cache_path):
            u = normalize_url(e.get("url", ""))
            if u and u not in seen_urls:
                seen_urls.add(u)
                all_entries.append({"url": u, "timestamp": e.get("timestamp", "")})

    if not args.skip_cdx:
        log.info("  starting CDX URL discovery...")
        sections = [s.strip() for s in args.sections.split(",") if s.strip()]
        patterns = [f"{args.host}/{s}/*" for s in sections] if sections else [f"{args.host}/*"]
        log.info(f"  querying {len(patterns)} CDX pattern(s), chunk_days={args.chunk_days}")

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
                    if not u or u in seen_urls:
                        continue
                    seen_urls.add(u)
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

    # Filter
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

    # Scrape
    checkpoint = Checkpoint(out_path)
    checkpoint.open()

    scraped = 0
    skipped = 0
    rejected = 0
    fetch_fail = 0

    try:
        for entry in article_entries:
            url = entry["url"]

            if url in checkpoint.seen_urls:
                skipped += 1
                continue

            resp = polite_get(session, url)
            if resp is None:
                fetch_fail += 1
                continue

            parsed = extract_magyarnemzet_article(resp.text)
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
                    f"fetch_fail={fetch_fail}  rejected={rejected}"
                )

            if args.limit and scraped >= args.limit:
                log.info(f"  reached --limit {args.limit}, stopping")
                break

            time.sleep(1.5)
    finally:
        checkpoint.close()

    log.info(
        f"  done.  scraped={scraped}  skipped={skipped}  "
        f"fetch_fail={fetch_fail}  rejected={rejected}"
    )
    log.info(f"  CSV written to: {out_path}")


# ─────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        description="Scrape MagyarNemzet.hu via sitemap or CDX."
    )
    ap.add_argument("--start", required=True, help="start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="end date YYYY-MM-DD (inclusive)")
    ap.add_argument("--out", required=True, help="output CSV path")
    ap.add_argument("--mode", choices=["sitemap", "cdx"], default="sitemap",
                    help="discovery mode: sitemap (default) or cdx")
    ap.add_argument("--host", default=DEFAULT_HOST)
    ap.add_argument("--sitemap", default=DEFAULT_SITEMAP,
                    help="root sitemap URL (for sitemap mode)")
    ap.add_argument("--cdx-cache", default="",
                    help="JSONL path to cache CDX results (for cdx mode)")
    ap.add_argument("--limit", type=int, default=0,
                    help="max articles to scrape (0 = no limit)")
    ap.add_argument("--skip-cdx", action="store_true",
                    help="skip CDX query, use only --cdx-cache for URLs")
    ap.add_argument("--sections", default=",".join(DEFAULT_SECTIONS),
                    help="comma-separated section prefixes (for cdx mode)")
    ap.add_argument("--chunk-days", type=int, default=14,
                    help="CDX time-window size in days (default: 14)")
    ap.add_argument("--diagnose", action="store_true",
                    help="fetch and diagnose first article URL, then exit")
    args = ap.parse_args()

    date_from = datetime.strptime(args.start, "%Y-%m-%d").date()
    date_to = datetime.strptime(args.end, "%Y-%m-%d").date()
    out_path = Path(args.out)

    log.info(f"  MagyarNemzet scraper — {date_from} → {date_to}")
    log.info(f"  output: {out_path}")

    session = make_session(DEFAULT_UA)
    session.headers.update({"Accept-Language": "hu;q=0.9,en;q=0.5"})

    # ── Diagnose mode ──
    if args.diagnose:
        log.info("  DIAGNOSE mode — finding first article URL...")
        # Try sitemap first
        from scraper_utils import fetch_sitemap, parse_sitemap
        xml = fetch_sitemap(session, args.sitemap)
        if xml:
            sub_urls, entries = parse_sitemap(xml)
            log.info(f"  sitemapindex has {len(sub_urls)} sub-sitemaps, {len(entries)} direct entries")
            # Explore first few sub-sitemaps
            test_url = None
            for sub_url in sub_urls[-3:]:  # try recent ones
                log.info(f"  trying sub-sitemap: {sub_url}")
                sub_xml = fetch_sitemap(session, sub_url)
                if not sub_xml:
                    continue
                _, sub_entries = parse_sitemap(sub_xml)
                log.info(f"    entries: {len(sub_entries)}")
                for e in sub_entries[:5]:
                    url = normalize_url(e.get("loc", ""))
                    if looks_like_article_url(url):
                        test_url = url
                        break
                if test_url:
                    break

            if test_url:
                log.info(f"  DIAGNOSING: {test_url}")
                resp = polite_get(session, test_url)
                if resp:
                    parsed = extract_magyarnemzet_article(resp.text)
                    for k, v in parsed.items():
                        val = str(v)
                        if len(val) > 300:
                            val = val[:300] + "..."
                        print(f"  {k}: {val}")
                else:
                    log.error(f"  could not fetch {test_url}")
            else:
                log.error("  no article URL found in sitemaps")
        return

    # ── Run ──
    if args.mode == "sitemap":
        run_sitemap_mode(args, session, date_from, date_to, out_path)
    else:
        run_cdx_mode(args, session, date_from, date_to, out_path)


if __name__ == "__main__":
    main()
