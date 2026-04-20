"""
Telex.hu scraper — CDX discovery + live article fetch.

Telex.hu is a liberal Hungarian news portal. It does NOT have a consent wall
and serves full article HTML to automated fetchers. However, it has NO sitemaps
in robots.txt, so we use the Wayback CDX API for historical URL discovery,
then fetch articles LIVE (not via Wayback snapshots) for best HTML quality.

For very recent articles (last ~24h), RSS feeds supplement CDX discovery.

Article URL structure:
    https://telex.hu/{section}/{YYYY}/{MM}/{DD}/{slug}
    e.g. https://telex.hu/kulfold/2024/06/15/oroszorszag-ukrajna-haboru

Covers the gap 2024-03-28 → 2026-02-23 (or any date range).
portal="Telex", illiberal="liberal".

USAGE
-----
    python scrape_telex_cdx.py --start 2024-03-28 --end 2026-02-23 \
        --out ../../data/telex_supplement.csv \
        --cdx-cache ../../data/telex_cdx_urls.jsonl
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
    Article, Checkpoint, DEFAULT_UA,
    clean_text, first_sentence, log,
    make_document_id, make_session, parse_date_str,
    polite_get, wayback_cdx_urls,
)

PORTAL_NAME = "Telex"
PORTAL_CODE = "HU_telex"
ILLIBERAL = "liberal"
DEFAULT_HOST = "telex.hu"

# Telex sections where politics / war / economy coverage lives.
DEFAULT_SECTIONS = [
    "kulfold",
    "belfold",
    "gazdasag",
    "kult",
    "tech",
    "video",
]

# Also fetch RSS for recent articles
RSS_URLS = [
    "https://telex.hu/rss",
    "https://telex.hu/rss/kulfold",
    "https://telex.hu/rss/belfold",
]

# ─────────────────────────────────────────────────────────────────
# URL filtering
# ─────────────────────────────────────────────────────────────────
# Telex article URLs contain a date: /{section}/{YYYY}/{MM}/{DD}/{slug}
_ARTICLE_RE = re.compile(
    r"telex\.hu/[a-z][a-z0-9-]*/\d{4}/\d{2}/\d{2}/[a-z0-9]",
    re.IGNORECASE,
)

_BAD_PARTS = (
    "/tag/", "/szerzo/", "/autor/", "/rovat/", "/cimke/",
    "/videok/", "/podcast/", "/english/",
    "/impresszum", "/kapcsolat", "/adatvedelem", "/rss",
    "/felhasznalasi-feltetelek",
)


def looks_like_article_url(url: str) -> bool:
    lower = url.lower().split("?", 1)[0].split("#", 1)[0]
    if "telex.hu" not in lower:
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
# Telex article extractor (for live HTML)
# ─────────────────────────────────────────────────────────────────
def extract_telex_article(html: str) -> dict:
    """
    Parse Telex article from live HTML.

    Telex may use:
      - JSON-LD NewsArticle (modern structured data)
      - og:title, og:description (OpenGraph)
      - article:published_time (meta)
      - <article> tag or specific container classes for body
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
        # Try extracting date from URL path: /section/YYYY/MM/DD/slug
        # Not ideal but works as a last resort
        pass

    # Body extraction from HTML if JSON-LD body is empty or short
    if len(body) < 200:
        # Telex is a Next.js/React site. Try __NEXT_DATA__ first — it often
        # contains the full article content as structured JSON props.
        next_data_script = soup.find("script", {"id": "__NEXT_DATA__"})
        if next_data_script and next_data_script.string:
            try:
                nd = json.loads(next_data_script.string)
                # Walk props looking for article content fields
                props = nd.get("props", {}).get("pageProps", {})
                # Common patterns: props.article.content, props.post.body, etc.
                for key in ("article", "post", "data", "content"):
                    obj = props.get(key, {})
                    if isinstance(obj, dict):
                        for bkey in ("content", "body", "articleBody", "htmlContent", "text"):
                            raw = obj.get(bkey, "")
                            if raw and len(str(raw)) > 200:
                                # May be HTML or plain text
                                if "<" in str(raw):
                                    inner = BeautifulSoup(str(raw), "lxml")
                                    parts = [p.get_text(strip=True)
                                             for p in inner.find_all("p")
                                             if p.get_text(strip=True)]
                                    candidate = clean_text("\n".join(parts))
                                else:
                                    candidate = clean_text(str(raw))
                                if len(candidate) > len(body):
                                    body = candidate
            except (json.JSONDecodeError, KeyError, TypeError):
                pass

    if len(body) < 200:
        # Telex uses various container patterns; try multiple selectors
        for selector in [
            "article",
            ".article-html-content",
            ".article__body",
            ".article-content",
            ".article_container",
            "[data-testid='article-body']",
            ".content-body",
            ".post-body",
            ".entry-content",
            # Telex-specific: look for divs with rich text content
            "[class*='articleBody']",
            "[class*='article-body']",
            "[class*='richtext']",
            "[class*='RichText']",
        ]:
            el = soup.select_one(selector)
            if not el:
                continue
            paragraphs = []
            for p in el.find_all("p"):
                text = p.get_text(strip=True)
                if text and len(text) > 20:
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

    # Last resort: collect all <p> tags on the page that look like
    # article content (long enough, not navigation/footer)
    if len(body) < 200:
        all_p = soup.find_all("p")
        paragraphs = []
        for p in all_p:
            text = p.get_text(strip=True)
            if text and len(text) > 50:
                # Skip obvious non-article content
                lower = text.lower()
                if any(skip in lower for skip in [
                    "cookie", "süti", "adatvédelm", "feliratkoz",
                    "hírlevél", "copyright", "©",
                ]):
                    continue
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
# RSS URL discovery (supplement for recent articles)
# ─────────────────────────────────────────────────────────────────
def discover_rss_urls(session, host: str) -> list[dict]:
    """Discover article URLs from RSS feeds."""
    found = []
    seen = set()
    for rss_url in RSS_URLS:
        try:
            resp = session.get(rss_url, timeout=15)
            if resp.status_code != 200:
                continue
            links = re.findall(
                r'<link>(https?://' + re.escape(host) + r'/[^<]+)</link>',
                resp.text,
            )
            for link in links:
                clean = normalize_url(link)
                if clean not in seen and looks_like_article_url(clean):
                    seen.add(clean)
                    found.append({"url": clean, "timestamp": ""})
        except Exception as e:
            log.debug(f"  RSS error {rss_url}: {e}")
    log.info(f"  RSS discovery: {len(found)} article URLs")
    return found


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
# Main
# ─────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        description="Scrape Telex.hu via CDX URL discovery + live fetch."
    )
    ap.add_argument("--start", required=True, help="start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="end date YYYY-MM-DD (inclusive)")
    ap.add_argument("--out", required=True, help="output CSV path")
    ap.add_argument("--host", default=DEFAULT_HOST,
                    help=f"Telex host (default: {DEFAULT_HOST})")
    ap.add_argument("--cdx-cache", default="",
                    help="JSONL path to cache CDX results (resumable)")
    ap.add_argument("--limit", type=int, default=0,
                    help="max articles to scrape (0 = no limit)")
    ap.add_argument("--skip-cdx", action="store_true",
                    help="skip CDX query, use only --cdx-cache + RSS for URLs")
    ap.add_argument("--skip-rss", action="store_true",
                    help="skip RSS discovery")
    ap.add_argument("--sections", default=",".join(DEFAULT_SECTIONS),
                    help="comma-separated section prefixes")
    ap.add_argument("--chunk-days", type=int, default=14,
                    help="CDX time-window size in days (default: 14)")
    ap.add_argument("--diagnose", action="store_true",
                    help="fetch and diagnose first article, then exit")
    args = ap.parse_args()

    date_from = datetime.strptime(args.start, "%Y-%m-%d").date()
    date_to = datetime.strptime(args.end, "%Y-%m-%d").date()
    out_path = Path(args.out)
    cdx_cache_path = Path(args.cdx_cache) if args.cdx_cache else None

    log.info(f"  Telex CDX+live scraper — {date_from} → {date_to}")
    log.info(f"  host: {args.host}")
    log.info(f"  output: {out_path}")
    if cdx_cache_path:
        log.info(f"  CDX cache: {cdx_cache_path}")

    session = make_session(DEFAULT_UA)
    session.headers.update({"Accept-Language": "hu;q=0.9,en;q=0.5"})

    # ── 1. URL discovery ──
    all_entries: list[dict] = []
    seen_urls: set[str] = set()

    # Load CDX cache
    if cdx_cache_path and cdx_cache_path.exists():
        for e in load_cdx_cache(cdx_cache_path):
            u = normalize_url(e.get("url", ""))
            if u and u not in seen_urls:
                seen_urls.add(u)
                all_entries.append({"url": u, "timestamp": e.get("timestamp", "")})

    # RSS discovery (adds recent URLs)
    if not args.skip_rss:
        for entry in discover_rss_urls(session, args.host):
            u = entry["url"]
            if u not in seen_urls:
                seen_urls.add(u)
                all_entries.append(entry)

    # CDX discovery
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

    # ── 2. Filter ──
    article_entries: list[dict] = []
    seen_norm: set[str] = set()
    # Pre-filter by date in URL path where possible.
    # Telex URLs contain /YYYY/MM/DD/ — skip URLs clearly outside date range.
    _url_date_re = re.compile(r"/(\d{4})/(\d{2})/(\d{2})/")
    pre_date_skip = 0
    for entry in all_entries:
        url = normalize_url(entry["url"])
        if not looks_like_article_url(url):
            continue
        if url in seen_norm:
            continue
        # Fast date check from URL path
        m = _url_date_re.search(url)
        if m:
            try:
                url_date = Date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                if url_date < date_from or url_date > date_to:
                    pre_date_skip += 1
                    continue
            except ValueError:
                pass  # invalid date in URL, keep it
        seen_norm.add(url)
        article_entries.append({"url": url, "timestamp": entry["timestamp"]})
    log.info(f"  candidate article URLs after filtering: {len(article_entries)}"
             f"  (pre-date-skipped: {pre_date_skip})")

    # ── Diagnose mode ──
    if args.diagnose:
        if not article_entries:
            log.error("  no article URLs to diagnose")
            return
        url = article_entries[0]["url"]
        log.info(f"  DIAGNOSING: {url}")
        resp = polite_get(session, url)
        if resp is None:
            log.error(f"  could not fetch {url}")
            return
        parsed = extract_telex_article(resp.text)
        for k, v in parsed.items():
            val = str(v)
            if len(val) > 300:
                val = val[:300] + "..."
            print(f"  {k}: {val}")
        return

    # ── 3. Scrape (live fetch — no consent wall) ──
    checkpoint = Checkpoint(out_path)
    checkpoint.open()

    scraped = 0
    skipped = 0
    rejected = 0
    fetch_fail = 0

    processed = 0
    try:
        for entry in article_entries:
            url = entry["url"]

            if url in checkpoint.seen_urls:
                skipped += 1
                continue

            processed += 1
            if processed <= 3 or processed % 100 == 0:
                log.info(f"  fetching [{processed}]: {url[:80]}")

            # Fetch LIVE (Telex has no consent wall)
            try:
                resp = polite_get(session, url)
            except Exception as e:
                log.warning(f"  unexpected error fetching {url[:80]}: {e}")
                fetch_fail += 1
                continue
            if resp is None:
                fetch_fail += 1
                continue

            parsed = extract_telex_article(resp.text)
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


if __name__ == "__main__":
    main()
