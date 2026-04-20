"""
Pravda.sk scraper — CDX-based URL discovery (Wayback Machine).

Pravda's XML sitemaps carry NO per-URL dates (confirmed 2026-04:
45000-URL sub-sitemaps with zero <lastmod> / <news:publication_date>
entries), so the sitemap-based crawler has to fetch every single article
just to check its date — which is infeasible for a 2-year backfill.

This variant uses the Internet Archive CDX API to discover article URLs
from snapshots taken during the requested date range, then fetches each
URL live and extracts data from OpenGraph meta tags + the <article> HTML
element (Pravda has no JSON-LD NewsArticle, only a BreadcrumbList).

Covers the gap 2024-03-23 → 2026-02-23 (or any date range you supply).
portal="Pravda", illiberal="illiberal".

USAGE
-----
    python scrape_pravda_cdx.py --start 2024-03-23 --end 2026-02-23 \\
        --out ../../data/pravda_supplement.csv \\
        --cdx-cache ../../data/pravda_cdx_urls.jsonl

Optional:
    --host spravy.pravda.sk       # Pravda subdomain (default)
    --sections svet,domace,...    # restrict CDX to specific sections
    --limit 500                   # cap total articles
    --include-paywalled           # keep articles marked isAccessibleForFree=false
    --no-robots                   # skip robots.txt (discouraged)
    --skip-cdx                    # reuse --cdx-cache only, no new CDX query
    --chunk-days 14               # CDX time-window size (shrink if 504s)
"""

from __future__ import annotations

import argparse
import json
import re
import socket
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from bs4 import BeautifulSoup

from scraper_utils import (
    Article, Checkpoint, DEFAULT_UA,
    clean_text, first_sentence, log,
    load_robots, make_document_id, make_session, parse_date_str,
    polite_get, robots_allows, wayback_cdx_urls,
)

PORTAL_NAME = "Pravda"
PORTAL_CODE = "SK_pravda"
ILLIBERAL = "illiberal"
DEFAULT_HOST = "www.pravda.sk"  # spravy.pravda.sk has intermittent DNS issues

# Pravda editorial sections where news / Russia-Ukraine coverage lives.
# Querying section-by-section avoids CDX 504 timeouts caused by huge
# host-wide patterns.
DEFAULT_SECTIONS = [
    "svet",
    "domace",
    "ekonomika",
    "regiony",
]

# ─────────────────────────────────────────────────────────────────
# URL filtering
# ─────────────────────────────────────────────────────────────────
# Pravda article URLs: /<section>/clanok/<numeric-id>-<slug>/
_ARTICLE_RE = re.compile(r"/clanok/\d{4,}-", re.IGNORECASE)

_BAD_PARTS = (
    "/tag/", "/tagy/", "/autor/", "/redakcia", "/reklama",
    "/video/", "/galeria/", "/foto/", "/diskusia/", "/hlasovania/",
    "/kontakt", "/podmienky", "/predplatne", "/newsletter", "/rss",
    "/sutaz/", "/hry/", "/temy/",
)


def looks_like_article_url(url: str, host: str) -> bool:
    lower = url.lower().split("?", 1)[0].split("#", 1)[0]
    if "pravda.sk" not in lower:
        return False
    if not (lower.startswith(f"https://{host}/")
            or lower.startswith(f"http://{host}/")):
        return False
    if any(b in lower for b in _BAD_PARTS):
        return False
    return bool(_ARTICLE_RE.search(lower))


def normalize_url(url: str) -> str:
    url = url.strip()
    url = url.split("?", 1)[0].split("#", 1)[0]
    if url.startswith("http://"):
        url = "https://" + url[len("http://"):]
    # Rewrite spravy.pravda.sk → www.pravda.sk (DNS fix)
    url = url.replace("://spravy.pravda.sk/", "://www.pravda.sk/")
    return url.rstrip("/")


# ─────────────────────────────────────────────────────────────────
# DNS sanity
# ─────────────────────────────────────────────────────────────────
def _host_resolves(host: str) -> bool:
    try:
        socket.gethostbyname(host)
        return True
    except OSError:
        return False


def _pick_host(preferred: str, fallback: str = "www.pravda.sk") -> str:
    if _host_resolves(preferred):
        return preferred
    log.warning(f"  DNS: {preferred} does not resolve — falling back to {fallback}")
    if _host_resolves(fallback):
        return fallback
    log.error(f"  DNS: neither {preferred} nor {fallback} resolves — aborting")
    raise SystemExit(2)


# ─────────────────────────────────────────────────────────────────
# CDX cache (resumable discovery)
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
# Pravda-specific HTML extractor
# ─────────────────────────────────────────────────────────────────
# Pravda does NOT use JSON-LD NewsArticle. Data sources:
#   - og:title              → headline
#   - og:description        → description
#   - article:published_time → datePublished  (meta property)
#   - <article class="article-detail"> <p> tags → articleBody
#
def extract_pravda_article(html: str) -> dict:
    """Parse Pravda article from OG meta + <article> HTML tag."""
    soup = BeautifulSoup(html, "lxml")

    def og(prop: str) -> str:
        tag = soup.find("meta", property=prop)
        return clean_text(tag["content"]) if tag and tag.get("content") else ""

    def meta(name: str) -> str:
        tag = (soup.find("meta", attrs={"property": name})
               or soup.find("meta", attrs={"name": name}))
        return (tag["content"].strip()) if tag and tag.get("content") else ""

    headline = og("og:title")
    description = og("og:description")
    date_published = meta("article:published_time")

    # Body: concatenate <p> tags inside <article> element
    body = ""
    article_el = soup.find("article")
    if article_el:
        paragraphs = [p.get_text(strip=True) for p in article_el.find_all("p")]
        body = clean_text("\n".join(p for p in paragraphs if p))

    # Fallback: if <article> body is very short, try main content div
    if len(body) < 100:
        # Try common Pravda content containers
        for selector in [".article-detail__body", ".article-body",
                         "[itemprop='articleBody']", ".story-content"]:
            el = soup.select_one(selector)
            if el:
                paragraphs = [p.get_text(strip=True) for p in el.find_all("p")]
                candidate = clean_text("\n".join(p for p in paragraphs if p))
                if len(candidate) > len(body):
                    body = candidate

    return {
        "headline": headline,
        "articleBody": body,
        "description": description,
        "datePublished": date_published,
        "isAccessibleForFree": True,  # Pravda is free
    }


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
        description="Scrape Pravda.sk via Wayback Machine CDX for URL discovery."
    )
    ap.add_argument("--start", required=True, help="start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="end date YYYY-MM-DD (inclusive)")
    ap.add_argument("--out", required=True, help="output CSV path")
    ap.add_argument("--host", default=DEFAULT_HOST,
                    help=f"Pravda subhost (default: {DEFAULT_HOST})")
    ap.add_argument("--cdx-cache", default="",
                    help="JSONL path to cache CDX discovery results (resumable)")
    ap.add_argument("--limit", type=int, default=0,
                    help="max articles to scrape (0 = no limit)")
    ap.add_argument("--include-paywalled", action="store_true",
                    help="include articles marked isAccessibleForFree=false")
    ap.add_argument("--no-robots", action="store_true", help="skip robots.txt check")
    ap.add_argument("--skip-cdx", action="store_true",
                    help="skip CDX query, use only --cdx-cache for URLs")
    ap.add_argument("--sections", default=",".join(DEFAULT_SECTIONS),
                    help="comma-separated section prefixes "
                         "(empty string = host-wide pattern, may 504)")
    ap.add_argument("--chunk-days", type=int, default=14,
                    help="CDX time-window size in days (default: 14). "
                         "Lower (7) if you still see 504s.")
    args = ap.parse_args()

    date_from = datetime.strptime(args.start, "%Y-%m-%d").date()
    date_to = datetime.strptime(args.end, "%Y-%m-%d").date()
    out_path = Path(args.out)
    cdx_cache_path = Path(args.cdx_cache) if args.cdx_cache else None

    # DNS sanity + possible fallback
    host = _pick_host(args.host)

    log.info(f"  Pravda CDX scraper — {date_from} → {date_to}")
    log.info(f"  host: {host}")
    log.info(f"  output: {out_path}")
    log.info(f"  include paywalled: {args.include_paywalled}")
    if cdx_cache_path:
        log.info(f"  CDX cache: {cdx_cache_path}")

    session = make_session(DEFAULT_UA)
    rp = None
    if not args.no_robots:
        rp = load_robots(f"https://{host}/", DEFAULT_UA, session=session)

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
            patterns = [f"{host}/{s}/*" for s in sections]
        else:
            patterns = [f"{host}/*"]
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

    # ── 2. Filter to article-looking URLs, dedupe, pre-filter by CDX timestamp ──
    article_entries: list[dict] = []
    seen_norm: set[str] = set()
    ts_from = date_from.strftime("%Y%m%d")
    ts_to = date_to.strftime("%Y%m%d") + "235959"
    pre_date_skip = 0
    for entry in all_entries:
        url = entry["url"]
        if not looks_like_article_url(url, host):
            continue
        if url in seen_norm:
            continue
        # Pre-filter by CDX timestamp — skip URLs archived outside date range
        ts = entry.get("timestamp", "")
        if ts and len(ts) >= 8:
            ts_date = ts[:8]  # YYYYMMDD
            if ts_date < ts_from or ts_date > ts_to[:8]:
                pre_date_skip += 1
                continue
        # Pre-filter by clanok ID — articles < 740000 are published before
        # our date range (2024-03). Skip to avoid wasting time fetching them.
        clanok_match = re.search(r'/clanok/(\d+)-', url)
        if clanok_match:
            clanok_id = int(clanok_match.group(1))
            if clanok_id < 740000:
                pre_date_skip += 1
                continue
        seen_norm.add(url)
        article_entries.append({"url": url, "timestamp": ts})
    log.info(f"  candidate article URLs after filtering: {len(article_entries)}"
             f"  (pre-date-skipped: {pre_date_skip})")

    # ── 3. Checkpoint & scrape loop ──
    checkpoint = Checkpoint(out_path)
    checkpoint.open()
    # Also add www.pravda.sk variants of existing spravy.pravda.sk URLs
    # to avoid re-fetching articles from old checkpoint entries
    extra = set()
    for u in checkpoint.seen_urls:
        if "spravy.pravda.sk" in u:
            extra.add(u.replace("spravy.pravda.sk", "www.pravda.sk"))
        elif "www.pravda.sk" in u:
            extra.add(u.replace("www.pravda.sk", "spravy.pravda.sk"))
    checkpoint.seen_urls.update(extra)
    log.info(f"  checkpoint URLs (with host variants): {len(checkpoint.seen_urls)}")

    scraped = 0
    skipped = 0
    rejected = 0
    no_snapshot = 0
    paywalled_skipped = 0

    def try_fetch(sess, url, timestamp):
        """
        Try live fetch first (better HTML quality), fall back to Wayback.
        Uses longer delay (5s) to avoid being blocked by Pravda.
        """
        import time as _time
        # Try live fetch first — better HTML, has OG tags
        try:
            _time.sleep(3)  # extra delay on top of polite_get's own delay
            resp = polite_get(sess, url)
            if resp is not None:
                return resp
        except Exception:
            pass
        # Fall back to Wayback snapshot
        if timestamp:
            wb_url = f"https://web.archive.org/web/{timestamp}id_/{url}"
            try:
                resp = polite_get(sess, wb_url)
                return resp
            except Exception as e:
                log.warning(f"  wayback fetch error: {e}")
        return None

    processed = 0
    try:
        for entry in article_entries:
            url = entry["url"]
            timestamp = entry.get("timestamp", "")

            if url in checkpoint.seen_urls:
                skipped += 1
                continue
            if rp is not None and not robots_allows(rp, url, DEFAULT_UA):
                log.debug(f"  robots.txt disallows: {url}")
                rejected += 1
                continue

            processed += 1
            if processed <= 3 or processed % 100 == 0:
                log.info(f"  fetching [{processed}]: {url[:80]}  ts={timestamp[:8]}")

            resp = try_fetch(session, url, timestamp)
            if resp is None:
                no_snapshot += 1
                continue

            parsed = extract_pravda_article(resp.text)
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
    finally:
        checkpoint.close()

    log.info(
        f"  done.  scraped={scraped}  skipped={skipped}  "
        f"no_snapshot={no_snapshot}  rejected={rejected}"
    )
    log.info(f"  CSV written to: {out_path}")


if __name__ == "__main__":
    main()
