"""
Onet.pl scraper — replaces Gazeta Wyborcza as the Polish liberal portal.

Onet publishes NO public sitemap, so URLs for the historical period are
discovered via the Internet Archive's CDX (Wayback Machine) API. For each
discovered URL we then fetch the live Onet HTML (not the Wayback snapshot)
and parse the JSON-LD NewsArticle payload.

Covers the full extended period 2022-02-01 → 2026-02-23 (4 years around the
war anniversary), or any date range you supply.

Produces a CSV with the same schema as the existing
PL_M_*_document_level_with_preds.csv files. The ML-derived columns
(CAP codes, sentiment, NER, English translations, electoral_cycle)
are written as empty strings — run them through your existing ML
pipeline afterwards.

USAGE
-----
    pip install -r requirements.txt
    python scrape_onet.py --start 2022-02-01 --end 2026-02-23 \\
        --out ../../data/onet_supplement.csv \\
        --cdx-cache ../../data/onet_cdx_urls.jsonl

Optional:
    --host wiadomosci.onet.pl   # which Onet subhost to crawl (default)
    --limit 500                  # cap total articles
    --include-paywalled          # also keep articles marked isAccessibleForFree=false
    --no-robots                  # skip robots.txt check (discouraged)

LEGAL / ETHICAL NOTES
---------------------
 - Academic, non-commercial research purpose declared in User-Agent.
 - Only public content is collected. Articles marked
   `isAccessibleForFree: false` are SKIPPED by default.
 - URL discovery uses Internet Archive's public CDX API (standard
   research method, no bypass of any paywall).
 - Full article bodies are read from the live Onet HTML only when the
   publisher's own JSON-LD lists them as public.
 - robots.txt is respected on onet.pl by default.
 - Rate limiting: ~1.5 s polite delay between requests; 2 s between
   CDX monthly chunks.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from scraper_utils import (
    Article, Checkpoint, DEFAULT_UA,
    extract_jsonld_article, first_sentence, log,
    load_robots, make_document_id, make_session, parse_date_str,
    polite_get, robots_allows, wayback_cdx_urls,
)

PORTAL_NAME = "Onet"
PORTAL_CODE = "PL_onet"
ILLIBERAL = "liberal"
DEFAULT_HOST = "wiadomosci.onet.pl"

# Broad host-wide CDX queries hit 504 timeouts on the Internet Archive.
# Query section-by-section instead — each section returns a manageable
# number of rows.  These are the editorial sections where Russia-Ukraine
# war coverage is most likely to live.
DEFAULT_SECTIONS = [
    "kraj",
    "swiat",
    "tylko-w-onecie",
    "polityka",
    "unia-europejska",
    "wojna-w-ukrainie",
]


# ─────────────────────────────────────────────────────────────────
# URL filtering
# ─────────────────────────────────────────────────────────────────
# Onet article URLs typically look like:
#   https://wiadomosci.onet.pl/kraj/<slug>/<shortid>
#   https://wiadomosci.onet.pl/swiat/<slug>/<shortid>
#   https://wiadomosci.onet.pl/tylko-w-onecie/<slug>/<shortid>
# The trailing segment is a short alphanumeric id, e.g. "abc1234".
_ARTICLE_RE = re.compile(r"/[a-z0-9-]+/[a-z0-9-]+/[a-z0-9]{5,}$", re.IGNORECASE)

_BAD_PARTS = (
    "/tag/", "/tagi/", "/autor/", "/author/", "/kategoria/",
    "/galeria/", "/video/", "/wideo/", "/forum/", "/szukaj/",
    "/regulamin", "/kontakt", "/redakcja", "/polityka-prywatnosci",
    "/pogoda", "/horoskop", "/quiz/", "/quizy/", "/ogloszenia",
    "/dzialy", "/kanaly", "/tematy/", "/serwisy",
    "/?", "#",
)


def looks_like_article_url(url: str, host: str) -> bool:
    lower = url.lower()
    if host not in lower:
        return False
    if not (lower.startswith(f"https://{host}/") or lower.startswith(f"http://{host}/")):
        return False
    if any(b in lower for b in _BAD_PARTS):
        return False
    # Extract path
    try:
        path = lower.split(host, 1)[1]
    except IndexError:
        return False
    # Strip query string / fragment before regex match
    path = path.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    return bool(_ARTICLE_RE.search(path))


def normalize_url(url: str) -> str:
    """Strip wayback prefixes, query strings, and trailing slashes."""
    # Some CDX results may arrive as http://; force https on the live fetch
    url = url.strip()
    # Drop query strings (Onet article URLs don't need them; they introduce dupes)
    url = url.split("?", 1)[0].split("#", 1)[0]
    if url.startswith("http://"):
        url = "https://" + url[len("http://"):]
    return url.rstrip("/")


# ─────────────────────────────────────────────────────────────────
# CDX URL cache (resumable discovery)
# ─────────────────────────────────────────────────────────────────
def load_cdx_cache(path: Path) -> list[dict]:
    """Read a JSONL file of previously-discovered CDX entries."""
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
    """Append a CDX entry to the JSONL cache."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


# ─────────────────────────────────────────────────────────────────
# Article build
# ─────────────────────────────────────────────────────────────────
def build_article(url: str, parsed: dict) -> Article | None:
    """Turn a parsed JSON-LD payload into an Article ready for CSV."""
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
        description="Scrape Onet.pl via Wayback Machine CDX for URL discovery."
    )
    ap.add_argument("--start", required=True, help="start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="end date YYYY-MM-DD (inclusive)")
    ap.add_argument("--out", required=True, help="output CSV path")
    ap.add_argument("--host", default=DEFAULT_HOST,
                    help=f"Onet subhost to crawl (default: {DEFAULT_HOST})")
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
                    help="comma-separated list of section prefixes to query "
                         "(empty string = query the whole host as one pattern, "
                         "which often times out)")
    ap.add_argument("--chunk-days", type=int, default=14,
                    help="CDX time-window size in days (default: 14). "
                         "Use smaller (7) if you still see 504 timeouts.")
    args = ap.parse_args()

    date_from = datetime.strptime(args.start, "%Y-%m-%d").date()
    date_to = datetime.strptime(args.end, "%Y-%m-%d").date()
    out_path = Path(args.out)
    cdx_cache_path = Path(args.cdx_cache) if args.cdx_cache else None

    log.info(f"  Onet scraper — {date_from} → {date_to}")
    log.info(f"  host: {args.host}")
    log.info(f"  output: {out_path}")
    log.info(f"  include paywalled: {args.include_paywalled}")
    if cdx_cache_path:
        log.info(f"  CDX cache: {cdx_cache_path}")

    session = make_session(DEFAULT_UA)
    rp = None
    if not args.no_robots:
        rp = load_robots(f"https://{args.host}/", DEFAULT_UA, session=session)

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

    # ── 2. Filter to article-looking URLs, dedupe, sort by timestamp ──
    ts_from = date_from.strftime("%Y%m%d")
    ts_to = date_to.strftime("%Y%m%d") + "235959"
    article_entries: list[dict] = []
    seen_norm: set[str] = set()
    ts_skipped = 0
    for entry in all_entries:
        url = entry["url"]
        if not looks_like_article_url(url, args.host):
            continue
        if url in seen_norm:
            continue
        seen_norm.add(url)
        # Pre-filter by CDX timestamp to skip obviously old entries
        ts = entry.get("timestamp", "")
        if ts and (ts < ts_from or ts > ts_to):
            ts_skipped += 1
            continue
        article_entries.append(entry)
    # Sort by timestamp so we process newest first (most likely to be in range)
    article_entries.sort(key=lambda e: e.get("timestamp", ""), reverse=True)
    log.info(f"  candidate article URLs after filtering: {len(article_entries)}"
             f"  (CDX timestamp pre-skipped: {ts_skipped})")

    # ── 3. Checkpoint & scrape loop ──
    checkpoint = Checkpoint(out_path)
    checkpoint.open()

    scraped = 0
    skipped = 0
    rejected = 0
    paywalled_skipped = 0
    processed = 0
    wayback_hits = 0

    def _try_fetch_article(session, url, timestamp, include_paywalled):
        """Try live URL first; if no article found, try Wayback snapshot.
        Returns (parsed_dict, source_label) or (None, None)."""
        # 1) Live fetch
        resp = polite_get(session, url)
        if resp is not None:
            parsed = extract_jsonld_article(resp.text)
            if parsed.get("headline"):
                return parsed, "live"

        # 2) Wayback Machine fallback — use id_ flag to get original HTML
        if timestamp:
            wb_url = f"https://web.archive.org/web/{timestamp}id_/{url}"
            resp_wb = polite_get(session, wb_url)
            if resp_wb is not None:
                parsed_wb = extract_jsonld_article(resp_wb.text)
                if parsed_wb.get("headline"):
                    return parsed_wb, "wayback"

        return None, None

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
                log.info(f"  fetching [{processed}]: {url[:80]}")
            if processed % 500 == 0:
                log.info(f"  progress: processed={processed}  scraped={scraped}  "
                         f"skipped={skipped}  rejected={rejected}  "
                         f"wayback_hits={wayback_hits}")

            parsed, source = _try_fetch_article(
                session, url, timestamp, args.include_paywalled
            )
            if parsed is None:
                rejected += 1
                continue

            if source == "wayback":
                wayback_hits += 1

            if not parsed.get("isAccessibleForFree", True) and not args.include_paywalled:
                paywalled_skipped += 1
                log.debug(f"  paywalled, skipping: {url}")
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
                    f"paywalled={paywalled_skipped}  rejected={rejected}  "
                    f"wayback_hits={wayback_hits}"
                )

            if args.limit and scraped >= args.limit:
                log.info(f"  reached --limit {args.limit}, stopping")
                break
    finally:
        checkpoint.close()

    log.info(
        f"  done.  scraped={scraped}  skipped={skipped}  "
        f"paywalled={paywalled_skipped}  rejected={rejected}  "
        f"wayback_hits={wayback_hits}"
    )
    log.info(f"  CSV written to: {out_path}")


if __name__ == "__main__":
    main()
