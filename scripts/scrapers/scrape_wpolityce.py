"""
wPolityce.pl scraper — polite sitemap-based crawler for the missing period.

Covers the gap 2023-08-13 → 2024-02-23 (or any date range you supply).
Uses robots.txt-aware requests, ~1.5s polite delay, resumable CSV output.

Produces a CSV with the same schema as PL_M_wpolityce_document_level_with_preds.csv.
The ML-derived columns (CAP codes, sentiment, NER) are left empty — run them
through your existing ML pipeline afterwards.

USAGE
-----
    pip install -r requirements.txt
    python scrape_wpolityce.py --start 2023-08-13 --end 2024-02-23 \\
        --out ../../data/wpolityce_supplement.csv

Optional:
    --sitemap https://wpolityce.pl/sitemap.xml   # root sitemap URL override
    --limit 500                                   # cap total articles
    --no-robots                                   # skip robots.txt check (discouraged)
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, date, timezone
from pathlib import Path

from bs4 import BeautifulSoup

# Allow "python scrape_wpolityce.py" from inside scrapers/ folder
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scraper_utils import (
    Article, Checkpoint, DEFAULT_UA,
    clean_text, first_sentence, log,
    load_robots, make_document_id, make_session, parse_date_str,
    polite_get, probe_sitemaps, robots_allows, walk_sitemaps,
)

PORTAL_NAME = "wPolityce"
PORTAL_CODE = "PL_wpolityce"
ILLIBERAL = "illiberal"
# wpolityce.pl/sitemap.xml 301-redirects to the media CDN; use the CDN URL
# directly to avoid HEAD-probe quirks on the origin.
ROOT_SITEMAP = "https://media.wpolityce.pl/sitemaps/https/index.xml"
BASE_HOST = "wpolityce.pl"


# ─────────────────────────────────────────────────────────────────
# HTML extraction
# ─────────────────────────────────────────────────────────────────
def extract_article(html: str, url: str) -> Article | None:
    """
    Extract structured fields from a wPolityce article page.

    Strategy:
    1. Try JSON-LD (NewsArticle / Article) — most reliable.
    2. Fall back to OpenGraph / meta tags.
    3. Fall back to visible <article>, <h1>, <p> elements.
    """
    soup = BeautifulSoup(html, "lxml")

    title = ""
    date_str = ""
    text = ""

    # ── 1. JSON-LD ──
    import json
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            payload = json.loads(script.string or "{}")
        except (ValueError, TypeError):
            continue
        # Sometimes a list of objects
        candidates = payload if isinstance(payload, list) else [payload]
        for obj in candidates:
            if not isinstance(obj, dict):
                continue
            t = obj.get("@type", "")
            if isinstance(t, list):
                t = next((x for x in t if "Article" in x), "")
            if "Article" not in str(t):
                continue
            title = title or obj.get("headline", "") or ""
            date_str = date_str or obj.get("datePublished", "") or ""
            body = obj.get("articleBody") or ""
            if body:
                text = body
            break
        if title and date_str and text:
            break

    # ── 2. Meta tags ──
    if not title:
        og_title = soup.find("meta", {"property": "og:title"})
        if og_title and og_title.get("content"):
            title = og_title["content"]
    if not title and soup.find("h1"):
        title = soup.find("h1").get_text(strip=True)

    if not date_str:
        for prop in ("article:published_time", "og:article:published_time"):
            m = soup.find("meta", {"property": prop})
            if m and m.get("content"):
                date_str = m["content"]
                break
    if not date_str:
        time_tag = soup.find("time")
        if time_tag and time_tag.get("datetime"):
            date_str = time_tag["datetime"]

    # ── 3. Body fallback ──
    if not text:
        # wPolityce markup: <div class="article__content"> or <div class="news-content">
        container = (
            soup.find("div", class_=lambda c: c and "article" in c.lower() and "content" in c.lower())
            or soup.find("article")
            or soup.find("div", class_=lambda c: c and "news" in c.lower() and "content" in c.lower())
            or soup.find("main")
        )
        if container is not None:
            for bad in container.find_all(["script", "style", "aside", "figure", "figcaption"]):
                bad.decompose()
            paragraphs = [p.get_text(" ", strip=True) for p in container.find_all("p")]
            text = "\n".join(p for p in paragraphs if p)

    # ── Clean & validate ──
    title = clean_text(title)
    text = clean_text(text)
    d = parse_date_str(date_str)
    iso_date = d.isoformat() if d else ""

    if not (title and text and iso_date):
        log.debug(f"  skipping incomplete article: {url}")
        return None

    article = Article(
        document_id=make_document_id(PORTAL_CODE, iso_date, url),
        document_title=title,
        first_sentence=first_sentence(text),
        document_text=text,
        date=iso_date,
        portal=PORTAL_NAME,
        illiberal=ILLIBERAL,
        url=url,
        scraped_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )
    return article


# ─────────────────────────────────────────────────────────────────
# URL filtering
# ─────────────────────────────────────────────────────────────────
def looks_like_article_url(url: str) -> bool:
    """
    Keep real article URLs, drop tag/category/author/static pages.

    wPolityce article URLs look like:
        https://wpolityce.pl/polityka/<id>-<slug>
        https://wpolityce.pl/spoleczenstwo/<id>-<slug>
        https://wpolityce.pl/swiat/<id>-<slug>
    """
    bad_parts = (
        "/tag/", "/tagi/", "/autor/", "/author/", "/kategoria/",
        "/galeria/", "/video/", "/wideo/", "/forum/", "/szukaj/",
        "/regulamin", "/kontakt", "/redakcja", "/polityka-prywatnosci",
    )
    lower = url.lower()
    if not lower.startswith("https://wpolityce.pl/") and not lower.startswith("http://wpolityce.pl/"):
        return False
    if any(b in lower for b in bad_parts):
        return False
    # wPolityce article URLs contain a numeric id after the section
    # e.g. /polityka/678123-title-slug
    import re
    return bool(re.search(r"/\d{4,}-", lower))


# ─────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Scrape wPolityce.pl via sitemaps.")
    ap.add_argument("--start", required=True, help="start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="end date YYYY-MM-DD (inclusive)")
    ap.add_argument("--out", required=True, help="output CSV path")
    ap.add_argument("--sitemap", default=ROOT_SITEMAP, help="root sitemap URL")
    ap.add_argument("--limit", type=int, default=0, help="max articles to scrape (0 = no limit)")
    ap.add_argument("--no-robots", action="store_true", help="skip robots.txt check")
    args = ap.parse_args()

    date_from = datetime.strptime(args.start, "%Y-%m-%d").date()
    date_to = datetime.strptime(args.end, "%Y-%m-%d").date()
    out_path = Path(args.out)

    log.info(f"  wPolityce scraper — {date_from} → {date_to}")
    log.info(f"  output: {out_path}")

    session = make_session(DEFAULT_UA)
    rp = None
    if not args.no_robots:
        rp = load_robots(f"https://{BASE_HOST}/", DEFAULT_UA, session=session)

    # Sitemap discovery with fallback
    sitemap_urls = [args.sitemap] if args.sitemap else []
    if sitemap_urls:
        try:
            r = session.head(sitemap_urls[0], timeout=15, allow_redirects=True)
            if r.status_code != 200:
                log.warning(f"  provided sitemap returned HTTP {r.status_code}, falling back to discovery")
                sitemap_urls = probe_sitemaps(session, BASE_HOST, DEFAULT_UA)
        except Exception as e:
            log.warning(f"  provided sitemap failed: {e}, falling back to discovery")
            sitemap_urls = probe_sitemaps(session, BASE_HOST, DEFAULT_UA)
    else:
        sitemap_urls = probe_sitemaps(session, BASE_HOST, DEFAULT_UA)

    if not sitemap_urls:
        log.error("  no usable sitemap URL found — aborting")
        return
    log.info(f"  using sitemap URL(s): {sitemap_urls}")

    checkpoint = Checkpoint(out_path)
    checkpoint.open()

    scraped = 0
    skipped = 0
    rejected = 0

    def iter_all_sitemaps():
        for root in sitemap_urls:
            yield from walk_sitemaps(session, root, date_from, date_to)

    try:
        for entry in iter_all_sitemaps():
            url = entry["loc"]

            if not looks_like_article_url(url):
                continue
            if url in checkpoint.seen_urls:
                skipped += 1
                continue
            if rp is not None and not robots_allows(rp, url, DEFAULT_UA):
                log.debug(f"  robots.txt disallows: {url}")
                rejected += 1
                continue

            resp = polite_get(session, url)
            if resp is None:
                rejected += 1
                continue

            article = extract_article(resp.text, url)
            if article is None:
                rejected += 1
                continue

            # Sitemap date may differ from parsed body date — re-check window
            d = parse_date_str(article.date)
            if d and not (date_from <= d <= date_to):
                skipped += 1
                continue

            checkpoint.write(article)
            scraped += 1
            if scraped % 25 == 0:
                log.info(f"  scraped={scraped}  skipped={skipped}  rejected={rejected}")

            if args.limit and scraped >= args.limit:
                log.info(f"  reached --limit {args.limit}, stopping")
                break
    finally:
        checkpoint.close()

    log.info(f"  done.  scraped={scraped}  skipped={skipped}  rejected={rejected}")
    log.info(f"  CSV written to: {out_path}")


if __name__ == "__main__":
    main()
