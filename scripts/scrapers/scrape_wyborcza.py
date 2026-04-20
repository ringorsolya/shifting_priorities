"""
Gazeta Wyborcza (wyborcza.pl) scraper — public content only.

GW is largely paywalled. This scraper collects what is publicly accessible:
  - headline
  - lead / first paragraph ("first_sentence")
  - full article body ONLY when the page serves it in the public HTML
  - publication date

For paywalled articles, the body will be empty and the article is SKIPPED
by default (--keep-paywalled to still record title + lead).

Covers the gap 2023-11-09 → 2024-02-23 (or any date range you supply).
Uses robots.txt-aware requests, ~2s polite delay, resumable CSV output.

Produces a CSV with the same schema as PL_M_gazetawyborcza_document_level_with_preds2.csv.
The ML-derived columns (CAP codes, sentiment, NER) are left empty — run them
through your existing ML pipeline afterwards.

USAGE
-----
    pip install -r requirements.txt
    python scrape_wyborcza.py --start 2023-11-09 --end 2024-02-23 \\
        --out ../../data/wyborcza_supplement.csv

Optional:
    --sitemap https://wyborcza.pl/sitemap.xml   # root sitemap URL override
    --limit 500                                  # cap total articles
    --keep-paywalled                             # still write rows with empty body
    --no-robots                                  # skip robots.txt check (discouraged)

LEGAL / ETHICAL NOTES
---------------------
 - Only public content is collected; no bypassing of the paywall.
 - Academic, non-commercial research purpose declared in User-Agent.
 - robots.txt is respected by default.
 - Rate limiting: ~2s polite delay between requests.
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent))

from scraper_utils import (
    Article, Checkpoint, DEFAULT_UA,
    clean_text, first_sentence, log,
    load_robots, make_document_id, make_session, parse_date_str,
    polite_get, probe_sitemaps, robots_allows, walk_sitemaps,
)

PORTAL_NAME = "Gazeta Wyborcza"
PORTAL_CODE = "PL_gazetawyborcza"
ILLIBERAL = "liberal"
ROOT_SITEMAP = "https://wyborcza.pl/sitemap.xml"
BASE_HOST = "wyborcza.pl"


# ─────────────────────────────────────────────────────────────────
# Paywall detection
# ─────────────────────────────────────────────────────────────────
PAYWALL_MARKERS = (
    "artykuł tylko dla prenumeratorów",
    "zaloguj się, aby przeczytać",
    "wykup prenumerat",
    "dostęp cyfrowy",
    "dla prenumeratorów",
    "premium",
)


def is_paywalled(soup: BeautifulSoup, body_text: str) -> bool:
    """Heuristic paywall check for GW."""
    # Meta flag used by some CMS deployments
    paid = soup.find("meta", {"name": "content-access"})
    if paid and "paid" in (paid.get("content") or "").lower():
        return True
    meta_paywall = soup.find("meta", {"property": "article:content_tier"})
    if meta_paywall and "premium" in (meta_paywall.get("content") or "").lower():
        return True

    # Visible marker in the body
    lower = body_text.lower()
    if any(m in lower for m in PAYWALL_MARKERS):
        return True

    # Very short body is typical for the public teaser
    if len(body_text) < 300:
        return True

    return False


# ─────────────────────────────────────────────────────────────────
# HTML extraction
# ─────────────────────────────────────────────────────────────────
def extract_article(html: str, url: str, keep_paywalled: bool = False) -> Article | None:
    soup = BeautifulSoup(html, "lxml")

    title = ""
    date_str = ""
    lead = ""
    text = ""

    # ── JSON-LD ──
    import json
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            payload = json.loads(script.string or "{}")
        except (ValueError, TypeError):
            continue
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
            desc = obj.get("description") or ""
            if body:
                text = body
            if desc and not lead:
                lead = desc
            break
        if title and date_str:
            break

    # ── Meta tags ──
    if not title:
        og = soup.find("meta", {"property": "og:title"})
        if og and og.get("content"):
            title = og["content"]
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

    if not lead:
        og_desc = soup.find("meta", {"property": "og:description"})
        if og_desc and og_desc.get("content"):
            lead = og_desc["content"]
        else:
            meta_desc = soup.find("meta", {"name": "description"})
            if meta_desc and meta_desc.get("content"):
                lead = meta_desc["content"]

    # ── Body fallback ──
    if not text:
        container = (
            soup.find("article")
            or soup.find("div", class_=lambda c: c and "article" in c.lower())
            or soup.find("main")
        )
        if container is not None:
            for bad in container.find_all(["script", "style", "aside", "figure", "figcaption", "nav"]):
                bad.decompose()
            paragraphs = [p.get_text(" ", strip=True) for p in container.find_all("p")]
            text = "\n".join(p for p in paragraphs if p)

    # ── Clean & paywall check ──
    title = clean_text(title)
    lead = clean_text(lead)
    text = clean_text(text)
    d = parse_date_str(date_str)
    iso_date = d.isoformat() if d else ""

    paywalled = is_paywalled(soup, text)

    if not (title and iso_date):
        log.debug(f"  missing title/date: {url}")
        return None

    if paywalled:
        if not keep_paywalled:
            log.debug(f"  paywalled, skipping: {url}")
            return None
        # Keep with lead only
        text = lead or text
        if not text:
            return None

    if not text:
        log.debug(f"  empty body: {url}")
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
    Accept real article URLs on the wyborcza.pl domain.

    GW URLs look like:
        https://wyborcza.pl/7,75398,30123456,slug-slug.html
        https://wyborcza.pl/magazyn/7,124059,30123456,...
    """
    bad_parts = (
        "/tag,", "/tag/", "/autor,", "/autor/", "/redakcja",
        "/regulamin", "/kontakt", "/polityka-prywatnosci",
        "/galeria", "/wideo", "/video",
    )
    lower = url.lower()
    if BASE_HOST not in lower:
        return False
    if any(b in lower for b in bad_parts):
        return False
    # GW article URLs contain comma-separated numeric ids and end in .html
    return (".html" in lower) and bool(re.search(r",\d{4,},", lower))


# ─────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Scrape public content from Gazeta Wyborcza (wyborcza.pl).")
    ap.add_argument("--start", required=True, help="start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="end date YYYY-MM-DD (inclusive)")
    ap.add_argument("--out", required=True, help="output CSV path")
    ap.add_argument("--sitemap", default=ROOT_SITEMAP, help="root sitemap URL")
    ap.add_argument("--limit", type=int, default=0, help="max articles to scrape (0 = no limit)")
    ap.add_argument("--keep-paywalled", action="store_true",
                    help="write rows for paywalled articles (title + lead only)")
    ap.add_argument("--no-robots", action="store_true", help="skip robots.txt check")
    args = ap.parse_args()

    date_from = datetime.strptime(args.start, "%Y-%m-%d").date()
    date_to = datetime.strptime(args.end, "%Y-%m-%d").date()
    out_path = Path(args.out)

    log.info(f"  Gazeta Wyborcza scraper — {date_from} → {date_to}")
    log.info(f"  output: {out_path}")
    log.info(f"  keep paywalled: {args.keep_paywalled}")

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
    paywalled_skipped = 0

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

            article = extract_article(resp.text, url, keep_paywalled=args.keep_paywalled)
            if article is None:
                paywalled_skipped += 1
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
                    f"paywalled={paywalled_skipped}  rejected={rejected}"
                )

            if args.limit and scraped >= args.limit:
                log.info(f"  reached --limit {args.limit}, stopping")
                break
    finally:
        checkpoint.close()

    log.info(
        f"  done.  scraped={scraped}  skipped={skipped}  "
        f"paywalled={paywalled_skipped}  rejected={rejected}"
    )
    log.info(f"  CSV written to: {out_path}")


if __name__ == "__main__":
    main()
