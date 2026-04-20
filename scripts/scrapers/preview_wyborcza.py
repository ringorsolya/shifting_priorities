"""
Gazeta Wyborcza — diagnostic preview (no subscription).

Runs a SMALL sample against the GW sitemap to show what content is actually
served publicly vs. what is behind the paywall. Produces:

  1. A console report with counts, averages, and samples.
  2. A CSV with every fetched article tagged as 'public' / 'paywalled' / 'incomplete',
     including title, lead, body length, and URL — so you can inspect manually.

Intended as a dry run BEFORE committing to a full scrape.

USAGE
-----
    python preview_wyborcza.py --start 2024-01-01 --end 2024-01-31 \\
        --limit 30 --out preview_wyborcza.csv

The script NEVER attempts to bypass the paywall. It simply records what the
portal serves to an anonymous client.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from statistics import mean, median

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent))

from scraper_utils import (
    DEFAULT_UA, clean_text, log,
    load_robots, make_session, parse_date_str,
    polite_get, probe_sitemaps, robots_allows, walk_sitemaps,
)

ROOT_SITEMAP = "https://wyborcza.pl/sitemap.xml"
BASE_HOST = "wyborcza.pl"

PAYWALL_MARKERS = (
    "artykuł tylko dla prenumeratorów",
    "zaloguj się, aby przeczytać",
    "wykup prenumerat",
    "dostęp cyfrowy",
    "dla prenumeratorów",
)


def classify(html: str) -> dict:
    """Return a record describing what we can see on this page."""
    soup = BeautifulSoup(html, "lxml")

    title = ""
    date_str = ""
    lead = ""
    body = ""
    has_jsonld_body = False

    # JSON-LD
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
            b = obj.get("articleBody") or ""
            if b:
                body = b
                has_jsonld_body = True
            desc = obj.get("description") or ""
            if desc and not lead:
                lead = desc
            break

    # OG fallback
    if not title:
        og = soup.find("meta", {"property": "og:title"})
        if og and og.get("content"):
            title = og["content"]
    if not lead:
        og_desc = soup.find("meta", {"property": "og:description"})
        if og_desc and og_desc.get("content"):
            lead = og_desc["content"]

    # Body fallback
    if not body:
        container = soup.find("article") or soup.find("main")
        if container is not None:
            for bad in container.find_all(["script", "style", "aside", "figure", "figcaption", "nav"]):
                bad.decompose()
            paragraphs = [p.get_text(" ", strip=True) for p in container.find_all("p")]
            body = "\n".join(p for p in paragraphs if p)

    title = clean_text(title)
    lead = clean_text(lead)
    body = clean_text(body)
    d = parse_date_str(date_str)
    iso_date = d.isoformat() if d else ""

    lower = body.lower()
    paywall_marker_hit = next((m for m in PAYWALL_MARKERS if m in lower), "")

    # Meta-based paywall flag
    meta_tier = soup.find("meta", {"property": "article:content_tier"})
    tier = (meta_tier.get("content") if meta_tier else "") or ""

    # Classification heuristic
    if paywall_marker_hit or "premium" in tier.lower():
        status = "paywalled"
    elif len(body) < 300:
        status = "paywalled"  # very short = likely teaser
    elif title and body and iso_date:
        status = "public"
    else:
        status = "incomplete"

    return {
        "status": status,
        "title": title,
        "date": iso_date,
        "lead": lead,
        "body_chars": len(body),
        "lead_chars": len(lead),
        "has_jsonld_body": has_jsonld_body,
        "paywall_marker": paywall_marker_hit,
        "content_tier_meta": tier,
        "body_preview": body[:300],
    }


def looks_like_article_url(url: str) -> bool:
    lower = url.lower()
    if BASE_HOST not in lower:
        return False
    bad = ("/tag,", "/tag/", "/autor,", "/autor/", "/redakcja", "/regulamin",
           "/kontakt", "/polityka-prywatnosci", "/galeria", "/wideo", "/video")
    if any(b in lower for b in bad):
        return False
    return (".html" in lower) and bool(re.search(r",\d{4,},", lower))


def main():
    ap = argparse.ArgumentParser(description="Preview what Gazeta Wyborcza serves publicly.")
    ap.add_argument("--start", required=True, help="start date YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="end date YYYY-MM-DD")
    ap.add_argument("--limit", type=int, default=30, help="sample size (default 30)")
    ap.add_argument("--out", default="preview_wyborcza.csv", help="output CSV path")
    ap.add_argument("--sitemap", default=ROOT_SITEMAP)
    ap.add_argument("--no-robots", action="store_true")
    args = ap.parse_args()

    date_from = datetime.strptime(args.start, "%Y-%m-%d").date()
    date_to = datetime.strptime(args.end, "%Y-%m-%d").date()
    out_path = Path(args.out)

    log.info(f"  GW preview — {date_from} → {date_to}  sample={args.limit}")

    session = make_session(DEFAULT_UA)
    rp = None if args.no_robots else load_robots(f"https://{BASE_HOST}/", DEFAULT_UA, session=session)

    # ── Sitemap discovery ──
    sitemap_urls: list[str]
    if args.sitemap:
        # Still verify the explicitly-requested one is reachable
        log.info(f"  testing provided sitemap: {args.sitemap}")
        try:
            r = session.head(args.sitemap, timeout=15, allow_redirects=True)
            if r.status_code != 200:
                log.warning(f"  provided sitemap returned HTTP {r.status_code}, falling back to discovery")
                sitemap_urls = probe_sitemaps(session, BASE_HOST, DEFAULT_UA)
            else:
                sitemap_urls = [args.sitemap]
        except Exception as e:
            log.warning(f"  provided sitemap failed: {e}, falling back to discovery")
            sitemap_urls = probe_sitemaps(session, BASE_HOST, DEFAULT_UA)
    else:
        sitemap_urls = probe_sitemaps(session, BASE_HOST, DEFAULT_UA)

    if not sitemap_urls:
        log.error("  no usable sitemap URL found — check the site manually or pass --sitemap.")
        log.error(f"  try opening https://{BASE_HOST}/robots.txt in your browser and look for 'Sitemap:' lines.")
        return

    log.info(f"  using {len(sitemap_urls)} sitemap URL(s)")

    records = []
    tried = 0

    def iter_all_sitemaps():
        for root in sitemap_urls:
            log.info(f"  walking sitemap: {root}")
            yield from walk_sitemaps(session, root, date_from, date_to)

    for entry in iter_all_sitemaps():
        url = entry["loc"]
        if not looks_like_article_url(url):
            continue
        if rp is not None and not robots_allows(rp, url, DEFAULT_UA):
            continue

        resp = polite_get(session, url)
        tried += 1
        if resp is None:
            records.append({"url": url, "status": "http_error", "title": "", "date": "",
                            "body_chars": 0, "lead_chars": 0, "lead": "",
                            "has_jsonld_body": False, "paywall_marker": "",
                            "content_tier_meta": "", "body_preview": ""})
        else:
            rec = classify(resp.text)
            rec["url"] = url
            records.append(rec)

        if tried >= args.limit:
            break

    # ── Report ──
    if not records:
        log.warning("  no records — check sitemap URL / date range / URL filter")
        return

    by_status = {}
    for r in records:
        by_status.setdefault(r["status"], []).append(r)

    total = len(records)
    log.info("")
    log.info("══════ SUMMARY ══════")
    log.info(f"  total fetched:   {total}")
    for s in ("public", "paywalled", "incomplete", "http_error"):
        n = len(by_status.get(s, []))
        if n:
            log.info(f"  {s:12s}:  {n:3d}  ({100*n/total:4.1f}%)")

    for s in ("public", "paywalled"):
        rs = by_status.get(s, [])
        if not rs:
            continue
        body_lens = [r["body_chars"] for r in rs]
        lead_lens = [r["lead_chars"] for r in rs]
        log.info("")
        log.info(f"  {s.upper()} — body chars:  "
                 f"mean={mean(body_lens):.0f}  median={median(body_lens):.0f}  "
                 f"min={min(body_lens)}  max={max(body_lens)}")
        log.info(f"  {s.upper()} — lead chars:  "
                 f"mean={mean(lead_lens):.0f}  median={median(lead_lens):.0f}")
        log.info(f"  sample titles:")
        for r in rs[:5]:
            log.info(f"    • {r['date']}  {r['title'][:90]}")

    # ── CSV ──
    fieldnames = ["url", "status", "date", "title", "body_chars", "lead_chars",
                  "has_jsonld_body", "paywall_marker", "content_tier_meta",
                  "lead", "body_preview"]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in records:
            w.writerow(r)

    log.info("")
    log.info(f"  details written to: {out_path}")
    log.info(f"  open it to inspect each URL's status, title, lead, and body preview.")


if __name__ == "__main__":
    main()
