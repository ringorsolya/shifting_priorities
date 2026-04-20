"""
Novinky.cz scraper (liberal, Seznam) — sitemap-based.

Covers the extended research period 2022-02-01 → 2026-02-23, overlapping
with the existing corpus (dedupe during merge).

Produces a CSV with the same schema as CZ_M_novinky_document_level_with_preds.csv.

USAGE
-----
    python scrape_novinky.py --start 2022-02-01 --end 2026-02-23 \\
        --out ../../data/novinky_supplement.csv
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from scraper_utils import PortalConfig, run_portal_scrape

BAD_PARTS = (
    "/tag/", "/tagy/", "/autor/", "/rubrika/", "/rubrika-",
    "/video/", "/foto/", "/galerie/", "/diskuze/", "/hledej/",
    "/podminky", "/kontakt", "/redakce", "/reklama", "/o-novinkach",
    "/predplatne", "/newsletter", "/rss",
)

# Novinky.cz article URLs usually end with a long numeric id, e.g.
# https://www.novinky.cz/clanek/zahranicni-evropa-volby-40562890
_ARTICLE_RE = re.compile(r"-\d{6,}(?:/|$)")


def is_article_url(url: str) -> bool:
    lower = url.lower().split("?", 1)[0].split("#", 1)[0]
    if "novinky.cz" not in lower:
        return False
    if any(b in lower for b in BAD_PARTS):
        return False
    return bool(_ARTICLE_RE.search(lower))


def main():
    ap = argparse.ArgumentParser(description="Scrape Novinky.cz via sitemaps.")
    ap.add_argument("--start", required=True, help="start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="end date YYYY-MM-DD (inclusive)")
    ap.add_argument("--out", required=True, help="output CSV path")
    ap.add_argument("--sitemap", default="https://www.novinky.cz/sitemap.xml",
                    help="root sitemap URL")
    ap.add_argument("--limit", type=int, default=0,
                    help="max articles to scrape (0 = no limit)")
    ap.add_argument("--no-robots", action="store_true", help="skip robots.txt check")
    args = ap.parse_args()

    config = PortalConfig(
        portal_name="Novinky",
        portal_code="CZ_novinky",
        illiberal="liberal",
        base_host="www.novinky.cz",
        root_sitemap=args.sitemap,
        accept_language="cs;q=0.9,en;q=0.7",
        is_article_url=is_article_url,
    )

    run_portal_scrape(
        config,
        date_from=datetime.strptime(args.start, "%Y-%m-%d").date(),
        date_to=datetime.strptime(args.end, "%Y-%m-%d").date(),
        out_path=Path(args.out),
        limit=args.limit,
        check_robots=not args.no_robots,
        sitemap_override=args.sitemap,
    )


if __name__ == "__main__":
    main()
