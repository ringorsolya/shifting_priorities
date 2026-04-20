"""
iDnes.cz / MF Dnes scraper (illiberal, MAFRA) — sitemap-based.

Covers the extended research period 2022-02-01 → 2026-02-23.

Produces a CSV with the same schema as CZ_M_mfdnes_document_level_with_preds.csv.
portal="MF Dnes", illiberal="illiberal".

NOTE: iDnes article URLs have a distinctive `.A<YYMMDD>_<HHMMSS>_...` ID
format embedded in the path, e.g.:
    https://www.idnes.cz/zpravy/zahranicni/rusko-ukrajina.A240401_123456_zahranicni_aha
We use that signature for the URL filter.

USAGE
-----
    python scrape_idnes.py --start 2022-02-01 --end 2026-02-23 \\
        --out ../../data/idnes_supplement.csv
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
    "/diskuse/", "/diskuze/", "/foto/", "/galerie/", "/video/",
    "/tema/", "/tagy/", "/autor/", "/redakce", "/reklama",
    "/podminky", "/predplatne", "/newsletter", "/rss", "/hry/",
    "/soutez/", "/archiv/",
)

# Canonical iDnes article path ends in ".A<YYMMDD>_<HHMMSS>_..."
_ARTICLE_RE = re.compile(r"\.A\d{6}_\d{6}_", re.IGNORECASE)


def is_article_url(url: str) -> bool:
    lower = url.lower().split("?", 1)[0].split("#", 1)[0]
    if "idnes.cz" not in lower:
        return False
    # Skip sports / lifestyle subdomains — MF Dnes corpus is news-focused.
    for host_skip in ("isport.blesk.cz", "fotbal.idnes.cz", "hokej.idnes.cz",
                      "onadnes.cz", "bazar.idnes.cz", "reality.idnes.cz",
                      "auto.idnes.cz", "bydleni.idnes.cz", "mobil.idnes.cz",
                      "technet.idnes.cz", "hry.idnes.cz", "rajce.idnes.cz"):
        if host_skip in lower:
            return False
    if any(b in lower for b in BAD_PARTS):
        return False
    return bool(_ARTICLE_RE.search(lower))


def main():
    ap = argparse.ArgumentParser(description="Scrape iDnes.cz via sitemaps.")
    ap.add_argument("--start", required=True, help="start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="end date YYYY-MM-DD (inclusive)")
    ap.add_argument("--out", required=True, help="output CSV path")
    ap.add_argument("--sitemap", default="https://www.idnes.cz/sitemap.xml",
                    help="root sitemap URL")
    ap.add_argument("--limit", type=int, default=0,
                    help="max articles to scrape (0 = no limit)")
    ap.add_argument("--no-robots", action="store_true", help="skip robots.txt check")
    args = ap.parse_args()

    config = PortalConfig(
        portal_name="MF Dnes",
        portal_code="CZ_mfdnes",
        illiberal="illiberal",
        base_host="www.idnes.cz",
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
