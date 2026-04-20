"""
Pravda.sk scraper (illiberal) — sitemap-based.

Covers the gap 2024-03-23 → 2026-02-23 (or any date range you supply).

Produces a CSV with the same schema as SK_M_pravda_document_level_with_preds.csv.
portal="Pravda", illiberal="illiberal".

NOTE: Pravda.sk news content lives historically at spravy.pravda.sk.
If that subdomain is unreachable from your network, fall back to
www.pravda.sk with `--host www.pravda.sk`.

Article URL patterns:
    https://spravy.pravda.sk/svet/clanok/700001-valka-ukrajina/
    https://www.pravda.sk/spravy/svet/clanok/700001-valka-ukrajina/

USAGE
-----
    python scrape_pravda.py --start 2024-03-23 --end 2026-02-23 \\
        --out ../../data/pravda_supplement.csv
"""

from __future__ import annotations

import argparse
import re
import socket
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from scraper_utils import PortalConfig, log, run_portal_scrape

BAD_PARTS = (
    "/tag/", "/tagy/", "/autor/", "/redakcia", "/reklama",
    "/video/", "/galeria/", "/foto/", "/diskusia/", "/hlasovania/",
    "/kontakt", "/podmienky", "/predplatne", "/newsletter", "/rss",
    "/sutaz/", "/hry/", "/temy/",
)

# Pravda.sk article URL: /<section>/clanok/<id>-<slug>
_ARTICLE_RE = re.compile(r"/clanok/\d{4,}-", re.IGNORECASE)


def is_article_url(url: str) -> bool:
    lower = url.lower().split("?", 1)[0].split("#", 1)[0]
    if "pravda.sk" not in lower:
        return False
    if any(b in lower for b in BAD_PARTS):
        return False
    return bool(_ARTICLE_RE.search(lower))


def _host_resolves(host: str) -> bool:
    try:
        socket.gethostbyname(host)
        return True
    except OSError:
        return False


def _pick_host(preferred: str, fallback: str = "www.pravda.sk") -> tuple[str, str]:
    """Return (host, sitemap) picking a host that resolves via DNS."""
    if _host_resolves(preferred):
        return preferred, f"https://{preferred}/sitemap.xml"
    log.warning(f"  DNS: {preferred} does not resolve — falling back to {fallback}")
    if _host_resolves(fallback):
        return fallback, f"https://{fallback}/sitemap.xml"
    log.error(f"  DNS: neither {preferred} nor {fallback} resolves — aborting")
    raise SystemExit(2)


def main():
    ap = argparse.ArgumentParser(description="Scrape Pravda.sk via sitemaps.")
    ap.add_argument("--start", required=True, help="start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="end date YYYY-MM-DD (inclusive)")
    ap.add_argument("--out", required=True, help="output CSV path")
    ap.add_argument("--sitemap", default="",
                    help="root sitemap URL (default: derived from --host)")
    ap.add_argument("--host", default="spravy.pravda.sk",
                    help="base host (default: spravy.pravda.sk; falls back to "
                         "www.pravda.sk if the default doesn't resolve)")
    ap.add_argument("--limit", type=int, default=0,
                    help="max articles to scrape (0 = no limit)")
    ap.add_argument("--no-robots", action="store_true", help="skip robots.txt check")
    args = ap.parse_args()

    # Pick a host that actually resolves on this machine.
    host, auto_sitemap = _pick_host(args.host)
    sitemap = args.sitemap or auto_sitemap

    config = PortalConfig(
        portal_name="Pravda",
        portal_code="SK_pravda",
        illiberal="illiberal",
        base_host=host,
        root_sitemap=sitemap,
        accept_language="sk;q=0.9,cs;q=0.7,en;q=0.5",
        is_article_url=is_article_url,
    )

    run_portal_scrape(
        config,
        date_from=datetime.strptime(args.start, "%Y-%m-%d").date(),
        date_to=datetime.strptime(args.end, "%Y-%m-%d").date(),
        out_path=Path(args.out),
        limit=args.limit,
        check_robots=not args.no_robots,
        sitemap_override=sitemap,
    )


if __name__ == "__main__":
    main()
