"""
Diagnostic: fetch a single sub-sitemap and report how many of its URL
entries carry news:publication_date or lastmod.

If a sitemap has per-URL dates, walk_sitemaps() can date-filter them
cheaply — the scraper is fast.
If it doesn't, every URL has to be fetched and parsed, which is slow.

USAGE
-----
    python check_sitemap_dates.py <SITEMAP_URL>

Example:
    python check_sitemap_dates.py https://www.novinky.cz/sitemaps/sitemap_articles_0.xml
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from scraper_utils import fetch_sitemap, make_session, DEFAULT_UA
from bs4 import BeautifulSoup


def main(url: str):
    session = make_session(DEFAULT_UA)
    xml = fetch_sitemap(session, url)
    if xml is None:
        print(f"FAIL: cannot fetch {url}")
        return 1
    soup = BeautifulSoup(xml, "lxml-xml")

    urls = soup.find_all("url")
    total = len(urls)
    have_news = sum(1 for u in urls if u.find("news:publication_date") or u.find("publication_date"))
    have_lastmod = sum(1 for u in urls if u.find("lastmod"))
    have_any = sum(1 for u in urls if u.find("lastmod") or u.find("news:publication_date") or u.find("publication_date"))

    print(f"sitemap: {url}")
    print(f"  total <url> entries      : {total}")
    print(f"  have <news:publication_date>: {have_news}")
    print(f"  have <lastmod>           : {have_lastmod}")
    print(f"  have any date            : {have_any} "
          f"({100 * have_any / total:.1f}% if total else '--')")

    if have_any < total * 0.5:
        print()
        print("  ⚠  Less than half of entries carry a date.")
        print("     walk_sitemaps() will have to yield everything,")
        print("     and the scraper will fetch ALL URLs to check dates.")
        print("     For a 1-day test this may take many hours.")
    else:
        print()
        print("  ✓  Dates available — date-filtering during sitemap walk is cheap.")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)
    sys.exit(main(sys.argv[1]))
