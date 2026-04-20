"""
Quick diagnostic: test article extraction for Telex.hu and MagyarNemzet.hu.

1) Finds a real article URL (from RSS for Telex, sitemap for MN)
2) Fetches it live
3) Runs the extractor and shows what it found
4) Also runs diagnose_article.py on it for full metadata dump

Usage:
    python test_hu_extract.py
    python test_hu_extract.py --portal telex
    python test_hu_extract.py --portal mn
"""
import argparse
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from scraper_utils import make_session, DEFAULT_UA, polite_get, fetch_sitemap, parse_sitemap


def find_telex_article(session) -> str | None:
    """Find a real article URL from Telex RSS."""
    print("  Looking for Telex article via RSS...")
    try:
        r = session.get("https://telex.hu/rss", timeout=15)
        links = re.findall(r'<link>(https?://telex\.hu/[^<]+)</link>', r.text)
        for link in links:
            clean = link.split("?")[0].split("#")[0]
            if clean == "https://telex.hu" or "/rss" in clean:
                continue
            # Check it looks like an article (has date in path)
            if re.search(r'/\d{4}/\d{2}/\d{2}/', clean):
                return clean
    except Exception as e:
        print(f"  RSS error: {e}")
    return None


def find_mn_article(session) -> str | None:
    """Find a real article URL from MagyarNemzet sitemap."""
    print("  Looking for MN article via sitemapindex.xml...")
    try:
        xml = fetch_sitemap(session, "https://magyarnemzet.hu/sitemapindex.xml")
        if not xml:
            print("  Could not fetch sitemapindex")
            return None
        sub_urls, _ = parse_sitemap(xml)
        print(f"  Found {len(sub_urls)} sub-sitemaps")
        # Try the last few (most recent)
        for sub_url in sub_urls[-3:]:
            print(f"  Trying: {sub_url}")
            sub_xml = fetch_sitemap(session, sub_url)
            if not sub_xml:
                continue
            _, entries = parse_sitemap(sub_xml)
            print(f"    Entries: {len(entries)}")
            for e in entries[:20]:
                url = e.get("loc", "").split("?")[0].split("#")[0]
                if re.search(r'magyarnemzet\.hu/[a-z][a-z-]*/\d{4}/\d{2}/', url):
                    return url
    except Exception as e:
        print(f"  Sitemap error: {e}")
    return None


def test_extraction(portal: str, url: str, session):
    """Test the portal's extractor on a real article."""
    print(f"\n{'=' * 60}")
    print(f"  EXTRACTION TEST: {portal}")
    print(f"  URL: {url}")
    print(f"{'=' * 60}")

    resp = polite_get(session, url)
    if resp is None:
        print("  FAIL: could not fetch")
        return

    print(f"  Status: {resp.status_code}, Length: {len(resp.text)}")

    if portal == "telex":
        from scrape_telex_cdx import extract_telex_article
        parsed = extract_telex_article(resp.text)
    elif portal == "mn":
        from scrape_magyarnemzet import extract_magyarnemzet_article
        parsed = extract_magyarnemzet_article(resp.text)
    else:
        print(f"  Unknown portal: {portal}")
        return

    print("\n  --- Extracted fields ---")
    for k, v in parsed.items():
        val = str(v)
        if len(val) > 400:
            val = val[:400] + f"... ({len(str(v))} chars total)"
        print(f"  {k}: {val}")

    # Verdict
    print("\n  --- Verdict ---")
    ok = True
    if not parsed.get("headline"):
        print("  ✗ NO headline")
        ok = False
    else:
        print(f"  ✓ headline: {parsed['headline'][:80]}")
    if not parsed.get("datePublished"):
        print("  ✗ NO datePublished")
        ok = False
    else:
        print(f"  ✓ datePublished: {parsed['datePublished']}")
    body_len = len(parsed.get("articleBody", ""))
    if body_len < 200:
        print(f"  ✗ body too short ({body_len} chars)")
        ok = False
    else:
        print(f"  ✓ body: {body_len} chars")

    if ok:
        print("  ✓✓ EXTRACTION OK — scraper should work")
    else:
        print("  ✗✗ EXTRACTION FAILED — need to adjust selectors")

    # Also run generic diagnose_article.py for full metadata
    print(f"\n  --- Full diagnose_article.py output ---")
    subprocess.run([sys.executable, "diagnose_article.py", url])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--portal", choices=["telex", "mn", "both"], default="both")
    args = ap.parse_args()

    session = make_session(DEFAULT_UA)
    session.headers.update({"Accept-Language": "hu;q=0.9,en;q=0.5"})

    if args.portal in ("telex", "both"):
        url = find_telex_article(session)
        if url:
            test_extraction("telex", url, session)
        else:
            print("  Could not find a Telex article URL")

    if args.portal in ("mn", "both"):
        url = find_mn_article(session)
        if url:
            test_extraction("mn", url, session)
        else:
            print("  Could not find a MagyarNemzet article URL")


if __name__ == "__main__":
    main()
