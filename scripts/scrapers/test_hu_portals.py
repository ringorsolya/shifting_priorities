"""Diagnose Telex.hu and MagyarNemzet.hu: reachability, sitemap, CDX, RSS, HTML structure."""
import re
import requests
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from scraper_utils import make_session, DEFAULT_UA

session = make_session(DEFAULT_UA)


def diagnose_portal(name, host, rss_urls, cdx_pattern, article_url_hint=None):
    print("=" * 60)
    print(f"  {name} ({host})")
    print("=" * 60)

    # 1) Reachability
    print("\n--- Reachability ---")
    try:
        r = session.get(f"https://{host}/", timeout=15)
        print(f"  Status: {r.status_code}, Length: {len(r.text)}")
        has_consent = "consent" in r.text.lower() or "cookie" in r.text.lower()
        print(f"  Consent/cookie mention: {has_consent}")
    except Exception as e:
        print(f"  FAIL: {e}")

    # 2) Robots.txt / sitemap
    print("\n--- robots.txt sitemaps ---")
    try:
        r = session.get(f"https://{host}/robots.txt", timeout=10)
        sitemaps = re.findall(r'Sitemap:\s*(https?://\S+)', r.text, re.IGNORECASE)
        print(f"  Sitemaps found: {len(sitemaps)}")
        for s in sitemaps[:5]:
            print(f"    {s}")
    except Exception as e:
        print(f"  FAIL: {e}")

    # 3) RSS
    print("\n--- RSS feeds ---")
    for rss_url in rss_urls:
        try:
            r = session.get(rss_url, timeout=10)
            links = re.findall(r'<link>(https?://[^<]+)</link>', r.text)
            print(f"  {rss_url}")
            print(f"    Status: {r.status_code}, Links: {len(links)}")
            for u in links[:3]:
                print(f"      {u}")
        except Exception as e:
            print(f"  {rss_url}: FAIL ({e})")

    # 4) CDX coverage
    print("\n--- CDX coverage (2024) ---")
    try:
        r = requests.get("https://web.archive.org/cdx/search/cdx", params={
            "url": cdx_pattern,
            "from": "20240601", "to": "20240630",
            "output": "json", "limit": "20",
            "fl": "original,timestamp,statuscode",
            "filter": "statuscode:200",
            "collapse": "urlkey",
        }, timeout=120)
        rows = r.json()
        print(f"  CDX rows (June 2024): {len(rows)-1}")
        for row in rows[1:5]:
            print(f"    {row[0]}")
    except Exception as e:
        print(f"  FAIL: {e}")

    # 5) Sitemap date check
    print("\n--- Sitemap date check ---")
    try:
        r = session.get(f"https://{host}/robots.txt", timeout=10)
        sitemaps = re.findall(r'Sitemap:\s*(https?://\S+)', r.text, re.IGNORECASE)
        if sitemaps:
            # Try the first sitemap
            subprocess.run([sys.executable, "check_sitemap_dates.py", sitemaps[0]])
    except Exception as e:
        print(f"  FAIL: {e}")

    # 6) Article diagnosis (try RSS or CDX)
    print("\n--- Article HTML diagnosis ---")
    test_url = article_url_hint
    if not test_url:
        # Try RSS
        for rss_url in rss_urls:
            try:
                r = session.get(rss_url, timeout=10)
                links = re.findall(r'<link>(https?://' + re.escape(host) + r'/[^<]+)</link>', r.text)
                for link in links:
                    clean = link.split("?")[0].split("#")[0]
                    if clean != f"https://{host}/" and clean != f"https://{host}":
                        test_url = clean
                        break
            except:
                pass
            if test_url:
                break

    if test_url:
        print(f"  Testing: {test_url}")
        subprocess.run([sys.executable, "diagnose_article.py", test_url])
    else:
        print("  No article URL found for diagnosis")


# ── TELEX.HU ──
diagnose_portal(
    name="Telex.hu",
    host="telex.hu",
    rss_urls=[
        "https://telex.hu/rss",
        "https://telex.hu/rss/kulfold",
        "https://telex.hu/rss/belfold",
    ],
    cdx_pattern="telex.hu/*",
)

# ── MAGYAR NEMZET ──
diagnose_portal(
    name="MagyarNemzet.hu",
    host="magyarnemzet.hu",
    rss_urls=[
        "https://magyarnemzet.hu/feed/",
        "https://magyarnemzet.hu/feed/kulfold/",
        "https://magyarnemzet.hu/feed/belfold/",
    ],
    cdx_pattern="magyarnemzet.hu/*",
)
