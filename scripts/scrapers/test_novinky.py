"""Novinky.cz + Aktuality.sk diagnostic: RSS, reachability, CDX, HTML structure."""
import re
import requests
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from scraper_utils import make_session, DEFAULT_UA

session = make_session(DEFAULT_UA)

# ════════════════════════════════════════════════════
# NOVINKY.CZ
# ════════════════════════════════════════════════════
print("=" * 60)
print("NOVINKY.CZ")
print("=" * 60)

# 1) Reachability
print("\n=== Reachability ===")
try:
    r = session.get("https://www.novinky.cz/clanek/", timeout=15)
    print(f"  Status: {r.status_code}, Length: {len(r.text)}")
    has_consent = "consent" in r.text.lower() or "cookie" in r.text.lower()
    print(f"  Consent wall: {has_consent}")
except Exception as e:
    print(f"  FAIL: {e}")

# 2) RSS
print("\n=== RSS feeds ===")
for rss_url in ["https://www.novinky.cz/rss",
                 "https://www.novinky.cz/rss/domaci",
                 "https://www.novinky.cz/rss/zahranicni"]:
    try:
        r = session.get(rss_url, timeout=10)
        links = re.findall(r'<link>(https?://www\.novinky\.cz/[^<]+)</link>', r.text)
        print(f"  {rss_url}: status={r.status_code} links={len(links)}")
        for u in links[:2]:
            print(f"    {u}")
    except Exception as e:
        print(f"  {rss_url}: FAIL ({e})")

# 3) CDX coverage
print("\n=== CDX coverage ===")
try:
    r = requests.get("https://web.archive.org/cdx/search/cdx", params={
        "url": "www.novinky.cz/clanek/*",
        "from": "20240601", "to": "20240630",
        "output": "json", "limit": "20",
        "fl": "original,timestamp,statuscode",
        "filter": "statuscode:200",
        "collapse": "urlkey",
    }, timeout=120)
    rows = r.json()
    print(f"  CDX rows: {len(rows)-1}")
    for row in rows[1:5]:
        print(f"    {row[0]}")
except Exception as e:
    print(f"  FAIL: {e}")

# 4) Diagnose first article
print("\n=== Article diagnosis ===")
test_url = None
# Try from RSS first
try:
    r = session.get("https://www.novinky.cz/rss", timeout=10)
    links = re.findall(r'<link>(https?://www\.novinky\.cz/clanek/[^<]+)</link>', r.text)
    if links:
        test_url = links[0].split("?")[0].split("#")[0]
except:
    pass
# Try from CDX if no RSS
if not test_url:
    try:
        r = requests.get("https://web.archive.org/cdx/search/cdx", params={
            "url": "www.novinky.cz/clanek/*",
            "from": "20240101", "to": "20241231",
            "output": "json", "limit": "5",
            "fl": "original,timestamp,statuscode",
            "filter": "statuscode:200", "collapse": "urlkey",
        }, timeout=120)
        for row in r.json()[1:]:
            if "/clanek/" in row[0]:
                test_url = row[0]
                break
    except:
        pass

if test_url:
    if test_url.startswith("http://"):
        test_url = "https://" + test_url[7:]
    print(f"  Testing: {test_url}")
    subprocess.run([sys.executable, "diagnose_article.py", test_url])
else:
    print("  No article URL found for testing")

# ════════════════════════════════════════════════════
# AKTUALITY.SK
# ════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("AKTUALITY.SK")
print("=" * 60)

# 1) Reachability
print("\n=== Reachability ===")
try:
    r = session.get("https://www.aktuality.sk/", timeout=15)
    print(f"  Status: {r.status_code}, Length: {len(r.text)}")
    has_consent = "consent" in r.text.lower() or "cookie" in r.text.lower()
    print(f"  Consent wall: {has_consent}")
except Exception as e:
    print(f"  FAIL: {e}")

# 2) Sitemap check (corrected URL)
print("\n=== Sitemap check ===")
try:
    r = session.get("https://www.aktuality.sk/sitemap-articles-time-limited-index.xml", timeout=15)
    print(f"  Status: {r.status_code}, Length: {len(r.text)}")
    subs = re.findall(r'<loc>(.*?)</loc>', r.text)
    print(f"  Sub-sitemaps: {len(subs)}")
    for u in subs[:5]:
        print(f"    {u}")
except Exception as e:
    print(f"  FAIL: {e}")

# 3) CDX coverage
print("\n=== CDX coverage ===")
try:
    r = requests.get("https://web.archive.org/cdx/search/cdx", params={
        "url": "www.aktuality.sk/clanok/*",
        "from": "20240601", "to": "20240630",
        "output": "json", "limit": "20",
        "fl": "original,timestamp,statuscode",
        "filter": "statuscode:200", "collapse": "urlkey",
    }, timeout=120)
    rows = r.json()
    print(f"  CDX rows: {len(rows)-1}")
    for row in rows[1:5]:
        print(f"    {row[0]}")
except Exception as e:
    print(f"  FAIL: {e}")

# 4) Diagnose first article
print("\n=== Article diagnosis ===")
test_url = None
try:
    r = requests.get("https://web.archive.org/cdx/search/cdx", params={
        "url": "www.aktuality.sk/clanok/*",
        "from": "20240101", "to": "20241231",
        "output": "json", "limit": "5",
        "fl": "original,timestamp,statuscode",
        "filter": "statuscode:200", "collapse": "urlkey",
    }, timeout=120)
    for row in r.json()[1:]:
        if "/clanok/" in row[0]:
            test_url = row[0]
            break
except:
    pass

if test_url:
    if test_url.startswith("http://"):
        test_url = "https://" + test_url[7:]
    print(f"  Testing: {test_url}")
    subprocess.run([sys.executable, "diagnose_article.py", test_url])
else:
    print("  No article URL found — trying live page")
    try:
        r = session.get("https://www.aktuality.sk/", timeout=15)
        links = re.findall(r'href="(https?://www\.aktuality\.sk/clanok/[^"]+)"', r.text)
        if links:
            test_url = links[0].split("?")[0]
            print(f"  Found from homepage: {test_url}")
            subprocess.run([sys.executable, "diagnose_article.py", test_url])
        else:
            print("  No article links found on homepage either")
    except Exception as e:
        print(f"  FAIL: {e}")
