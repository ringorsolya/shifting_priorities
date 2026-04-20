"""Quick iDnes diagnostic: CDX URL lookup + fetch test."""
import re
import requests
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from scraper_utils import make_session, DEFAULT_UA, polite_get

# 1) Can we reach iDnes at all?
print("=== Testing iDnes reachability ===")
session = make_session(DEFAULT_UA)
try:
    r = session.get("https://www.idnes.cz/zpravy/domaci", timeout=15)
    print(f"  Status: {r.status_code}, Length: {len(r.text)}")
    # Try to find article links in the page HTML
    article_links = re.findall(r'href="(https?://www\.idnes\.cz/zpravy/[^"]*\.A\d{6}_[^"]*)"', r.text)
    unique_links = list(dict.fromkeys(article_links))[:5]
    print(f"  Article links found in HTML: {len(article_links)} (unique: {len(unique_links)})")
    for link in unique_links:
        print(f"    {link}")
except Exception as e:
    print(f"  FAIL: {e}")
    unique_links = []

# 2) CDX search — try multiple patterns
print("\n=== CDX article URL search ===")
patterns = [
    "www.idnes.cz/zpravy/domaci/*",
    "www.idnes.cz/zpravy/zahranicni/*",
    "www.idnes.cz/zpravy/*",
]
for pattern in patterns:
    try:
        r = requests.get("https://web.archive.org/cdx/search/cdx", params={
            "url": pattern,
            "from": "20240601",
            "to": "20240630",
            "output": "json",
            "limit": "50",
            "fl": "original,timestamp,statuscode",
            "filter": "statuscode:200",
        }, timeout=120)
        rows = r.json()
        total = len(rows) - 1
        # Count actual articles
        articles = [row[0] for row in rows[1:] if ".A" in row[0]]
        print(f"  {pattern}: {total} total, {len(articles)} articles")
        for url in articles[:3]:
            print(f"    {url}")
    except Exception as e:
        print(f"  {pattern}: FAIL: {e}")

# 3) Diagnose a real article (from HTML scrape or CDX)
test_url = None
if unique_links:
    test_url = unique_links[0]
if not test_url:
    # Try to find one from CDX
    for pattern in patterns:
        try:
            r = requests.get("https://web.archive.org/cdx/search/cdx", params={
                "url": pattern,
                "from": "20240101",
                "to": "20241231",
                "output": "json",
                "limit": "200",
                "fl": "original,timestamp,statuscode",
                "filter": "statuscode:200",
            }, timeout=120)
            for row in r.json()[1:]:
                if ".A" in row[0]:
                    test_url = row[0]
                    break
        except:
            pass
        if test_url:
            break

if test_url:
    if test_url.startswith("http://"):
        test_url = "https://" + test_url[7:]
    print(f"\n=== Diagnosing article: {test_url} ===")
    subprocess.run([sys.executable, "diagnose_article.py", test_url])
else:
    print("\n  No article URLs found. iDnes may need direct HTML scraping (no CDX).")
    print("  Trying to diagnose a link from the live page...")
    # Last resort: fetch the section page and find an article
    try:
        r = session.get("https://www.idnes.cz/zpravy/domaci", timeout=15)
        links = re.findall(r'href="(https?://www\.idnes\.cz/[^"]*)"', r.text)
        for link in links:
            if ".A" in link and "/zpravy/" in link:
                print(f"  Found live article: {link}")
                subprocess.run([sys.executable, "diagnose_article.py", link])
                break
    except Exception as e:
        print(f"  FAIL: {e}")
