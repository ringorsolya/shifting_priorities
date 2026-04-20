"""iDnes deep diagnostic: check what we actually get back, try RSS, try Wayback HTML."""
import re
import requests
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from scraper_utils import make_session, DEFAULT_UA

session = make_session(DEFAULT_UA)

# 1) What does the 17k page actually contain?
print("=== Page content analysis ===")
try:
    r = session.get("https://www.idnes.cz/zpravy/domaci", timeout=15)
    html = r.text
    print(f"  Length: {len(html)}")
    print(f"  Contains '<noscript>': {'<noscript>' in html}")
    print(f"  Contains 'javascript': {'javascript' in html.lower()}")
    print(f"  <a> tags: {len(re.findall(r'<a ', html))}")
    print(f"  Links with .A pattern: {len(re.findall(r'\.A\\d{6}', html))}")
    # Show title
    title = re.search(r'<title>(.*?)</title>', html)
    print(f"  <title>: {title.group(1) if title else '(none)'}")
    # Is it a cookie/consent wall?
    print(f"  Contains 'cookie': {'cookie' in html.lower()}")
    print(f"  Contains 'consent': {'consent' in html.lower()}")
    print(f"  Contains 'gdpr': {'gdpr' in html.lower()}")
    # Show first 500 chars of body
    body_start = html.find('<body')
    if body_start >= 0:
        print(f"\n  First 800 chars after <body>:")
        print(f"  {html[body_start:body_start+800]}")
except Exception as e:
    print(f"  FAIL: {e}")

# 2) Try RSS feeds
print("\n=== RSS feed check ===")
rss_urls = [
    "https://servis.idnes.cz/rss.aspx?c=zpravodaj",
    "https://servis.idnes.cz/rss.aspx?c=zahranicni",
    "https://www.idnes.cz/rss",
    "https://www.idnes.cz/rss/zpravy",
]
for rss_url in rss_urls:
    try:
        r = session.get(rss_url, timeout=10)
        items = re.findall(r'<link>(https?://[^<]+)</link>', r.text)
        article_items = [u for u in items if ".A" in u or "/zpravy/" in u]
        print(f"  {rss_url}")
        print(f"    Status: {r.status_code}, Items: {len(items)}, Articles: {len(article_items)}")
        for u in article_items[:3]:
            print(f"      {u}")
    except Exception as e:
        print(f"  {rss_url}: FAIL ({e})")

# 3) Try fetching with full browser UA
print("\n=== Fetch with browser User-Agent ===")
browser_ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
try:
    r = requests.get("https://www.idnes.cz/zpravy/domaci",
                     headers={"User-Agent": browser_ua, "Accept-Language": "cs;q=0.9"},
                     timeout=15)
    html = r.text
    print(f"  Length: {len(html)}")
    links = re.findall(r'href="(https?://www\.idnes\.cz/[^"]*\.A\d{6}[^"]*)"', html)
    unique = list(dict.fromkeys(links))[:5]
    print(f"  Article links: {len(links)} (unique: {len(unique)})")
    for u in unique:
        print(f"    {u}")
except Exception as e:
    print(f"  FAIL: {e}")

# 4) Try Wayback snapshot of an article
print("\n=== Wayback snapshot test ===")
try:
    # Find a recent snapshot of any iDnes article
    r = requests.get("https://web.archive.org/cdx/search/cdx", params={
        "url": "www.idnes.cz/zpravy/domaci/*domaci*",
        "from": "20240101",
        "to": "20241231",
        "output": "json",
        "limit": "20",
        "fl": "original,timestamp,statuscode",
        "filter": "statuscode:200",
        "matchType": "prefix",
    }, timeout=120)
    rows = r.json()
    print(f"  CDX rows (domaci pattern): {len(rows)-1}")
    for row in rows[1:5]:
        print(f"    ts={row[1]} url={row[0]}")
except Exception as e:
    print(f"  FAIL: {e}")
