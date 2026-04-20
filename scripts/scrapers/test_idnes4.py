"""Test Wayback snapshot approach for iDnes (bypasses consent wall)."""
import re
import requests
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from scraper_utils import make_session, DEFAULT_UA
from bs4 import BeautifulSoup

session = make_session(DEFAULT_UA)

# 1) Find a Wayback snapshot for a known iDnes article
# Use the Wayback Availability API
article_url = "https://www.idnes.cz/zpravy/zahranicni/rusko-telegram-blokovani-armada-valka-ukrajina.A260416_123557_zahranicni_kha"

print("=== Wayback availability check ===")
try:
    r = requests.get("https://archive.org/wayback/available", params={
        "url": article_url,
    }, timeout=30)
    data = r.json()
    snap = data.get("archived_snapshots", {}).get("closest", {})
    print(f"  Available: {bool(snap)}")
    if snap:
        print(f"  Timestamp: {snap.get('timestamp')}")
        print(f"  URL: {snap.get('url')}")
except Exception as e:
    print(f"  FAIL: {e}")

# 2) Try older articles that are more likely archived
# Construct a known pattern from 2024
print("\n=== CDX search for iDnes articles (broader, 2024 full year) ===")
try:
    r = requests.get("https://web.archive.org/cdx/search/cdx", params={
        "url": "www.idnes.cz/zpravy/zahranicni/*",
        "from": "20240101",
        "to": "20241231",
        "output": "json",
        "limit": "500",
        "fl": "original,timestamp,statuscode",
        "filter": "statuscode:200",
        "collapse": "urlkey",  # deduplicate by URL
    }, timeout=120)
    rows = r.json()
    total = len(rows) - 1
    articles = []
    non_articles = 0
    for row in rows[1:]:
        url = row[0]
        if ".A" in url or re.search(r'\.\w\d{6}_', url):
            articles.append((row[1], url))
        else:
            non_articles += 1
    print(f"  Total unique URLs: {total}")
    print(f"  Article URLs: {len(articles)}")
    print(f"  Non-article URLs: {non_articles}")
    for ts, url in articles[:5]:
        print(f"    ts={ts} {url}")
except Exception as e:
    print(f"  FAIL: {e}")

# 3) If we found articles, try fetching via Wayback and check for body
if articles:
    ts, orig_url = articles[0]
    if orig_url.startswith("http://"):
        orig_url = "https://" + orig_url[7:]
    wayback_url = f"https://web.archive.org/web/{ts}id_/{orig_url}"
    print(f"\n=== Fetching Wayback snapshot ===")
    print(f"  URL: {wayback_url}")
    try:
        r = session.get(wayback_url, timeout=30)
        print(f"  Status: {r.status_code}, Length: {len(r.text)}")
        soup = BeautifulSoup(r.text, "lxml")

        # Check for article body
        og_title = ""
        tag = soup.find("meta", property="og:title")
        if tag:
            og_title = tag.get("content", "")
        print(f"  og:title: {og_title}")

        # Find divs with substantial text
        best_div = None
        best_len = 0
        for div in soup.find_all(["div", "article", "section"]):
            ps = div.find_all("p")
            text = " ".join(p.get_text(strip=True) for p in ps)
            if len(text) > best_len:
                best_len = len(text)
                best_div = div

        if best_div:
            cls = best_div.get("class", [])
            print(f"  Best content div: class={cls} text_len={best_len}")
            ps = best_div.find_all("p")
            print(f"  <p> count: {len(ps)}")
            if ps:
                print(f"  First <p>: {ps[0].get_text(strip=True)[:200]}")
                print(f"  Last <p>: {ps[-1].get_text(strip=True)[:200]}")

        # Also check <article> tag
        article = soup.find("article")
        if article:
            print(f"\n  <article> tag FOUND: class={article.get('class')}")
            ps = article.find_all("p")
            print(f"  <p> inside article: {len(ps)}")
        else:
            print(f"\n  <article> tag: NOT FOUND in Wayback snapshot either")

        # Check for opener/perex
        opener = soup.find(class_=re.compile(r"opener|perex|art-full|bbtext", re.I))
        if opener:
            print(f"  Found class matching opener/perex/art/bbtext: {opener.get('class')}")
            print(f"  Text: {opener.get_text(strip=True)[:300]}")

    except Exception as e:
        print(f"  FAIL: {e}")
else:
    print("\n  No article URLs in CDX — trying Wayback snapshot of RSS article directly")
    rss_url = "https://www.idnes.cz/zpravy/zahranicni/rusko-telegram-blokovani-armada-valka-ukrajina.A260416_123557_zahranicni_kha"
    print(f"  Checking Wayback for: {rss_url}")
    try:
        r = requests.get("https://archive.org/wayback/available", params={"url": rss_url}, timeout=30)
        data = r.json()
        snap = data.get("archived_snapshots", {}).get("closest", {})
        if snap:
            wayback_url = snap["url"].replace("/web/", "/web/", 1)
            # Add id_ to get raw HTML
            wayback_url = re.sub(r'/web/(\d+)/', r'/web/\1id_/', wayback_url)
            print(f"  Snapshot found: {wayback_url}")
            r2 = session.get(wayback_url, timeout=30)
            print(f"  Status: {r2.status_code}, Length: {len(r2.text)}")
        else:
            print("  No Wayback snapshot available")
    except Exception as e:
        print(f"  FAIL: {e}")
