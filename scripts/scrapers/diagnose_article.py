"""
Quick diagnostic: fetch one article URL and report what structured data
is available (JSON-LD, OpenGraph, meta tags, HTML structure).

Usage:
    python diagnose_article.py <URL>
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from scraper_utils import make_session, DEFAULT_UA, polite_get

from bs4 import BeautifulSoup


def diagnose(url: str):
    session = make_session(DEFAULT_UA)
    resp = polite_get(session, url)
    if resp is None:
        print("FAIL: could not fetch", url)
        return

    print(f"URL: {url}")
    print(f"Status: {resp.status_code}")
    print(f"Content-Length: {len(resp.text)}")
    print()

    soup = BeautifulSoup(resp.text, "lxml")

    # 1. JSON-LD
    ld_scripts = soup.find_all("script", {"type": "application/ld+json"})
    print(f"=== JSON-LD blocks: {len(ld_scripts)} ===")
    for i, s in enumerate(ld_scripts):
        try:
            payload = json.loads(s.string or "{}")
            # Pretty-print but truncate articleBody
            if isinstance(payload, dict):
                body = payload.get("articleBody", "")
                if body and len(body) > 200:
                    payload["articleBody"] = body[:200] + "..."
                # Recurse into @graph
                graph = payload.get("@graph", [])
                for item in graph:
                    if isinstance(item, dict):
                        b = item.get("articleBody", "")
                        if b and len(b) > 200:
                            item["articleBody"] = b[:200] + "..."
            print(f"  [{i}] {json.dumps(payload, indent=2, ensure_ascii=False)[:2000]}")
        except Exception as e:
            print(f"  [{i}] parse error: {e}")
            print(f"      raw: {(s.string or '')[:500]}")
    print()

    # 2. OpenGraph
    og_tags = soup.find_all("meta", property=lambda v: v and v.startswith("og:"))
    print(f"=== OpenGraph tags: {len(og_tags)} ===")
    for tag in og_tags:
        print(f"  {tag.get('property')}: {(tag.get('content') or '')[:200]}")
    print()

    # 3. Article-related meta tags
    print("=== Relevant meta tags ===")
    for name in ["author", "description", "article:published_time",
                 "article:modified_time", "article:section", "article:tag",
                 "datePublished", "date", "DC.date", "pubdate"]:
        tag = soup.find("meta", attrs={"name": name}) or \
              soup.find("meta", attrs={"property": name})
        if tag:
            print(f"  {name}: {(tag.get('content') or '')[:200]}")
    print()

    # 4. <article> or main content tag
    article_tag = soup.find("article")
    print(f"=== <article> tag: {'FOUND' if article_tag else 'NOT FOUND'} ===")
    if article_tag:
        # Show class/id
        print(f"  class: {article_tag.get('class')}")
        print(f"  id: {article_tag.get('id')}")
        # Show first heading
        h = article_tag.find(["h1", "h2"])
        if h:
            print(f"  first heading: {h.get_text(strip=True)[:200]}")
        # Show all <p> count and first paragraph
        ps = article_tag.find_all("p")
        print(f"  <p> tags inside <article>: {len(ps)}")
        if ps:
            print(f"  first <p>: {ps[0].get_text(strip=True)[:300]}")
    print()

    # 5. <time> tags
    time_tags = soup.find_all("time")
    print(f"=== <time> tags: {len(time_tags)} ===")
    for t in time_tags[:5]:
        print(f"  datetime={t.get('datetime')}  text={t.get_text(strip=True)[:100]}")
    print()

    # 6. Title tag
    title = soup.find("title")
    print(f"=== <title>: {title.get_text(strip=True)[:200] if title else '(none)'} ===")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python diagnose_article.py <URL>")
        sys.exit(1)
    diagnose(sys.argv[1])
