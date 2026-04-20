"""Find iDnes article body container."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from scraper_utils import make_session, DEFAULT_UA
from bs4 import BeautifulSoup

session = make_session(DEFAULT_UA)
url = "https://www.idnes.cz/zpravy/zahranicni/rusko-telegram-blokovani-armada-valka-ukrajina.A260416_123557_zahranicni_kha"
r = session.get(url, timeout=15)
soup = BeautifulSoup(r.text, "lxml")

# Show all divs with substantial text
print("=== Divs with >100 chars of text ===")
for div in soup.find_all(["div", "section", "main"]):
    text = div.get_text(strip=True)
    if len(text) > 100:
        cls = div.get("class", [])
        did = div.get("id", "")
        tag = div.name
        # Count direct <p> children
        ps = div.find_all("p", recursive=False)
        all_ps = div.find_all("p")
        print(f"  <{tag}> class={cls} id={did!r} "
              f"text={len(text)} direct_p={len(ps)} all_p={len(all_ps)}")

# Show all <p> tags with their parent
print("\n=== All <p> tags (first 20) ===")
for i, p in enumerate(soup.find_all("p")[:20]):
    parent = p.parent
    pcls = parent.get("class", []) if parent else []
    pid = parent.get("id", "") if parent else ""
    ptag = parent.name if parent else ""
    text = p.get_text(strip=True)[:120]
    print(f"  [{i}] parent=<{ptag}> class={pcls} id={pid!r} → {text}")

# Show if there's an 'opener' or 'perex' or 'art-' class
print("\n=== Article-related classes ===")
for cls_pattern in ["art", "opener", "perex", "body", "content", "text", "story", "detail"]:
    elements = soup.find_all(class_=lambda c: c and cls_pattern in " ".join(c).lower())
    for el in elements[:3]:
        text = el.get_text(strip=True)
        print(f"  class={el.get('class')} tag={el.name} text_len={len(text)}")
