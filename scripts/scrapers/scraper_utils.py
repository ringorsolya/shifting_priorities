"""
Shared utilities for the Polish news scrapers (wPolityce, Gazeta Wyborcza).

Provides:
- Polite HTTP session with retries, timeouts, rate limiting, custom UA
- robots.txt checker
- Sitemap parser (supports nested sitemap indexes, gzip, news sitemaps)
- Checkpoint / resume helpers
- Output row template matching the existing CSV schema
- Safe filename / text utilities

Requirements:
    pip install requests beautifulsoup4 lxml python-dateutil tenacity
"""

from __future__ import annotations

import csv
csv.field_size_limit(10 * 1024 * 1024)  # 10 MB — some articles have very long text fields
import gzip
import hashlib
import io
import json
import logging
import random
import re
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from pathlib import Path
from typing import Iterable, Iterator, Optional, Callable
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

# ─────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scraper")


# ─────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────
DEFAULT_UA = (
    "V4-MediaResearch/1.0 "
    "(academic research; contact: ring.orsolya@gmail.com) "
    "Python/requests"
)

# Polite delay between requests (seconds). Jittered to avoid thundering herd.
MIN_DELAY = 1.0
MAX_DELAY = 2.5

DEFAULT_TIMEOUT = 30  # seconds per request
MAX_RETRIES = 3


# ─────────────────────────────────────────────────────────────────
# Output schema — matches the original CSV columns
# ─────────────────────────────────────────────────────────────────
SCHEMA_COLUMNS = [
    "document_id", "document_title",
    "first_sentence", "first_sentence_english",
    "document_text", "document_text_english",
    "date", "electoral_cycle", "portal", "illiberal",
    "document_cap_media2_code", "document_cap_media2_label",
    "document_cap_major_code", "document_cap_major_label",
    "document_sentiment3", "document_ner", "document_nerw",
    # extra bookkeeping columns (not in original schema, useful to have)
    "url", "scraped_at",
]


@dataclass
class Article:
    """Single scraped article — raw fields only. ML fields left empty for downstream pipeline."""
    document_id: str = ""
    document_title: str = ""
    first_sentence: str = ""
    document_text: str = ""
    date: str = ""
    portal: str = ""
    illiberal: str = ""
    url: str = ""
    scraped_at: str = ""

    # Empty placeholders — to be filled by existing ML pipeline
    first_sentence_english: str = ""
    document_text_english: str = ""
    electoral_cycle: str = ""
    document_cap_media2_code: str = ""
    document_cap_media2_label: str = ""
    document_cap_major_code: str = ""
    document_cap_major_label: str = ""
    document_sentiment3: str = ""
    document_ner: str = ""
    document_nerw: str = ""

    def to_row(self) -> dict:
        d = asdict(self)
        return {c: d.get(c, "") for c in SCHEMA_COLUMNS}


# ─────────────────────────────────────────────────────────────────
# HTTP session
# ─────────────────────────────────────────────────────────────────
def make_session(user_agent: str = DEFAULT_UA) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": user_agent,
        "Accept-Language": "pl,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return s


def polite_get(session: requests.Session, url: str,
               timeout: int = DEFAULT_TIMEOUT,
               min_delay: float = MIN_DELAY,
               max_delay: float = MAX_DELAY) -> Optional[requests.Response]:
    """GET with retries, polite delay, and soft-fail on network errors."""
    delay = random.uniform(min_delay, max_delay)
    time.sleep(delay)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=timeout, allow_redirects=True)
            if resp.status_code == 200:
                return resp
            if resp.status_code in (429, 503):
                wait = 2 ** attempt * 5
                log.warning(f"  {resp.status_code} on {url} — backing off {wait}s")
                time.sleep(wait)
                continue
            if resp.status_code in (404, 410):
                log.debug(f"  {resp.status_code} gone: {url}")
                return None
            log.warning(f"  HTTP {resp.status_code} — {url}")
            return None
        except requests.exceptions.TooManyRedirects as e:
            log.warning(f"  too many redirects: {url}")
            return None
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            log.warning(f"  network error attempt {attempt}: {e}")
            time.sleep(2 ** attempt)

    log.error(f"  giving up on {url}")
    return None


# ─────────────────────────────────────────────────────────────────
# robots.txt
# ─────────────────────────────────────────────────────────────────
def load_robots(base_url: str, user_agent: str = DEFAULT_UA,
                session: Optional[requests.Session] = None) -> RobotFileParser:
    """
    Load robots.txt using `requests` (with certifi CA bundle) rather than
    urllib.request — this avoids the common macOS Python 3.13 SSL error
    'CERTIFICATE_VERIFY_FAILED: unable to get local issuer certificate'.
    """
    rp = RobotFileParser()
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp.set_url(robots_url)

    sess = session or make_session(user_agent)
    try:
        resp = sess.get(robots_url, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
        if resp.status_code == 200:
            rp.parse(resp.text.splitlines())
            log.info(f"  loaded robots.txt from {robots_url}")
            # Cache the raw text so callers can inspect it for Sitemap: lines
            rp._raw_text = resp.text  # type: ignore[attr-defined]
        else:
            log.warning(f"  robots.txt returned HTTP {resp.status_code}")
    except Exception as e:
        log.warning(f"  could not load robots.txt: {e}")
    return rp


def sitemap_urls_from_robots(rp: RobotFileParser) -> list[str]:
    """Extract any `Sitemap:` URLs listed in robots.txt."""
    raw = getattr(rp, "_raw_text", "") or ""
    urls = []
    for line in raw.splitlines():
        line = line.strip()
        if line.lower().startswith("sitemap:"):
            url = line.split(":", 1)[1].strip()
            if url:
                urls.append(url)
    return urls


def probe_sitemaps(session: requests.Session, base_host: str,
                   user_agent: str = DEFAULT_UA) -> list[str]:
    """
    Try to discover sitemap URLs for a host:
      1. Read Sitemap: lines from robots.txt (authoritative).
      2. Probe a list of common conventional paths.
    Returns the URLs that actually respond 200.
    """
    candidates: list[str] = []
    base = f"https://{base_host}"

    # 1. robots.txt
    rp = load_robots(base, user_agent, session=session)
    for u in sitemap_urls_from_robots(rp):
        if u not in candidates:
            candidates.append(u)

    # 2. common conventional paths
    for path in (
        "/sitemap.xml", "/sitemap_index.xml", "/sitemap-index.xml",
        "/sitemaps.xml", "/sitemap/sitemap.xml", "/news-sitemap.xml",
        "/sitemap-news.xml", "/sitemap/news.xml",
    ):
        u = base + path
        if u not in candidates:
            candidates.append(u)

    found: list[str] = []
    for u in candidates:
        status: int | str = "fail"
        final_url = u
        try:
            # Try HEAD first (cheap), but fall back to GET because many CDNs
            # (e.g. media.wpolityce.pl) either block HEAD or return non-200 for it.
            resp = session.head(u, timeout=15, allow_redirects=True)
            if resp.status_code != 200:
                resp = session.get(u, timeout=15, allow_redirects=True, stream=True)
                # Read just a tiny bit to be sure the body is XML-like
                try:
                    _peek = next(resp.iter_content(chunk_size=512), b"")
                finally:
                    resp.close()
            status = resp.status_code
            final_url = resp.url or u
            if status == 200:
                found.append(final_url)
        except Exception as e:
            status = f"err:{type(e).__name__}"
        log.info(f"  sitemap probe  {status}  {u}"
                 + (f"  → {final_url}" if final_url != u else ""))

    # Deduplicate while preserving order
    seen, uniq = set(), []
    for u in found:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def robots_allows(rp: RobotFileParser, url: str, ua: str = DEFAULT_UA) -> bool:
    try:
        return rp.can_fetch(ua, url)
    except Exception:
        return True  # be lenient if robots.txt was unparseable


# ─────────────────────────────────────────────────────────────────
# Sitemap parsing
# ─────────────────────────────────────────────────────────────────
def fetch_sitemap(session: requests.Session, url: str) -> Optional[bytes]:
    """Fetch a sitemap URL; transparently decompress .gz."""
    resp = polite_get(session, url)
    if resp is None:
        return None
    content = resp.content
    if url.endswith(".gz") or content[:2] == b"\x1f\x8b":
        try:
            content = gzip.decompress(content)
        except OSError:
            pass
    return content


def parse_sitemap(xml_bytes: bytes) -> tuple[list[str], list[dict]]:
    """
    Parse a sitemap.xml. Returns (sub_sitemap_urls, url_entries).

    url_entries is a list of dicts: {"loc": str, "lastmod": str|None, "news_date": str|None}
    """
    soup = BeautifulSoup(xml_bytes, "lxml-xml")

    # sitemap index
    sitemap_locs = [sm.find("loc").text.strip()
                    for sm in soup.find_all("sitemap") if sm.find("loc")]

    # url entries
    url_entries = []
    for u in soup.find_all("url"):
        loc = u.find("loc").text.strip() if u.find("loc") else None
        lastmod = u.find("lastmod").text.strip() if u.find("lastmod") else None
        news_date = None
        news = u.find("news:publication_date") or u.find("publication_date")
        if news:
            news_date = news.text.strip()
        if loc:
            url_entries.append({"loc": loc, "lastmod": lastmod, "news_date": news_date})

    return sitemap_locs, url_entries


def walk_sitemaps(session: requests.Session, root_url: str,
                  date_from: date, date_to: date,
                  max_depth: int = 4, verbose: bool = True) -> Iterator[dict]:
    """
    Recursively walk sitemap indexes, yielding url entries whose lastmod/news_date
    overlaps the requested date range. Sub-sitemaps with lastmod outside the window
    are pruned for efficiency.
    """
    queue: list[tuple[str, int]] = [(root_url, 0)]
    seen: set[str] = set()
    counters = {"sitemaps": 0, "sub_pruned": 0, "entries_seen": 0, "entries_dated_skip": 0, "entries_yielded": 0, "entries_nodate": 0}

    while queue:
        url, depth = queue.pop(0)
        if url in seen or depth > max_depth:
            continue
        seen.add(url)

        xml = fetch_sitemap(session, url)
        if xml is None:
            if verbose:
                log.warning(f"  sitemap unreachable: {url}")
            continue

        sub, entries = parse_sitemap(xml)
        counters["sitemaps"] += 1
        if verbose:
            log.info(f"  sitemap  depth={depth}  subs={len(sub)}  urls={len(entries)}  {url}")

        # Prune sub-sitemaps by lastmod when possible
        sub_soup = BeautifulSoup(xml, "lxml-xml")
        sub_info = []
        for sm in sub_soup.find_all("sitemap"):
            loc_el = sm.find("loc")
            lastmod_el = sm.find("lastmod")
            if not loc_el:
                continue
            sub_info.append((loc_el.text.strip(), lastmod_el.text.strip() if lastmod_el else None))
        if not sub_info and sub:
            # fallback — no pruning possible
            sub_info = [(s, None) for s in sub]

        for s_url, s_lastmod in sub_info:
            d = parse_date_str(s_lastmod) if s_lastmod else None
            if d and d < date_from:
                counters["sub_pruned"] += 1
                continue  # whole sub-sitemap is older than our window
            queue.append((s_url, depth + 1))

        for e in entries:
            counters["entries_seen"] += 1
            d = parse_date_str(e.get("news_date") or e.get("lastmod") or "")
            if d is None:
                counters["entries_nodate"] += 1
                # Without a date we can't filter — yield and let caller decide
                yield e
                counters["entries_yielded"] += 1
                continue
            if not (date_from <= d <= date_to):
                counters["entries_dated_skip"] += 1
                continue
            yield e
            counters["entries_yielded"] += 1

    if verbose:
        log.info(f"  walk done: {counters}")


# ─────────────────────────────────────────────────────────────────
# Date parsing
# ─────────────────────────────────────────────────────────────────
def parse_date_str(s: str) -> Optional[date]:
    if not s:
        return None
    try:
        return dateparser.parse(s).date()
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────
# Checkpoint / resume
# ─────────────────────────────────────────────────────────────────
class Checkpoint:
    """
    Append-only CSV writer that also tracks already-scraped URLs
    so the crawler can resume after interruption.
    """
    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        self.seen_urls: set[str] = set()
        self._writer = None
        self._fh = None
        self._load_existing()

    def _load_existing(self):
        if self.csv_path.exists():
            with open(self.csv_path, "r", encoding="utf-8", newline="") as fh:
                rd = csv.DictReader(fh)
                for row in rd:
                    if row.get("url"):
                        self.seen_urls.add(row["url"])
            log.info(f"  loaded checkpoint: {len(self.seen_urls)} URLs already scraped")

    def open(self):
        exists = self.csv_path.exists() and self.csv_path.stat().st_size > 0
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.csv_path, "a", encoding="utf-8", newline="")
        self._writer = csv.DictWriter(self._fh, fieldnames=SCHEMA_COLUMNS,
                                      quoting=csv.QUOTE_ALL)
        if not exists:
            self._writer.writeheader()
            self._fh.flush()  # header visible on disk immediately

    def write(self, article: Article):
        if self._writer is None:
            self.open()
        self._writer.writerow(article.to_row())
        self.seen_urls.add(article.url)
        self._fh.flush()

    def close(self):
        if self._fh:
            self._fh.close()


# ─────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────
def make_document_id(portal_code: str, date_str: str, url: str) -> str:
    """Produce a stable document_id in the existing corpus style."""
    h = hashlib.md5(url.encode()).hexdigest()[:8]
    clean_date = re.sub(r"[^0-9]", "", date_str)[:8] or "00000000"
    return f"{portal_code}_{clean_date}_{h}"


def first_sentence(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return ""
    m = re.search(r"(.+?[\.\!\?])(\s|$)", text, re.DOTALL)
    if m:
        return m.group(1).strip()
    return text[:300]


def clean_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    return text


# ─────────────────────────────────────────────────────────────────
# Wayback Machine CDX API — historical URL discovery
# ─────────────────────────────────────────────────────────────────
CDX_ENDPOINT = "https://web.archive.org/cdx/search/cdx"


def wayback_cdx_urls(session: requests.Session, url_pattern: str,
                     date_from: date, date_to: date,
                     month_chunks: bool = True,
                     chunk_days: int = 0,
                     page_size: int = 15000,
                     timeout: int = 180) -> Iterator[dict]:
    """
    Query the Internet Archive CDX API and yield unique original URLs
    that were archived in the date range. Useful when the target site
    exposes no public sitemap (e.g. onet.pl, tvn24.pl).

    `url_pattern` can be a host with wildcard, e.g. 'wiadomosci.onet.pl/*'
    or a section prefix 'wiadomosci.onet.pl/kraj/*'. Narrower patterns
    avoid 504 timeouts on busy portals.

    `chunk_days` overrides `month_chunks` — e.g. chunk_days=7 splits into
    weekly slices. Useful when monthly queries time out.

    On 504/503 in the middle of a chunk the query is automatically retried
    with a halved time window (recursive bisection), so a single bad
    month doesn't tank the whole discovery run.

    Yields dicts: {"url": str, "timestamp": str}
    """
    from datetime import timedelta

    def _one_query(d_from: date, d_to: date, depth: int = 0) -> Iterator[dict]:
        if d_from > d_to:
            return
        params = {
            "url": url_pattern,
            "from": d_from.strftime("%Y%m%d"),
            "to": d_to.strftime("%Y%m%d"),
            "output": "json",
            "fl": "original,timestamp,statuscode",
            "collapse": "urlkey",
            "limit": str(page_size),
        }
        log.info(f"  CDX query {url_pattern}  {d_from} → {d_to}")
        last_status = None
        for attempt in range(1, 4):
            try:
                resp = session.get(CDX_ENDPOINT, params=params, timeout=timeout)
                last_status = resp.status_code
                if resp.status_code == 200:
                    try:
                        data = resp.json()
                    except ValueError:
                        log.warning(f"    CDX gave non-JSON response")
                        time.sleep(5 * attempt)
                        continue
                    if not data:
                        log.info(f"    got 0 rows (empty response)")
                        return
                    # first row is header: ['original', 'timestamp', 'statuscode']
                    rows = data[1:]
                    kept = 0
                    for row in rows:
                        if len(row) < 2:
                            continue
                        # If statuscode is present, drop non-200 (redirects)
                        if len(row) >= 3 and row[2] and row[2] != "200":
                            continue
                        kept += 1
                        yield {"url": row[0], "timestamp": row[1]}
                    log.info(f"    got {len(rows)} rows, kept {kept} "
                             f"(after statuscode filter)")
                    return
                else:
                    log.warning(f"    CDX {resp.status_code} — retrying")
            except Exception as e:
                log.warning(f"    CDX error: {e} — retrying")
            time.sleep(5 * attempt)

        # If we gave up and the window is wider than one day, bisect.
        span = (d_to - d_from).days
        if span >= 2 and depth < 4:
            mid = d_from + timedelta(days=span // 2)
            log.warning(f"    CDX {last_status} — bisecting "
                        f"{d_from}→{d_to} into {d_from}→{mid} and "
                        f"{mid + timedelta(days=1)}→{d_to}")
            yield from _one_query(d_from, mid, depth + 1)
            yield from _one_query(mid + timedelta(days=1), d_to, depth + 1)
        else:
            log.error(f"    CDX query gave up: {d_from} → {d_to}")

    # ── chunking strategy ──
    if chunk_days and chunk_days > 0:
        cur = date_from
        while cur <= date_to:
            window_end = min(cur + timedelta(days=chunk_days - 1), date_to)
            yield from _one_query(cur, window_end)
            cur = window_end + timedelta(days=1)
            time.sleep(2)  # polite between chunk requests
    elif month_chunks:
        # Iterate month by month to keep each response reasonable
        cur = date(date_from.year, date_from.month, 1)
        while cur <= date_to:
            # compute end-of-month
            if cur.month == 12:
                next_month = date(cur.year + 1, 1, 1)
            else:
                next_month = date(cur.year, cur.month + 1, 1)
            window_end = min(next_month - timedelta(days=1), date_to)
            window_start = max(cur, date_from)
            yield from _one_query(window_start, window_end)
            cur = next_month
            time.sleep(2)  # polite between chunk requests
    else:
        yield from _one_query(date_from, date_to)


def extract_jsonld_article(html: str) -> dict:
    """
    Parse JSON-LD NewsArticle/Article out of an HTML page.
    Returns a dict with canonical fields — empty strings if not found.
    """
    soup = BeautifulSoup(html, "lxml")

    def find_type(obj, tname):
        if not obj:
            return None
        if isinstance(obj, list):
            for x in obj:
                r = find_type(x, tname)
                if r:
                    return r
            return None
        if not isinstance(obj, dict):
            return None
        t = obj.get("@type", "")
        if isinstance(t, list):
            if tname in t:
                return obj
        elif t == tname:
            return obj
        for v in obj.values():
            r = find_type(v, tname)
            if r:
                return r
        return None

    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            payload = json.loads(script.string or "{}")
        except (ValueError, TypeError):
            continue
        na = find_type(payload, "NewsArticle") or find_type(payload, "Article")
        if not na:
            continue
        headline = na.get("headline") or ""
        body = na.get("articleBody") or ""
        date_pub = na.get("datePublished") or ""
        description = na.get("description") or ""
        is_free = na.get("isAccessibleForFree")
        # Normalize is_free: may be True/False/'True'/'False'/None
        if isinstance(is_free, str):
            is_free = is_free.strip().lower() == "true"
        elif is_free is None:
            # Assume free unless explicitly marked otherwise
            is_free = True
        return {
            "headline": clean_text(headline),
            "articleBody": clean_text(body),
            "description": clean_text(description),
            "datePublished": date_pub,
            "isAccessibleForFree": bool(is_free),
        }
    return {"headline": "", "articleBody": "", "description": "",
            "datePublished": "", "isAccessibleForFree": True}


# ─────────────────────────────────────────────────────────────────
# Generic portal scrape runner — used by the sitemap-based scrapers
# (Novinky, iDnes, Pravda, Aktuality, wPolityce). Shared here so we
# don't duplicate the main loop in every script.
# ─────────────────────────────────────────────────────────────────
@dataclass
class PortalConfig:
    portal_name: str            # human-readable portal name, e.g. "Novinky"
    portal_code: str            # short code for document_id, e.g. "CZ_novinky"
    illiberal: str              # "liberal" or "illiberal"
    base_host: str              # e.g. "www.novinky.cz"
    root_sitemap: str           # full URL of the root sitemap (or empty for probe)
    accept_language: str = "en;q=0.9"
    # Callable that decides whether a URL is an article (vs tag/static page)
    is_article_url: Optional[Callable[[str], bool]] = None
    # Callable (html, url) -> parsed dict like extract_jsonld_article's return value
    extract_fn: Optional[Callable[[str, str], dict]] = None
    # Default False: skip articles the publisher marks isAccessibleForFree=false
    include_paywalled: bool = False


def run_portal_scrape(config: PortalConfig, date_from: date, date_to: date,
                      out_path: Path, limit: int = 0, check_robots: bool = True,
                      sitemap_override: str = ""):
    """
    Generic main loop: sitemap discovery → URL filter → fetch → JSON-LD parse →
    checkpoint write. Works for any portal whose site serves JSON-LD NewsArticle
    and publishes a sitemap.xml.
    """
    log.info(f"  {config.portal_name} scraper — {date_from} → {date_to}")
    log.info(f"  output: {out_path}")

    # Per-portal session (lets us set Accept-Language to the right locale)
    session = make_session(DEFAULT_UA)
    session.headers.update({"Accept-Language": config.accept_language})

    rp = None
    if check_robots:
        rp = load_robots(f"https://{config.base_host}/", DEFAULT_UA,
                         session=session)

    # ── Sitemap discovery ──
    sitemap_urls: list[str] = []
    candidate = sitemap_override or config.root_sitemap
    if candidate:
        try:
            r = session.head(candidate, timeout=15, allow_redirects=True)
            if r.status_code == 200:
                sitemap_urls = [candidate]
            elif r.status_code in (403, 405):
                # HEAD rejected; try a real GET
                r2 = session.get(candidate, timeout=30, stream=True,
                                 allow_redirects=True)
                if r2.status_code == 200:
                    sitemap_urls = [candidate]
                    r2.close()
                else:
                    r2.close()
        except Exception as e:
            log.warning(f"  configured sitemap failed: {e}")
    if not sitemap_urls:
        log.info(f"  probing common sitemap paths for {config.base_host}")
        sitemap_urls = probe_sitemaps(session, config.base_host, DEFAULT_UA)
    if not sitemap_urls:
        log.error("  no usable sitemap URL found — aborting")
        return
    log.info(f"  using sitemap URL(s): {sitemap_urls}")

    # ── Scrape loop ──
    extractor = config.extract_fn or (lambda html, url: extract_jsonld_article(html))
    url_filter = config.is_article_url or (lambda u: True)

    checkpoint = Checkpoint(out_path)
    checkpoint.open()
    scraped = skipped = rejected = paywalled_skipped = 0

    def iter_all():
        for root in sitemap_urls:
            yield from walk_sitemaps(session, root, date_from, date_to)

    try:
        for entry in iter_all():
            url = entry["loc"]
            if not url_filter(url):
                continue
            if url in checkpoint.seen_urls:
                skipped += 1
                continue
            if rp is not None and not robots_allows(rp, url, DEFAULT_UA):
                log.debug(f"  robots.txt disallows: {url}")
                rejected += 1
                continue

            resp = polite_get(session, url)
            if resp is None:
                rejected += 1
                continue

            parsed = extractor(resp.text, url)
            if not parsed.get("headline") or not parsed.get("articleBody"):
                rejected += 1
                continue
            if not parsed.get("isAccessibleForFree", True) and not config.include_paywalled:
                paywalled_skipped += 1
                continue

            d = parse_date_str(parsed.get("datePublished", ""))
            iso_date = d.isoformat() if d else ""
            if not iso_date:
                rejected += 1
                continue
            if not (date_from <= d <= date_to):
                skipped += 1
                continue

            art = Article(
                document_id=make_document_id(config.portal_code, iso_date, url),
                document_title=parsed["headline"],
                first_sentence=first_sentence(parsed["articleBody"]),
                document_text=parsed["articleBody"],
                date=iso_date,
                portal=config.portal_name,
                illiberal=config.illiberal,
                url=url,
                scraped_at=datetime.now().astimezone().isoformat(timespec="seconds"),
            )
            checkpoint.write(art)
            scraped += 1
            if scraped % 25 == 0:
                log.info(f"  scraped={scraped}  skipped={skipped}  "
                         f"paywalled={paywalled_skipped}  rejected={rejected}")
            if limit and scraped >= limit:
                log.info(f"  reached --limit {limit}, stopping")
                break
    finally:
        checkpoint.close()

    log.info(f"  done.  scraped={scraped}  skipped={skipped}  "
             f"paywalled={paywalled_skipped}  rejected={rejected}")
    log.info(f"  CSV written to: {out_path}")
