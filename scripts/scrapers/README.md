# V4 news scrapers — corpus supplement

Scrapers for the V4 portals used in the Ukraine-war media analysis:

| Country | Portal          | Orientation | Script                  | URL discovery        | Notes                     |
|---------|-----------------|-------------|-------------------------|----------------------|---------------------------|
| PL      | wPolityce       | illiberal   | `scrape_wpolityce.py`   | Sitemap (CDN)        | —                         |
| PL      | Onet            | liberal     | `scrape_onet.py`        | Wayback CDX API      | **replaces Gazeta Wyborcza** |
| PL      | Gazeta Wyborcza | liberal     | `scrape_wyborcza.py`    | Sitemap              | **not recommended — TDM opt-out** |
| CZ      | Novinky         | liberal     | `scrape_novinky.py`     | Sitemap              | —                         |
| CZ      | MF Dnes         | illiberal   | `scrape_idnes.py`       | Sitemap              | idnes.cz, news-only       |
| SK      | Pravda          | illiberal   | `scrape_pravda.py`      | Sitemap              | spravy.pravda.sk          |
| SK      | Aktuality       | liberal     | `scrape_aktuality.py`   | Sitemap              | —                         |

### Why Onet replaces Gazeta Wyborcza

Agora S.A. (publisher of Gazeta Wyborcza) placed an explicit
**text-and-data-mining opt-out** in `wyborcza.pl/robots.txt` and blocks
common academic/AI crawlers (ClaudeBot, GPTBot, PerplexityBot, Scrapy, …).
Respecting that opt-out, we instead use **Onet.pl** — the largest
liberal-leaning Polish news portal, which has no such opt-out and serves
its `NewsArticle` payloads in public JSON-LD.

### Coverage

- **wPolityce (PL):** last article 2023-08-12 → gap `2023-08-13 → 2024-02-23`
- **Onet (PL):** no prior coverage → extended window
  `2022-02-01 → 2026-02-23` (4 years around the war anniversary)
- **Gazeta Wyborcza (PL):** gap `2023-11-09 → 2024-02-23` (kept for reference;
  scraper present but TDM opt-out means you should not run it)
- **Novinky (CZ):** last article 2024-04-01 → gap `2024-04-02 → 2026-02-23`
- **MF Dnes / iDnes.cz (CZ):** last article 2024-04-03 → gap `2024-04-04 → 2026-02-23`
- **Pravda (SK):** last article 2024-03-22 → gap `2024-03-23 → 2026-02-23`
- **Aktuality (SK):** last article 2024-03-22 → gap `2024-03-23 → 2026-02-23`

Note: we scrape only the gap (no overlap) for CZ/SK portals to keep
runtime manageable — Novinky's full archive alone has ~750k URLs across
75 sub-sitemaps. Onet is the exception (full 4-year window) because it
was not in the corpus before.

## Strategy

### Sitemap-based (wPolityce, Gazeta Wyborcza)

1. **Sitemap crawl** — fetch the root sitemap.xml, walk nested sitemap indexes,
   extract URLs with their lastmod / news:publication_date.
2. **Date-range filter** — keep only URLs whose sitemap-reported date falls in
   the requested window.
3. **URL filter** — drop tag/author/category/static pages via pattern match.
4. **robots.txt** — checked before each fetch; disallowed URLs skipped.
5. **Polite fetch** — 1–2.5 s jittered delay, exponential backoff on 429/503,
   academic-research User-Agent with a contact email.
6. **HTML extraction** — JSON-LD first (most reliable), then OpenGraph / meta
   tags, then heuristic body extraction.
7. **Checkpoint** — append-only CSV with URL de-duplication, so you can
   interrupt and resume at any time.

### CDX-based (Onet)

Onet publishes no public sitemap, so historical URLs are discovered via the
Internet Archive's **CDX API** (`https://web.archive.org/cdx/search/cdx`).
The query is run one month at a time to keep responses manageable, with
a polite 2 s delay between chunks. Discovered URLs are optionally cached
to a JSONL file so URL discovery is resumable.

For each discovered URL, the **live Onet HTML** is fetched (not the
Wayback snapshot) and its JSON-LD `NewsArticle` payload parsed. Articles
flagged `isAccessibleForFree: false` are skipped by default.

## Output schema

Both scripts write CSVs with the **same 19 columns** as the existing
`PL_M_*_document_level_with_preds.csv` files, plus `url` and `scraped_at`
for bookkeeping:

```
document_id, document_title,
first_sentence, first_sentence_english,
document_text, document_text_english,
date, electoral_cycle, portal, illiberal,
document_cap_media2_code, document_cap_media2_label,
document_cap_major_code, document_cap_major_label,
document_sentiment3, document_ner, document_nerw,
url, scraped_at
```

**Important**: the ML-derived columns (`document_cap_*`, `document_sentiment3`,
`document_ner`, `document_nerw`, `*_english`, `electoral_cycle`) are written
as **empty strings**. Run the supplement through your existing ML pipeline
(translation → CAP classification → sentiment → NER) before merging with the
main corpus and re-running `01_load_and_filter.py`.

## Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Fill the wPolityce gap (full period)
python scrape_wpolityce.py \
    --start 2023-08-13 \
    --end   2024-02-23 \
    --out   ../../data/wpolityce_supplement.csv

# Scrape Onet for the full extended period (4 years around the war anniversary)
python scrape_onet.py \
    --start 2022-02-01 \
    --end   2026-02-23 \
    --out   ../../data/onet_supplement.csv \
    --cdx-cache ../../data/onet_cdx_urls.jsonl

# Czech: gap-fill (existing corpus already covers 2021-03 → 2024-04)
python scrape_novinky.py --start 2024-04-02 --end 2026-02-23 \
    --out ../../data/novinky_supplement.csv
python scrape_idnes.py   --start 2024-04-04 --end 2026-02-23 \
    --out ../../data/idnes_supplement.csv

# Slovak: gap-fill
python scrape_pravda.py    --start 2024-03-23 --end 2026-02-23 \
    --out ../../data/pravda_supplement.csv
python scrape_aktuality.py --start 2024-03-23 --end 2026-02-23 \
    --out ../../data/aktuality_supplement.csv

# (NOT RECOMMENDED — Agora opted out of TDM) Gazeta Wyborcza
python scrape_wyborcza.py \
    --start 2023-11-09 \
    --end   2024-02-23 \
    --out   ../../data/wyborcza_supplement.csv
```

All scripts accept:

- `--limit N` — cap the total number of scraped articles (useful for a dry run)
- `--no-robots` — skip the robots.txt check (discouraged)

Sitemap-based scripts (`scrape_wpolityce.py`, `scrape_wyborcza.py`) also accept:

- `--sitemap URL` — override the root sitemap if the default 404s

`scrape_wyborcza.py` also accepts:

- `--keep-paywalled` — write rows even for paywalled articles, using
  headline + lead only (body will be the meta description). Off by default.

`scrape_onet.py` also accepts:

- `--host` — which Onet subhost to crawl (default: `wiadomosci.onet.pl`;
  you can also point it at `www.onet.pl`, `wiadomosci.onet.pl/tylko-w-onecie`,
  etc. — any host pattern CDX accepts)
- `--cdx-cache PATH` — JSONL file that caches discovered CDX URLs, so a
  re-run re-uses the CDX inventory instead of re-querying the archive
- `--skip-cdx` — skip the CDX query entirely and reuse only `--cdx-cache`
- `--include-paywalled` — keep articles the publisher marks
  `isAccessibleForFree: false` (off by default, to respect publisher intent)

### Dry run

Start with a small `--limit` to sanity-check the sitemap and selectors:

```bash
python scrape_wpolityce.py --start 2024-01-01 --end 2024-01-07 \
    --out /tmp/wpol_test.csv --limit 20

python scrape_onet.py --start 2024-01-01 --end 2024-01-07 \
    --out /tmp/onet_test.csv --cdx-cache /tmp/onet_cdx.jsonl --limit 20
```

### Preview mode for Gazeta Wyborcza (no subscription required)

Before committing to a full GW scrape, run the **preview** script. It fetches
a small sample, classifies each URL as `public` / `paywalled` / `incomplete`,
and prints a summary with body-length stats and sample titles:

```bash
python preview_wyborcza.py --start 2024-01-01 --end 2024-01-31 \
    --limit 30 --out preview_wyborcza.csv
```

The output CSV contains per-URL status + title + lead + first 300 chars of the
body, so you can open it in Excel and manually inspect what GW actually
serves to an anonymous client. The script NEVER attempts to bypass the
paywall — it just reports what you see without one.

### Resume after interruption

Just rerun the same command — the `Checkpoint` class reads the existing CSV
and skips any URL already recorded in the `url` column.

## Rate limiting & politeness

Configured in `scraper_utils.py`:

- `MIN_DELAY = 1.0 s`, `MAX_DELAY = 2.5 s` (jittered)
- `MAX_RETRIES = 3`, exponential backoff on 429/503
- `DEFAULT_TIMEOUT = 30 s`
- User-Agent identifies the project and includes a contact email

If a portal asks you to slow down (persistent 429s), raise `MIN_DELAY` /
`MAX_DELAY` in `scraper_utils.py` or rerun with a narrower date range.

## Legal & ethical notes

- **Academic, non-commercial research** — declared in the User-Agent.
- **Public content only** — no paywall bypass, no login, no cookies from
  paid sessions. Onet articles flagged `isAccessibleForFree: false` are
  skipped by default. GW articles behind the paywall are skipped; the
  optional `--keep-paywalled` flag in `scrape_wyborcza.py` still respects
  the paywall and captures only the publicly-served headline + lead.
- **Respecting TDM opt-outs** — Agora S.A. (publisher of Gazeta Wyborcza)
  explicitly opted out of text-and-data-mining in `wyborcza.pl/robots.txt`.
  You should **not** run `scrape_wyborcza.py`; use `scrape_onet.py` instead
  as the Polish liberal representative. The wyborcza script is kept only
  for reproducibility of earlier experiments.
- **Wayback CDX is a public read-only index.** Querying it does not
  bypass any paywall; it just lists what URLs existed when. Body content
  for Onet is read from the live site's public JSON-LD.
- **robots.txt respected** by default (both the target portal and the
  fetch check inside the crawl loop).
- **Rate limited** to reduce load on origin servers — 1–2.5 s jittered
  per request, 2 s between CDX monthly chunks.
- Keep scraped data private, use it only for the declared research, and
  cite the original sources in any publication.

## Integration with the main pipeline

After scraping and running your ML enrichment pipeline on the supplement
CSVs, merge them with the originals:

```python
import pandas as pd
from pathlib import Path

DATA = Path("../../data")

for portal in ["wpolityce", "onet"]:
    orig  = DATA / f"PL_M_{portal}_document_level_with_preds.csv"
    extra = DATA / f"{portal}_supplement_enriched.csv"
    out   = DATA / f"PL_M_{portal}_document_level_with_preds_merged.csv"

    frames = []
    if orig.exists():
        frames.append(pd.read_csv(orig))
    frames.append(pd.read_csv(extra))
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["document_id"])
    df.to_csv(out, index=False)
```

For Onet, there is no prior `PL_M_onet_*` file in the corpus — the
supplement *is* the Onet dataset. Either rename
`onet_supplement_enriched.csv` to `PL_M_onet_document_level_with_preds.csv`
or update `config.py` to point at the supplement directly.

Then point `config.py` at the merged files and rerun `run_all.py`.

## Files

```
scrapers/
├── README.md              # this file
├── requirements.txt       # scraper-only dependencies
├── scraper_utils.py       # shared HTTP / sitemap / CDX / checkpoint utilities
│                           (incl. PortalConfig + run_portal_scrape helpers)
├── scrape_wpolityce.py    # PL, illiberal   — sitemap-based (CDN)
├── scrape_onet.py         # PL, liberal     — Wayback CDX + live HTML
├── scrape_wyborcza.py     # PL, liberal     — NOT RECOMMENDED (TDM opt-out)
├── scrape_novinky.py      # CZ, liberal     — sitemap-based
├── scrape_idnes.py        # CZ, illiberal   — sitemap-based (MF Dnes)
├── scrape_pravda.py       # SK, illiberal   — sitemap-based (spravy.pravda.sk)
├── scrape_aktuality.py    # SK, liberal     — sitemap-based
└── preview_wyborcza.py    # GW dry-run diagnostic: what's public vs paywalled
```
