# Harmonised per-portal corpus

This directory contains one CSV per portal — the unified, methodologically
consistent dataset used in the Ukraine-war media coverage analysis. Each row
is one article; each portal has a single CSV combining what was previously
split between an *original-corpus* (whole-archive) scrape and a *supplement*
(news-section-only) scrape.

## Files

| File | Portal | Country | Pole | Rows | Ukraine-war rows |
|---|---|---|---|---:|---:|
| `mf_dnes_harmonized.csv` | MF Dnes | CZ | Illiberal | 147,516 | 14,809 |
| `novinky_harmonized.csv` | Novinky | CZ | Liberal | 110,521 | 15,750 |
| `magyar_nemzet_harmonized.csv` | Magyar Nemzet | HU | Illiberal | 145,230 | 20,000 |
| `telex_harmonized.csv` | Telex | HU | Liberal | 107,657 | 19,093 |
| `wpolityce_harmonized.csv` | wPolityce | PL | Illiberal | 117,091 | 28,943 |
| `onet_harmonized.csv` | Onet | PL | Liberal | 36,512 | 3,835 |
| `pravda_harmonized.csv` | Pravda | SK | Illiberal | 112,057 | 17,256 |
| `aktuality_harmonized.csv` | Aktuality | SK | Liberal | 97,874 | 18,616 |
| **Total** | | | | **874,458** | **138,302** |

Period: 1 January 2022 — 23 February 2026.

The total file size is approximately 5.2 GB; the files are gitignored and not
committed to the repository. The article-supporting deposit is on
[Zenodo / OSF — TBD] with DOI [TBD].

## Schema

| Column | Type | Description |
|---|---|---|
| `document_id` | string | Unique identifier (e.g. `CZ_M_mfdnes_20230306_1`) |
| `date` | YYYY-MM-DD | Publication date |
| `portal` | string | Portal name (one of: MF Dnes, Novinky, Magyar Nemzet, Telex, wPolityce, Onet, Pravda, Aktuality) |
| `country` | string | ISO-2 country code (CZ / HU / PL / SK) |
| `illiberal` | int (0/1) | 1 for illiberal portals, 0 for liberal portals |
| `document_title` | string | Article title in original language |
| `first_sentence` | string | First sentence in original language |
| `first_sentence_english` | string | First sentence translated to English (where available) |
| `document_text` | string | Full article body in original language |
| `document_text_english` | string | Full article body translated to English (where available) |
| `document_ner` | string | Named-entity list grouped by type, format `LOC: …; ORG: …; PER: …; MISC: …` |
| `document_nerw` | string | Named-entity list as raw token sequence (no type labels) |
| `document_cap_major_code` | int | Comparative Agendas Project major-topic code (1–24) |
| `document_cap_major_label` | string | Major-topic label (e.g. *International Affairs*, *Defense*, *Energy*) |
| `document_cap_media2_code` | int | Media-2 sub-topic code |
| `document_cap_media2_label` | string | Media-2 sub-topic label |
| `document_sentiment3` | string | Sentiment label: *Negative* / *Neutral* / *Positive* (assigned only to Ukraine-war articles) |
| `is_ukraine_war` | int (0/1) | Keyword filter flag — 1 if `document_nerw` matches any Ukraine keyword (Russia, Putin, Moscow, Ukraine, Zelensky, Kyiv in CZ/HU/PL/SK) |
| `source` | string | `supplement` or `original` — which raw scrape this row came from (see Source selection below) |
| `url` | string | Article URL (only populated for supplement-sourced rows) |
| `scraped_at` | timestamp | When the row was scraped (only populated for supplement rows) |
| `electoral_cycle` | string | Electoral-cycle annotation (only populated for original-sourced rows) |

## Source selection

The starting datasets were collected through two complementary scraping efforts:
the *original corpus* (whole-archive scrapers covering 2022 to early 2024 across
all sections of each portal) and the *supplement corpus* (news-section-only
scrapers covering 2022-01 to 2026-02 with consistent methodology across portals).

For each `(portal, year-month)` pair, we count the number of CAP-classified
Ukraine-war articles in each source and pick the source with the greater count.
The `(portal, year-month)` decision is the unit of granularity — within any
single month, the data come from one source only; sources are never mixed within
the same month. The `source` column records which source each row came from.

Source split per portal — months whose CAP-Ukraine count was higher in the
supplement vs. in the original. All 50 months (2022-01 to 2026-02) are covered
in the harmonised file for every portal; Onet has no original-corpus scraper
so its months are all supplement-sourced.

| Portal | Supplement-month | Original-month | Total months |
|---|---:|---:|---:|
| MF Dnes | 23 | 27 | 50 |
| Novinky | 23 | 27 | 50 |
| Magyar Nemzet | 36 | 14 | 50 |
| Telex | 25 | 25 | 50 |
| wPolityce | 37 | 13 | 50 |
| Onet | 50 | 0 | 50 |
| Pravda | 23 | 27 | 50 |
| Aktuality | 27 | 23 | 50 |

(Note: for Onet, 2022-01 has data but 0 Ukraine-flagged articles — the war
hadn't started — and 2026-02 has only 7 articles in total because CDX/Wayback
indexing of recent content is patchy. Both months are present in the harmonised
file with their actual non-Ukraine articles.)

## How the files are produced

The harmonised files are not stored in the repository (large size). They are
regenerated by running `python3 scripts/build_harmonized_csvs.py` from the repo
root, given the original-corpus CSVs (`{COUNTRY}_M_{portal}_document_level_with_preds.csv`)
and the supplement CSVs (`data/{portal}_supplement.csv`). The script does a
two-pass scan: pass 1 counts CAP-Ukraine articles per `(portal, ym)` per source
to decide the per-month winner; pass 2 streams rows from the winning source into
the per-portal output. Memory usage is bounded (rows are written incrementally,
not buffered in RAM).

## Row order

Rows are written in source-stream order: supplement rows first (alphabetical by
source file), then original rows (file-order from the `ORIG_FILES` dict in the
script). Within each block, rows preserve their order in the source CSV
(typically chronological but not guaranteed). If you need strict chronological
ordering, sort by `(date, document_id)` after loading, e.g. with pandas:

```python
import pandas as pd
df = pd.read_csv("aktuality_harmonized.csv").sort_values(["date","document_id"])
```

## Caveats and known limitations

- **Onet** is supplement-only because no original-corpus scraper was deployed
  for it. All 50 months are present in the supplement; 2022-01 has 0
  Ukraine-flagged articles because the war hadn't started, and 2026-02 has
  only 7 articles in total because CDX/Wayback indexing of very recent
  content is incomplete. The portal's overall volume is the lowest of the
  eight (~36.5K rows) because the supplement scrape captured a relatively
  small share of its full output.
- **Magyar Nemzet** sitemap coverage drops from ~3,800 to ~700 articles per
  month from 2025-03 onwards. This is a real reduction in the available
  scrape, not a methodology change. The `total` per month is honest.
- **Pravda** content originally lived under `spravy.pravda.sk`, which no longer
  resolves; redirects to `www.pravda.sk` are honoured by the scraper. A
  custom OG-meta parser was written for the 2024-08 to 2025-01 gap.
- **MF Dnes** has 62K+ articles with title `"NA"` in 2022-03 to 2022-06 in the
  original corpus, an artefact of the whole-archive scraper. These are
  filtered out before writing to the harmonised files.
- **English translations** are not available for all articles; the
  `*_english` columns are empty when no translation was generated.
- **CAP labels** were generated by the multilingual model
  `poltextlab/xlm-roberta-large-pooled-cap-v4`. The threshold below which
  the model declines to assign a topic is recorded as `No Policy Content`
  (~7% of Ukraine-war articles).
- **Sentiment labels** were generated by
  `poltextlab/xlm-roberta-large-pooled-sentiment-v2` and are only populated
  for articles flagged as Ukraine-war.
