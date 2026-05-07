# Data and Methods (draft)

## 1. Research design and data sources

This study analyses media coverage of the Russia–Ukraine war across the four Visegrád countries (Czech Republic, Hungary, Poland, Slovakia) over the period from 1 January 2022 to 23 February 2026. For each country we sampled two general-interest online news outlets representing contrasting political positions on the illiberal–liberal axis, yielding eight portals in total:

| Country | Illiberal portal | Liberal portal |
|---|---|---|
| Czech Republic | MF Dnes (idnes.cz) | Novinky (novinky.cz) |
| Hungary | Magyar Nemzet (magyarnemzet.hu) | Telex (telex.hu) |
| Poland | wPolityce (wpolityce.pl) | Onet (onet.pl) |
| Slovakia | Pravda (pravda.sk) | Aktuality (aktuality.sk) |

The pairing follows established categorisations of post-2010 media-political alignments in Central Europe and was held constant throughout the observation window.

## 2. Corpus construction

The corpus was assembled from two complementary scraping efforts that we treat as a unified dataset after a per-(portal, month) coverage adjudication described below.

The **original corpus** was collected through a series of full-archive scrapers run between 2022 and early 2024 that retrieved articles from all sections of each portal (news, sport, culture, lifestyle, etc.). The original scrapers thus capture the full publishing volume of each outlet during their active period, but section composition differs between portals and the scrapers exhausted before the end of our observation window.

The **supplement corpus** was collected later (2024–2026) using a second generation of scrapers that target only the news section of each portal (e.g. `/zpravy/`, `/aktualne/`, `/krajina/`). These supplement scrapers are run with consistent parameters across all eight portals and use either the Common Crawl/Wayback CDX index or the portal's sitemap. Their coverage spans the full observation period (2022-01 to 2026-02) but for some portals the sitemap or CDX index is patchy in the mid-period.

For each portal-month pair we select the source — original or supplement — that contains the larger number of CAP-classified Ukraine-war articles (see §3.4). When the two sources are tied or the supplement is at least equal, we use the supplement (the methodologically consistent choice). The selection is applied at the (portal, month) level, never within a month, so totals and Ukraine-war counts for any given month always refer to a single source. No ratio-based normalisation is applied. The resulting source split is summarised below.

| Portal | Supplement-month | Original-month | Covered months |
|---|---|---|---|
| MF Dnes | 23 | 27 | 50 |
| Novinky | 23 | 27 | 50 |
| Magyar Nemzet | 36 | 14 | 50 |
| Telex | 25 | 25 | 50 |
| wPolityce | 37 | 13 | 50 |
| Onet | 48 | 0 | 48 |
| Pravda | 23 | 27 | 50 |
| Aktuality | 27 | 23 | 50 |

Onet is supplement-only because no original-corpus scraper was run for it; for two months (2022-01, 2026-02) the supplement also has no data, hence 48 of 50 months covered. For all other portals the harmonised source covers the complete 50-month period.

## 3. Annotation pipeline

Articles are stored as document-level rows with fields including `document_id`, `date`, `document_title`, `document_text`, English translations, and a battery of NLP-derived fields. The enrichment was performed in five stages.

### 3.1 Named-entity recognition (NER)

We applied language-specific spaCy models to the article text: `hu_core_news_lg` for Hungarian (Telex, Magyar Nemzet), `pl_core_news_lg` for Polish (wPolityce, Onet) and the multilingual `xx_ent_wiki_sm` for Czech and Slovak (MF Dnes, Novinky, Pravda, Aktuality). Two NER outputs are stored per article: `document_ner` is a typed listing in the format `LOC: …; ORG: …; PER: …; MISC: …`, while `document_nerw` is the raw token-level extraction without type labels. NER coverage is 100% for all 562K supplement articles and 100% for the harmonised final dataset.

### 3.2 Ukraine-war filter

An article is flagged as Ukraine-war coverage if its `document_nerw` contains any token matching one of the following case-insensitive language-specific keywords: *Rusko*, *Putin*, *Moskva*, *Ukrajina*, *Zelenskyj*, *Kyjev* (Czech/Slovak); *Oroszország*, *Putyin*, *Moszkva*, *Ukrajna*, *Zelenszkij*, *Kijev* (Hungarian); *Rosja*, *Moskwa*, *Ukraina*, *Zełenski*, *Kijów* (Polish). The keyword set focuses on entities (countries, capitals, presidents) rather than process terms (war, invasion) to maximise recall while limiting false positives from unrelated foreign-affairs content.

### 3.3 Comparative Agendas Project (CAP) classification

Each Ukraine-war article was assigned a Major Topic from the Comparative Agendas Project codebook using the multilingual fine-tuned model `poltextlab/xlm-roberta-large-pooled-cap-v4`. The model returns a major-topic label (e.g. *International Affairs*, *Defense*, *Energy*, *Macroeconomics*, *Civil Rights*) and a confidence score. We retain the label as `document_cap_major_label` and the corresponding numeric code as `document_cap_major_code`. Articles for which the model could not assign a topic above its internal threshold are labelled `No Policy Content`.

To support whole-corpus comparisons, the same CAP model was also run on all non-Ukraine-war articles in the supplement corpus (`--step cap-all` in the enrichment pipeline), giving 100% CAP coverage on the unified harmonised dataset.

### 3.4 Sentiment classification

For each Ukraine-war article we assigned a three-class sentiment (Negative / Neutral / Positive) using `poltextlab/xlm-roberta-large-pooled-sentiment-v2`. The label is stored in `document_sentiment3`. Sentiment was computed only on Ukraine-war articles given the costs of GPU inference and the topical focus of the analysis.

### 3.5 Derived indices

Two policy-frame indices were constructed from CAP labels on the Ukraine-war subset:

* **Economic Focus Index (EFI)** — the share of Ukraine-war articles classified as *Energy* or *Macroeconomics* among all CAP-classified Ukraine-war articles per portal-month.
* **Humanitarian Focus Index (HFI)** — the share classified as *Civil Rights*, *Immigration*, or *Social Welfare*.

Both indices are bounded in [0, 1]. Country-level indices (Charts 9–10 of the public dashboard) are constructed analogously from the union of both portals within a country, weighted by article volume.

## 4. Descriptive statistics

The harmonised corpus contains **874,473 articles** from eight portals across the four V4 countries between 1 January 2022 and 23 February 2026. Of these, **138,304 (15.8%)** are flagged as Ukraine-war coverage by the keyword filter. Per-portal totals and Ukraine-war shares are shown in Table 1.

**Table 1.** Article totals and Ukraine-war shares per portal.

| Portal | Country | Pole | Total articles | Ukraine-war | Share |
|---|---|---|---:|---:|---:|
| MF Dnes | CZ | Illiberal | 147,518 | 14,809 | 10.0% |
| Novinky | CZ | Liberal | 110,523 | 15,750 | 14.3% |
| Magyar Nemzet | HU | Illiberal | 145,232 | 20,000 | 13.8% |
| Telex | HU | Liberal | 107,662 | 19,094 | 17.7% |
| wPolityce | PL | Illiberal | 117,092 | 28,943 | 24.7% |
| Onet | PL | Liberal | 36,513 | 3,835 | 10.5% |
| Pravda | SK | Illiberal | 112,059 | 17,257 | 15.4% |
| Aktuality | SK | Liberal | 97,874 | 18,616 | 19.0% |

Country-level aggregates show that Polish portals devote the highest share of coverage to the war (21.3%, driven by wPolityce's 24.7%), while Czech portals are the most reserved (11.8%). Hungary (15.5%) and Slovakia (17.1%) fall in the middle. The Onet figure is anomalously low because the supplement-only scrape captured a relatively small share of the portal's output; we treat the Onet absolute volume as an under-estimate but its CAP and sentiment distributions remain interpretable as samples.

The CAP distribution within Ukraine-war articles is heavily concentrated in a single category: **International Affairs** accounts for 71.1% of all CAP-classified Ukraine-war articles, followed by *Defense* (9.7%), *Government Operations* (3.4%), *Energy* (3.2%) and *Culture* (1.7%). 7.0% are labelled *No Policy Content*. Because International Affairs dominance is partly tautological — the Russia–Ukraine war is by definition a foreign-affairs topic — we report most CAP analyses on a renormalised basis that excludes International Affairs (Charts 5a–5h and 6a–6h of the dashboard), which reveals substantial cross-portal variation in the secondary policy frames.

**Table 2.** Sentiment distribution across Ukraine-war articles (% of articles per portal).

| Portal | Negative | Neutral | Positive |
|---|---:|---:|---:|
| MF Dnes | 67.9 | 19.0 | 13.1 |
| Novinky | 69.2 | 20.9 | 9.9 |
| Magyar Nemzet | 69.6 | 16.9 | 13.4 |
| Telex | 51.2 | 40.3 | 8.5 |
| wPolityce | 77.4 | 9.7 | 12.9 |
| Onet | 74.9 | 11.2 | 13.9 |
| Pravda | 72.0 | 18.0 | 10.0 |
| Aktuality | 45.8 | 29.7 | 24.5 |

The sentiment distribution shows two distinct patterns. Most portals carry a strong negative tilt (65–77% negative articles), consistent with war coverage that emphasises violence, casualties, and adversarial framing. Two portals stand out: Telex (51% negative, 40% neutral) and Aktuality (46% negative, 30% neutral, 25% positive), both liberal outlets that report the war with a substantially more measured, less affect-loaded register.

**Table 3.** Period-aggregate EFI and HFI per portal (Ukraine-war subset).

| Portal | EFI (Economic) | HFI (Humanitarian) |
|---|---:|---:|
| MF Dnes | 0.035 | 0.007 |
| Novinky | 0.041 | 0.008 |
| Magyar Nemzet | 0.051 | 0.015 |
| Telex | 0.035 | 0.005 |
| wPolityce | 0.042 | 0.017 |
| Onet | 0.037 | 0.039 |
| Pravda | 0.033 | 0.002 |
| Aktuality | 0.029 | 0.003 |

The economic focus is highest in Magyar Nemzet (5.1%), reflecting the well-documented Hungarian government strategy of foregrounding energy supply and economic costs as the primary public-discourse frames around the war. The humanitarian focus is highest in Onet (3.9%), an order of magnitude above the Slovak liberal outlet Aktuality (0.3%). The temporal trajectory of both indices (Charts 6/7 and 9/10) shows a peak in mid-2022 corresponding to the onset of the European energy crisis, followed by gradual decline through 2024–2026.

## 5. Topical structure beyond International Affairs

For each portal we extract the most frequent named entities (LOC / ORG / PER) within Ukraine-war articles assigned to the four most common non-International-Affairs CAP categories: *Defense*, *Government Operations*, *Energy*, and *Culture*. Common Ukraine-related tokens (*Russia*, *Putin*, *Moscow*, *Ukraine*, *Zelensky*, *Kyiv* and their morphological forms) are filtered out so the topical signal underneath the war keywords becomes visible. The resulting word clouds (Chart 11 of the dashboard) reveal sharply distinct national framings: Hungarian portals' *Government Operations* coverage is dominated by domestic political actors (*Fidesz*, *Orbán*, *Magyar Péter*, *Gulyás Gergely*), while Czech *Energy* coverage centres on cross-border infrastructure (*Gazprom*, *Nord Stream 1*, *Družba*) and the national utility *ČEZ*. The sharp topical separation between portals — within the same CAP category — provides a first descriptive answer to the question of how V4 illiberal and liberal media agendas diverge once the dominant foreign-affairs frame is bracketed.

## 6. Reproducibility

All scraping code, enrichment pipeline, dashboard generator and the harmonised per-portal CSVs are available at https://github.com/ringorsolya/shifting_priorities. The interactive dashboard (`docs/index.html`) provides Plotly-based time-series, sentiment distributions, CAP composition, country-level indices and topic word clouds; it is regenerated from the harmonised corpus by `scripts/export_dashboard.py`. The full BERTopic topic-model output for the EFI and HFI subsets, split by sentiment and quarter, is included in the dashboard as a clickable heatmap (Section "Topic Models — EFI & HFI by Sentiment").

The raw harmonised CSVs total approximately 5.2 GB and are too large for direct GitHub hosting; they are deposited at [Zenodo / OSF — TBD] with DOI [TBD]. To regenerate them locally from the original-corpus and supplement source CSVs, run `python3 scripts/build_harmonized_csvs.py` from the repository root. The script's per-(portal, month) coverage adjudication is deterministic: a `.use_orig_cache.json` file is written under `harmonized/` so subsequent invocations skip the counting phase.
