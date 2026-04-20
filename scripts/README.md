# V4 Ukraine-War Media Analysis

Economic and humanitarian framing in Visegrad Four (V4) media coverage of the Russia–Ukraine war (2022–2024).

## Research question

How prominent were economic hardship themes and moral obligation themes in V4 media coverage of the Russia–Ukraine war, and do illiberal and liberal outlets differ systematically in their thematic focus?

## Dataset

Eight online news portals from four Central European countries, each pair consisting of one illiberal/populist and one liberal outlet:

| Country  | Illiberal portal | Liberal portal  |
|----------|-----------------|-----------------|
| Czechia  | MF Dnes         | Novinky         |
| Hungary  | Magyar Nemzet   | Telex           |
| Poland   | wPolityce       | Gazeta Wyborcza |
| Slovakia | Pravda          | Aktuality       |

Period: 24 February 2022 – 23 February 2024 (526,207 articles; 81,894 Ukraine-war subset).

Key columns used: `document_id`, `date`, `portal`, `illiberal`, `document_cap_major_label`, `document_sentiment3`, `document_nerw`.

## Method

1. **Ukraine-war filter**: multilingual keyword dictionary applied to `document_nerw` (named entities, weighted). Keywords cover Czech, Slovak, Hungarian, and Polish terms for Russia, Putin, Moscow, Ukraine, Zelenskyy, and Kyiv.

2. **Composite indices** (per portal-month):
   - **GFI (Economic Focus Index)** = (Macroeconomics + Energy) / total Ukraine articles
   - **HFI (Humanitarian Focus Index)** = (Civil Rights + Immigration + Social Welfare) / total Ukraine articles
   - Categories sourced from `document_cap_major_label` (Comparative Agendas Project coding).

3. **Statistical tests**: Mann–Whitney U (with rank-biserial effect size), country-level breakdown, OLS regression with `illiberal`, country dummies, and time trend.

## Repository structure

```
scripts/
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── config.py                 # Paths, constants, keyword dictionary, plot styling
├── 01_load_and_filter.py     # Step 1–2: load CSVs, date filter, Ukraine keyword filter
├── 02_descriptives.py        # Step 3: descriptive statistics (tables)
├── 03_indices.py             # Step 4: compute GFI and HFI per portal-month
├── 04_plots.py               # Step 5: generate all 10 plots (PNG)
├── 05_hypothesis_tests.py    # Step 6: Mann–Whitney U, OLS regression, summary
└── run_all.py                # Master runner: executes full pipeline
```

Expected data layout (adjust `DATA_DIR` in `config.py`):

```
data/
├── CZ_M_mfdnes_document_level_with_preds.csv
├── CZ_M_novinky_document_level_with_preds.csv
├── HU_M_indextelex_document_level_with_preds.csv
├── HU_M_magyarnemzet_document_level_with_preds.csv
├── PL_M_gazetawyborcza_document_level_with_preds2.csv
├── PL_M_wpolityce_document_level_with_preds.csv
├── SK_M_aktuality_document_level_with_preds.csv
└── SK_M_pravda_document_level_with_preds.csv
```

## Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Run full pipeline
cd scripts/
python run_all.py

# Or run individual steps
python 01_load_and_filter.py
python 02_descriptives.py
python 03_indices.py
python 04_plots.py
python 05_hypothesis_tests.py
```

Before running, update `DATA_DIR` in `config.py` to point to the folder containing the CSV files. By default it expects a `data/` folder one level above `scripts/`.

## Output

- `output/df_indices.csv` — tidy data frame with GFI/HFI per portal-month
- `output/plots/plot01_gfi_time.png` — Monthly GFI time series (faceted by country)
- `output/plots/plot02_hfi_time.png` — Monthly HFI time series (faceted by country)
- `output/plots/plot03_gfi_bar.png` — Mean GFI by portal (bar chart)
- `output/plots/plot04_hfi_bar.png` — Mean HFI by portal (bar chart)
- `output/plots/plot05_scatter_gfi_hfi.png` — GFI vs HFI scatter plot
- `output/plots/plot06_ukraine_share_bar.png` — Total vs Ukraine articles (stacked bar)
- `output/plots/plot07_ukraine_share_pct.png` — Ukraine share % (horizontal bar)
- `output/plots/plot08_monthly_total_vs_ukraine.png` — Monthly volume: corpus vs Ukraine subset
- `output/plots/plot09_monthly_ukraine_by_portal.png` — Monthly Ukraine volume per portal
- `output/plots/plot10_sentiment_ukraine.png` — Sentiment distribution in Ukraine-war articles

## Key findings

- **GFI**: Illiberal outlets show significantly higher economic focus (Mann–Whitney U = 4,786, p = 0.013, r = −0.22). Strongest in Hungary (p < 0.001) and Poland (p = 0.001).
- **HFI**: Overall difference is marginally significant (p = 0.08). Hungary drives the illiberal-side HFI due to Magyar Nemzet's high immigration framing.
- **OLS**: Illiberal dummy is a significant positive predictor of both GFI (β = 0.015, p = 0.002) and HFI (β = 0.005, p < 0.001). GFI declines over time as energy salience faded.
- The hypothesis is partially supported: illiberal outlets emphasise economic hardship more strongly; the humanitarian dimension is more nuanced and country-dependent.
