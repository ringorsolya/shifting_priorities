"""
Step 1–2: Load data, filter to date range, identify Ukraine-war articles.

Input:  CSV files in DATA_DIR
Output: data/df_all.pkl, data/df_ukraine.pkl
"""

import pandas as pd
import warnings
from pathlib import Path

from config import (
    DATA_DIR, DATE_START, DATE_END, USECOLS,
    UKRAINE_KEYWORDS, PORTAL_COUNTRY,
)

warnings.filterwarnings("ignore")
pd.set_option("display.width", 140)


def load_corpus(data_dir: Path) -> pd.DataFrame:
    """Load all portal CSVs and combine into a single DataFrame."""
    files = sorted(data_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")

    frames = []
    for f in files:
        df = pd.read_csv(f, usecols=USECOLS, low_memory=False)
        frames.append(df)
        print(f"  Loaded {f.name}: {df.shape}")

    return pd.concat(frames, ignore_index=True)


def prepare_corpus(df: pd.DataFrame) -> pd.DataFrame:
    """Parse dates, filter to study period, recode variables."""
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[(df["date"] >= DATE_START) & (df["date"] <= DATE_END)].copy()

    # Recode illiberal: string -> binary
    df["illiberal"] = df["illiberal"].map({"illiberal": 1, "liberal": 0})

    # Derive country from portal name
    df["country"] = df["portal"].map(PORTAL_COUNTRY)

    # Year-month period
    df["year_month"] = df["date"].dt.to_period("M")

    return df


def filter_ukraine(df: pd.DataFrame) -> pd.DataFrame:
    """Apply multilingual keyword dictionary to identify Ukraine-war articles."""
    pattern = "|".join(UKRAINE_KEYWORDS)
    mask = (
        df["document_nerw"]
        .fillna("")
        .str.contains(pattern, case=False, regex=True)
    )
    return df[mask].copy()


def print_summary(df_all: pd.DataFrame, df_ukraine: pd.DataFrame) -> None:
    """Print descriptive summary of the filtering step."""
    print("\n" + "=" * 70)
    print("STEP 1 — CORPUS AFTER DATE FILTER")
    print("=" * 70)
    print(f"Shape: {df_all.shape}")
    print(f"Portals: {sorted(df_all['portal'].unique())}")
    for c in ["portal", "illiberal", "document_cap_major_label", "document_nerw", "date"]:
        print(f"  Missing {c}: {df_all[c].isna().sum()}")

    print("\n" + "=" * 70)
    print("STEP 2 — UKRAINE-WAR DICTIONARY FILTER")
    print("=" * 70)
    total = df_all.groupby("portal").size().rename("total")
    ukr = df_ukraine.groupby("portal").size().rename("ukraine")
    share = pd.concat([total, ukr], axis=1).fillna(0)
    share["ukraine"] = share["ukraine"].astype(int)
    share["share_%"] = (share["ukraine"] / share["total"] * 100).round(2)
    print(share.to_string())
    print(
        f"\nTotal corpus: {len(df_all):,} | "
        f"Ukraine subset: {len(df_ukraine):,} "
        f"({len(df_ukraine) / len(df_all) * 100:.1f}%)"
    )


def main():
    print("Loading CSV files...")
    df_all = load_corpus(DATA_DIR)

    print("Preparing corpus (date filter, recoding)...")
    df_all = prepare_corpus(df_all)

    print("Applying Ukraine-war keyword filter...")
    df_ukraine = filter_ukraine(df_all)

    print_summary(df_all, df_ukraine)

    # Save intermediate files
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df_all.to_pickle(DATA_DIR / "df_all.pkl")
    df_ukraine.to_pickle(DATA_DIR / "df_ukraine.pkl")
    print(f"\nSaved: {DATA_DIR / 'df_all.pkl'}")
    print(f"Saved: {DATA_DIR / 'df_ukraine.pkl'}")


if __name__ == "__main__":
    main()
