"""
Step 3: Descriptive statistics for full corpus and Ukraine-war subset.

Input:  data/df_all.pkl, data/df_ukraine.pkl
Output: Printed tables (console)
"""

import pandas as pd
import warnings

from config import DATA_DIR

warnings.filterwarnings("ignore")
pd.set_option("display.max_rows", 200)
pd.set_option("display.width", 160)


def print_article_counts(df: pd.DataFrame, label: str) -> None:
    print(f"\n--- Articles by portal & illiberal ({label}) ---")
    t = df.groupby(["portal", "illiberal"]).size().reset_index(name="n")
    print(t.to_string(index=False))


def print_monthly_counts(df: pd.DataFrame, label: str) -> None:
    print(f"\n--- Monthly article counts ({label}) ---")
    t = df.groupby("year_month").size().reset_index(name="n")
    print(t.to_string(index=False))


def print_cap_distribution(df: pd.DataFrame, label: str, top_n: int = 15) -> None:
    print(f"\n--- Top {top_n} CAP categories ({label}) ---")
    t = df["document_cap_major_label"].value_counts().head(top_n).reset_index()
    t.columns = ["category", "n"]
    t["pct"] = (t["n"] / len(df) * 100).round(2)
    print(t.to_string(index=False))


def print_sentiment(df: pd.DataFrame, label: str) -> None:
    print(f"\n--- Sentiment by portal ({label}) ---")
    ct = pd.crosstab(df["portal"], df["document_sentiment3"])
    print(ct.to_string())

    pct = pd.crosstab(
        df["portal"], df["document_sentiment3"], normalize="index"
    ).round(4) * 100
    pct.columns = [f"{c}_%" for c in pct.columns]
    print(pct.to_string())

    print(f"\n--- Sentiment by illiberal ({label}) ---")
    ct2 = pd.crosstab(df["illiberal"], df["document_sentiment3"])
    print(ct2.to_string())


def print_ukraine_shares(df_all: pd.DataFrame, df_ukraine: pd.DataFrame) -> None:
    """Detailed Ukraine-share breakdown."""
    print("\n--- Ukraine share by portal ---")
    total = df_all.groupby(["country", "portal", "illiberal"]).size().rename("total")
    ukr = df_ukraine.groupby(["country", "portal", "illiberal"]).size().rename("ukraine")
    t = pd.concat([total, ukr], axis=1).fillna(0).astype(int)
    t["share_%"] = (t["ukraine"] / t["total"] * 100).round(2)
    print(t.reset_index().to_string(index=False))

    print("\n--- Ukraine share by country ---")
    by_c = t.reset_index().groupby("country").agg(
        total=("total", "sum"), ukraine=("ukraine", "sum")
    ).reset_index()
    by_c["share_%"] = (by_c["ukraine"] / by_c["total"] * 100).round(2)
    print(by_c.to_string(index=False))

    print("\n--- Ukraine share by illiberal ---")
    by_i = t.reset_index().groupby("illiberal").agg(
        total=("total", "sum"), ukraine=("ukraine", "sum")
    ).reset_index()
    by_i["share_%"] = (by_i["ukraine"] / by_i["total"] * 100).round(2)
    print(by_i.to_string(index=False))

    print("\n--- Monthly Ukraine share ---")
    m_all = df_all.groupby("year_month").size().rename("total")
    m_ukr = df_ukraine.groupby("year_month").size().rename("ukraine")
    monthly = pd.concat([m_all, m_ukr], axis=1).fillna(0).astype(int)
    monthly["share_%"] = (monthly["ukraine"] / monthly["total"] * 100).round(2)
    print(monthly.to_string())


def main():
    df_all = pd.read_pickle(DATA_DIR / "df_all.pkl")
    df_ukraine = pd.read_pickle(DATA_DIR / "df_ukraine.pkl")

    print("=" * 70)
    print("STEP 3 — DESCRIPTIVE STATISTICS")
    print("=" * 70)

    # 3.1 Article counts
    print_article_counts(df_all, "full corpus")
    print_article_counts(df_ukraine, "Ukraine subset")

    # 3.2 Monthly counts
    print_monthly_counts(df_all, "full corpus")
    print_monthly_counts(df_ukraine, "Ukraine subset")

    # 3.3 CAP distribution
    print_cap_distribution(df_all, "full corpus")
    print_cap_distribution(df_ukraine, "Ukraine subset")

    # 3.4 Sentiment
    print_sentiment(df_all, "full corpus")
    print_sentiment(df_ukraine, "Ukraine subset")

    # 3.5 Ukraine shares (detailed)
    print_ukraine_shares(df_all, df_ukraine)


if __name__ == "__main__":
    main()
