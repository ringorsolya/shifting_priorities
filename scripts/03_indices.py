"""
Step 4: Calculate EFI (Economic Focus Index) and HFI (Humanitarian Focus Index).

EFI = (Macroeconomics + Energy) / total Ukraine-war articles per portal-month
HFI = (Civil Rights + Immigration + Social Welfare) / total Ukraine-war articles per portal-month

Input:  data/df_ukraine.pkl
Output: output/df_indices.csv, data/df_indices.pkl
"""

import pandas as pd
import warnings

from config import (
    DATA_DIR, OUTPUT_DIR,
    EFI_CATEGORIES, HFI_CATEGORIES,
)

warnings.filterwarnings("ignore")
pd.set_option("display.max_rows", 200)
pd.set_option("display.width", 160)


def compute_indices(df: pd.DataFrame) -> pd.DataFrame:
    """Compute EFI and HFI per portal-month."""
    cap = df["document_cap_major_label"].fillna("").str.strip().str.lower()

    df = df.copy()
    df["is_macro"] = cap.isin(["macroeconomics"]).astype(int)
    df["is_energy"] = cap.isin(["energy"]).astype(int)
    df["is_civil"] = cap.isin(["civil rights"]).astype(int)
    df["is_immigration"] = cap.isin(["immigration"]).astype(int)
    df["is_welfare"] = cap.isin(["social welfare"]).astype(int)

    grp = (
        df.groupby(["portal", "illiberal", "country", "year_month"])
        .agg(
            n_total=("document_id", "size"),
            n_macro=("is_macro", "sum"),
            n_energy=("is_energy", "sum"),
            n_civil=("is_civil", "sum"),
            n_immigration=("is_immigration", "sum"),
            n_welfare=("is_welfare", "sum"),
        )
        .reset_index()
    )

    grp["EFI"] = (grp["n_macro"] + grp["n_energy"]) / grp["n_total"]
    grp["HFI"] = (grp["n_civil"] + grp["n_immigration"] + grp["n_welfare"]) / grp["n_total"]
    grp["year_month_str"] = grp["year_month"].astype(str)
    grp["date_plot"] = grp["year_month"].dt.to_timestamp()

    return grp


def print_summary(df_indices: pd.DataFrame) -> None:
    print("--- EFI summary by portal ---")
    print(
        df_indices.groupby("portal")["EFI"]
        .describe()[["mean", "std", "min", "max"]]
        .round(4)
        .to_string()
    )

    print("\n--- EFI summary by illiberal ---")
    print(
        df_indices.groupby("illiberal")["EFI"]
        .describe()[["mean", "std", "min", "max"]]
        .round(4)
        .to_string()
    )

    print("\n--- HFI summary by portal ---")
    print(
        df_indices.groupby("portal")["HFI"]
        .describe()[["mean", "std", "min", "max"]]
        .round(4)
        .to_string()
    )

    print("\n--- HFI summary by illiberal ---")
    print(
        df_indices.groupby("illiberal")["HFI"]
        .describe()[["mean", "std", "min", "max"]]
        .round(4)
        .to_string()
    )


def main():
    df_ukraine = pd.read_pickle(DATA_DIR / "df_ukraine.pkl")

    print("=" * 70)
    print("STEP 4 — EFI AND HFI INDICES")
    print("=" * 70)
    print(f"  EFI categories: {EFI_CATEGORIES}")
    print(f"  HFI categories: {HFI_CATEGORIES}")

    df_indices = compute_indices(df_ukraine)

    print(f"\n  Portal-month observations: {len(df_indices)}")
    print_summary(df_indices)

    # Save
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_indices.to_csv(OUTPUT_DIR / "df_indices.csv", index=False)
    df_indices.to_pickle(DATA_DIR / "df_indices.pkl")
    print(f"\nSaved: {OUTPUT_DIR / 'df_indices.csv'}")


if __name__ == "__main__":
    main()
