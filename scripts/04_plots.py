"""
Step 5: Visualisations — 10 plots (5 index plots + 5 descriptive plots).

Input:  data/df_all.pkl, data/df_ukraine.pkl, data/df_indices.pkl
Output: output/plots/*.png
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Patch
import warnings

from config import (
    DATA_DIR, PLOTS_DIR,
    COUNTRIES, PORTAL_ORDER, PORTAL_COUNTRY, PORTAL_COLORS,
    COLOR_ILLIBERAL, COLOR_LIBERAL, COLOR_ILLIBERAL_LIGHT, COLOR_LIBERAL_LIGHT,
)

warnings.filterwarnings("ignore")


# ── Helper ──────────────────────────────────────────────────────────
def _get_illiberal_map(df_indices):
    return df_indices.groupby("portal")["illiberal"].first().to_dict()


def _save(fig, name):
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    path = PLOTS_DIR / name
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {path.name}")


# ── Index plots (1–5) ──────────────────────────────────────────────
def plot_index_timeseries(df_indices, index_col, title, filename):
    """Line chart of an index over time, faceted by country."""
    ill_map = _get_illiberal_map(df_indices)
    fig, axes = plt.subplots(2, 2, figsize=(14, 9), sharey=True)
    for ax, c in zip(axes.flat, COUNTRIES):
        sub = df_indices[df_indices["country"] == c]
        for p in sorted(sub["portal"].unique()):
            ps = sub[sub["portal"] == p].sort_values("date_plot")
            ls = "-" if ill_map.get(p, 0) == 1 else "--"
            ax.plot(
                ps["date_plot"], ps[index_col],
                label=p, color=PORTAL_COLORS.get(p, "gray"),
                linestyle=ls, linewidth=1.5,
            )
        ax.set_title(c, fontsize=13, fontweight="bold")
        ax.legend(fontsize=8)
        ax.set_ylabel(index_col)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax.tick_params(axis="x", rotation=45, labelsize=7)
    fig.suptitle(title, fontsize=14, fontweight="bold")
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    _save(fig, filename)


def plot_index_bar(df_indices, index_col, title, filename):
    """Bar chart of mean index value by portal, grouped by country."""
    means = (
        df_indices.groupby(["country", "portal", "illiberal"])[index_col]
        .mean()
        .reset_index()
        .sort_values(["country", "illiberal"])
    )
    fig, ax = plt.subplots(figsize=(12, 6))
    labels, vals, colors = [], [], []
    offset = 0
    prev_c = None
    x_adj = []
    for _, row in means.iterrows():
        if prev_c and row["country"] != prev_c:
            offset += 0.8
        x_adj.append(len(x_adj) + offset)
        labels.append(f"{row['portal']}\n({row['country']})")
        vals.append(row[index_col])
        colors.append(COLOR_ILLIBERAL if row["illiberal"] == 1 else COLOR_LIBERAL)
        prev_c = row["country"]

    ax.bar(x_adj, vals, color=colors, edgecolor="white", width=0.7)
    ax.set_xticks(x_adj)
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel(f"Mean {index_col}")
    ax.set_title(title, fontsize=13, fontweight="bold")
    ax.legend(handles=[
        Patch(color=COLOR_ILLIBERAL, label="Illiberal"),
        Patch(color=COLOR_LIBERAL, label="Liberal"),
    ])
    plt.tight_layout()
    _save(fig, filename)


def plot_scatter_gfi_hfi(df_indices, filename):
    """Scatter plot: EFI vs HFI per portal-month."""
    fig, ax = plt.subplots(figsize=(10, 8))
    for ill_val, marker, label in [(1, "o", "Illiberal"), (0, "s", "Liberal")]:
        sub = df_indices[df_indices["illiberal"] == ill_val]
        c = COLOR_ILLIBERAL if ill_val == 1 else COLOR_LIBERAL
        ax.scatter(sub["EFI"], sub["HFI"], alpha=0.4, s=30, c=c, marker=marker, label=label)

    for c in COUNTRIES:
        sub = df_indices[df_indices["country"] == c]
        ax.annotate(
            c, (sub["EFI"].mean(), sub["HFI"].mean()),
            fontsize=11, fontweight="bold", ha="center",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="yellow", alpha=0.7),
        )

    ax.set_xlabel("EFI (Economic Focus)")
    ax.set_ylabel("HFI (Humanitarian Focus)")
    ax.set_title("EFI vs HFI per Portal-Month", fontsize=13, fontweight="bold")
    ax.legend(fontsize=10)
    plt.tight_layout()
    _save(fig, filename)


# ── Descriptive plots (6–10) ──────────────────────────────────────
def plot_ukraine_share_stacked(df_all, df_ukraine, filename):
    """Stacked bar: total vs Ukraine articles per portal."""
    total = df_all.groupby("portal").size().reindex(PORTAL_ORDER)
    ukr = df_ukraine.groupby("portal").size().reindex(PORTAL_ORDER).fillna(0).astype(int)
    non_ukr = total - ukr

    ill_map = {p: (1 if PORTAL_COLORS[p] == COLOR_ILLIBERAL else 0) for p in PORTAL_ORDER}

    fig, ax = plt.subplots(figsize=(13, 6))
    x = np.arange(len(PORTAL_ORDER))
    c_non = [COLOR_ILLIBERAL_LIGHT if ill_map[p] else COLOR_LIBERAL_LIGHT for p in PORTAL_ORDER]
    c_ukr = [COLOR_ILLIBERAL if ill_map[p] else COLOR_LIBERAL for p in PORTAL_ORDER]

    ax.bar(x, non_ukr.values, color=c_non, edgecolor="white", width=0.65)
    ax.bar(x, ukr.values, bottom=non_ukr.values, color=c_ukr, edgecolor="white", width=0.65)

    for i, p in enumerate(PORTAL_ORDER):
        pct = ukr[p] / total[p] * 100
        ax.text(i, total[p] + 1500, f"{pct:.1f}%", ha="center", va="bottom", fontsize=9, fontweight="bold")

    ax.set_xticks(x)
    ax.set_xticklabels([f"{p}\n({PORTAL_COUNTRY[p]})" for p in PORTAL_ORDER], fontsize=9)
    ax.set_ylabel("Number of articles")
    ax.set_title("Total Articles vs Ukraine-War Articles per Portal\n(dictionary-based keyword filter)", fontsize=13, fontweight="bold")
    ax.legend(handles=[
        Patch(color=COLOR_ILLIBERAL, label="Ukraine — Illiberal"),
        Patch(color=COLOR_LIBERAL, label="Ukraine — Liberal"),
        Patch(color=COLOR_ILLIBERAL_LIGHT, label="Other — Illiberal"),
        Patch(color=COLOR_LIBERAL_LIGHT, label="Other — Liberal"),
    ], fontsize=8, loc="upper right")
    ax.set_ylim(0, total.max() * 1.12)
    plt.tight_layout()
    _save(fig, filename)


def plot_ukraine_share_horizontal(df_all, df_ukraine, filename):
    """Horizontal bar: Ukraine share % per portal."""
    total = df_all.groupby("portal").size().reindex(PORTAL_ORDER)
    ukr = df_ukraine.groupby("portal").size().reindex(PORTAL_ORDER).fillna(0).astype(int)
    shares = ukr / total * 100

    ill_map = {p: (1 if PORTAL_COLORS[p] == COLOR_ILLIBERAL else 0) for p in PORTAL_ORDER}

    fig, ax = plt.subplots(figsize=(10, 5))
    colors = [COLOR_ILLIBERAL if ill_map[p] else COLOR_LIBERAL for p in PORTAL_ORDER]
    ax.barh(range(len(PORTAL_ORDER)), shares.values, color=colors, edgecolor="white", height=0.6)
    for i, v in enumerate(shares.values):
        ax.text(v + 0.5, i, f"{v:.1f}%", va="center", fontsize=10, fontweight="bold")

    ax.set_yticks(range(len(PORTAL_ORDER)))
    ax.set_yticklabels([f"{p} ({PORTAL_COUNTRY[p]})" for p in PORTAL_ORDER], fontsize=10)
    ax.set_xlabel("Ukraine-war share (%)")
    ax.set_title("Share of Ukraine-War Articles per Portal (%)", fontsize=13, fontweight="bold")
    ax.legend(handles=[Patch(color=COLOR_ILLIBERAL, label="Illiberal"), Patch(color=COLOR_LIBERAL, label="Liberal")])
    ax.invert_yaxis()
    ax.set_xlim(0, 47)
    plt.tight_layout()
    _save(fig, filename)


def plot_monthly_total_vs_ukraine(df_all, df_ukraine, filename):
    """Area + line chart: monthly total vs Ukraine volume + share %."""
    m_all = df_all.groupby("year_month").size()
    m_ukr = df_ukraine.groupby("year_month").size()
    months_dt = m_all.index.to_timestamp()

    fig, ax1 = plt.subplots(figsize=(13, 5.5))
    ax1.fill_between(months_dt, m_all.values, alpha=0.2, color="#7f8c8d")
    ax1.plot(months_dt, m_all.values, color="#7f8c8d", linewidth=2, label="Total corpus")
    ax1.fill_between(months_dt, m_ukr.values, alpha=0.35, color=COLOR_ILLIBERAL)
    ax1.plot(months_dt, m_ukr.values, color=COLOR_ILLIBERAL, linewidth=2, label="Ukraine-war subset")
    ax1.set_ylabel("Number of articles")
    ax1.set_title("Monthly Article Volume: Full Corpus vs Ukraine-War Subset", fontsize=13, fontweight="bold")

    ax2 = ax1.twinx()
    share = m_ukr / m_all * 100
    ax2.plot(months_dt, share.values, color="#f39c12", linewidth=2, linestyle="--", label="Ukraine share %")
    ax2.set_ylabel("Ukraine share (%)", color="#f39c12")
    ax2.tick_params(axis="y", labelcolor="#f39c12")

    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, fontsize=9, loc="upper right")
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax1.tick_params(axis="x", rotation=45, labelsize=8)
    plt.tight_layout()
    _save(fig, filename)


def plot_monthly_ukraine_by_portal(df_ukraine, filename):
    """Line chart: monthly Ukraine-war volume per portal, faceted by country."""
    ill_map = {p: (1 if PORTAL_COLORS[p] == COLOR_ILLIBERAL else 0) for p in PORTAL_ORDER}
    fig, axes = plt.subplots(2, 2, figsize=(14, 9), sharey=False)
    for ax, c in zip(axes.flat, COUNTRIES):
        sub = df_ukraine[df_ukraine["country"] == c]
        for p in sorted(sub["portal"].unique()):
            ps = sub[sub["portal"] == p].groupby("year_month").size()
            dates = ps.index.to_timestamp()
            ls = "-" if ill_map.get(p, 0) == 1 else "--"
            ax.plot(dates, ps.values, label=p, color=PORTAL_COLORS.get(p, "gray"), linestyle=ls, linewidth=1.5)
        ax.set_title(c, fontsize=13, fontweight="bold")
        ax.legend(fontsize=8)
        ax.set_ylabel("Ukraine articles")
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=4))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax.tick_params(axis="x", rotation=45, labelsize=7)
    fig.suptitle("Monthly Ukraine-War Article Volume per Portal (by country)", fontsize=14, fontweight="bold")
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    _save(fig, filename)


def plot_sentiment_ukraine(df_ukraine, filename):
    """Grouped bar: sentiment distribution in Ukraine-war articles by portal."""
    ct = pd.crosstab(df_ukraine["portal"], df_ukraine["document_sentiment3"], normalize="index") * 100
    ct = ct.reindex(PORTAL_ORDER)

    fig, ax = plt.subplots(figsize=(13, 6))
    x = np.arange(len(PORTAL_ORDER))
    w = 0.22
    for i, (col, color) in enumerate(zip(["Negative", "Neutral", "Positive"], [COLOR_ILLIBERAL, "#95a5a6", "#27ae60"])):
        vals = ct[col].values
        ax.bar(x + i * w, vals, width=w, color=color, edgecolor="white", label=col)
        for j, v in enumerate(vals):
            ax.text(x[j] + i * w, v + 0.8, f"{v:.0f}%", ha="center", fontsize=7)

    ax.set_xticks(x + w)
    ax.set_xticklabels([f"{p}\n({PORTAL_COUNTRY[p]})" for p in PORTAL_ORDER], fontsize=9)
    ax.set_ylabel("Share (%)")
    ax.set_title("Sentiment Distribution — Ukraine-War Articles (by portal)", fontsize=13, fontweight="bold")
    ax.legend(fontsize=9)
    plt.tight_layout()
    _save(fig, filename)


# ── Main ──────────────────────────────────────────────────────────
def main():
    df_all = pd.read_pickle(DATA_DIR / "df_all.pkl")
    df_ukraine = pd.read_pickle(DATA_DIR / "df_ukraine.pkl")
    df_ukraine["country"] = df_ukraine["portal"].map(PORTAL_COUNTRY)
    df_indices = pd.read_pickle(DATA_DIR / "df_indices.pkl")

    print("=" * 70)
    print("STEP 5 — VISUALISATIONS")
    print("=" * 70)

    # Index plots
    plot_index_timeseries(df_indices, "EFI",
        "Monthly Economic Focus Index (EFI) — Ukraine-War Articles", "plot01_gfi_time.png")
    plot_index_timeseries(df_indices, "HFI",
        "Monthly Humanitarian Focus Index (HFI) — Ukraine-War Articles", "plot02_hfi_time.png")
    plot_index_bar(df_indices, "EFI",
        "Mean Economic Focus Index (EFI) by Portal", "plot03_gfi_bar.png")
    plot_index_bar(df_indices, "HFI",
        "Mean Humanitarian Focus Index (HFI) by Portal", "plot04_hfi_bar.png")
    plot_scatter_gfi_hfi(df_indices, "plot05_scatter_gfi_hfi.png")

    # Descriptive plots
    plot_ukraine_share_stacked(df_all, df_ukraine, "plot06_ukraine_share_bar.png")
    plot_ukraine_share_horizontal(df_all, df_ukraine, "plot07_ukraine_share_pct.png")
    plot_monthly_total_vs_ukraine(df_all, df_ukraine, "plot08_monthly_total_vs_ukraine.png")
    plot_monthly_ukraine_by_portal(df_ukraine, "plot09_monthly_ukraine_by_portal.png")
    plot_sentiment_ukraine(df_ukraine, "plot10_sentiment_ukraine.png")

    print(f"\n  All 10 plots saved to {PLOTS_DIR}/")


if __name__ == "__main__":
    main()
