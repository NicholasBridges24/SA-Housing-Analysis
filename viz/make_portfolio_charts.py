#!/usr/bin/env python3
"""
Generate simple PNG charts from warehouse tables.
"""

import argparse
import os

import duckdb
import matplotlib.pyplot as plt
import pandas as pd


DEFAULT_DB = "outputs/warehouse.duckdb"
DEFAULT_OUT = "outputs/figures"
DEFAULT_DQ_CSV = "outputs/public/dq_missingness.csv"


def read_table(con, table):
    return con.execute(f"SELECT * FROM {table}").fetch_df()


def ensure_datetime(df, col):
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def top_keys_by_volume(df, key_col, vol_col, n):
    g = df.groupby(key_col, dropna=False)[vol_col].sum().sort_values(ascending=False).head(n)
    return [k for k in g.index.tolist() if pd.notna(k)]


def chart_sa2_median_trend(mart_sa2_month, out_path, top_n=8):
    df = ensure_datetime(mart_sa2_month.copy(), "month_start_date")
    top_sa2 = top_keys_by_volume(df, "sa2_name", "n_sales", top_n)
    df = df[df["sa2_name"].isin(top_sa2)].copy()

    plt.figure(figsize=(10, 6))
    for sa2 in top_sa2:
        sub = df[df["sa2_name"] == sa2].sort_values("month_start_date")
        plt.plot(sub["month_start_date"], sub["median_price"], label=sa2)
    plt.title(f"SA2 Monthly Median Price (Top {len(top_sa2)} by sales)")
    plt.xlabel("Month")
    plt.ylabel("Median price")
    plt.legend(fontsize="small")
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()


def chart_sa2_yoy_trend(mart_sa2_month, out_path, top_n=8):
    df = ensure_datetime(mart_sa2_month.copy(), "month_start_date")
    top_sa2 = top_keys_by_volume(df, "sa2_name", "n_sales", top_n)
    df = df[df["sa2_name"].isin(top_sa2)].copy()

    plt.figure(figsize=(10, 6))
    for sa2 in top_sa2:
        sub = df[df["sa2_name"] == sa2].sort_values("month_start_date")
        plt.plot(sub["month_start_date"], sub["yoy_change"], label=sa2)
    plt.title(f"SA2 YoY Change in Median Price (Top {len(top_sa2)} by sales)")
    plt.xlabel("Month")
    plt.ylabel("YoY change (%)")
    plt.axhline(0.0, linewidth=1)
    plt.legend(fontsize="small")
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()


def chart_dq_missingness(con, out_path, dq_missingness_csv=None, top_n=20):
    df = None
    if con is not None:
        try:
            df = read_table(con, "dq_missingness")
        except Exception:
            df = None

    if df is None:
        if dq_missingness_csv is None or not os.path.exists(dq_missingness_csv):
            raise FileNotFoundError("dq_missingness not found in DuckDB and csv fallback missing.")
        df = pd.read_csv(dq_missingness_csv)

    if "column" not in df.columns or "missing_pct" not in df.columns:
        cols = [c.lower() for c in df.columns]
        df.columns = cols
    if "column" not in df.columns or "missing_pct" not in df.columns:
        raise ValueError(f"dq_missingness must include column + missing_pct. Got: {list(df.columns)}")

    plot_df = df[["column", "missing_pct"]].copy()
    plot_df = plot_df.sort_values("missing_pct", ascending=False).head(top_n).iloc[::-1]

    plt.figure(figsize=(10, 6))
    plt.barh(plot_df["column"].astype(str), plot_df["missing_pct"].astype(float))
    plt.title(f"Missingness by Column (Top {len(plot_df)})")
    plt.xlabel("Missing (%)")
    plt.ylabel("Column")
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()


def chart_price_hist_with_cap(stg_sales, out_path, cap=919_000):
    prices = pd.to_numeric(stg_sales["price_int"], errors="coerce").dropna().astype(int)
    plt.figure(figsize=(10, 6))
    plt.hist(prices, bins=60)
    plt.axvline(cap, linewidth=2)
    plt.title("Price Distribution (Cap shown as vertical line)")
    plt.xlabel("Price")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--top", type=int, default=8, help="top n SA2")
    parser.add_argument("--only", nargs="*", default=None, help="sa2_median sa2_yoy dq_missing price_hist")
    args = parser.parse_args()

    db_path = os.path.abspath(os.path.expanduser(DEFAULT_DB))
    out_dir = os.path.abspath(os.path.expanduser(DEFAULT_OUT))
    dq_csv = os.path.abspath(os.path.expanduser(DEFAULT_DQ_CSV))
    os.makedirs(out_dir, exist_ok=True)

    wanted = set(args.only) if args.only else {"sa2_median", "sa2_yoy", "dq_missing", "price_hist"}

    if not os.path.exists(db_path):
        raise FileNotFoundError(f"DuckDB not found: {db_path}")

    con = duckdb.connect(db_path)
    try:
        mart_sa2 = read_table(con, "mart_sa2_month")
        stg_sales = read_table(con, "stg_sales")

        if "sa2_median" in wanted:
            chart_sa2_median_trend(mart_sa2, os.path.join(out_dir, "sa2_median_trend_top.png"), top_n=args.top)
        if "sa2_yoy" in wanted:
            chart_sa2_yoy_trend(mart_sa2, os.path.join(out_dir, "sa2_yoy_trend_top.png"), top_n=args.top)
        if "dq_missing" in wanted:
            chart_dq_missingness(con, os.path.join(out_dir, "dq_missingness_top.png"), dq_missingness_csv=dq_csv, top_n=20)
        if "price_hist" in wanted:
            chart_price_hist_with_cap(stg_sales, os.path.join(out_dir, "price_hist_with_cap.png"), cap=919_000)
    finally:
        con.close()

    print("wrote charts to outputs/figures")


if __name__ == "__main__":
    main()
