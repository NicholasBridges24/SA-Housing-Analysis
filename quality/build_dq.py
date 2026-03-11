#!/usr/bin/env python3
"""
Generate DQ tables + markdown report from staged public sales.
"""

import argparse
import os

import numpy as np
import pandas as pd


def _df_to_md(df):
    headers = [str(c) for c in df.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in df.itertuples(index=False):
        lines.append("| " + " | ".join(str(v) for v in row) + " |")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--stg-path",
        default="data/public_sample/stg_sales_public.csv",
        help="staging file",
    )
    args = parser.parse_args()

    stg_path = args.stg_path
    out_dir = "outputs/public"
    os.makedirs(out_dir, exist_ok=True)

    if stg_path.lower().endswith(".parquet"):
        df = pd.read_parquet(stg_path)
    else:
        df = pd.read_csv(stg_path)

    # Missingness
    missing = pd.DataFrame(
        {
            "column": df.columns,
            "missing_count": df.isna().sum().values,
            "missing_pct": (df.isna().mean() * 100).round(2).values,
        }
    ).sort_values("missing_pct", ascending=False)
    missing.to_csv(os.path.join(out_dir, "dq_missingness.csv"), index=False)

    # Invalids
    invalid_rules = {
        "price_int_null_or_nonpositive": (df["price_int"].isna() | (df["price_int"] <= 0)),
        "sold_date_null": df["sold_date"].isna(),
        "bedrooms_negative": df["bedrooms"].fillna(0) < 0,
        "bathrooms_negative": df["bathrooms"].fillna(0) < 0,
        "car_spaces_negative": df["car_spaces"].fillna(0) < 0,
        "land_size_sqm_nonpositive": df["land_size_sqm"].notna() & (df["land_size_sqm"] <= 0),
    }
    invalids = pd.DataFrame(
        {
            "rule": list(invalid_rules.keys()),
            "invalid_count": [int(mask.sum()) for mask in invalid_rules.values()],
            "invalid_pct": [round(mask.mean() * 100, 2) for mask in invalid_rules.values()],
        }
    )
    invalids.to_csv(os.path.join(out_dir, "dq_invalids.csv"), index=False)

    # Duplicates (by key fields)
    dup_subset = [
        "sold_date",
        "suburb_clean",
        "price_int",
        "bedrooms",
        "bathrooms",
        "car_spaces",
        "land_size_sqm",
        "property_type",
    ]
    dup_counts = df.duplicated(subset=dup_subset, keep=False)
    dup_rows = df.loc[dup_counts, dup_subset]
    dup_summary = (
        dup_rows.value_counts()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    dup_summary.to_csv(os.path.join(out_dir, "dq_duplicates.csv"), index=False)

    # Freshness / coverage
    min_date = pd.to_datetime(df["sold_date"]).min()
    max_date = pd.to_datetime(df["sold_date"]).max()
    months = df["sold_month"].dropna().sort_values().unique()
    months_covered = len(months)

    freshness = pd.DataFrame(
        [
            {
                "total_rows": len(df),
                "min_sold_date": min_date,
                "max_sold_date": max_date,
                "months_covered": months_covered,
            }
        ]
    )
    freshness.to_csv(os.path.join(out_dir, "dq_freshness.csv"), index=False)

    month_coverage = (
        df.groupby("sold_month")["sale_id"]
        .count()
        .reset_index(name="n_sales")
        .sort_values("sold_month")
    )
    month_coverage.to_csv(os.path.join(out_dir, "dq_month_coverage.csv"), index=False)

    # Outliers (robust z-score on log(price))
    price = df["price_int"].dropna()
    log_price = np.log(price)
    median = log_price.median()
    mad = np.median(np.abs(log_price - median))
    if mad == 0:
        z = pd.Series(np.zeros(len(log_price)), index=price.index)
    else:
        z = 0.6745 * (log_price - median) / mad
    outlier_mask = z.abs() > 3.5
    outliers = df.loc[outlier_mask.index[outlier_mask], [
        "sale_id",
        "sold_date",
        "suburb_clean",
        "sa2_code",
        "price_int",
    ]].copy()
    outliers["robust_z"] = z[outlier_mask].values
    outliers.to_csv(os.path.join(out_dir, "dq_outliers.csv"), index=False)

    # Markdown report
    report_path = "outputs/public/dq_report.md"
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("# Data Quality Report\n\n")
        f.write(f"- Total rows: {len(df):,}\n")
        f.write(f"- Date range: {min_date.date()} to {max_date.date()}\n")
        f.write(f"- Months covered: {months_covered}\n")
        f.write(f"- Price max: {df['price_int'].max():,}\n")
        f.write("\n## Missingness (top 10)\n\n")
        f.write(_df_to_md(missing.head(10)))
        f.write("\n\n## Invalids\n\n")
        f.write(_df_to_md(invalids))
        f.write("\n\n## Duplicates\n\n")
        f.write(
            f"Rows in duplicate groups (based on {', '.join(dup_subset)}): "
            f"{len(dup_rows):,}\n"
        )
        if not dup_summary.empty:
            f.write("\nTop duplicate keys:\n\n")
            f.write(_df_to_md(dup_summary.head(10)))
        f.write("\n\n## Outliers\n\n")
        f.write(f"Outliers (robust z-score > 3.5): {len(outliers):,}\n")
        if not outliers.empty:
            f.write("\nSample outliers:\n\n")
            f.write(_df_to_md(outliers.head(10)))
        f.write("\n\n## Notes\n\n")
        f.write(
            "- Price ceiling appears at ~$919k. This dataset was scraped with an upper cap, "
            "so insights are within-sample only.\n"
        )

    print("wrote dq csvs")
    print("wrote outputs/public/dq_report.md")


if __name__ == "__main__":
    main()
