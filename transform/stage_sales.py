#!/usr/bin/env python3
"""
Stage a public-safe sales table.
"""

import argparse
import os
import re

import pandas as pd


PRICE_INVALID_TOKENS = {
    "contact agent",
    "price on application",
    "poa",
    "tba",
    "auction",
    "withheld",
    "not disclosed",
    "na",
    "n/a",
}


def parse_price(value):
    if value is None:
        return pd.NA
    s = str(value).strip().lower()
    if not s or s in PRICE_INVALID_TOKENS:
        return pd.NA

    s = s.replace("million", "m").replace("mill", "m")
    s = s.replace("thousand", "k").replace("k", "k")
    s = s.replace("$", "").replace(",", "")
    s = (
        s.replace("–", "-")
        .replace("—", "-")
        .replace("â€“", "-")
        .replace("â€”", "-")
        .replace("to", "-")
    )

    tokens = re.findall(r"\d+\.?\d*\s*[mk]?", s)
    if not tokens:
        return pd.NA

    values = []
    for tok in tokens:
        t = tok.replace(" ", "")
        mult = 1.0
        if t.endswith("m"):
            mult = 1_000_000.0
            t = t[:-1]
        elif t.endswith("k"):
            mult = 1_000.0
            t = t[:-1]
        try:
            values.append(float(t) * mult)
        except ValueError:
            pass

    if not values:
        return pd.NA
    if len(values) == 1:
        return int(round(values[0]))
    return int(round(sum(values) / len(values)))


def parse_land_size(value):
    if value is None:
        return pd.NA
    s = str(value).strip().lower()
    if not s:
        return pd.NA
    s = s.replace(",", "").replace(" ", "")
    s = s.replace("m²", "m2").replace("mÂ²", "m2")

    if "ha" in s:
        m = re.findall(r"\d+\.?\d*", s)
        if not m:
            return pd.NA
        return float(m[0]) * 10_000.0

    if "m2" in s or "sqm" in s:
        m = re.findall(r"\d+\.?\d*", s)
        if not m:
            return pd.NA
        return float(m[0])

    m = re.findall(r"\d+\.?\d*", s)
    if not m:
        return pd.NA
    return float(m[0])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-path", default="data/sales_with_sa2.parquet", help="input parquet")
    args = parser.parse_args()

    in_path = args.in_path
    out_parquet = "outputs/public/stg_sales_public.parquet"
    out_csv = "outputs/public/stg_sales_public.csv"
    os.makedirs(os.path.dirname(out_parquet), exist_ok=True)

    df = pd.read_parquet(in_path)

    df["price_parsed"] = df["price"].apply(parse_price)
    df["price_int_fixed"] = df["price_parsed"].fillna(df["price_int"])
    df["price_parse_issue"] = (
        df["price_parsed"].notna()
        & df["price_int"].notna()
        & (df["price_parsed"] - df["price_int"]).abs().gt(1000)
    )
    df["land_size_sqm"] = df["land_size"].apply(parse_land_size)

    df["sold_date"] = pd.to_datetime(df["sold_date"], errors="coerce")
    df["sold_year"] = df["sold_date"].dt.year
    df["sold_month"] = df["sold_date"].dt.to_period("M").dt.to_timestamp()
    df["suburb_clean"] = df["suburb"].astype(str).str.upper().str.strip().str.replace("-", " ")

    df["sa2_code"] = df["SA2_CODE21"].astype("Int64").astype(str)
    df.loc[df["sa2_code"] == "<NA>", "sa2_code"] = pd.NA
    df["sa2_name"] = df["SA2_NAME21"]

    for col in ["bedrooms", "bathrooms", "car_spaces"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(0).astype("Int64")

    df = df.reset_index(drop=True)
    df["sale_id"] = df.index + 1

    safe_cols = [
        "sale_id",
        "sold_date",
        "sold_year",
        "sold_month",
        "suburb",
        "suburb_clean",
        "sa2_code",
        "sa2_name",
        "property_type",
        "bedrooms",
        "bathrooms",
        "car_spaces",
        "land_size_sqm",
        "price_int_fixed",
        "price_parse_issue",
    ]
    staged = df[safe_cols].rename(columns={"price_int_fixed": "price_int"})

    staged.to_parquet(out_parquet, index=False)
    staged.to_csv(out_csv, index=False)

    print("wrote staged parquet+csv")
    print(f"rows: {len(staged):,}")
    print(f"max price: {staged['price_int'].max():,.0f}")
    print(f"land size non-null: {staged['land_size_sqm'].notna().sum():,}")


if __name__ == "__main__":
    main()
