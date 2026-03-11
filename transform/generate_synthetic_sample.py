#!/usr/bin/env python3
"""
Generate a synthetic public sample table with the same schema as stg_sales_public.

This script uses an existing sample as a statistical seed and creates new, non-real rows.
"""

import argparse
import os

import numpy as np
import pandas as pd


def _weighted_choice(values, weights, n, rng):
    probs = weights / weights.sum()
    idx = rng.choice(len(values), size=n, p=probs)
    return values[idx]


def _sample_dates(seed, n, rng):
    month_counts = seed["sold_month"].value_counts().sort_index()
    months = pd.to_datetime(month_counts.index)
    chosen_months = _weighted_choice(months.to_numpy(), month_counts.to_numpy(dtype=float), n, rng)
    chosen_months = pd.to_datetime(chosen_months)

    days = rng.integers(1, 29, size=n)
    sold_dates = pd.to_datetime(
        {
            "year": chosen_months.year,
            "month": chosen_months.month,
            "day": days,
        }
    )
    sold_month = sold_dates.dt.to_period("M").dt.to_timestamp()
    return sold_dates, sold_month


def _sample_numeric(seed_col, n, rng, as_int=False, jitter=0.0):
    s = pd.to_numeric(seed_col, errors="coerce")
    missing_rate = s.isna().mean()
    vals = s.dropna().to_numpy()
    out = np.full(n, np.nan)
    if len(vals) > 0:
        picked = vals[rng.integers(0, len(vals), size=n)]
        if jitter > 0:
            picked = picked * rng.normal(1.0, jitter, size=n)
        out = picked
    missing_mask = rng.random(n) < missing_rate
    out[missing_mask] = np.nan
    if as_int:
        return pd.Series(np.where(np.isnan(out), np.nan, np.round(out)))
    return pd.Series(out)


def _sample_price(seed, property_type, n, rng):
    out = np.zeros(n, dtype=float)
    global_prices = pd.to_numeric(seed["price_int"], errors="coerce").dropna().to_numpy()
    for ptype in property_type.unique():
        mask = property_type == ptype
        k = int(mask.sum())
        type_prices = pd.to_numeric(seed.loc[seed["property_type"] == ptype, "price_int"], errors="coerce").dropna().to_numpy()
        base = type_prices if len(type_prices) >= 30 else global_prices
        picked = base[rng.integers(0, len(base), size=k)]
        noisy = picked * rng.normal(1.0, 0.08, size=k)
        out[mask.to_numpy()] = np.clip(np.round(noisy), 100000, 919000)
    return pd.Series(out.astype(int))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=5000, help="rows")
    parser.add_argument("--seed", type=int, default=42, help="random seed")
    args = parser.parse_args()

    seed_path = "data/public_sample/stg_sales_public.csv"
    out_path = "data/public_sample/stg_sales_public.csv"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    seed = pd.read_csv(seed_path)
    rng = np.random.default_rng(args.seed)
    n = args.n

    combos = (
        seed[["suburb", "suburb_clean", "sa2_code", "sa2_name"]]
        .value_counts()
        .reset_index(name="w")
    )
    combo_idx = rng.choice(len(combos), size=n, p=(combos["w"] / combos["w"].sum()).to_numpy())
    chosen_combo = combos.iloc[combo_idx].reset_index(drop=True)

    prop_counts = seed["property_type"].fillna("house").value_counts()
    property_type = _weighted_choice(
        prop_counts.index.to_numpy(), prop_counts.to_numpy(dtype=float), n, rng
    )
    property_type = pd.Series(property_type, name="property_type")

    sold_date, sold_month = _sample_dates(seed, n, rng)

    synthetic = pd.DataFrame(
        {
            "sale_id": np.arange(1, n + 1),
            "sold_date": sold_date.dt.strftime("%Y-%m-%d"),
            "sold_year": sold_date.dt.year.astype(int),
            "sold_month": sold_month.dt.strftime("%Y-%m-%d"),
            "suburb": chosen_combo["suburb"].astype(str),
            "suburb_clean": chosen_combo["suburb_clean"].astype(str),
            "sa2_code": chosen_combo["sa2_code"].astype(str).str.replace(".0", "", regex=False),
            "sa2_name": chosen_combo["sa2_name"].astype(str),
            "property_type": property_type.astype(str),
        }
    )

    synthetic["bedrooms"] = _sample_numeric(seed["bedrooms"], n, rng, as_int=True, jitter=0.1)
    synthetic["bathrooms"] = _sample_numeric(seed["bathrooms"], n, rng, as_int=True, jitter=0.08)
    synthetic["car_spaces"] = _sample_numeric(seed["car_spaces"], n, rng, as_int=True, jitter=0.1)
    synthetic["land_size_sqm"] = _sample_numeric(seed["land_size_sqm"], n, rng, as_int=False, jitter=0.12)
    synthetic["price_int"] = _sample_price(seed, property_type, n, rng)
    synthetic["price_parse_issue"] = False

    for c in ["bedrooms", "bathrooms", "car_spaces"]:
        synthetic[c] = synthetic[c].clip(lower=0)

    synthetic.to_csv(out_path, index=False)
    print(f"wrote data/public_sample/stg_sales_public.csv ({len(synthetic):,} rows)")


if __name__ == "__main__":
    main()
