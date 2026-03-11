#!/usr/bin/env python3
"""
Create a small public sample for demo runs.
"""

import argparse
import os

import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=5000, help="rows")
    args = parser.parse_args()

    in_path = "outputs/public/stg_sales_public.csv"
    if in_path.lower().endswith(".parquet"):
        df = pd.read_parquet(in_path)
    else:
        df = pd.read_csv(in_path)
    sample = df.sample(n=min(args.n, len(df)), random_state=999)

    out_csv = "data/public_sample/stg_sales_public.csv"
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    sample.to_csv(out_csv, index=False)

    print(f"wrote sample csv ({len(sample):,} rows)")


if __name__ == "__main__":
    main()
