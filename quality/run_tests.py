#!/usr/bin/env python3
"""
Lightweight warehouse contract tests.
Writes a CSV report and returns non-zero on critical failures.
"""

import os
import sys

import duckdb
import pandas as pd


def main():
    db_path = "outputs/warehouse.duckdb"
    stg_path = "data/public_sample/stg_sales_public.csv"
    out_path = "outputs/public/dq_tests.csv"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    con = duckdb.connect(db_path)

    results = []

    def add_result(name, passed, severity, details):
        results.append(
            {
                "test": name,
                "status": "PASS" if passed else "FAIL",
                "severity": severity,
                "details": details,
            }
        )

    # Uniqueness: sale_id
    total, distinct = con.execute(
        "SELECT COUNT(*) AS total, COUNT(DISTINCT sale_id) AS distinct FROM fct_sales"
    ).fetchone()
    add_result(
        "fct_sales.sale_id_unique",
        total == distinct,
        "critical",
        f"total={total}, distinct={distinct}",
    )

    # Not null keys (critical for date only)
    null_sold_date = con.execute(
        "SELECT SUM(CASE WHEN sold_date_key IS NULL THEN 1 ELSE 0 END) FROM fct_sales"
    ).fetchone()[0]
    add_result(
        "fct_sales.sold_date_key_not_null",
        null_sold_date == 0,
        "critical",
        f"null_sold_date_key={null_sold_date}",
    )

    # Coverage checks for optional dims
    null_sa2 = con.execute(
        "SELECT SUM(CASE WHEN sa2_key IS NULL THEN 1 ELSE 0 END) FROM fct_sales"
    ).fetchone()[0]
    null_suburb = con.execute(
        "SELECT SUM(CASE WHEN suburb_key IS NULL THEN 1 ELSE 0 END) FROM fct_sales"
    ).fetchone()[0]
    null_prop_type = con.execute(
        "SELECT SUM(CASE WHEN property_type_key IS NULL THEN 1 ELSE 0 END) FROM fct_sales"
    ).fetchone()[0]
    add_result(
        "fct_sales.sa2_key_coverage",
        (1 - (null_sa2 / total)) >= 0.995 if total else True,
        "warning",
        f"null_sa2_key={null_sa2}",
    )
    add_result(
        "fct_sales.suburb_key_coverage",
        (1 - (null_suburb / total)) >= 0.995 if total else True,
        "warning",
        f"null_suburb_key={null_suburb}",
    )
    add_result(
        "fct_sales.property_type_key_coverage",
        (1 - (null_prop_type / total)) >= 0.99 if total else True,
        "warning",
        f"null_property_type_key={null_prop_type}",
    )

    # Not null price
    null_price = con.execute(
        "SELECT SUM(CASE WHEN price_int IS NULL OR price_int <= 0 THEN 1 ELSE 0 END) FROM fct_sales"
    ).fetchone()[0]
    add_result(
        "fct_sales.price_int_valid",
        null_price == 0,
        "critical",
        f"invalid_count={null_price}",
    )

    # Domain checks
    mins = con.execute(
        "SELECT MIN(bedrooms), MIN(bathrooms), MIN(car_spaces) FROM fct_sales"
    ).fetchone()
    add_result(
        "fct_sales.domains_non_negative",
        all(v is None or v >= 0 for v in mins),
        "warning",
        f"mins={mins}",
    )

    # Coverage: SA2 mapped >= 99.5%
    if stg_path.lower().endswith(".parquet"):
        con.execute(
            f"CREATE OR REPLACE VIEW stg_sales_tmp AS SELECT * FROM read_parquet('{stg_path}')"
        )
    else:
        con.execute(
            f"CREATE OR REPLACE VIEW stg_sales_tmp AS SELECT * FROM read_csv_auto('{stg_path}', header=true)"
        )
    mapped, total = con.execute(
        "SELECT SUM(CASE WHEN sa2_code IS NOT NULL THEN 1 ELSE 0 END), COUNT(*) FROM stg_sales_tmp"
    ).fetchone()
    coverage = mapped / total if total else 0
    add_result(
        "stg_sales.sa2_coverage",
        coverage >= 0.995,
        "warning",
        f"coverage={coverage:.4f}",
    )

    # Outlier count sanity (<= 5% of rows)
    try:
        outliers = pd.read_csv("outputs/public/dq_outliers.csv")
        outlier_ratio = len(outliers) / total if total else 0
        add_result(
            "dq_outliers.ratio",
            outlier_ratio <= 0.05,
            "warning",
            f"ratio={outlier_ratio:.4f}",
        )
    except Exception as e:
        add_result("dq_outliers.ratio", False, "warning", f"error={e}")

    df = pd.DataFrame(results)
    df.to_csv(out_path, index=False)
    print("wrote outputs/public/dq_tests.csv")

    con.close()

    critical_fail = (df["severity"] == "critical") & (df["status"] == "FAIL")
    if critical_fail.any():
        sys.exit(1)


if __name__ == "__main__":
    main()
