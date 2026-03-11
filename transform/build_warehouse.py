#!/usr/bin/env python3
"""
Build DuckDB warehouse (dims + fact) and BI marts from staged public sales.
"""

import argparse
import os

import duckdb


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--stg-path",
        default="data/public_sample/stg_sales_public.csv",
        help="staging file",
    )
    args = parser.parse_args()

    stg_path = os.path.abspath(args.stg_path)
    db_path = os.path.abspath("outputs/warehouse.duckdb")
    out_dir = os.path.abspath("outputs/public")
    os.makedirs(out_dir, exist_ok=True)

    con = duckdb.connect(db_path)
    con.execute("PRAGMA threads=4;")

    if stg_path.lower().endswith(".parquet"):
        con.execute(
            "CREATE OR REPLACE TABLE stg_sales AS SELECT * FROM read_parquet(?)",
            [stg_path],
        )
    else:
        con.execute(
            "CREATE OR REPLACE TABLE stg_sales AS SELECT * FROM read_csv_auto(?, header=true)",
            [stg_path],
        )

    min_date, max_date = con.execute(
        "SELECT min(sold_date)::date, max(sold_date)::date FROM stg_sales"
    ).fetchone()

    con.execute(
        f"""
        CREATE OR REPLACE TABLE dim_date AS
        SELECT
            row_number() OVER (ORDER BY d) AS date_key,
            d::date AS date,
            date_trunc('month', d)::date AS month_start_date,
            year(d) AS year,
            month(d) AS month,
            strftime(d, '%Y-%m') AS year_month,
            quarter(d) AS quarter,
            dayofweek(d) AS day_of_week
        FROM generate_series(date '{min_date}', date '{max_date}', interval 1 day) AS t(d)
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE dim_sa2 AS
        SELECT
            row_number() OVER (ORDER BY sa2_code) AS sa2_key,
            sa2_code,
            sa2_name
        FROM (
            SELECT DISTINCT sa2_code, sa2_name
            FROM stg_sales
            WHERE sa2_code IS NOT NULL
        )
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE dim_suburb AS
        SELECT
            row_number() OVER (ORDER BY suburb_clean, sa2_code) AS suburb_key,
            suburb_clean,
            suburb,
            sa2_code
        FROM (
            SELECT DISTINCT suburb_clean, suburb, sa2_code
            FROM stg_sales
            WHERE suburb_clean IS NOT NULL
        )
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE dim_property_type AS
        SELECT
            row_number() OVER (ORDER BY property_type) AS property_type_key,
            property_type
        FROM (
            SELECT DISTINCT property_type
            FROM stg_sales
            WHERE property_type IS NOT NULL
        )
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE fct_sales AS
        SELECT
            s.sale_id,
            d.date_key AS sold_date_key,
            sa2.sa2_key,
            sub.suburb_key,
            pt.property_type_key,
            s.price_int,
            s.land_size_sqm,
            s.bedrooms,
            s.bathrooms,
            s.car_spaces,
            CASE
                WHEN s.price_int < 300000 THEN '<300k'
                WHEN s.price_int < 400000 THEN '300-399k'
                WHEN s.price_int < 500000 THEN '400-499k'
                WHEN s.price_int < 600000 THEN '500-599k'
                WHEN s.price_int < 700000 THEN '600-699k'
                WHEN s.price_int < 800000 THEN '700-799k'
                WHEN s.price_int < 900000 THEN '800-899k'
                WHEN s.price_int < 1000000 THEN '900-999k'
                ELSE '1m+'
            END AS price_band
        FROM stg_sales s
        LEFT JOIN dim_date d ON d.date = s.sold_date::date
        LEFT JOIN dim_sa2 sa2 ON sa2.sa2_code = s.sa2_code
        LEFT JOIN dim_suburb sub ON sub.suburb_clean = s.suburb_clean AND sub.sa2_code = s.sa2_code
        LEFT JOIN dim_property_type pt ON pt.property_type = s.property_type
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE mart_sa2_month AS
        WITH month_dim AS (
            SELECT DISTINCT
                month_start_date,
                strftime(month_start_date, '%Y-%m') AS year_month
            FROM dim_date
        ),
        sa2_month_grid AS (
            SELECT
                s.sa2_code,
                m.month_start_date,
                m.year_month
            FROM dim_sa2 s
            CROSS JOIN month_dim m
        ),
        base AS (
            SELECT
                s.sa2_code,
                date_trunc('month', s.sold_date)::date AS month_start_date,
                s.price_int
            FROM stg_sales s
            WHERE s.price_int IS NOT NULL AND s.sa2_code IS NOT NULL
        ),
        monthly_sales AS (
            SELECT
                sa2_code,
                month_start_date,
                count(*) AS n_sales,
                median(price_int) AS median_price,
                quantile_cont(price_int, 0.25) AS p25_price,
                quantile_cont(price_int, 0.75) AS p75_price
            FROM base
            GROUP BY sa2_code, month_start_date
        ),
        agg AS (
            SELECT
                g.sa2_code,
                g.month_start_date,
                g.year_month,
                coalesce(ms.n_sales, 0) AS n_sales,
                ms.median_price,
                ms.p25_price,
                ms.p75_price
            FROM sa2_month_grid g
            LEFT JOIN monthly_sales ms
                ON g.sa2_code = ms.sa2_code
               AND g.month_start_date = ms.month_start_date
        ),
        with_yoy AS (
            SELECT
                *,
                lag(median_price, 12) OVER (PARTITION BY sa2_code ORDER BY month_start_date) AS lag_12m_median_price,
                lag(n_sales, 12) OVER (PARTITION BY sa2_code ORDER BY month_start_date) AS lag_12m_n_sales,
                CASE
                    WHEN median_price IS NULL THEN NULL
                    ELSE
                        100.0 * (median_price - lag(median_price, 12) OVER (PARTITION BY sa2_code ORDER BY month_start_date))
                        / NULLIF(lag(median_price, 12) OVER (PARTITION BY sa2_code ORDER BY month_start_date), 0)
                END AS yoy_change_raw,
                median(median_price) OVER (PARTITION BY sa2_code ORDER BY month_start_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_12m_median,
                stddev_samp(median_price) OVER (PARTITION BY sa2_code ORDER BY month_start_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS volatility
            FROM agg
        ),
        stabilized AS (
            SELECT
                *,
                CASE
                    WHEN n_sales >= 5 AND lag_12m_n_sales >= 5 THEN yoy_change_raw
                    ELSE NULL
                END AS yoy_change,
                CASE
                    WHEN n_sales >= 5 AND lag_12m_n_sales >= 5 THEN TRUE
                    ELSE FALSE
                END AS yoy_eligible
            FROM with_yoy
        ),
        scored AS (
            SELECT
                *,
                (yoy_change - avg(yoy_change) OVER ()) / NULLIF(stddev_samp(yoy_change) OVER (), 0) AS yoy_z,
                (ln(n_sales + 1) - avg(ln(n_sales + 1)) OVER ()) / NULLIF(stddev_samp(ln(n_sales + 1)) OVER (), 0) AS volume_z
            FROM stabilized
        )
        SELECT
            s.sa2_code,
            d.sa2_name,
            s.month_start_date,
            s.year_month,
            s.n_sales,
            s.median_price,
            s.p25_price,
            s.p75_price,
            s.yoy_change_raw,
            s.yoy_change,
            s.yoy_eligible,
            s.lag_12m_n_sales,
            s.rolling_12m_median,
            s.volatility,
            0.7 * s.yoy_z + 0.3 * s.volume_z AS heat_score
        FROM scored s
        LEFT JOIN dim_sa2 d ON d.sa2_code = s.sa2_code
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE mart_suburb_month AS
        WITH base AS (
            SELECT
                s.suburb_clean,
                s.suburb,
                s.sa2_code,
                date_trunc('month', s.sold_date)::date AS month_start_date,
                strftime(s.sold_date, '%Y-%m') AS year_month,
                s.price_int
            FROM stg_sales s
            WHERE s.price_int IS NOT NULL AND s.suburb_clean IS NOT NULL
        ),
        agg AS (
            SELECT
                suburb_clean,
                suburb,
                sa2_code,
                month_start_date,
                year_month,
                count(*) AS n_sales,
                median(price_int) AS median_price,
                quantile_cont(price_int, 0.25) AS p25_price,
                quantile_cont(price_int, 0.75) AS p75_price
            FROM base
            GROUP BY suburb_clean, suburb, sa2_code, month_start_date, year_month
        ),
        with_yoy AS (
            SELECT
                *,
                lag(median_price, 12) OVER (PARTITION BY suburb_clean ORDER BY month_start_date) AS lag_12m_median_price,
                lag(n_sales, 12) OVER (PARTITION BY suburb_clean ORDER BY month_start_date) AS lag_12m_n_sales,
                100.0 * (median_price - lag(median_price, 12) OVER (PARTITION BY suburb_clean ORDER BY month_start_date))
                / NULLIF(lag(median_price, 12) OVER (PARTITION BY suburb_clean ORDER BY month_start_date), 0) AS yoy_change_raw,
                median(median_price) OVER (PARTITION BY suburb_clean ORDER BY month_start_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_12m_median,
                stddev_samp(median_price) OVER (PARTITION BY suburb_clean ORDER BY month_start_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS volatility
            FROM agg
        ),
        stabilized AS (
            SELECT
                *,
                CASE
                    WHEN n_sales >= 5 AND lag_12m_n_sales >= 5 THEN yoy_change_raw
                    ELSE NULL
                END AS yoy_change,
                CASE
                    WHEN n_sales >= 5 AND lag_12m_n_sales >= 5 THEN TRUE
                    ELSE FALSE
                END AS yoy_eligible
            FROM with_yoy
        ),
        scored AS (
            SELECT
                *,
                (yoy_change - avg(yoy_change) OVER ()) / NULLIF(stddev_samp(yoy_change) OVER (), 0) AS yoy_z,
                (ln(n_sales) - avg(ln(n_sales)) OVER ()) / NULLIF(stddev_samp(ln(n_sales)) OVER (), 0) AS volume_z
            FROM stabilized
        )
        SELECT
            suburb_clean,
            suburb,
            sa2_code,
            month_start_date,
            year_month,
            n_sales,
            median_price,
            p25_price,
            p75_price,
            yoy_change_raw,
            yoy_change,
            yoy_eligible,
            lag_12m_n_sales,
            rolling_12m_median,
            volatility,
            0.7 * yoy_z + 0.3 * volume_z AS heat_score
        FROM scored
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE mart_adelaide_month AS
        WITH monthly AS (
            SELECT
                d.month_start_date,
                count(*) AS n_sales,
                median(s.price_int) AS median_price,
                quantile_cont(s.price_int, 0.25) AS p25_price,
                quantile_cont(s.price_int, 0.75) AS p75_price
            FROM fct_sales s
            JOIN dim_date d
                ON d.date_key = s.sold_date_key
            WHERE s.price_int IS NOT NULL
            GROUP BY d.month_start_date
        ),
        enriched AS (
            SELECT
                *,
                100.0 * (median_price - lag(median_price, 12) OVER (ORDER BY month_start_date))
                / NULLIF(lag(median_price, 12) OVER (ORDER BY month_start_date), 0) AS yoy_change,
                median(median_price) OVER (
                    ORDER BY month_start_date
                    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                ) AS rolling_12m_median,
                stddev_samp(median_price) OVER (
                    ORDER BY month_start_date
                    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                ) AS volatility
            FROM monthly
        )
        SELECT
            month_start_date,
            strftime(month_start_date, '%Y-%m') AS year_month,
            n_sales,
            median_price,
            p25_price,
            p75_price,
            yoy_change,
            rolling_12m_median,
            volatility
        FROM enriched
        ORDER BY month_start_date
        """
    )

    # Export marts for BI
    con.execute(
        f"COPY mart_sa2_month TO '{os.path.join(out_dir, 'mart_sa2_month.parquet')}' (FORMAT 'parquet')"
    )
    con.execute(
        f"COPY mart_sa2_month TO '{os.path.join(out_dir, 'mart_sa2_month.csv')}' (HEADER, DELIMITER ',')"
    )
    con.execute(
        f"COPY mart_suburb_month TO '{os.path.join(out_dir, 'mart_suburb_month.parquet')}' (FORMAT 'parquet')"
    )
    con.execute(
        f"COPY mart_suburb_month TO '{os.path.join(out_dir, 'mart_suburb_month.csv')}' (HEADER, DELIMITER ',')"
    )
    con.execute(
        f"COPY mart_adelaide_month TO '{os.path.join(out_dir, 'mart_adelaide_month.parquet')}' (FORMAT 'parquet')"
    )
    con.execute(
        f"COPY mart_adelaide_month TO '{os.path.join(out_dir, 'mart_adelaide_month.csv')}' (HEADER, DELIMITER ',')"
    )

    con.close()
    print("built warehouse")
    print("wrote marts to outputs/public")


if __name__ == "__main__":
    main()
