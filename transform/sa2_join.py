#!/usr/bin/env python3
"""
Spatial join sales points to SA2 polygons.
"""

import argparse
import os
import sys

import geopandas as gpd
import pandas as pd


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_SALES = os.path.join(PROJECT_ROOT, "outputs", "models", "processed_geodata.pkl")
DEFAULT_SA2 = os.path.join(
    PROJECT_ROOT,
    "data",
    "raw",
    "abs",
    "asgs_2021",
    "sa2_2021",
    "SA2_2021_AUST_GDA2020.shp",
)
DEFAULT_OUT = os.path.join(PROJECT_ROOT, "data", "sales_with_sa2.parquet")


def ensure_crs(gdf, label, assumed_epsg=4326):
    if gdf.crs is None:
        print(
            f"warn: {label}.crs missing, assuming EPSG:{assumed_epsg}",
            file=sys.stderr,
        )
        gdf = gdf.set_crs(epsg=assumed_epsg)
    return gdf


def load_sales(path):
    obj = pd.read_pickle(path)
    if isinstance(obj, dict):
        obj = next(iter(obj.values()))
    if not isinstance(obj, gpd.GeoDataFrame):
        obj = gpd.GeoDataFrame(obj, geometry="geometry", crs=getattr(obj, "crs", None))
    return obj


def spatial_join_sales_to_sa2(sales, sa2):
    if "SA2_CODE21" not in sa2.columns or "SA2_NAME21" not in sa2.columns:
        raise ValueError("SA2 shapefile must include SA2_CODE21 and SA2_NAME21.")

    sa2_slim = sa2[["SA2_CODE21", "SA2_NAME21", "geometry"]].copy()

    sales = ensure_crs(sales, "sales")
    sa2_slim = ensure_crs(sa2_slim, "sa2")
    if sales.crs != sa2_slim.crs:
        sa2_slim = sa2_slim.to_crs(sales.crs)

    before = len(sales)
    sales = sales[sales.geometry.notna()].copy()
    try:
        sales = sales[sales.is_valid].copy()
    except Exception:
        pass
    dropped = before - len(sales)
    if dropped:
        print(f"dropped {dropped} rows with bad geometry", file=sys.stderr)

    try:
        joined = gpd.sjoin(sales, sa2_slim, how="left", predicate="within")
    except TypeError:
        joined = gpd.sjoin(sales, sa2_slim, how="left", op="within")

    if "index_right" in joined.columns:
        joined = joined.drop(columns=["index_right"])
    return joined


def report_coverage(joined):
    n = len(joined)
    n_mapped = int(joined["SA2_CODE21"].notna().sum()) if "SA2_CODE21" in joined.columns else 0
    pct = (n_mapped / n * 100.0) if n else 0.0
    print(f"SA2 mapping coverage: {n_mapped}/{n} ({pct:.2f}%)")

    if n and n_mapped < n and "suburb" in joined.columns:
        unmapped = joined[joined["SA2_CODE21"].isna()]
        top = unmapped["suburb"].astype(str).value_counts().head(10)
        if len(top):
            print("Top unmapped suburbs (first 10):")
            for name, count in top.items():
                print(f"  {name}: {count}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sales-path", default=DEFAULT_SALES, help="sales pickle path")
    parser.add_argument("--sa2-path", default=DEFAULT_SA2, help="SA2 shapefile path")
    parser.add_argument("--out-path", default=DEFAULT_OUT, help="output parquet path")
    args = parser.parse_args()

    sales_path = os.path.abspath(os.path.expanduser(args.sales_path))
    sa2_path = os.path.abspath(os.path.expanduser(args.sa2_path))
    out_path = os.path.abspath(os.path.expanduser(args.out_path))
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    if not os.path.exists(sales_path):
        raise FileNotFoundError(f"Sales file not found: {sales_path}")
    if not os.path.exists(sa2_path):
        raise FileNotFoundError(f"SA2 shapefile not found: {sa2_path}")

    print("reading sales...", file=sys.stderr)
    sales = load_sales(sales_path)
    if "geometry" not in sales.columns:
        raise ValueError("Sales data must include geometry.")

    print("reading sa2...", file=sys.stderr)
    sa2 = gpd.read_file(sa2_path)

    joined = spatial_join_sales_to_sa2(sales, sa2)
    report_coverage(joined)

    print("wrote data/sales_with_sa2.parquet", file=sys.stderr)
    joined.to_parquet(out_path, index=False)


if __name__ == "__main__":
    main()
