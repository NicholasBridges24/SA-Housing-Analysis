#!/usr/bin/env python3
"""
Create SA2 TopoJSON for Power BI Shape Map.
"""

import argparse
import json
import os
import geopandas as gpd
from topojson import Topology


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--simplify",
        type=float,
        default=0.0005,
        help="simplify tolerance",
    )
    parser.add_argument(
        "--sa-only",
        action="store_true",
        help="keep SA2 codes starting with 4",
    )
    parser.add_argument(
        "--no-data-filter",
        action="store_true",
        help="skip data-based SA2 filtering",
    )
    args = parser.parse_args()

    shp_path = "data/raw/abs/asgs_2021/sa2_2021/SA2_2021_AUST_GDA2020.shp"
    out_path = "outputs/public/maps/sa2_2021_sa_topo_id.json"
    codes_csv = "outputs/public/mart_sa2_month.csv"
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    if not os.path.exists(shp_path):
        raise FileNotFoundError(f"Shapefile not found: {shp_path}")

    gdf = gpd.read_file(shp_path)

    # Ensure expected columns exist
    if "SA2_CODE21" not in gdf.columns or "SA2_NAME21" not in gdf.columns:
        raise ValueError("Expected SA2_CODE21 and SA2_NAME21 columns not found.")

    # Filter SA only if requested
    if args.sa_only:
        gdf = gdf[gdf["SA2_CODE21"].astype(str).str.startswith("4")].copy()

    # Filter to SA2s present in data (reduces huge outback polygons)
    if not args.no_data_filter and codes_csv:
        if os.path.exists(codes_csv):
            codes_df = gpd.pd.read_csv(codes_csv)
            if "sa2_code" in codes_df.columns:
                keep_codes = set(codes_df["sa2_code"].dropna().astype(str).unique())
                gdf = gdf[gdf["SA2_CODE21"].astype(str).isin(keep_codes)].copy()
        else:
            print("warn: outputs/public/mart_sa2_month.csv not found, skipping data filter")

    # Reproject to WGS84 for Power BI
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326", allow_override=True)
    else:
        gdf = gdf.to_crs("EPSG:4326")

    # Keep only needed columns
    gdf["SA2_CODE21"] = gdf["SA2_CODE21"].astype(str)
    gdf = gdf[["SA2_CODE21", "SA2_NAME21", "geometry"]].copy()

    topo = Topology(
        gdf,
        object_name="sa2",
        prequantize=True,
        toposimplify=args.simplify if args.simplify and args.simplify > 0 else False,
    )

    topo_json = json.loads(topo.to_json())
    obj_name = next(iter(topo_json["objects"].keys()))
    geoms = topo_json["objects"][obj_name]["geometries"]
    for g in geoms:
        props = g.get("properties", {})
        code = props.get("SA2_CODE21")
        if code is None:
            raise ValueError("Missing SA2_CODE21 on a geometry. Check topojson properties.")
        g["id"] = str(code)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(topo_json, f, ensure_ascii=False)
    print("wrote topojson")
    print(f"features: {len(gdf):,}")


if __name__ == "__main__":
    main()
