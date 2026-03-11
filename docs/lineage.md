# Lineage

Private inputs used locally:
- `processed_geodata.pkl` (sales points with geometry)
- `SA2_2021_AUST_GDA2020.*` (ABS SA2 boundary files)

Pipeline:
1. `transform/sa2_join.py`
   - point-in-polygon join: sale point -> SA2
   - output: `data/sales_with_sa2.parquet`
2. `transform/stage_sales.py`
   - parse fields and drop private columns
   - output: `outputs/public/stg_sales_public.*`
3. `transform/build_warehouse.py`
   - build dimensions, fact table, monthly marts (DuckDB)
   - output: `outputs/warehouse.duckdb` + marts in `outputs/public/`
4. `quality/build_dq.py`
   - build DQ tables + markdown report
5. Power BI layer
   - reads the marts and DQ CSVs
