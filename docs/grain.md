# Grain & Keys

**Staging**
- `stg_sales_public`: one row per **sale** (public-safe subset).

**Warehouse**
- `fct_sales`: one row per **sale**.
- `dim_date`: one row per **day**.
- `dim_sa2`: one row per **SA2**.
- `dim_suburb`: one row per **suburb (SAL name)**.
- `dim_property_type`: one row per **property type**.

**Marts**
- `mart_sa2_month`: one row per **SA2 + month_start_date** across the full calendar range (dense grid; `n_sales` can be 0).
- `mart_suburb_month`: one row per **suburb + month_start_date**.
