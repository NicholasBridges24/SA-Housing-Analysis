# Data Quality Report

- Total rows: 5,000
- Date range: 2004-11-10 to 2025-08-21
- Months covered: 221
- Price max: 919,000

## Missingness (top 10)

| column | missing_count | missing_pct |
| --- | --- | --- |
| land_size_sqm | 4006 | 80.12 |
| car_spaces | 378 | 7.56 |
| bedrooms | 196 | 3.92 |
| bathrooms | 185 | 3.7 |
| sale_id | 0 | 0.0 |
| sold_date | 0 | 0.0 |
| sold_year | 0 | 0.0 |
| sa2_code | 0 | 0.0 |
| suburb_clean | 0 | 0.0 |
| suburb | 0 | 0.0 |

## Invalids

| rule | invalid_count | invalid_pct |
| --- | --- | --- |
| price_int_null_or_nonpositive | 0 | 0.0 |
| sold_date_null | 0 | 0.0 |
| bedrooms_negative | 0 | 0.0 |
| bathrooms_negative | 0 | 0.0 |
| car_spaces_negative | 0 | 0.0 |
| land_size_sqm_nonpositive | 0 | 0.0 |

## Duplicates

Rows in duplicate groups (based on sold_date, suburb_clean, price_int, bedrooms, bathrooms, car_spaces, land_size_sqm, property_type): 0


## Outliers

Outliers (robust z-score > 3.5): 0


## Notes

- Price ceiling appears at ~$919k. This dataset was scraped with an upper cap, so insights are within-sample only.
