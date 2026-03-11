# Metrics Dictionary (Core)

**Common**
- `n_sales`: count of sales in the grain.
- `median_price`: median of `price_int`.
- `p25_price`, `p75_price`: 25th/75th percentiles of `price_int`.
- `yoy_change_raw`: raw percent change in `median_price` vs same month prior year.
- `yoy_change`: YoY percent change (only populated when both current month and lag-12 month have `n_sales >= 5`).
- `yoy_eligible`: boolean flag for whether YoY is considered reliable under the threshold rule.
- `lag_12m_n_sales`: sales count in the month being compared from one year prior.
- `rolling_12m_median`: 12-month rolling median of `median_price`.
- `volatility`: 12-month rolling stddev of `median_price`.
- `heat_score`: `0.7 * z(yoy_change) + 0.3 * z(ln(n_sales))`.

**Sales fields**
- `price_int`: parsed price (integer dollars).
- `land_size_sqm`: land size in square meters (parsed from text).
- `sold_date`: actual sale date.
- `month_start_date`: first day of month (date, not string).
