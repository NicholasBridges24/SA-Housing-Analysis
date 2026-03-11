# Project Summary

## Why I Chose This

I wanted one portfolio project that felt like real analyst work, not just a dashboard mockup.  
Adelaide housing was a good fit because it let me combine:
- spatial reporting (SA2/suburb)
- clear KPI design
- data quality checks and caveat handling

## Goal

My goal was to build a small, reproducible reporting product:
- clean/stage sales data
- model it into report-ready marts
- expose quality and caveats directly in the output
- present it in Power BI with useful pages, not visual noise

## What I Built

- `fct_sales` plus core dimensions in DuckDB
- `mart_sa2_month`, `mart_suburb_month`, and `mart_adelaide_month`
- DQ outputs:
  - missingness
  - invalid values
  - duplicate checks
  - freshness/coverage
  - outlier flags
- Power BI pages:
  - executive overview
  - SA2 explorer map
  - DQ audit

## Data Scope

- Full working dataset (private local): ~191k rows
- Public repo demo dataset: synthetic sample, same schema and pipeline shape

I made this split on purpose so I can share the engineering work safely without exposing private/raw data.

## Key Decisions

- I used DuckDB to keep local analytics fast and simple.
- I built marts at monthly grain because BI reporting needs stable, explicit tables.
- I guarded YoY metrics with minimum-volume eligibility so spikes are not blindly trusted.
- I treated DQ as a first-class deliverable, not an afterthought.

## What I Found

- The dataset is useful for trend analysis, but it has meaningful caveats.
- The price ceiling in the source scrape limits interpretation of upper-market behavior.
- Spatial joins are strong but not perfect, so coverage should always be visible.

That is why I included both caveat documentation and a dedicated DQ page in the report.

## If I Took This To Production

- automated ingestion and scheduled refresh
- CI checks before publishing model outputs
- semantic model governance and measure versioning
- freshness + quality alerts for breakage and drift

## What This Project Says About Me

I'm not just building charts. I'm thinking about:
- metric stability
- trust and caveats
- model design for downstream BI
- how to ship work another person can actually reuse
