.PHONY: synthetic warehouse dq test charts

synthetic:
	python transform/generate_synthetic_sample.py

warehouse:
	python transform/build_warehouse.py --stg-path data/public_sample/stg_sales_public.csv

dq:
	python quality/build_dq.py --stg-path data/public_sample/stg_sales_public.csv

test:
	python quality/run_tests.py

charts:
	python viz/make_portfolio_charts.py --db outputs/warehouse.duckdb --out outputs/figures
