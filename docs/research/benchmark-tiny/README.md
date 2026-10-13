# Quality-gate benchmark (silver.orders)
Reproduce: `uv venv -p 3.12 .venv && uv pip install -p .venv/bin/python "datacontract-cli[duckdb]" great-expectations pointblank pandera polars duckdb pandas soda-duckdb && ./run_all.sh`
Fixtures in `data/` (bad batch has 5 deliberate defects). Raw outputs in `results/`. Versions in `results/versions.txt`.
