# Five-tool comparison, corrected harness

The five-run tool comparison behind the tool chart and table (`docs/figures/generate_assets.py`): Soda,
Pointblank, datacontract-cli, Pandera and GX Core at 100k and 1M orders, 5 repeats each, on the reference
machine (Apple M1 Pro). It replaces the tool numbers in `2026-10-14-v0.4.1`, which used the earlier positional GX
mapping. Produced by `make bench-tools TOOL_SIZES='100000 1000000' REPEATS=5 BENCH_OUT=<dir>` on a working tree
that already contained the harness fix (`tracked_worktree_dirty: true`; the fix landed in PR #3/#4).
`tools_metadata.json` records the SHA-256 of `tools.csv`, `tools_raw.jsonl` and the executed harness copy in
`tools_logs/run_tools.py`; the chart generator refuses to run if any of them differ. The absolute interpreter path
in `tools_metadata.json` is shortened to `<repo>`.
