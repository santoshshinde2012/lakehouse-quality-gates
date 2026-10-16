# Figures

Scripts that rebuild every data chart and diagram from files in this repository. Output goes to
`docs/figures/build/` (ignored by git). Each script checks its inputs (row counts, verdicts, hashes) and fails
rather than drawing a chart from incomplete data.

```bash
R="uv run --no-project --with matplotlib --with seaborn python"
$R docs/figures/generate_assets.py              # tool comparison chart + table, gate and lakehouse diagrams
$R docs/figures/generate_gate_benchmarks.py     # gate latency and client memory against batch size
$R docs/figures/generate_wap_benchmarks.py      # write-audit-publish cost
$R docs/figures/generate_snapshot_proof.py      # snapshot ids before and after a blocked batch
$R docs/figures/generate_survey_chart.py        # survey signals on data quality and trust
uv run --no-project --with matplotlib --with pyyaml python docs/figures/generate_lifecycle.py   # agree-before-enforce lifecycle
```

| Script | Input |
|---|---|
| `generate_assets.py` | `docs/results/benchmarks/2026-10-14-tool-rerun/` (`--tool-data` for another run, e.g. `2026-10-16-instabox`) |
| `generate_gate_benchmarks.py` | `docs/results/benchmarks/2026-10-14-v0.4.1/perf_raw.csv`, `perf_raw.jsonl.gz`, `specs.json` |
| `generate_wap_benchmarks.py` | `docs/results/benchmarks/2026-10-14-wap/wap_overhead.csv`, `2026-10-14-v0.4.1/airflow_runs.csv` |
| `generate_snapshot_proof.py` | `docs/results/runs/2026-10-14-v1.0-review/e2e-wap/good.json`, `bad.json` |
| `generate_lifecycle.py` | `contracts/retail/*/*.yml`: fail and warn checks per layer (a check warns when its threshold has `level: warn`); matplotlib shapes and the built-in DejaVu Sans font, no external icons |
| `generate_survey_chart.py` | `survey_sources.csv`: each figure with its publisher, sample and public URL |

The survey figures come from separate self-reported surveys with different populations and questions; read them as
signals, not one comparable measure. `diagram_icons.py` holds the outline icons the diagrams use.
