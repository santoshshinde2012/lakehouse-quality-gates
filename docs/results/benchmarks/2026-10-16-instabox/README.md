# Second environment: InstaBox Linux VM

The same benchmarks as `2026-10-14-v0.4.1` and `2026-10-14-tool-rerun`, rerun on a different machine to show the
results are not specific to one laptop. Treat it as a second environment, not a like-for-like speed comparison:
the CPU architecture, core count, memory and Docker setup all differ.

| | This run | Reference runs |
|---|---|---|
| Machine | InstaBox cloud VM, AMD EPYC, x86_64, 4 vCPU, 7.8 GiB RAM, no swap | Apple M1 Pro, arm64, 10 cores, 16 GiB |
| OS | Debian GNU/Linux 13 (trixie), kernel 6.18 | macOS 26.6 |
| Docker | 29.8.2, native Linux engine | Docker Desktop 29.8.1 (10 CPUs, 8 GB) |
| Code | `lakehouse-quality-gates` `d04c8cd` (clean tree), `local-data-lakehouse` `49d1c3a` | see each folder |
| Trino | 483, default 1 GiB heap | 483 (5M rows used a 4 GiB heap) |

Commands (from the repo root, stack up with `make up-full TRINO=1` in the lakehouse and `make dq-up`):

```bash
make bench-tools TOOL_SIZES='100000 1000000' REPEATS=5 BENCH_OUT=<dir>
make bench SIZES='10000 100000 1000000' WARM=5 BENCH_OUT=<dir>
make bench-report BENCH_OUT=<dir>
```

5M orders and Airflow were not run (8 GiB of RAM). Files:

- `tools.csv`, `tools_raw.jsonl`, `tools_metadata.json`, `tools_versions.json`, `tools_logs/`: five-tool comparison,
  5 repeats per tool and size; every run returned the expected verdicts.
- `perf_raw.csv`, `perf_raw.jsonl.gz`, `pipeline_raw.csv`, `summary.md`: gate latency, 1 cold + 5 warm runs per
  layer and engine; all 162 gate runs passed.
- `specs/`: `lscpu`, `free -h`, `df -h`, `/etc/os-release`, `uname -a`, Docker, Compose and uv versions, and the run
  parameters. The host name is redacted and absolute paths in `tools_metadata.json` are shortened to `<repo>` and
  `<results>`; nothing else was edited.
- `specs.json` was written before `benchmarks/specs.py` learned to read Linux hardware, so its `cpu`, `cpu_cores`
  and `memory_bytes` fields are empty; use `specs/lscpu.txt` and `specs/free.txt` for those.

Two Linux-only problems surfaced and are fixed in the same change: `benchmarks/specs.py` called macOS `sysctl`,
and the runner container (uid 10001) could not write the bind-mounted `reports/` folder (`make reports-dir`).
The gate benchmark was rerun from scratch after the permission fix.

Charts: `python docs/figures/generate_assets.py --tool-data docs/results/benchmarks/2026-10-16-instabox --output-dir docs/figures/build/instabox`.
