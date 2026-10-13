"""Tiny tool comparison: the same six checks on 7-row good/bad silver.orders fixtures, one tool at a time.

Writes results/*.txt, results/summary.csv and results/versions.txt next to this file's README
(docs/research/benchmark-tiny/results). Run it with `make bench-tiny`, which uses benchmarks/tools/.venv.
"""

from __future__ import annotations

import csv
import re
import subprocess
import sys
import time
from importlib.metadata import distributions
from pathlib import Path

HERE = Path(__file__).resolve().parent
RESULTS = HERE.parents[2] / "docs" / "research" / "benchmark-tiny" / "results"
BIN = Path(sys.executable).parent


def config_lines(path: Path) -> int:
    return sum(1 for line in path.read_text().splitlines() if not re.fullmatch(r"\s*(#.*)?", line))


def main() -> int:
    RESULTS.mkdir(parents=True, exist_ok=True)
    py = sys.executable
    runs = [
        ("soda_v4", "configs/soda_orders.yml", [py, "scripts/run_soda.py"]),
        ("gx_core", "scripts/run_gx.py", [py, "scripts/run_gx.py"]),
        ("pointblank", "scripts/run_pointblank.py", [py, "scripts/run_pointblank.py"]),
        ("pandera", "scripts/run_pandera.py", [py, "scripts/run_pandera.py"]),
        ("datacontract_good", "configs/odcs_orders_good.yaml",
         [str(BIN / "datacontract"), "test", "configs/odcs_orders_good.yaml"]),
        ("datacontract_bad", "configs/odcs_orders_bad.yaml",
         [str(BIN / "datacontract"), "test", "configs/odcs_orders_bad.yaml"]),
    ]  # fmt: skip
    rows = []
    for name, config, cmd in runs:
        start = time.perf_counter()
        proc = subprocess.run(cmd, cwd=HERE, capture_output=True, text=True, check=False)
        wall = time.perf_counter() - start
        (RESULTS / f"{name}.txt").write_text(proc.stdout + proc.stderr)
        rows.append({"tool": name, "wall_seconds_incl_import": f"{wall:.3f}", "exit_code": proc.returncode,
                     "config_lines": config_lines(HERE / config)})  # fmt: skip
        print(f"{name:18} exit={proc.returncode} {wall:6.2f}s")
    with (RESULTS / "summary.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    freeze = sorted(f"{d.metadata['Name']}=={d.version}" for d in distributions())
    (RESULTS / "versions.txt").write_text("\n".join(freeze) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
