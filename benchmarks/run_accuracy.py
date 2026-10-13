"""Detection accuracy: inject each defect category at several rates and compare gates to ground truth.

For every (category, rate): generate a batch with that one defect, seed the live medallion, run all three
gates on host DuckDB, and record which checks failed or warned and the counts Soda reported, next to the
ground truth from scripts/gen_scale_data.py. A clean batch is the false-positive control.

    uv run python -m benchmarks.run_accuracy --rows 100000 --rates 0.001 0.01 0.1
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from benchmarks.common import LAYERS, RESULTS, ROOT, append_csv, append_jsonl, run_probe

sys.path.insert(0, str(ROOT / "scripts"))
from gen_scale_data import DEFECTS

FIELDS = (
    "rows", "defect", "rate", "injected", "layer", "expected_status", "status", "verdict_ok",
    "expected_fail", "actual_fail", "expected_warn", "actual_warn", "tp", "fp", "fn", "reported_count",
)  # fmt: skip
_COUNT_KEYS = ("missing_count", "duplicate_count", "invalid_count", "failed_rows_count")


def expected_status(fails: list[str], warns: list[str]) -> str:
    return "blocked" if fails else "passed_with_warnings" if warns else "passed"


def reported_count(checks: list[dict[str, Any]], names: list[str]) -> str:
    counts: list[str] = []
    for check in checks:
        if check["name"] in names:
            metrics = check.get("metrics", {})
            counts.extend(str(metrics[k]) for k in _COUNT_KEYS if k in metrics)
    return ";".join(counts)


def evaluate(rows: int, defect: str | None, rate: float) -> None:
    with tempfile.TemporaryDirectory(prefix="lqg-acc-") as tmp:
        gen = [sys.executable, "scripts/gen_scale_data.py", "--rows", str(rows), "--out", tmp]
        if defect:
            gen += ["--defect", defect, "--rate", str(rate)]
        subprocess.run(gen, cwd=ROOT, check=True, capture_output=True)
        truth = json.loads((Path(tmp) / "ground_truth.json").read_text())
        subprocess.run(
            [sys.executable, "-m", "benchmarks.seed_timed", tmp], cwd=ROOT, check=True, capture_output=True
        )
    for layer in LAYERS:
        payload = run_probe("duckdb", layer).payload
        checks: list[dict[str, Any]] = payload["checks"]
        failed = sorted(c["name"] for c in checks if c["outcome"] == "failed")
        warned = sorted(c["name"] for c in checks if c["outcome"] == "warned")
        exp_f = sorted(truth["expected_fails"].get(layer, []))
        exp_w = sorted(truth["expected_warns"].get(layer, []))
        want = expected_status(exp_f, exp_w)
        flagged, expected = set(failed) | set(warned), set(exp_f) | set(exp_w)
        row = {
            "rows": rows, "defect": defect or "none", "rate": rate if defect else 0.0,
            "injected": truth["injected"], "layer": layer, "expected_status": want,
            "status": payload["status"], "verdict_ok": want == payload["status"],
            "expected_fail": "|".join(exp_f), "actual_fail": "|".join(failed),
            "expected_warn": "|".join(exp_w), "actual_warn": "|".join(warned),
            "tp": len(flagged & expected), "fp": len(flagged - expected), "fn": len(expected - flagged),
            "reported_count": reported_count(checks, exp_f + exp_w),
        }  # fmt: skip
        append_csv(RESULTS / "accuracy.csv", row, FIELDS)
        append_jsonl(RESULTS / "accuracy.jsonl", {**row, "checks": checks})
        print(f"{defect or 'clean':18} {rate:<6} {layer:6} want={want:8} got={payload['status']:8} "
              f"count={row['reported_count']} injected={truth['injected']}")  # fmt: skip


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--rates", type=float, nargs="+", default=[0.001, 0.01, 0.1])
    parser.add_argument("--defects", nargs="+", default=sorted(DEFECTS), choices=sorted(DEFECTS))
    args = parser.parse_args(argv)
    evaluate(args.rows, None, 0.0)
    for defect in args.defects:
        for rate in args.rates:
            evaluate(args.rows, defect, rate)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
