"""Generate a deterministic retail batch at any scale, optionally with injected defects.

Writes ``customers.csv`` and ``orders_day1..N.csv`` in the layout ``soda-dq seed`` reads, plus
``ground_truth.json`` describing exactly which defects were injected and how many rows each touched.

    python scripts/gen_scale_data.py --rows 1000000 --out /tmp/batch --seed 42
    python scripts/gen_scale_data.py --rows 100000 --out /tmp/bad --defect negative_amount --rate 0.01

Clean data passes every contract in ``contracts/retail``. Each defect category maps to the checks that
must catch it (see ``DEFECTS``); the benchmark harness compares gate results to that ground truth.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

STATUSES = ("paid", "PAID", "Shipped", "shipped", "cancelled", "refunded ")
CITIES = ("Pune", "Mumbai", "Bengaluru", "Delhi", "Chennai", "Hyderabad", "Kolkata", "Jaipur")
START = datetime(2024, 1, 1)
DAYS = 90
FUTURE_TS = "2099-01-01T12:00:00"


@dataclass(frozen=True)
class Defect:
    """A defect category and the checks (per layer) that must flag it."""

    target: str  # "orders" or "customers"
    fails: dict[str, tuple[str, ...]]
    warns: dict[str, tuple[str, ...]] = field(default_factory=dict)


DEFECTS: dict[str, Defect] = {
    "missing_customer": Defect(
        "orders",
        fails={"bronze": ("Every order has a customer",), "silver": ("Customer id present",)},
    ),
    "duplicate_in_file": Defect("orders", fails={"bronze": ("No order duplicated inside one source file",)}),
    "negative_amount": Defect(
        "orders",
        fails={"silver": ("Amount is not negative",)},
        warns={"bronze": ("Amount is not negative (enforced in silver)",)},
    ),
    "orphan_customer": Defect("orders", fails={"silver": ("Customer exists in silver.customers",)}),
    "future_timestamp": Defect(
        "orders",
        fails={
            "silver": ("No order timestamped in the future",),
            "gold": ("No metrics for future days",),
        },
    ),
    "malformed_email": Defect("customers", fails={"silver": ("Email is well-formed",)}),
    "unknown_status": Defect(
        "orders", fails={}, warns={"bronze": ("Status is a known value (case-insensitive)",)}
    ),
}


@dataclass
class GroundTruth:
    rows: int
    customers: int
    seed: int
    files: int
    defect: str | None
    rate: float
    injected: int
    expected_fails: dict[str, list[str]]
    expected_warns: dict[str, list[str]]


def customers_for(rows: int) -> int:
    return max(10, rows // 10)


def generate(
    rows: int, out: Path, seed: int = 42, files: int = 5, defect: str | None = None, rate: float = 0.0
) -> GroundTruth:
    if defect is not None and defect not in DEFECTS:
        raise ValueError(f"unknown defect {defect!r}; choose from {sorted(DEFECTS)}")
    rng = random.Random(seed)
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("orders_day*.csv"):
        old.unlink()
    n_customers = customers_for(rows)
    target_n = n_customers if defect and DEFECTS[defect].target == "customers" else rows
    injected = 0 if defect is None else max(1, round(rate * target_n))
    hit = set(rng.sample(range(target_n), injected)) if injected else set()

    with (out / "customers.csv").open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["customer_id", "name", "email", "city", "signup_date"])
        for i in range(n_customers):
            email = f"user{i}@example.com"
            if defect == "malformed_email" and i in hit:
                email = f"user{i}-at-example"
            signup = date(2023, 1, 1) + timedelta(days=i % 365)
            writer.writerow([f"c-{i:07d}", f"Customer {i}", email, CITIES[i % len(CITIES)], signup])

    per_file = -(-rows // files)
    for f in range(files):
        lo, hi = f * per_file, min(rows, (f + 1) * per_file)
        with (out / f"orders_day{f + 1}.csv").open("w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["order_id", "customer_id", "order_ts", "status", "amount"])
            for i in range(lo, hi):
                customer = f"c-{rng.randrange(n_customers):07d}"
                ts = (START + timedelta(seconds=rng.randrange(DAYS * 86400))).isoformat()
                status = STATUSES[rng.randrange(len(STATUSES))]
                amount = f"{rng.uniform(1, 500):.2f}"
                bad = defect is not None and DEFECTS[defect].target == "orders" and i in hit
                if bad and defect == "missing_customer":
                    customer = ""
                elif bad and defect == "negative_amount":
                    amount = f"-{rng.uniform(1, 500):.2f}"
                elif bad and defect == "orphan_customer":
                    customer = f"x-{i:07d}"
                elif bad and defect == "future_timestamp":
                    ts = FUTURE_TS
                elif bad and defect == "unknown_status":
                    status = "teleported"
                row = [f"o-{i:09d}", customer, ts, status, amount]
                writer.writerow(row)
                if bad and defect == "duplicate_in_file":
                    writer.writerow(row)

    spec = DEFECTS[defect] if defect else None
    truth = GroundTruth(
        rows=rows,
        customers=n_customers,
        seed=seed,
        files=files,
        defect=defect,
        rate=rate,
        injected=injected,
        expected_fails={k: list(v) for k, v in (spec.fails if spec else {}).items()},
        expected_warns={k: list(v) for k, v in (spec.warns if spec else {}).items()},
    )
    (out / "ground_truth.json").write_text(json.dumps(asdict(truth), indent=2))
    return truth


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rows", type=int, required=True, help="number of orders")
    parser.add_argument("--out", type=Path, required=True, help="output directory")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--files", type=int, default=5, help="orders_day*.csv files")
    parser.add_argument("--defect", choices=sorted(DEFECTS), default=None)
    parser.add_argument("--rate", type=float, default=0.0, help="fraction of rows to corrupt")
    args = parser.parse_args(argv)
    truth = generate(args.rows, args.out, args.seed, args.files, args.defect, args.rate)
    print(json.dumps(asdict(truth)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
