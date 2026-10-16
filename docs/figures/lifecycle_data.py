"""Fail and warn checks per layer, counted from the contracts; pure logic plus one loader."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import yaml

CONTRACTS = Path(__file__).resolve().parents[2] / "contracts" / "retail"
LAYERS = ("bronze", "silver", "gold")


def contract_checks(contract: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """Dataset-level checks followed by every column's checks."""
    found = list(contract.get("checks") or [])
    for column in contract.get("columns") or []:
        found += column.get("checks") or []
    return found


def is_warn(check: Mapping[str, Any]) -> bool:
    """A check warns when its threshold says `level: warn`; anything else fails the gate."""
    body = next(iter(check.values()), None) or {}
    return (body.get("threshold") or {}).get("level") == "warn"


def count_levels(contracts: Iterable[Mapping[str, Any]]) -> tuple[int, int]:
    """(fail, warn) checks over the contracts of one layer."""
    checks = [check for contract in contracts for check in contract_checks(contract)]
    warn = sum(is_warn(check) for check in checks)
    return len(checks) - warn, warn


def load_layer_counts(folder: Path = CONTRACTS, layers: Iterable[str] = LAYERS) -> dict[str, tuple[int, int]]:
    return {
        layer: count_levels(
            yaml.safe_load(path.read_text(encoding="utf-8"))
            for path in sorted((folder / layer).glob("*.yml"))
        )
        for layer in layers
    }
