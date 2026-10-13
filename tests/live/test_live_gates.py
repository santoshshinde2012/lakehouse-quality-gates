"""T2, live: the gates against the running lakehouse (Lakekeeper + RustFS) with host DuckDB.

These tests overwrite the live retail tables with the repo's fixtures and restore the good batch at the end.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.live.conftest import soda_dq

pytestmark = pytest.mark.live


@pytest.fixture(scope="module")
def good_tables(lakehouse: Path) -> Iterator[None]:
    seeded = soda_dq("seed", "fixtures/good_batch", "--yes")
    assert seeded.returncode == 0, seeded.stdout + seeded.stderr
    yield
    restored = soda_dq("seed", "fixtures/good_batch", "--yes")
    assert restored.returncode == 0, restored.stdout + restored.stderr


def test_good_batch_passes_every_gate(good_tables: None) -> None:
    result = soda_dq("run", "-e", "duckdb")
    assert result.returncode == 0, result.stdout + result.stderr


def test_bad_batch_blocks_every_gate(good_tables: None) -> None:
    seeded = soda_dq("seed", "fixtures/bad_batch", "--yes")
    assert seeded.returncode == 0, seeded.stdout + seeded.stderr
    try:
        codes = {
            layer: soda_dq("verify", layer, "-e", "duckdb").returncode
            for layer in ("bronze", "silver", "gold")
        }
    finally:
        soda_dq("seed", "fixtures/good_batch", "--yes")
    assert codes == {"bronze": 1, "silver": 1, "gold": 1}


def test_list_renders_live_dataset_names(lakehouse: Path) -> None:
    result = soda_dq("list", "-e", "duckdb")
    assert result.returncode == 0, result.stderr
    assert "lakehouse_duckdb/silver/orders" in result.stdout
