"""End-to-end CLI runs with the real composition root, Soda and DuckDB."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from soda_lakehouse_dq.cli import app
from tests.conftest import PROJECT_ROOT

pytestmark = pytest.mark.integration
runner = CliRunner()


@pytest.fixture(autouse=True)
def _env(monkeypatch, tmp_path):
    monkeypatch.setenv("DQ_ENGINE", "duckdb-local")
    monkeypatch.setenv("DQ_REPORTS_DIR", str(tmp_path))


def test_list_and_validate():
    listed = runner.invoke(app, ["list"])
    assert listed.exit_code == 0
    assert "lakehouse_duckdb/silver/orders" in listed.output
    assert runner.invoke(app, ["validate"]).exit_code == 0


def test_list_for_trino_renders_catalog_prefix():
    listed = runner.invoke(app, ["list", "-e", "trino"])
    assert "lakehouse_trino/lakehouse/gold/daily_order_metrics" in listed.output


def test_run_good_then_bad(tmp_path):
    assert runner.invoke(app, ["run"]).exit_code == 0
    bad = runner.invoke(app, ["run", "--sample-dir", str(PROJECT_ROOT / "fixtures" / "bad_batch")])
    assert bad.exit_code == 1
    assert "retail/bronze: blocked" in bad.output
    assert "retail/silver" not in bad.output


def test_verify_layer_with_variable(tmp_path):
    result = runner.invoke(
        app,
        [
            "verify",
            "bronze",
            "--set",
            "BRONZE_MAX_AGE_HOURS=48",
            "--sample-dir",
            str(PROJECT_ROOT / "fixtures" / "good_batch"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert list(tmp_path.glob("*_retail_bronze.xml"))


def test_seed_requires_confirmation():
    assert runner.invoke(app, ["seed", str(PROJECT_ROOT / "fixtures" / "bad_batch")]).exit_code == 2


def test_seed_without_lakehouse_repo_is_config_error(monkeypatch, tmp_path):
    monkeypatch.setenv("DQ_LDL_REPO_PATH", str(tmp_path / "missing"))
    result = runner.invoke(app, ["seed", str(PROJECT_ROOT / "fixtures" / "bad_batch"), "--yes"])
    assert result.exit_code == 2


def test_seed_writes_counts_through_repo_client(monkeypatch, tmp_path):
    pkg = tmp_path / "src" / "lakehouse_client"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text(
        "import duckdb\n"
        "def duckdb_connect(url=None, wh=None, alias='lakehouse'):\n"
        "    con = duckdb.connect()\n"
        "    con.execute(\"ATTACH ':memory:' AS lakehouse\")\n"
        "    return con\n"
    )
    monkeypatch.setenv("DQ_LDL_REPO_PATH", str(tmp_path))
    monkeypatch.chdir(PROJECT_ROOT)  # relative path, as in the README: soda-dq seed fixtures/bad_batch --yes
    result = runner.invoke(app, ["seed", "fixtures/bad_batch", "--yes"])
    assert result.exit_code == 0, result.output
    assert "bronze.orders_raw" in result.output
    assert "29 rows" in result.output
