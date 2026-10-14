"""Execute the real Spark WAP job against a metadata fake, including publication failures and races."""

from __future__ import annotations

import importlib.util
import json
import re
import sys
import types

import pytest

from tests.conftest import PROJECT_ROOT


@pytest.fixture
def job(monkeypatch):
    sql = types.ModuleType("pyspark.sql")
    sql.DataFrame = object
    sql.SparkSession = object
    sql.functions = types.ModuleType("pyspark.sql.functions")
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", sql.functions)
    spec = importlib.util.spec_from_file_location("wap_test_job", PROJECT_ROOT / "wap/wap_job.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Rows:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def collect(self):
        return self.rows


class Catalog:
    def tableExists(self, table):  # noqa: N802 (Spark API)
        return table.removeprefix("lakehouse.") in self.spark.refs


class MetadataSpark:
    def __init__(self):
        self.refs = {
            "bronze.orders_raw": {"main": 1, "audit_x": 2},
            "bronze.customers_raw": {"main": 10, "audit_x": 11},
        }
        self.snapshots = {"bronze.orders_raw": {1, 2, 999}, "bronze.customers_raw": {10, 11}}
        self.catalog = Catalog()
        self.catalog.spark = self
        self.commands = []
        self.fail_once = None
        self.advance_on_publish = False

    def sql(self, query):
        self.commands.append(query)
        if self.fail_once is not None and self.fail_once in query:
            self.fail_once = None
            raise RuntimeError("transient catalog error")
        match = re.fullmatch(r"SELECT snapshot_id FROM lakehouse\.(\S+)\.refs WHERE name = '([^']+)'", query)
        if match:
            table, branch = match.groups()
            pin = self.refs[table].get(branch)
            return Rows([] if pin is None else [(pin,)])
        match = re.fullmatch(r"SELECT name, snapshot_id FROM lakehouse\.(\S+)\.refs", query)
        if match:
            return Rows(self.refs[match[1]].items())
        if query.startswith("SELECT COUNT(*)"):
            return Rows([(5,)])
        match = re.fullmatch(
            r"ALTER TABLE lakehouse\.(\S+) CREATE BRANCH IF NOT EXISTS (\S+) "
            r"AS OF VERSION (\d+) RETAIN 1 DAYS",
            query,
        )
        if match:
            table, branch, pin = match.groups()
            if int(pin) not in self.snapshots[table]:
                raise RuntimeError("snapshot does not exist")
            self.refs[table].setdefault(branch, int(pin))
            return Rows()
        match = re.fullmatch(
            r"CALL lakehouse.system.fast_forward\(table => '([^']+)', branch => 'main', to => '([^']+)'\)",
            query,
        )
        if match:
            table, branch = match.groups()
            if self.advance_on_publish:
                self.refs["bronze.orders_raw"]["audit_x"] = 999
            self.refs[table]["main"] = self.refs[table][branch]
            return Rows()
        match = re.fullmatch(r"ALTER TABLE lakehouse\.(\S+) DROP BRANCH IF EXISTS (\S+)", query)
        if match:
            table, branch = match.groups()
            self.refs[table].pop(branch, None)
            return Rows()
        raise AssertionError(f"unexpected SQL: {query}")


PINS = {"bronze.orders_raw": 2, "bronze.customers_raw": 11}
BEFORE = {"bronze.orders_raw": 1, "bronze.customers_raw": 10}


def test_partial_publication_keeps_sources_and_retry_finishes(job):
    spark = MetadataSpark()
    spark.fail_once = "fast_forward(table => 'bronze.customers_raw'"
    with pytest.raises(RuntimeError, match="transient catalog error"):
        job.publish(spark, "bronze", "audit_x", PINS, BEFORE)
    assert spark.refs == {
        "bronze.orders_raw": {"main": 2, "audit_x": 2},
        "bronze.customers_raw": {"main": 10, "audit_x": 11},
    }
    result = job.publish(spark, "bronze", "audit_x", PINS, BEFORE)
    assert spark.refs == {table: {"main": pin, "audit_x": pin} for table, pin in PINS.items()}
    assert result["bronze.orders_raw"]["main_before"] == 1
    assert result["bronze.customers_raw"]["main_before"] == 10
    assert all(not any(ref.startswith("publish_") for ref in value["refs"]) for value in result.values())
    assert len([sql for sql in spark.commands if "fast_forward(table => 'bronze.orders_raw'" in sql]) == 1


def test_retry_after_explicit_source_cleanup_handles_already_dropped_branch(job):
    spark = MetadataSpark()
    job.publish(spark, "bronze", "audit_x", PINS)
    spark.refs["bronze.orders_raw"].pop("audit_x")
    assert spark.refs["bronze.orders_raw"] == {"main": 2}
    assert spark.refs["bronze.customers_raw"] == {"main": 11, "audit_x": 11}
    job.publish(spark, "bronze", "audit_x", PINS)
    assert spark.refs["bronze.orders_raw"] == {"main": 2}
    assert spark.refs["bronze.customers_raw"] == {"main": 11, "audit_x": 11}


def test_branch_advancement_between_preparation_and_commit_cannot_publish_new_head(job):
    spark = MetadataSpark()
    spark.advance_on_publish = True
    result = job.publish(spark, "bronze", "audit_x", PINS)
    assert result["bronze.orders_raw"]["main_snapshot"] == 2
    assert spark.refs["bronze.orders_raw"] == {"main": 2, "audit_x": 999}
    assert all("to => 'audit_x'" not in command for command in spark.commands)


def test_invalid_snapshot_preflight_moves_no_main_and_cleans_private_refs(job):
    spark = MetadataSpark()
    with pytest.raises(RuntimeError, match="snapshot does not exist"):
        job.publish(spark, "bronze", "audit_x", PINS | {"bronze.customers_raw": 123})
    assert spark.refs == {
        "bronze.orders_raw": {"main": 1, "audit_x": 2},
        "bronze.customers_raw": {"main": 10, "audit_x": 11},
    }


def test_missing_pins_fail_before_any_metadata_access(job):
    spark = MetadataSpark()
    with pytest.raises(ValueError, match="missing audited snapshots"):
        job.publish(spark, "bronze", "audit_x", {"bronze.orders_raw": 2})
    assert not spark.commands


@pytest.mark.parametrize(
    "changes",
    [
        {"gate": "blocked"},
        {"gate": None},
        {"layer": "silver"},
        {"branch": "audit_other"},
        {"pins": {}},
        {"pins": {"bronze.orders_raw": True, "bronze.customers_raw": 11}},
    ],
)
def test_publication_rejects_blocked_or_mismatched_manifests(job, tmp_path, changes):
    path = tmp_path / "audit.json"
    data = {
        "version": 1,
        "pipeline": "retail",
        "run_id": "r",
        "gate": "passed",
        "layer": "bronze",
        "branch": "audit_x",
        "pins": PINS,
    }
    path.write_text(json.dumps(data | changes))
    with pytest.raises(ValueError, match=r"manifest|pins|snapshot"):
        job.read_audit(str(path), "bronze", "audit_x")


def test_saved_manifest_publishes_after_original_branches_are_gone(job, tmp_path):
    spark = MetadataSpark()
    path = tmp_path / "audit.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "pipeline": "retail",
                "run_id": "r",
                "gate": "passed",
                "layer": "bronze",
                "branch": "audit_x",
                "pins": PINS,
                "main_before": BEFORE,
            }
        )
    )
    pins, before = job.read_audit(str(path), "bronze", "audit_x")
    job.publish(spark, "bronze", "audit_x", pins, before)
    for refs in spark.refs.values():
        refs.pop("audit_x")
    result = job.publish(spark, "bronze", "audit_x", pins, before)
    assert all(value["main_before"] == BEFORE[table] for table, value in result.items())


def test_retry_cleans_private_ref_after_cleanup_failure(job):
    spark = MetadataSpark()
    spark.fail_once = "bronze.orders_raw DROP BRANCH IF EXISTS publish_"
    with pytest.raises(RuntimeError, match="private-ref cleanup failed"):
        job.publish(spark, "bronze", "audit_x", PINS)
    assert any(ref.startswith("publish_") for ref in spark.refs["bronze.orders_raw"])
    job.publish(spark, "bronze", "audit_x", PINS)
    assert spark.refs == {table: {"main": pin, "audit_x": pin} for table, pin in PINS.items()}


def test_source_advanced_during_private_cleanup_is_never_deleted(job):
    class RacingSpark(MetadataSpark):
        def sql(self, query):
            if "DROP BRANCH IF EXISTS publish_" in query:
                self.refs["bronze.orders_raw"]["audit_x"] = 999
            return super().sql(query)

    spark = RacingSpark()
    job.publish(spark, "bronze", "audit_x", PINS)
    assert spark.refs["bronze.orders_raw"] == {"main": 2, "audit_x": 999}
    assert all("DROP BRANCH IF EXISTS audit_x" not in command for command in spark.commands)


@pytest.mark.parametrize("action", ["write", "publish", "drop"])
@pytest.mark.parametrize("branch", ["main", "publish_deadbeef"])
def test_reserved_source_branch_is_rejected_before_spark_starts(job, monkeypatch, capsys, action, branch):
    class NeverStart:
        def appName(self, name):  # noqa: N802 (Spark API)
            pytest.fail("reserved branch validation must precede Spark startup")

    job.SparkSession = types.SimpleNamespace(builder=NeverStart())
    monkeypatch.setattr(sys, "argv", ["wap_job.py", action, "--layer", "bronze", "--branch", branch])
    with pytest.raises(SystemExit) as exc:
        job.main()
    assert exc.value.code == 2
    assert "reserved publish_ prefix" in capsys.readouterr().err
