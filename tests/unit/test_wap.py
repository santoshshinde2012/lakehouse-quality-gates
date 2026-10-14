"""Write-audit-publish: domain, use cases, Spark/PyIceberg adapters, DuckDB snapshot pins (all with fakes)."""

from __future__ import annotations

import re
import sys
import types
from dataclasses import dataclass, field

import duckdb
import pytest

from soda_lakehouse_dq.adapters.datasources.duckdb_provider import DuckDBProvider
from soda_lakehouse_dq.adapters.wap.audit_json import JsonBranchAuditSink, load_audit
from soda_lakehouse_dq.adapters.wap.pinned_gate import PinnedSnapshotGate
from soda_lakehouse_dq.adapters.wap.pyiceberg_refs import PyIcebergSnapshotReader
from soda_lakehouse_dq.adapters.wap.spark_wap import SparkSubmitWap, parse_wap_json, subprocess_runner
from soda_lakehouse_dq.application.quality_gate import GatePolicy
from soda_lakehouse_dq.application.verify_layer import VerifyLayerUseCase
from soda_lakehouse_dq.application.wap import RunWapMedallionUseCase, VerifyBranchUseCase, dataset_table
from soda_lakehouse_dq.domain.errors import DataQualityError, DataSourceError
from soda_lakehouse_dq.domain.models import GateReport, GateStatus, Layer, Outcome
from soda_lakehouse_dq.domain.ports import AuditWriter, BranchGate, BranchPublisher, SnapshotReader
from soda_lakehouse_dq.domain.wap import BranchState, TableState, WapOutcome, branch_name, validate_branch
from tests.conftest import FIXED_NOW, FakeRepository, FakeVerifier, make_spec

TABLES = {Layer.BRONZE: "bronze.orders", Layer.SILVER: "silver.orders", Layer.GOLD: "gold.orders"}


@dataclass
class FakeSpark:
    """AuditWriter + BranchPublisher: main moves only on publish."""

    main: dict[str, int] = field(default_factory=lambda: dict.fromkeys(TABLES.values(), 1))
    branches: dict[tuple[str, str], int] = field(default_factory=dict)
    calls: list[tuple[str, str, str | None]] = field(default_factory=list)

    def write(self, layer: Layer, branch: str, source: str | None = None) -> BranchState:
        self.calls.append(("write", layer.value, source))
        table = TABLES[layer]
        self.branches[(table, branch)] = self.main[table] + 100
        return self._state("write", layer, branch)

    def publish(self, layer: Layer, branch: str, pins, *, main_before=None) -> BranchState:
        self.calls.append(("publish", layer.value, None))
        table = TABLES[layer]
        before = self.main[table]
        self.main[table] = pins[table]
        moved = TableState(table, self.main[table], 5, main_before=before)
        return BranchState("publish", layer, branch, 1.0, {table: moved})

    def drop(self, layer: Layer, branch: str) -> BranchState:
        self.calls.append(("drop", layer.value, None))
        self.branches.pop((TABLES[layer], branch), None)
        return self._state("drop", layer, branch)

    def _state(self, action: str, layer: Layer, branch: str) -> BranchState:
        table = TABLES[layer]
        snap = self.branches.get((table, branch))
        return BranchState(
            action, layer, branch, 1.0, {table: TableState(table, self.main[table], 5, snap, 5)}
        )

    def snapshot(self, table: str, ref: str) -> int | None:
        return self.main.get(table) if ref == "main" else self.branches.get((table, ref))


@dataclass
class FakeGate:
    blocked: set[Layer] = field(default_factory=set)
    calls: list[tuple[Layer, dict[str, int], str]] = field(default_factory=list)

    def verify(self, pipeline, layer, pins, run_id) -> GateReport:
        self.calls.append((layer, dict(pins), run_id))
        status = GateStatus.BLOCKED if layer in self.blocked else GateStatus.PASSED
        reasons = ("x",) if status is GateStatus.BLOCKED else ()
        return GateReport(run_id, pipeline, layer, "fake", status, (), reasons, FIXED_NOW)


def _repo() -> FakeRepository:
    return FakeRepository({layer: [make_spec("orders", layer)] for layer in Layer})


def _wap(clock, spark, gate, **kw):
    return RunWapMedallionUseCase(spark, spark, VerifyBranchUseCase(_repo(), spark, gate), clock, **kw)


def test_fakes_satisfy_ports():
    assert isinstance(FakeSpark(), AuditWriter)
    assert isinstance(FakeSpark(), BranchPublisher)
    assert isinstance(FakeSpark(), SnapshotReader)
    assert isinstance(FakeGate(), BranchGate)


def test_branch_names_are_safe():
    assert branch_name("20261014T105200Z-ab12CD") == "audit_20261014t105200z_ab12cd"
    assert len(branch_name("x" * 200)) == 64
    for bad in ("main", "publish_deadbeef", "publish_", "Audit", "a-b", "a;drop", ""):
        with pytest.raises(DataQualityError):
            validate_branch(bad)


def test_dataset_table():
    assert dataset_table("lakehouse_duckdb/silver/orders") == "silver.orders"
    with pytest.raises(DataQualityError):
        dataset_table("orders")


def test_good_batch_publishes_every_layer(clock):
    spark, gate = FakeSpark(), FakeGate()
    results = _wap(clock, spark, gate).execute("retail", source="/good")
    assert [r.outcome for r in results] == [WapOutcome.PUBLISHED] * 3
    assert spark.main == dict.fromkeys(TABLES.values(), 101)
    assert spark.branches == {(table, "audit_run_1"): 101 for table in TABLES.values()}
    assert spark.calls[0] == ("write", "bronze", "/good")
    assert spark.calls[2] == ("write", "silver", None)  # only bronze reads the batch
    assert gate.calls[0] == (Layer.BRONZE, {"bronze.orders": 101}, "run-1")
    assert results[0].published is not None
    assert results[0].published.tables["bronze.orders"].main_moved


def test_blocked_gate_leaves_main_and_keeps_branch(clock):
    spark, gate = FakeSpark(), FakeGate({Layer.SILVER})
    results = _wap(clock, spark, gate).execute("retail", branch="audit_x")
    assert [(r.layer, r.outcome) for r in results] == [
        (Layer.BRONZE, WapOutcome.PUBLISHED),
        (Layer.SILVER, WapOutcome.BLOCKED),
    ]
    assert spark.main["silver.orders"] == 1  # untouched
    assert spark.main["gold.orders"] == 1  # never written
    assert spark.branches == {
        ("bronze.orders", "audit_x"): 101,
        ("silver.orders", "audit_x"): 101,
    }  # kept for forensics
    assert ("publish", "silver", None) not in spark.calls
    assert results[-1].published is None


def test_blocked_branch_dropped_when_configured(clock):
    spark = FakeSpark()
    _wap(clock, spark, FakeGate({Layer.BRONZE}), drop_blocked=True).execute("retail", [Layer.BRONZE])
    assert spark.calls[-1] == ("drop", "bronze", None)
    assert spark.main["bronze.orders"] == 1
    assert not spark.branches


def test_verify_branch_requires_the_branch(clock):
    with pytest.raises(DataQualityError, match="nothing to audit"):
        VerifyBranchUseCase(_repo(), FakeSpark(), FakeGate()).execute("retail", Layer.GOLD, "audit_none", "r")
    with pytest.raises(DataQualityError, match="invalid audit branch"):
        VerifyBranchUseCase(_repo(), FakeSpark(), FakeGate()).execute("retail", Layer.GOLD, "main", "r")


def test_pinned_gate_runs_a_fresh_use_case_per_pin_set(clock):
    seen: list[dict[str, int]] = []

    def factory(pins):
        seen.append(dict(pins))
        return VerifyLayerUseCase(
            FakeRepository(), FakeVerifier({Layer.GOLD: (Outcome.FAILED,)}), GatePolicy(), clock
        )

    gate = PinnedSnapshotGate(factory)
    assert not gate.verify("retail", Layer.SILVER, {"silver.orders": 7}, "r1").blocked
    assert gate.verify("retail", Layer.GOLD, {"gold.x": 8}, "r2").blocked
    assert seen == [{"silver.orders": 7}, {"gold.x": 8}]


OUT = (
    'noise\nWAP_JSON {"action": "publish", "layer": "silver", "branch": "audit_x", "seconds": 2.5, '
    '"tables": {"silver.orders": {"main_before": 1, "main_snapshot": 2, "main_rows": 9, '
    '"branch_snapshot": null, "branch_rows": null, "exists": true, "refs": {"main": 2}}}}\n'
)


def test_parse_wap_json():
    state = parse_wap_json(OUT)
    assert state.layer is Layer.SILVER
    assert state.seconds == 2.5
    assert state.tables["silver.orders"] == TableState("silver.orders", 2, 9, None, None, 1, {"main": 2})
    with pytest.raises(DataSourceError, match="no WAP_JSON"):
        parse_wap_json("nothing")


def test_spark_adapter_commands():
    seen: list[list[str]] = []
    spark = SparkSubmitWap(runner=lambda args: seen.append(list(args)) or OUT)
    spark.write(Layer.BRONZE, "audit_x", "/opt/data/sample")
    spark.publish(Layer.SILVER, "audit_x", {"silver.orders": 123}, main_before={"silver.orders": 1})
    spark.drop(Layer.GOLD, "audit_x")
    spark.status(Layer.GOLD)
    spark.direct(Layer.BRONZE, "/d")
    assert seen[0] == [
        "docker", "exec", "ldl-spark", "/opt/spark/bin/spark-submit", "--master", "local[*]",
        "/opt/soda-wap/wap_job.py", "write", "--layer", "bronze", "--branch", "audit_x",
        "--source", "/opt/data/sample",
    ]  # fmt: skip
    assert seen[1][-9:-4] == ["publish", "--layer", "silver", "--branch", "audit_x"]
    assert seen[1][-4:] == ["--pins", '{"silver.orders": 123}', "--main-before", '{"silver.orders": 1}']
    assert seen[2][-5] == "drop"
    assert seen[3][-3:] == ["status", "--layer", "gold"]
    assert seen[4][-5:] == ["direct", "--layer", "bronze", "--source", "/d"]
    with pytest.raises(DataQualityError):
        spark.command("write", Layer.BRONZE, "x; rm -rf /")


def test_subprocess_runner_reports_failures():
    assert subprocess_runner()([sys.executable, "-c", "print('WAP_JSON {}')"]).strip() == "WAP_JSON {}"
    with pytest.raises(DataSourceError, match="exit 3"):
        subprocess_runner()([sys.executable, "-c", "import sys; print('boom', file=sys.stderr); sys.exit(3)"])
    with pytest.raises(DataSourceError, match="could not run"):
        subprocess_runner()(["/nonexistent/binary"])


class _NoSuchTableError(Exception):
    pass


def test_pyiceberg_reader(monkeypatch):
    exc_mod = types.ModuleType("pyiceberg.exceptions")
    exc_mod.NoSuchTableError = _NoSuchTableError
    monkeypatch.setitem(sys.modules, "pyiceberg", types.ModuleType("pyiceberg"))
    monkeypatch.setitem(sys.modules, "pyiceberg.exceptions", exc_mod)

    class Catalog:
        loads = 0

        def load_table(self, name):
            if name == "x.missing":
                raise _NoSuchTableError(name)
            if name == "x.broken":
                raise RuntimeError("catalog down")
            ref = types.SimpleNamespace(snapshot_id=42)
            return types.SimpleNamespace(metadata=types.SimpleNamespace(refs={"main": ref}))

    made: list[Catalog] = []
    reader = PyIcebergSnapshotReader(lambda: made.append(Catalog()) or made[-1])
    assert reader.snapshot("x.t", "main") == 42
    assert reader.snapshot("x.t", "audit_1") is None
    assert reader.snapshot("x.missing", "main") is None
    with pytest.raises(DataSourceError, match="catalog down"):
        reader.snapshot("x.broken", "main")
    assert len(made) == 1  # the catalog is built once


class PinRecorder:
    """Real DuckDB with ``AT (VERSION => n)`` stripped (only Iceberg tables support it)."""

    def __init__(self, con):
        self.con = con
        self.statements: list[str] = []

    def execute(self, sql, params=None):
        self.statements.append(sql)
        sql = re.sub(r" AT \(VERSION => \d+\)", "", sql)
        return self.con.execute(sql, params) if params is not None else self.con.execute(sql)


def test_duckdb_pins_read_branch_snapshots_through_views():
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS lakehouse")
    for schema in ("bronze", "silver"):
        con.execute(f"CREATE SCHEMA lakehouse.{schema}")
    con.execute("CREATE TABLE lakehouse.silver.orders AS SELECT 1 AS id")
    con.execute("CREATE TABLE lakehouse.silver.customers AS SELECT 2 AS id")
    con.execute("CREATE TABLE lakehouse.bronze.orders_raw AS SELECT 3 AS id")
    rec = PinRecorder(con)
    DuckDBProvider(lambda: rec, "ds", "lakehouse", pins={"silver.orders": 123}).configure(rec)
    views = [s for s in rec.statements if "CREATE OR REPLACE VIEW" in s]
    assert (
        'dq_audit."silver"."orders" AS SELECT * FROM "lakehouse"."silver"."orders" AT (VERSION => 123)'
        in views[2]
    )
    assert all("AT (VERSION" not in v for v in views if '"orders" AS' not in v or "bronze" in v)
    assert rec.statements[-1] == 'USE dq_audit."silver"'
    assert con.execute("SELECT id FROM silver.orders").fetchone() == (1,)
    assert con.execute("SELECT id FROM bronze.orders_raw").fetchone() == (3,)
    assert con.execute("SELECT current_database()").fetchone() == ("dq_audit",)


def test_missing_primary_branch_fails_even_when_another_primary_exists():
    repo = FakeRepository({Layer.SILVER: [make_spec("orders"), make_spec("customers")]})
    spark = FakeSpark(branches={("silver.orders", "audit_x"): 101})
    gate = FakeGate()
    with pytest.raises(DataQualityError, match=r"missing on primary tables: silver\.customers"):
        VerifyBranchUseCase(repo, spark, gate).execute("retail", Layer.SILVER, "audit_x", "r")
    assert not gate.calls


def test_reference_branch_can_fall_back_to_main():
    from dataclasses import replace

    contract = replace(make_spec("orders"), references=("ds/bronze/orders_raw",))
    repo = FakeRepository({Layer.SILVER: [contract]})
    spark = FakeSpark(branches={("silver.orders", "audit_x"): 101})
    gate = FakeGate()
    VerifyBranchUseCase(repo, spark, gate).execute("retail", Layer.SILVER, "audit_x", "r")
    assert gate.calls[0][1] == {"silver.orders": 101}


def test_missing_primary_cannot_be_hidden_by_a_reference_branch():
    from dataclasses import replace

    contract = replace(make_spec("orders"), references=("ds/bronze/orders_raw",))
    repo = FakeRepository({Layer.SILVER: [contract]})
    spark = FakeSpark(branches={("bronze.orders_raw", "audit_x"): 101})
    with pytest.raises(DataQualityError, match=r"silver\.orders"):
        VerifyBranchUseCase(repo, spark, FakeGate()).execute("retail", Layer.SILVER, "audit_x", "r")


def test_publish_is_bound_to_audited_snapshots_when_branch_advances(clock, tmp_path):
    spark = FakeSpark()
    sink = JsonBranchAuditSink(tmp_path)

    class MovingGate(FakeGate):
        def verify(self, pipeline, layer, pins, run_id):
            report = super().verify(pipeline, layer, pins, run_id)
            spark.branches[("silver.orders", "audit_x")] = 999
            return report

    verify = VerifyBranchUseCase(_repo(), spark, MovingGate(), sink)
    result = RunWapMedallionUseCase(spark, spark, verify, clock).execute(
        "retail", [Layer.SILVER], branch="audit_x"
    )[0]
    assert result.audited_pins == {"silver.orders": 101}
    assert spark.main["silver.orders"] == 101
    assert spark.branches[("silver.orders", "audit_x")] == 999
    audit = load_audit(tmp_path / "wap/audit_x/silver.audit.json")
    assert (tmp_path / "wap/audit_x/silver.audit.json").stat().st_mode & 0o777 == 0o644
    assert audit.pins == result.audited_pins
    assert audit.main_before == {"silver.orders": 1}


def test_failed_audit_persistence_prevents_publication(clock):
    class FailingSink:
        def write(self, audit):
            raise DataSourceError("audit disk unavailable")

    spark = FakeSpark()
    verify = VerifyBranchUseCase(_repo(), spark, FakeGate(), FailingSink())
    with pytest.raises(DataSourceError, match="audit disk unavailable"):
        RunWapMedallionUseCase(spark, spark, verify, clock).execute("retail", [Layer.BRONZE])
    assert spark.main["bronze.orders"] == 1
    assert all(action != "publish" for action, _, _ in spark.calls)
