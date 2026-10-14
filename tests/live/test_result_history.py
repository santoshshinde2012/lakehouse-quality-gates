"""T2: failed attempts survive a real Iceberg append with the existing history schema."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from soda_lakehouse_dq.adapters.sinks.iceberg_sink import TABLE, IcebergSink
from soda_lakehouse_dq.adapters.sinks.json_sink import JsonFileSink
from soda_lakehouse_dq.container import _iceberg_catalog_factory
from soda_lakehouse_dq.domain.models import ContractResult, ContractSpec, GateReport, GateStatus, Layer
from soda_lakehouse_dq.settings import Settings

pytestmark = pytest.mark.live


def test_history_records_execution_errors_and_empty_blocked_attempts(lakehouse: Path) -> None:
    from pyiceberg.expressions import EqualTo

    settings = Settings(ldl_repo_path=lakehouse, slack_webhook_url=None)
    catalog = _iceberg_catalog_factory(settings)()
    sink = IcebergSink(lambda: catalog)
    spec = ContractSpec(
        "retail.bronze.history_probe",
        "retail",
        Layer.BRONZE,
        Path("history_probe.yml"),
        Path("history_probe.yml"),
        "lakehouse_duckdb/bronze/orders_raw",
    )
    scenarios = (
        (
            (ContractResult(spec, (), ("Synthetic execution failure for history verification",)),),
            "execution_error",
        ),
        ((), "gate_decision"),
    )
    for results, record_type in scenarios:
        run_id = f"history_probe_{uuid4().hex}"
        report = GateReport(
            run_id,
            "retail",
            Layer.BRONZE,
            "duckdb",
            GateStatus.BLOCKED,
            results,
            ("Synthetic blocked attempt for history verification",),
            datetime.now(UTC),
        )
        JsonFileSink(settings.reports_dir).write(report)
        sink.write(report)
        rows = catalog.load_table(TABLE).scan(row_filter=EqualTo("run_id", run_id)).to_arrow().to_pylist()
        assert len(rows) == 1
        assert rows[0]["check_type"] == record_type
        assert rows[0]["gate_status"] == "blocked"
        assert rows[0]["outcome"] == "failed"
        if record_type == "gate_decision":
            assert rows[0]["contract"] is None
            assert rows[0]["dataset"] is None
