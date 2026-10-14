"""Optional Iceberg sink: append one row per check to ``ops.dq_results`` through PyIceberg.

Requires the ``lakehouse`` extra and a running Lakekeeper. Enabled with ``DQ_ICEBERG_SINK=true``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from soda_lakehouse_dq.domain.models import GateReport

TABLE = "ops.dq_results"


def report_rows(report: GateReport) -> list[dict[str, Any]]:
    return [
        {
            "run_id": report.run_id,
            "created_at": report.created_at,
            "pipeline": report.pipeline,
            "layer": report.layer.value,
            "engine": report.engine,
            "gate_status": report.status.value,
            "contract": r.contract.name,
            "dataset": r.contract.dataset,
            "check_name": c.name,
            "check_type": c.check_type,
            "column_name": c.column,
            "outcome": c.outcome.value,
        }
        for r in report.results
        for c in r.checks
    ]


class IcebergSink:
    def __init__(self, catalog_factory: Callable[[], Any], table: str = TABLE) -> None:
        self._catalog_factory = catalog_factory
        self._table = table

    def write(self, report: GateReport) -> None:
        rows = report_rows(report)
        if not rows:
            return
        import pyarrow as pa

        # Dataset-only checks have no column name. Inference would give an all-NULL field
        # Arrow's null type, which Iceberg v2 cannot create and which varies between runs.
        schema = pa.schema(
            [("run_id", pa.string()), ("created_at", pa.timestamp("us", tz="UTC"))]
            + [
                (name, pa.string())
                for name in (
                    "pipeline",
                    "layer",
                    "engine",
                    "gate_status",
                    "contract",
                    "dataset",
                    "check_name",
                    "check_type",
                    "column_name",
                    "outcome",
                )
            ]
        )
        data = pa.Table.from_pylist(rows, schema=schema)
        catalog = self._catalog_factory()
        namespace = self._table.split(".")[0]
        catalog.create_namespace_if_not_exists(namespace)
        table = catalog.create_table_if_not_exists(self._table, schema=data.schema)
        table.append(data)
