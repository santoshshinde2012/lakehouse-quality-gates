"""Composition root: the only place that knows every concrete class and wires them together."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from soda_lakehouse_dq.adapters.clock import SystemClock
from soda_lakehouse_dq.adapters.contracts import FileContractRepository
from soda_lakehouse_dq.adapters.datasources.base import DataSourceProvider
from soda_lakehouse_dq.adapters.datasources.duckdb_provider import (
    DuckDBProvider,
    lakehouse_connection_factory,
)
from soda_lakehouse_dq.adapters.datasources.trino_provider import TrinoProvider
from soda_lakehouse_dq.adapters.lakehouse_repo import import_lakehouse_client
from soda_lakehouse_dq.adapters.local_medallion import LocalMedallionBuilder
from soda_lakehouse_dq.adapters.notifiers.console import ConsoleNotifier
from soda_lakehouse_dq.adapters.notifiers.slack import SlackWebhookNotifier
from soda_lakehouse_dq.adapters.sinks.iceberg_sink import IcebergSink
from soda_lakehouse_dq.adapters.sinks.json_sink import JsonFileSink
from soda_lakehouse_dq.adapters.sinks.junit_sink import JUnitXmlSink
from soda_lakehouse_dq.adapters.sinks.markdown_sink import MarkdownSink
from soda_lakehouse_dq.adapters.soda_verifier import SodaContractVerifier
from soda_lakehouse_dq.adapters.wap.audit_json import JsonBranchAuditSink
from soda_lakehouse_dq.adapters.wap.pinned_gate import PinnedSnapshotGate
from soda_lakehouse_dq.adapters.wap.pyiceberg_refs import PyIcebergSnapshotReader
from soda_lakehouse_dq.adapters.wap.spark_wap import SparkSubmitWap, subprocess_runner
from soda_lakehouse_dq.application.quality_gate import GatePolicy
from soda_lakehouse_dq.application.verify_layer import RunMedallionUseCase, VerifyLayerUseCase
from soda_lakehouse_dq.application.wap import RunWapMedallionUseCase, VerifyBranchUseCase
from soda_lakehouse_dq.domain.errors import DataQualityError
from soda_lakehouse_dq.domain.ports import Notifier, ResultSink
from soda_lakehouse_dq.settings import Engine, Settings


@dataclass(frozen=True, slots=True)
class Container:
    settings: Settings
    repository: FileContractRepository
    verify_layer: VerifyLayerUseCase
    run_medallion: RunMedallionUseCase


@dataclass(frozen=True, slots=True)
class WapServices:
    """Write-audit-publish wiring. DuckDB only: it reads branch snapshots with ``AT (VERSION => id)``."""

    spark: SparkSubmitWap
    verify_branch: VerifyBranchUseCase
    run_wap: RunWapMedallionUseCase


def build_provider(settings: Settings, pins: Mapping[str, int] | None = None) -> DataSourceProvider:
    if pins and settings.engine is not Engine.DUCKDB:
        raise DataQualityError(f"auditing a branch needs DQ_ENGINE=duckdb, not {settings.engine.value}")
    match settings.engine:
        case Engine.DUCKDB:
            factory = lakehouse_connection_factory(
                settings.ldl_repo_path,
                settings.lakekeeper_url,
                settings.lakekeeper_warehouse,
                settings.catalog_alias,
            )
            return DuckDBProvider(
                factory,
                settings.duckdb_datasource_name,
                settings.catalog_alias,
                httpfs_client=settings.duckdb_httpfs_client,
                temp_directory=settings.duckdb_temp_directory,
                pins=pins,
            )
        case Engine.DUCKDB_LOCAL:
            return DuckDBProvider(
                LocalMedallionBuilder(settings.sample_dir), settings.duckdb_datasource_name, "lakehouse"
            )
        case Engine.TRINO:
            return TrinoProvider(
                settings.datasources_dir / "trino.yml",
                settings.trino_datasource_name,
                {"DQ_TRINO_HOST": settings.trino_host, "DQ_TRINO_PORT": str(settings.trino_port)},
            )
    raise DataQualityError(f"unsupported engine {settings.engine}")  # pragma: no cover


def _iceberg_catalog_factory(settings: Settings) -> Callable[[], Any]:
    def factory() -> Any:
        client = import_lakehouse_client(settings.ldl_repo_path)
        return client.catalog(settings.lakekeeper_url, settings.lakekeeper_warehouse)

    return factory


def build_sinks(settings: Settings) -> list[ResultSink]:
    registry: dict[str, type[JsonFileSink | JUnitXmlSink | MarkdownSink]] = {
        "json": JsonFileSink,
        "junit": JUnitXmlSink,
        "markdown": MarkdownSink,
    }
    unknown = set(settings.sinks) - registry.keys()
    if unknown:
        raise DataQualityError(f"unknown sinks: {sorted(unknown)}; choose from {sorted(registry)}")
    sinks: list[ResultSink] = [registry[name](settings.reports_dir) for name in settings.sinks]
    if settings.iceberg_sink:
        sinks.append(IcebergSink(_iceberg_catalog_factory(settings)))
    return sinks


def build_notifiers(settings: Settings) -> list[Notifier]:
    notifiers: list[Notifier] = [ConsoleNotifier()]
    if settings.slack_webhook_url is not None:
        notifiers.append(
            SlackWebhookNotifier(
                settings.slack_webhook_url.get_secret_value(),
                only_on_problems=settings.slack_only_on_problems,
            )
        )
    return notifiers


def _repository(settings: Settings) -> FileContractRepository:
    return FileContractRepository(
        settings.contracts_dir, settings.reports_dir / "rendered", {"dataset_root": settings.dataset_root()}
    )


def _verify_layer(
    settings: Settings, repository: FileContractRepository, pins: Mapping[str, int] | None = None
) -> VerifyLayerUseCase:
    return VerifyLayerUseCase(
        repository=repository,
        verifier=SodaContractVerifier(build_provider(settings, pins)),
        policy=GatePolicy(
            fail_on_warn=settings.fail_on_warn, allow_not_evaluated=settings.allow_not_evaluated
        ),
        clock=SystemClock(),
        sinks=build_sinks(settings),
        notifiers=build_notifiers(settings),
    )


def build_container(settings: Settings | None = None) -> Container:
    settings = settings or Settings()
    repository = _repository(settings)
    verify_layer = _verify_layer(settings, repository)
    return Container(settings, repository, verify_layer, RunMedallionUseCase(verify_layer, SystemClock()))


def build_wap(settings: Settings | None = None) -> WapServices:
    settings = settings or Settings()
    if settings.engine is not Engine.DUCKDB:
        raise DataQualityError(f"write-audit-publish needs DQ_ENGINE=duckdb, not {settings.engine.value}")
    repository = _repository(settings)
    spark = SparkSubmitWap(
        settings.wap_spark_container, settings.wap_job, subprocess_runner(settings.wap_timeout_s)
    )
    gate = PinnedSnapshotGate(lambda pins: _verify_layer(settings, repository, pins))
    verify_branch = VerifyBranchUseCase(
        repository,
        PyIcebergSnapshotReader(_iceberg_catalog_factory(settings)),
        gate,
        JsonBranchAuditSink(settings.reports_dir),
    )
    run_wap = RunWapMedallionUseCase(
        spark, spark, verify_branch, SystemClock(), drop_blocked=settings.wap_drop_blocked
    )
    return WapServices(spark, verify_branch, run_wap)
