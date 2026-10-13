"""Composition root: the only place that knows every concrete class and wires them together."""

from __future__ import annotations

from collections.abc import Callable
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
from soda_lakehouse_dq.application.quality_gate import GatePolicy
from soda_lakehouse_dq.application.verify_layer import RunMedallionUseCase, VerifyLayerUseCase
from soda_lakehouse_dq.domain.errors import DataQualityError
from soda_lakehouse_dq.domain.ports import Notifier, ResultSink
from soda_lakehouse_dq.settings import Engine, Settings


@dataclass(frozen=True, slots=True)
class Container:
    settings: Settings
    repository: FileContractRepository
    verify_layer: VerifyLayerUseCase
    run_medallion: RunMedallionUseCase


def build_provider(settings: Settings) -> DataSourceProvider:
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


def build_container(settings: Settings | None = None) -> Container:
    settings = settings or Settings()
    clock = SystemClock()
    repository = FileContractRepository(
        settings.contracts_dir, settings.reports_dir / "rendered", {"dataset_root": settings.dataset_root()}
    )
    verify_layer = VerifyLayerUseCase(
        repository=repository,
        verifier=SodaContractVerifier(build_provider(settings)),
        policy=GatePolicy(
            fail_on_warn=settings.fail_on_warn, allow_not_evaluated=settings.allow_not_evaluated
        ),
        clock=clock,
        sinks=build_sinks(settings),
        notifiers=build_notifiers(settings),
    )
    return Container(settings, repository, verify_layer, RunMedallionUseCase(verify_layer, clock))
