from __future__ import annotations

from pathlib import Path

import pytest

from soda_lakehouse_dq.adapters.datasources.duckdb_provider import (
    DuckDBProvider,
    _quote_ident,
    lakehouse_connection_factory,
)
from soda_lakehouse_dq.adapters.datasources.trino_provider import TrinoProvider
from soda_lakehouse_dq.adapters.notifiers.slack import SlackWebhookNotifier
from soda_lakehouse_dq.adapters.sinks.iceberg_sink import IcebergSink
from soda_lakehouse_dq.container import build_container, build_notifiers, build_provider, build_sinks
from soda_lakehouse_dq.domain.errors import DataQualityError, DataSourceError
from soda_lakehouse_dq.settings import Engine, Settings


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for key in ("DQ_ENGINE", "DQ_SLACK_WEBHOOK_URL", "DQ_SINKS", "DQ_ICEBERG_SINK"):
        monkeypatch.delenv(key, raising=False)


def test_dataset_root_per_engine():
    assert Settings(engine=Engine.DUCKDB).dataset_root() == "lakehouse_duckdb"
    assert Settings(engine=Engine.DUCKDB_LOCAL).dataset_root() == "lakehouse_duckdb"
    assert Settings(engine=Engine.TRINO).dataset_root() == "lakehouse_trino/lakehouse"


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("DQ_ENGINE", "trino")
    monkeypatch.setenv("DQ_LDL_REPO_PATH", "/x/ldl")
    monkeypatch.setenv("DQ_SINKS", '["json"]')
    s = Settings()
    assert s.engine is Engine.TRINO
    assert s.ldl_repo_path == Path("/x/ldl")
    assert s.sinks == ["json"]


def test_build_provider_per_engine(tmp_path):
    assert isinstance(build_provider(Settings(engine=Engine.DUCKDB)), DuckDBProvider)
    assert isinstance(build_provider(Settings(engine=Engine.DUCKDB_LOCAL)), DuckDBProvider)
    assert isinstance(build_provider(Settings(engine=Engine.TRINO)), TrinoProvider)


def test_build_sinks(tmp_path):
    assert len(build_sinks(Settings(reports_dir=tmp_path))) == 3
    with_iceberg = build_sinks(Settings(reports_dir=tmp_path, sinks=["json"], iceberg_sink=True))
    assert isinstance(with_iceberg[-1], IcebergSink)
    with pytest.raises(DataQualityError, match="unknown sinks"):
        build_sinks(Settings(sinks=["parquet"]))


def test_build_notifiers():
    assert len(build_notifiers(Settings())) == 1
    notifiers = build_notifiers(Settings(slack_webhook_url="https://hooks.slack.test/x"))  # type: ignore[arg-type]
    assert isinstance(notifiers[-1], SlackWebhookNotifier)


def test_build_container(tmp_path):
    container = build_container(Settings(engine=Engine.DUCKDB_LOCAL, reports_dir=tmp_path))
    assert container.verify_layer is not None
    assert container.settings.reports_dir == tmp_path


def test_quote_ident_rejects_injection():
    assert _quote_ident("lake_house") == '"lake_house"'
    with pytest.raises(DataSourceError):
        _quote_ident('x"; DROP')


def test_lakehouse_factory_requires_repo(tmp_path):
    factory = lakehouse_connection_factory(tmp_path, "http://localhost:8181", "lakehouse", "lakehouse")
    with pytest.raises(DataSourceError, match="DQ_LDL_REPO_PATH"):
        factory()


def test_lakehouse_factory_uses_repo_client(tmp_path):
    pkg = tmp_path / "src" / "lakehouse_client"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text(
        "def duckdb_connect(url=None, wh=None, alias='lakehouse'):\n    return ('con', url, wh, alias)\n"
    )
    factory = lakehouse_connection_factory(tmp_path, "http://lk:8181", "wh", "lh")
    assert factory() == ("con", "http://lk:8181", "wh", "lh")


def test_trino_provider_missing_file(tmp_path):
    with pytest.raises(DataSourceError), TrinoProvider(tmp_path / "nope.yml", "t").open():
        pass


def test_trino_provider_exports_env(tmp_path, monkeypatch):
    monkeypatch.delenv("DQ_TRINO_HOST", raising=False)
    yml = tmp_path / "trino.yml"
    yml.write_text("type: trino\n")
    with TrinoProvider(yml, "t", {"DQ_TRINO_HOST": "trino"}).open() as sources:
        assert sources.yaml_files == [yml]
    import os

    assert os.environ["DQ_TRINO_HOST"] == "trino"


def test_shipped_trino_yaml_parses():
    import yaml

    from tests.conftest import PROJECT_ROOT

    doc = yaml.safe_load((PROJECT_ROOT / "datasources" / "trino.yml").read_text())
    assert doc["type"] == "trino"
    assert doc["name"] == Settings().trino_datasource_name
    assert doc["connection"]["catalog"] == "lakehouse"


def test_blank_slack_url_disables_slack(monkeypatch):
    monkeypatch.setenv("DQ_SLACK_WEBHOOK_URL", "  ")
    assert Settings().slack_webhook_url is None
