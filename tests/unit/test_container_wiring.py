from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from soda_lakehouse_dq import cli, container
from soda_lakehouse_dq.adapters.wap.evidence_json import JsonWapEvidenceWriter, wap_evidence
from soda_lakehouse_dq.domain.errors import DataSourceError
from soda_lakehouse_dq.domain.ports import WapEvidenceWriter
from soda_lakehouse_dq.settings import Engine, Settings

pytestmark = pytest.mark.unit


def test_every_engine_has_a_provider():
    assert set(container.PROVIDERS) == set(Engine)


def test_pins_still_need_duckdb():
    with pytest.raises(Exception, match="needs DQ_ENGINE=duckdb"):
        container.build_provider(Settings(engine=Engine.TRINO), {"bronze.t": 1})


class FakeConnection:
    closed = False

    def close(self):
        self.closed = True


def test_seed_lakehouse_loads_in_scratch_dir_and_closes(monkeypatch, tmp_path):
    con = FakeConnection()
    seen = {}
    monkeypatch.setattr(container, "lakehouse_connection_factory", lambda *a: lambda: con)
    monkeypatch.setattr(container, "apply_session_settings", lambda c, *a: seen.setdefault("session", c))

    def load(c, source):
        seen["cwd"], seen["source"] = Path.cwd(), source
        return {"bronze.customers": 3}

    monkeypatch.setattr(container, "load_medallion", load)
    monkeypatch.chdir(tmp_path)
    assert container.seed_lakehouse(Settings(), Path("batch")) == {"bronze.customers": 3}
    assert seen["session"] is con
    assert seen["source"] == tmp_path / "batch"
    assert seen["cwd"] != tmp_path
    assert Path.cwd() == tmp_path
    assert con.closed


def test_seed_closes_connection_on_failure(monkeypatch):
    con = FakeConnection()
    monkeypatch.setattr(container, "lakehouse_connection_factory", lambda *a: lambda: con)
    monkeypatch.setattr(container, "apply_session_settings", lambda *a: None)

    def boom(*_):
        raise DataSourceError("load failed")

    monkeypatch.setattr(container, "load_medallion", boom)
    with pytest.raises(DataSourceError):
        container.seed_lakehouse(Settings(), Path("x"))
    assert con.closed


def test_seed_cli_uses_container(monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "seed_lakehouse", lambda settings, d: {"bronze.customers": 7})
    result = CliRunner().invoke(cli.app, ["seed", str(tmp_path), "--yes"])
    assert result.exit_code == 0, result.output
    assert "bronze.customers" in result.output
    assert "7 rows" in result.output


def test_evidence_writer_port_and_atomic_write(tmp_path):
    writer = container.evidence_writer(tmp_path / "out" / "evidence.json")
    assert isinstance(writer, WapEvidenceWriter)
    writer.check()
    writer.write([])
    assert json.loads((tmp_path / "out" / "evidence.json").read_text()) == []
    assert wap_evidence([]) == []


def test_evidence_writer_rejects_directory(tmp_path):
    with pytest.raises(DataSourceError, match="not a regular file"):
        JsonWapEvidenceWriter(tmp_path).check()
