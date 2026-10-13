from __future__ import annotations

import json
import xml.etree.ElementTree as ET

import pytest

from soda_lakehouse_dq.adapters.sinks.iceberg_sink import IcebergSink, report_rows
from soda_lakehouse_dq.adapters.sinks.json_sink import JsonFileSink
from soda_lakehouse_dq.adapters.sinks.junit_sink import JUnitXmlSink
from soda_lakehouse_dq.adapters.sinks.markdown_sink import MarkdownSink, render_markdown
from soda_lakehouse_dq.domain.models import GateStatus, Outcome
from tests.conftest import make_report, make_result

RESULT = make_result(Outcome.PASSED, Outcome.FAILED, Outcome.WARNED, Outcome.NOT_EVALUATED, errors=("boom",))
REPORT = make_report(GateStatus.BLOCKED, RESULT, reasons=("retail.silver.orders: failed: col: check1",))


def test_json_sink(tmp_path):
    JsonFileSink(tmp_path).write(REPORT)
    [path] = tmp_path.glob("*.json")
    assert path.name == "run-1_retail_silver.json"
    data = json.loads(path.read_text())
    assert data["status"] == "blocked"
    assert data["summary"]["failed"] == 1
    assert data["summary"]["total"] == 4
    assert data["contracts"][0]["checks"][1]["outcome"] == "failed"
    assert data["contracts"][0]["errors"] == ["boom"]


def test_junit_sink(tmp_path):
    JUnitXmlSink(tmp_path).write(REPORT)
    suite = ET.parse(next(tmp_path.glob("*.xml"))).getroot().find("testsuite")
    assert suite is not None
    assert suite.attrib == {
        "name": "retail.silver.orders",
        "tests": "5",
        "failures": "1",
        "errors": "1",
        "skipped": "1",
    }
    assert len(suite.findall("testcase/failure")) == 1
    assert len(suite.findall("testcase/error")) == 1


def test_markdown_sink(tmp_path):
    MarkdownSink(tmp_path).write(REPORT)
    text = next(tmp_path.glob("*.md")).read_text()
    assert text == render_markdown(REPORT)
    assert "**blocked**" in text
    assert "❌ | check1 | col" in text
    assert "- error: boom" in text
    assert "## Reasons" in text


def test_iceberg_rows_and_append():
    pytest.importorskip("pyarrow")
    rows = report_rows(REPORT)
    assert len(rows) == 4
    assert rows[1]["outcome"] == "failed"

    appended = []

    class Table:
        def append(self, data):
            appended.append(data.num_rows)

    class Catalog:
        def create_namespace_if_not_exists(self, ns):
            assert ns == "ops"

        def create_table_if_not_exists(self, name, schema):
            assert name == "ops.dq_results"
            return Table()

    IcebergSink(Catalog).write(REPORT)
    assert appended == [4]
