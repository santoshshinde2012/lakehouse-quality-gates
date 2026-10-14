from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from dataclasses import replace

import pytest

from soda_lakehouse_dq.adapters.sinks.iceberg_sink import IcebergSink, report_rows
from soda_lakehouse_dq.adapters.sinks.json_sink import JsonFileSink
from soda_lakehouse_dq.adapters.sinks.junit_sink import JUnitXmlSink
from soda_lakehouse_dq.adapters.sinks.markdown_sink import MarkdownSink, render_markdown
from soda_lakehouse_dq.application.quality_gate import GatePolicy
from soda_lakehouse_dq.domain.errors import DataQualityError
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


@pytest.mark.parametrize(
    ("outcome", "policy"),
    [(Outcome.WARNED, GatePolicy(fail_on_warn=True)), (Outcome.NOT_EVALUATED, GatePolicy())],
)
def test_junit_represents_policy_blocks_as_failures(tmp_path, outcome, policy):
    result = make_result(outcome)
    decision = policy.decide([result])
    report = make_report(decision.status, result, reasons=decision.reasons)
    JUnitXmlSink(tmp_path).write(report)
    suites = ET.parse(next(tmp_path.glob("*.xml"))).getroot().findall("testsuite")
    assert report.blocked
    assert sum(int(suite.attrib["failures"]) for suite in suites) == 1
    assert suites[-1].find("testcase/failure").text == "\n".join(report.reasons)
    original = suites[0].find("testcase")
    assert original.find("failure") is None
    assert original.find("skipped" if outcome is Outcome.NOT_EVALUATED else "system-out") is not None


def test_junit_represents_an_empty_blocked_gate_as_a_failure(tmp_path):
    decision = GatePolicy().decide([])
    report = replace(make_report(decision.status, reasons=decision.reasons), results=())
    JUnitXmlSink(tmp_path).write(report)
    suite = ET.parse(next(tmp_path.glob("*.xml"))).getroot().find("testsuite")
    assert suite.attrib["failures"] == "1"
    assert suite.find("testcase/failure").text == "no contracts were verified"


@pytest.mark.parametrize(("field", "value"), [("run_id", "../outside"), ("pipeline", "../outside")])
def test_report_names_cannot_write_outside_the_output_directory(tmp_path, field, value):
    directory = tmp_path / "reports"
    report = replace(REPORT, **{field: value})
    with pytest.raises(DataQualityError, match=f"{field} must be a single non-empty path component"):
        JsonFileSink(directory).write(report)
    assert not directory.exists()
    assert list(tmp_path.iterdir()) == []


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
    assert len(rows) == 5
    assert rows[1]["outcome"] == "failed"
    assert rows[-1]["check_type"] == "execution_error"

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
    assert appended == [5]


def test_iceberg_execution_errors_are_appended_without_check_results():
    pytest.importorskip("pyarrow")
    iceberg = pytest.importorskip("pyiceberg.catalog")
    result = make_result(errors=("invalid contract YAML", "query could not execute"))
    report = make_report(GateStatus.BLOCKED, result)
    captured = []

    class Table:
        def append(self, data):
            captured.append(data)

    class Catalog:
        def create_namespace_if_not_exists(self, ns):
            pass

        def create_table_if_not_exists(self, name, schema):
            iceberg.Catalog._convert_schema_if_needed(schema, format_version=2)
            return Table()

    IcebergSink(Catalog).write(report)
    [data] = captured
    assert data.num_rows == 2
    for row in data.to_pylist():
        assert row["run_id"] == report.run_id
        assert row["gate_status"] == "blocked"
        assert row["contract"] == result.contract.name
        assert row["dataset"] == result.contract.dataset
        assert row["check_type"] == "execution_error"
        assert row["check_name"] == "Contract execution error"
        assert row["outcome"] == "failed"
        assert row["column_name"] is None


def test_iceberg_empty_blocked_gate_has_a_decision_record():
    report = replace(make_report(GateStatus.BLOCKED, reasons=("no contracts were verified",)), results=())
    [row] = report_rows(report)
    assert row["run_id"] == report.run_id
    assert row["gate_status"] == "blocked"
    assert row["contract"] is None
    assert row["dataset"] is None
    assert row["check_name"] == "Quality gate decision"
    assert row["check_type"] == "gate_decision"
    assert row["outcome"] == "failed"


def test_iceberg_dataset_only_checks_have_a_supported_stable_schema():
    pa = pytest.importorskip("pyarrow")
    iceberg = pytest.importorskip("pyiceberg.catalog")
    result = make_result(Outcome.PASSED)
    result = replace(result, checks=(replace(result.checks[0], column=None),))
    report = make_report(GateStatus.PASSED, result)
    captured = []

    class Table:
        def append(self, data):
            captured.append(data)

    class Catalog:
        def create_namespace_if_not_exists(self, ns):
            pass

        def create_table_if_not_exists(self, name, schema):
            iceberg.Catalog._convert_schema_if_needed(schema, format_version=2)
            return Table()

    sink = IcebergSink(Catalog)
    sink.write(report)
    sink.write(REPORT)
    assert len(captured) == 2
    assert captured[0].schema == captured[1].schema
    assert captured[0].schema.field("column_name").type == pa.string()
    assert captured[0].column("column_name").to_pylist() == [None]
