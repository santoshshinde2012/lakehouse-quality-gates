"""JUnit XML sink: CI systems render each check as a test case."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

from soda_lakehouse_dq.adapters.sinks.serialize import report_path
from soda_lakehouse_dq.domain.models import GateReport, Outcome


class JUnitXmlSink:
    def __init__(self, directory: Path) -> None:
        self._directory = directory

    def write(self, report: GateReport) -> None:
        suites = ET.Element("testsuites", name=f"soda-dq {report.pipeline}/{report.layer.value}")
        for result in report.results:
            suite = ET.SubElement(
                suites,
                "testsuite",
                name=result.contract.name,
                tests=str(len(result.checks) + len(result.errors)),
                failures=str(result.count(Outcome.FAILED)),
                errors=str(len(result.errors)),
                skipped=str(result.count(Outcome.EXCLUDED) + result.count(Outcome.NOT_EVALUATED)),
            )
            for check in result.checks:
                label = f"{check.column}: {check.name}" if check.column else check.name
                case = ET.SubElement(suite, "testcase", classname=result.contract.dataset, name=label)
                detail = "\n".join([", ".join(f"{k}={v}" for k, v in check.metrics.items()), *check.details])
                if check.outcome is Outcome.FAILED:
                    ET.SubElement(case, "failure", message=f"{check.check_type} failed").text = detail
                elif check.outcome in (Outcome.EXCLUDED, Outcome.NOT_EVALUATED):
                    ET.SubElement(case, "skipped", message=check.outcome.value)
                elif check.outcome is Outcome.WARNED:
                    ET.SubElement(case, "system-out").text = f"WARN {detail}"
            for error in result.errors:
                case = ET.SubElement(suite, "testcase", classname=result.contract.dataset, name="execution")
                ET.SubElement(case, "error", message="contract execution error").text = error
        if report.blocked and not any(result.failed or result.errors for result in report.results):
            # Native outcomes alone do not represent policy failures: fail_on_warn and
            # NOT_EVALUATED can block while every check is reported as passing or skipped.
            suite = ET.SubElement(
                suites,
                "testsuite",
                name="quality gate decision",
                tests="1",
                failures="1",
                errors="0",
                skipped="0",
            )
            case = ET.SubElement(
                suite, "testcase", classname=f"{report.pipeline}/{report.layer.value}", name="gate"
            )
            ET.SubElement(case, "failure", message="quality gate blocked").text = "\n".join(report.reasons)
        ET.indent(suites)
        path: Path = report_path(self._directory, report, "xml")
        ET.ElementTree(suites).write(path, encoding="utf-8", xml_declaration=True)
