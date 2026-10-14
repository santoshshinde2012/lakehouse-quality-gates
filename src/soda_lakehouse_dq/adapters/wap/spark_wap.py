"""Spark adapter for write-audit-publish: ``docker exec ldl-spark spark-submit wap/wap_job.py <action>``.

The job (mounted by docker/docker-compose.wap.yml) prints one ``WAP_JSON {...}`` line per action; this adapter
builds the command, runs it through an injectable runner and maps that line to domain objects.
"""

from __future__ import annotations

import json
import math
import re
import subprocess
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from soda_lakehouse_dq.domain.errors import DataSourceError
from soda_lakehouse_dq.domain.models import Layer
from soda_lakehouse_dq.domain.wap import BranchState, TableState, validate_branch

Runner = Callable[[Sequence[str]], str]
MARKER = "WAP_JSON "


def subprocess_runner(timeout_s: float = 900) -> Runner:
    def run(args: Sequence[str]) -> str:
        try:
            done = subprocess.run(list(args), capture_output=True, text=True, timeout=timeout_s, check=False)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise DataSourceError(f"WAP Spark step could not run: {exc}") from exc
        if done.returncode != 0:
            tail = "\n".join(f"{done.stdout}\n{done.stderr}".strip().splitlines()[-15:])
            raise DataSourceError(f"WAP Spark step failed (exit {done.returncode}):\n{tail}")
        return done.stdout

    return run


def _int(value: Any, *, snapshot: bool = False) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value < (1 if snapshot else 0):
        raise ValueError("snapshot ids must be positive integers; row counts must be nonnegative integers")
    return value


def _table_state(name: str, data: Any) -> TableState:
    if not isinstance(name, str) or not re.fullmatch(r"[a-z0-9_]+\.[a-z0-9_]+", name):
        raise ValueError(f"invalid table {name!r}")
    if not isinstance(data, dict):
        raise ValueError(f"invalid state for table {name!r}")
    refs = data.get("refs", {})
    if not isinstance(refs, dict) or any(
        not isinstance(ref, str) or not ref or type(pin) is not int or pin <= 0 for ref, pin in refs.items()
    ):
        raise ValueError(f"invalid refs for table {name!r}")
    return TableState(
        table=name,
        main_snapshot=_int(data.get("main_snapshot"), snapshot=True),
        main_rows=_int(data.get("main_rows")),
        branch_snapshot=_int(data.get("branch_snapshot"), snapshot=True),
        branch_rows=_int(data.get("branch_rows")),
        main_before=_int(data.get("main_before"), snapshot=True),
        refs=refs,
    )


def parse_wap_json(output: str) -> BranchState:
    lines = [line for line in output.splitlines() if line.startswith(MARKER)]
    if not lines:
        raise DataSourceError("WAP Spark step printed no WAP_JSON line")
    try:
        data = json.loads(lines[-1][len(MARKER) :])
        if not isinstance(data, dict) or data.get("action") not in {
            "write",
            "direct",
            "publish",
            "drop",
            "status",
        }:
            raise ValueError("invalid WAP action")
        branch = data["branch"]
        if branch is not None and (
            not isinstance(branch, str) or not re.fullmatch(r"[a-z0-9_]{1,64}", branch)
        ):
            raise ValueError("invalid WAP branch")
        seconds = data["seconds"]
        if type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds < 0:
            raise ValueError("invalid WAP duration")
        tables = data["tables"]
        if not isinstance(tables, dict) or not tables:
            raise ValueError("WAP table states are required")
        return BranchState(
            action=data["action"],
            layer=Layer(data["layer"]),
            branch=branch,
            seconds=float(seconds),
            tables={name: _table_state(name, state) for name, state in tables.items()},
        )
    except (KeyError, ValueError, TypeError, OverflowError) as exc:
        raise DataSourceError(f"invalid WAP_JSON response: {exc}") from exc


class SparkSubmitWap:
    """Implements ``AuditWriter`` and ``BranchPublisher`` with the lakehouse's Spark container."""

    def __init__(
        self,
        container: str = "ldl-spark",
        job: str = "/opt/soda-wap/wap_job.py",
        runner: Runner | None = None,
        spark_submit: str = "/opt/spark/bin/spark-submit",
    ) -> None:
        self._container = container
        self._job = job
        self._runner = runner or subprocess_runner()
        self._spark_submit = spark_submit

    def command(self, action: str, layer: Layer, branch: str | None, source: str | None = None) -> list[str]:
        args = ["docker", "exec", self._container, self._spark_submit, "--master", "local[*]", self._job]
        args += [action, "--layer", layer.value]
        if branch is not None:
            args += ["--branch", validate_branch(branch)]
        if source is not None:
            args += ["--source", source]
        return args

    def _run(self, action: str, layer: Layer, branch: str | None, source: str | None = None) -> BranchState:
        return self._result(self._runner(self.command(action, layer, branch, source)), action, layer, branch)

    @staticmethod
    def _result(output: str, action: str, layer: Layer, branch: str | None) -> BranchState:
        state = parse_wap_json(output)
        if (state.action, state.layer, state.branch) != (action, layer, branch):
            raise DataSourceError("WAP Spark response action/layer/branch does not match the requested step")
        return state

    def write(self, layer: Layer, branch: str, source: str | None = None) -> BranchState:
        return self._run("write", layer, branch, source)

    def publish(
        self,
        layer: Layer,
        branch: str,
        pins: Mapping[str, int],
        *,
        main_before: Mapping[str, int | None] | None = None,
    ) -> BranchState:
        args = self.command("publish", layer, branch)
        args += ["--pins", json.dumps(dict(pins)), "--main-before", json.dumps(dict(main_before or {}))]
        return self._result(self._runner(args), "publish", layer, branch)

    def drop(self, layer: Layer, branch: str) -> BranchState:
        return self._run("drop", layer, branch)

    def direct(self, layer: Layer, source: str | None = None) -> BranchState:
        """The same build written straight to main, no branch: the baseline of the WAP overhead benchmark."""
        return self._run("direct", layer, None, source)

    def status(self, layer: Layer, branch: str | None = None) -> BranchState:
        return self._run("status", layer, branch)
