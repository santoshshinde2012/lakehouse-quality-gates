"""soda-dq: contract-gated medallion. Exit 0 = passed (warnings allowed), 1 = blocked, 2 = usage error."""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated

import typer

from soda_lakehouse_dq.adapters.datasources.duckdb_provider import (
    apply_session_settings,
    lakehouse_connection_factory,
)
from soda_lakehouse_dq.adapters.local_medallion import load_medallion
from soda_lakehouse_dq.container import build_container, build_wap
from soda_lakehouse_dq.domain.errors import DataQualityError
from soda_lakehouse_dq.domain.models import GateReport, Layer, VariableValue, coerce_variable
from soda_lakehouse_dq.domain.wap import BranchState, WapLayerResult, WapOutcome
from soda_lakehouse_dq.logging_config import configure_logging
from soda_lakehouse_dq.settings import Engine, Settings

app = typer.Typer(no_args_is_help=True, add_completion=False, help=__doc__)
wap_app = typer.Typer(
    no_args_is_help=True,
    help="Write-audit-publish: build a layer on an Iceberg audit branch, gate it, fast-forward main on pass.",
)
app.add_typer(wap_app, name="wap")

# Batches as seen from inside ldl-spark (docker/docker-compose.wap.yml mounts our fixtures there).
WAP_BATCHES = {"good": "/opt/data/sample", "bad": "/opt/soda-wap-fixtures/bad_batch"}

EngineOpt = Annotated[Engine | None, typer.Option("--engine", "-e", help="Override DQ_ENGINE")]
PipelineOpt = Annotated[str, typer.Option("--pipeline", "-p", help="Pipeline folder under contracts/")]
VarOpt = Annotated[list[str] | None, typer.Option("--set", "-s", help="Contract variable KEY=VALUE")]
SampleOpt = Annotated[Path | None, typer.Option("--sample-dir", help="CSV dir for --engine duckdb-local")]


def _settings(engine: Engine | None, sample_dir: Path | None = None) -> Settings:
    overrides: dict[str, object] = {}
    if engine is not None:
        overrides["engine"] = engine
    if sample_dir is not None:
        overrides["sample_dir"] = sample_dir
    settings = Settings.model_validate(overrides)
    configure_logging(settings.log_level, settings.log_json)
    return settings


def _variables(pairs: list[str] | None) -> dict[str, VariableValue]:
    variables: dict[str, VariableValue] = {}
    for pair in pairs or []:
        key, sep, value = pair.partition("=")
        if not sep or not key:
            raise typer.BadParameter(f"expected KEY=VALUE, got {pair!r}")
        variables[key] = coerce_variable(value)
    return variables


def _echo(report: GateReport) -> None:
    color = typer.colors.RED if report.blocked else typer.colors.GREEN
    typer.secho(
        f"{report.pipeline}/{report.layer.value}: {report.status.value} "
        f"({report.total_checks} checks, run {report.run_id})",
        fg=color,
    )


@app.command()
def verify(
    layer: Annotated[Layer, typer.Argument(help="bronze | silver | gold")],
    pipeline: PipelineOpt = "retail",
    engine: EngineOpt = None,
    sample_dir: SampleOpt = None,
    var: VarOpt = None,
) -> None:
    """Verify one layer's contracts and apply the gate."""
    try:
        container = build_container(_settings(engine, sample_dir))
        report = container.verify_layer.execute(pipeline, layer, _variables(var))
    except DataQualityError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    _echo(report)
    raise typer.Exit(1 if report.blocked else 0)


@app.command()
def run(
    pipeline: PipelineOpt = "retail",
    engine: EngineOpt = None,
    sample_dir: SampleOpt = None,
    var: VarOpt = None,
) -> None:
    """Gate bronze → silver → gold, stopping at the first blocked layer."""
    try:
        container = build_container(_settings(engine, sample_dir))
        reports = container.run_medallion.execute(pipeline, variables=_variables(var))
    except DataQualityError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    for report in reports:
        _echo(report)
    raise typer.Exit(1 if any(r.blocked for r in reports) else 0)


@app.command()
def seed(
    sample_dir: Annotated[Path, typer.Argument(help="CSV batch: customers.csv + orders_*.csv")],
    yes: Annotated[bool, typer.Option("--yes", help="Confirm overwriting the live tables")] = False,
) -> None:
    """Rebuild the live retail bronze/silver/gold Iceberg tables from a CSV batch (demo only).

    Uses lakehouse_client.duckdb_connect() (DQ_LDL_REPO_PATH). Overwrites the five retail tables;
    restore the repo's data afterwards with `soda-dq seed fixtures/good_batch --yes` or its own
    `make demo-light` / `make e2e`.
    """
    if not yes:
        typer.secho("refusing to overwrite live tables without --yes", fg=typer.colors.RED, err=True)
        raise typer.Exit(2)
    settings = _settings(None)
    try:
        con = lakehouse_connection_factory(
            settings.ldl_repo_path,
            settings.lakekeeper_url,
            settings.lakekeeper_warehouse,
            settings.catalog_alias,
        )()
        try:
            apply_session_settings(con, settings.duckdb_httpfs_client, settings.duckdb_temp_directory)
            # DuckDB's Iceberg writer creates a scratch ./data directory; keep it out of the project
            # (and off a read-only container root filesystem).
            source = sample_dir.resolve()  # before chdir: a relative path is relative to the caller
            with tempfile.TemporaryDirectory() as scratch, contextlib.chdir(scratch):
                counts = load_medallion(con, source)
        finally:
            con.close()
    except DataQualityError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    for table, rows in counts.items():
        typer.echo(f"{table:28} {rows:>6} rows")


@app.command("list")
def list_contracts(pipeline: PipelineOpt = "retail", engine: EngineOpt = None) -> None:
    """List contracts per layer with their rendered dataset names."""
    container = build_container(_settings(engine))
    for layer in Layer.ordered():
        for spec in container.repository.list_contracts(pipeline, layer):
            typer.echo(f"{layer.value:7} {spec.name:40} {spec.dataset}")


@app.command()
def validate(pipeline: PipelineOpt = "retail", engine: EngineOpt = None) -> None:
    """Parse every contract with Soda (no data access) and report syntax errors."""
    os.environ.setdefault("SODA_CORE_TELEMETRY_ENABLED", "false")
    from soda_core.contracts import test_contract

    container = build_container(_settings(engine))
    bad = 0
    for layer in Layer.ordered():
        for spec in container.repository.list_contracts(pipeline, layer):
            errors = test_contract(contract_file_path=str(spec.rendered_path)).get_errors()
            bad += bool(errors)
            typer.secho(
                f"{'FAIL' if errors else 'ok  '} {spec.name}",
                fg=typer.colors.RED if errors else typer.colors.GREEN,
            )
            for error in errors:
                typer.echo(f"     {error}")
    raise typer.Exit(1 if bad else 0)


STATE_FIELDS = ("main_before", "main_snapshot", "main_rows", "branch_snapshot", "branch_rows", "refs")


def _state_json(state: BranchState | None) -> object:
    if state is None:
        return None
    return {
        "action": state.action,
        "seconds": state.seconds,
        "tables": {
            name: {k: (dict(v) if isinstance(v := getattr(s, k), Mapping) else v) for k in STATE_FIELDS}
            for name, s in state.tables.items()
        },
    }  # fmt: skip


def wap_evidence(results: list[WapLayerResult]) -> list[dict[str, object]]:
    """Per layer: outcome, gate status and the snapshot ids/row counts before and after."""
    return [
        {
            "layer": r.layer.value,
            "branch": r.branch,
            "outcome": r.outcome.value,
            "gate": r.report.status.value,
            "run_id": r.report.run_id,
            "reasons": list(r.report.reasons[:10]),
            "written": _state_json(r.written),
            "published": _state_json(r.published),
        }
        for r in results
    ]


LayerOpt = Annotated[list[Layer] | None, typer.Option("--layer", "-l", help="Layer(s); default with --all")]


@wap_app.command("run")
def wap_run(  # noqa: PLR0917 (typer options)
    layer: LayerOpt = None,
    all_layers: Annotated[bool, typer.Option("--all", help="bronze → silver → gold")] = False,
    batch: Annotated[str, typer.Option(help="good (lakehouse sample) | bad (fixtures/bad_batch)")] = "good",
    source: Annotated[str | None, typer.Option(help="CSV dir inside ldl-spark (overrides --batch)")] = None,
    branch: Annotated[str | None, typer.Option(help="Audit branch (default audit_<run id>)")] = None,
    pipeline: PipelineOpt = "retail",
    evidence: Annotated[Path | None, typer.Option(help="Write per-layer snapshot evidence JSON here")] = None,
) -> None:
    """Write each layer to an audit branch, verify it, publish to main only when the gate passes."""
    if not layer and not all_layers:
        raise typer.BadParameter("give --layer (repeatable) or --all")
    if batch not in WAP_BATCHES and source is None:
        raise typer.BadParameter(f"--batch must be one of {sorted(WAP_BATCHES)}")
    layers = Layer.ordered() if all_layers else tuple(sorted(set(layer or []), key=Layer.ordered().index))
    try:
        wap = build_wap(_settings(Engine.DUCKDB))
        results = wap.run_wap.execute(pipeline, layers, source=source or WAP_BATCHES[batch], branch=branch)
    except DataQualityError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    for r in results:
        _echo(r.report)
        tables = r.published.tables if r.published else r.written.tables
        for name, s in tables.items():
            if r.outcome is WapOutcome.PUBLISHED:
                typer.echo(
                    f"  published {name}: main {s.main_before} -> {s.main_snapshot} ({s.main_rows} rows)"
                )
            else:
                typer.echo(
                    f"  blocked   {name}: main stays {s.main_snapshot} ({s.main_rows} rows); "
                    f"branch {r.branch} kept at {s.branch_snapshot} ({s.branch_rows} rows)"
                )
    if evidence is not None:
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text(json.dumps(wap_evidence(results), indent=2) + "\n", encoding="utf-8")
    raise typer.Exit(1 if any(r.outcome is WapOutcome.BLOCKED for r in results) else 0)


@wap_app.command("verify")
def wap_verify(
    layer: Annotated[Layer, typer.Argument(help="bronze | silver | gold")],
    branch: Annotated[str, typer.Option(help="Audit branch to verify")],
    pipeline: PipelineOpt = "retail",
) -> None:
    """Gate one layer against an audit branch (used by the Airflow WAP DAG). Exit 1 = blocked."""
    try:
        wap = build_wap(_settings(Engine.DUCKDB))
        report = wap.verify_branch.execute(pipeline, layer, branch, run_id=branch)
    except DataQualityError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    _echo(report)
    raise typer.Exit(1 if report.blocked else 0)


@wap_app.command("status")
def wap_status(
    layer: Annotated[Layer, typer.Argument(help="bronze | silver | gold")],
    branch: Annotated[str | None, typer.Option(help="Also show this branch")] = None,
) -> None:
    """Snapshot ids and row counts of main (and a branch), read through Spark."""
    try:
        state = build_wap(_settings(Engine.DUCKDB)).spark.status(layer, branch)
    except DataQualityError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    typer.echo(json.dumps(_state_json(state), indent=2))


@wap_app.command("drop")
def wap_drop(
    layer: Annotated[Layer, typer.Argument(help="bronze | silver | gold")],
    branch: Annotated[str, typer.Option(help="Audit branch to delete")],
) -> None:
    """Delete an audit branch (e.g. after inspecting a blocked batch). main is not touched."""
    try:
        build_wap(_settings(Engine.DUCKDB)).spark.drop(layer, branch)
    except DataQualityError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc
    typer.echo(f"dropped {branch} from {layer.value}")


if __name__ == "__main__":  # pragma: no cover
    app()
