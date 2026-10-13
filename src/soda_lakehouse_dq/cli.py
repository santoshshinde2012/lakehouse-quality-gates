"""soda-dq: contract-gated medallion. Exit 0 = passed (warnings allowed), 1 = blocked, 2 = usage error."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated

import typer

from soda_lakehouse_dq.adapters.datasources.duckdb_provider import lakehouse_connection_factory
from soda_lakehouse_dq.adapters.local_medallion import load_medallion
from soda_lakehouse_dq.container import build_container
from soda_lakehouse_dq.domain.errors import DataQualityError
from soda_lakehouse_dq.domain.models import GateReport, Layer, VariableValue, coerce_variable
from soda_lakehouse_dq.logging_config import configure_logging
from soda_lakehouse_dq.settings import Engine, Settings

app = typer.Typer(no_args_is_help=True, add_completion=False, help=__doc__)

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
            counts = load_medallion(con, sample_dir)
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


if __name__ == "__main__":  # pragma: no cover
    app()
