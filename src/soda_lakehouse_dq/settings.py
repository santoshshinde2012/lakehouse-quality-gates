"""Typed configuration from environment variables (prefix ``DQ_``) and an optional ``.env`` file."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Engine(StrEnum):
    DUCKDB = "duckdb"  # DuckDB on the host, attached to Lakekeeper via lakehouse_client
    DUCKDB_LOCAL = "duckdb-local"  # DuckDB over CSV files, no Docker (demo / CI)
    TRINO = "trino"  # Trino 483 container (full profile + TRINO=1)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DQ_", env_file=".env", extra="ignore")

    engine: Engine = Engine.DUCKDB
    ldl_repo_path: Path = Field(
        default=PROJECT_ROOT.parent / "local-data-lakehouse", description="Checkout of local-data-lakehouse"
    )
    lakekeeper_url: str = "http://localhost:8181"
    lakekeeper_warehouse: str = "lakehouse"
    catalog_alias: str = "lakehouse"
    duckdb_httpfs_client: Literal["default", "httplib", "curl"] | None = None
    duckdb_temp_directory: str | None = None  # writable spill dir (e.g. /tmp/duckdb on a read-only rootfs)

    contracts_dir: Path = PROJECT_ROOT / "contracts"
    datasources_dir: Path = PROJECT_ROOT / "datasources"
    reports_dir: Path = PROJECT_ROOT / "reports"
    sample_dir: Path = PROJECT_ROOT / "fixtures" / "good_batch"

    duckdb_datasource_name: str = "lakehouse_duckdb"
    trino_datasource_name: str = "lakehouse_trino"
    trino_host: str = "localhost"
    trino_port: int = Field(default=8088, ge=1, le=65535)

    fail_on_warn: bool = False
    allow_not_evaluated: bool = False
    sinks: list[str] = Field(default_factory=lambda: ["json", "junit", "markdown"])
    iceberg_sink: bool = False
    slack_webhook_url: SecretStr | None = None
    slack_only_on_problems: bool = True
    log_json: bool = False
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    @field_validator("slack_webhook_url", mode="before")
    @classmethod
    def _blank_is_none(cls, value: object) -> object:
        """Compose passes ``DQ_SLACK_WEBHOOK_URL=`` when unset; treat blank as "no Slack"."""
        return None if isinstance(value, str) and not value.strip() else value

    def dataset_root(self) -> str:
        """Engine-specific prefix rendered into ``{{ dataset_root }}`` in every contract."""
        if self.engine is Engine.TRINO:
            return f"{self.trino_datasource_name}/{self.catalog_alias}"
        return self.duckdb_datasource_name
