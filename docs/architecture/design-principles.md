# Design principles

> [!NOTE]
> **At a glance.** SOLID, strict typing, one error hierarchy and validated configuration, each mapped to the real
> class or file that implements it.

How SOLID and the project's other engineering rules map onto concrete code. Paths are relative to
`src/soda_lakehouse_dq/`.

## Layering (ports and adapters)

| Layer | Files | May depend on |
|---|---|---|
| Domain | `domain/models.py`, `domain/ports.py`, `domain/errors.py` | stdlib only |
| Application | `application/quality_gate.py`, `application/verify_layer.py` | domain, structlog |
| Adapters | `adapters/**` | domain, third-party libraries (Soda, DuckDB, PyIceberg) |
| Composition | `container.py`, `cli.py`, `settings.py`, `logging_config.py` | everything; the only place that instantiates concrete classes |

## SOLID

| Principle | Where it shows up |
|---|---|
| **Single responsibility** | `GatePolicy` (quality_gate.py) only turns results into a decision. `VerifyLayerUseCase` only orchestrates repository → verifier → policy → sinks/notifiers. `FileContractRepository` (adapters/contracts.py) only finds and renders YAML. `SodaContractVerifier` (adapters/soda_verifier.py) only translates between Soda and domain models. `DuckDBProvider` / `TrinoProvider` only open sessions. Each sink writes one format, and `serialize.py` owns the shared dict shape. `adapters/lakehouse_repo.py` is the only module that touches `sys.path` to reach the lakehouse client. |
| **Open/closed** | A new engine is a `DataSourceProvider` plus one `match` arm in `container.build_provider`. A new report format is a `ResultSink` registered in `container.build_sinks`. A new channel is a `Notifier` added in `container.build_notifiers`. Contracts are YAML, so new checks need no code at all. |
| **Liskov substitution** | Every `ResultSink`, `Notifier`, `ContractVerifier` and `DataSourceProvider` is interchangeable. The unit tests swap in fakes (`tests/conftest.py`) and the use cases behave the same. Both providers yield the same `SodaDataSources` shape, with a no-op `prepare` by default. |
| **Interface segregation** | Ports are small `Protocol`s with one or two methods (`domain/ports.py`): `ContractRepository.list_contracts`, `ContractVerifier.verify`, `ResultSink.write`, `Notifier.notify`, `Clock.now/new_run_id`. The Soda-specific `DataSourceProvider` lives in `adapters/datasources/base.py` because only the Soda verifier needs it. |
| **Dependency inversion** | `VerifyLayerUseCase` receives its ports through its constructor and never imports Soda, DuckDB, Slack or the file system. `container.build_container` wires the concrete classes. Factories (connection, catalog, Slack transport, clock) are injected so tests stay deterministic and offline. |

## Other practices

| Area | Practice |
|---|---|
| Typing | `mypy --strict` over the package, `benchmarks/` and `scripts/`. Frozen, slotted dataclasses for domain models. `Literal` and `StrEnum` for closed choices (engine, log level, httpfs client). `Any` appears only where an untyped third-party object crosses a boundary (Soda results, DuckDB connections, PyIceberg catalogs). |
| Errors | One hierarchy under `DataQualityError`: `ContractNotFoundError`, `ContractRenderError`, `DataSourceError`, `GateBlockedError`, `NotificationError`. Adapters wrap low-level exceptions with context (`raise … from exc`). A failing notifier is logged and never hides the gate result. The CLI maps errors to exit codes (0 pass, 1 blocked, 2 usage or configuration). |
| Logging | structlog with key/value events (the full list is in [observability](observability.md)), and JSON output with `DQ_LOG_JSON=true`. |
| Config | pydantic-settings with the `DQ_` prefix, validated at startup (port range, enums). The Slack URL is a `SecretStr`, and an empty value means disabled. |
| Security | No secrets in the repo (`.env` is git-ignored, `.env.example` holds placeholders, and pre-commit runs `detect-private-key`). Slack accepts only `https://` URLs. The Docker image is multi-stage, digest-pinned, runs as the non-root user `dq`, and works on a read-only root filesystem. Soda telemetry is off by default. |
| Dependencies | Soda and DuckDB are pinned exactly, and other libraries are bounded and locked in `uv.lock` (CI runs `uv sync --frozen`). |
| Testing | Unit tests use fakes for every port. Integration tests run real Soda over DuckDB and the CLI end to end. Coverage is 97%. |

---

[← Write-audit-publish](write-audit-publish.md) · [Architecture index](README.md) · [System design →](../system-design.md)
