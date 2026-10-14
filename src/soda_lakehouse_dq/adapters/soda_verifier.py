"""Soda Core v4 adapter for the ContractVerifier port.

The only module that calls Soda's verification API. It maps Soda's result objects to domain values so
nothing else depends on Soda types.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import structlog

from soda_lakehouse_dq.adapters.datasources.base import DataSourceProvider
from soda_lakehouse_dq.domain.models import (
    CheckResult,
    ContractResult,
    ContractSpec,
    Outcome,
    VariableValue,
)

log = structlog.get_logger(__name__)

_OUTCOMES: dict[str, Outcome] = {
    "PASSED": Outcome.PASSED,
    "WARN": Outcome.WARNED,
    "FAILED": Outcome.FAILED,
    "NOT_EVALUATED": Outcome.NOT_EVALUATED,
    "EXCLUDED": Outcome.EXCLUDED,
}

VerifyFn = Callable[..., Any]


def _default_verify_fn() -> VerifyFn:
    # Soda sends OpenTelemetry usage data unless told otherwise; this project is local-only.
    os.environ.setdefault("SODA_CORE_TELEMETRY_ENABLED", "false")
    from soda_core.contracts import verify_contract_locally

    return verify_contract_locally  # type: ignore[no-any-return]


class SodaContractVerifier:
    def __init__(self, provider: DataSourceProvider, verify_fn: VerifyFn | None = None) -> None:
        self._provider = provider
        self._verify_fn = verify_fn
        self.engine = provider.engine

    def verify(
        self, contracts: Sequence[ContractSpec], variables: Mapping[str, VariableValue]
    ) -> Sequence[ContractResult]:
        verify_fn = self._verify_fn or _default_verify_fn()
        from soda_core.common.exceptions import YamlParserException

        results: list[ContractResult] = []
        with self._provider.open() as sources:
            for contract in contracts:
                log.info("dq.contract.verify", contract=contract.name, dataset=contract.dataset)
                for dataset in contract.datasets:
                    sources.prepare(dataset)
                try:
                    session = verify_fn(
                        data_sources=list(sources.impls),
                        data_source_file_paths=[str(p) for p in sources.yaml_files],
                        contract_file_path=str(contract.rendered_path),
                        variables=dict(variables),
                    )
                except YamlParserException as exc:
                    results.append(ContractResult(contract=contract, checks=(), errors=(str(exc),)))
                else:
                    results.append(map_session_result(contract, session))
        return results


def schema_details(check_result: Any) -> tuple[str, ...]:
    """Explain a schema check (Soda's SchemaCheckResult only exposes a count as its metric)."""
    details: list[str] = []
    missing = getattr(check_result, "expected_column_names_not_actual", None) or []
    extra = getattr(check_result, "actual_column_names_not_expected", None) or []
    mismatches = getattr(check_result, "column_data_type_mismatches", None) or []
    if missing:
        details.append(f"missing columns: {', '.join(missing)}")
    if extra:
        details.append(f"unexpected columns: {', '.join(extra)}")
    details.extend(
        f"{m.column}: expected {m.expected_data_type}, actual {m.actual_data_type}" for m in mismatches
    )
    if getattr(check_result, "are_columns_out_of_order", False) is True:
        details.append("columns are out of order")
    return tuple(details)


def map_session_result(contract: ContractSpec, session: Any) -> ContractResult:
    """Map a Soda ``ContractVerificationSessionResult`` (one contract) to a domain ContractResult."""
    checks: list[CheckResult] = []
    errors: list[str] = list(session.get_errors())
    started = ended = None
    for cvr in session.contract_verification_results:
        started = getattr(cvr, "started_timestamp", None)
        ended = getattr(cvr, "ended_timestamp", None)
        for cr in cvr.check_results:
            outcome_name = getattr(cr.outcome, "name", str(cr.outcome))
            checks.append(
                CheckResult(
                    name=cr.check.name or cr.check.type,
                    check_type=cr.check.type,
                    outcome=_OUTCOMES.get(outcome_name, Outcome.NOT_EVALUATED),
                    column=cr.check.column_name,
                    metrics=dict(cr.diagnostic_metric_values or {}),
                    details=schema_details(cr),
                )
            )
    if not session.contract_verification_results and not errors:
        errors.append("Soda returned no verification result")
    elif not checks and not errors:
        errors.append("Soda returned no check results")
    return ContractResult(
        contract=contract,
        checks=tuple(checks),
        errors=tuple(dict.fromkeys(errors)),
        started_at=started,
        ended_at=ended,
    )
