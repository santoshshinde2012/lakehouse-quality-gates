"""File-system contract repository.

Contracts live under ``<root>/<pipeline>/<layer>/*.yml``. Soda v4 does not resolve ``${var.*}`` in the
``dataset:`` key, so the engine-specific part of the dataset name is a ``{{ dataset_root }}`` token this
repository renders before Soda sees the file (DuckDB: ``lakehouse_duckdb``, Trino:
``lakehouse_trino/lakehouse``). Everything else stays plain Soda contract language.
"""

from __future__ import annotations

import hashlib
import os
import re
from collections.abc import Sequence
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import yaml

from soda_lakehouse_dq.domain.errors import ContractNotFoundError, ContractRenderError
from soda_lakehouse_dq.domain.models import ContractSpec, Layer

TOKEN = re.compile(r"\{\{\s*(\w+)\s*\}\}")


class FileContractRepository:
    def __init__(self, root: Path, render_dir: Path, tokens: dict[str, str]) -> None:
        self._root = root
        self._render_dir = render_dir
        self._tokens = dict(tokens)

    def list_contracts(self, pipeline: str, layer: Layer) -> Sequence[ContractSpec]:
        folder = self._root / pipeline / layer.value
        files = sorted([*folder.glob("*.yml"), *folder.glob("*.yaml")])
        if not files:
            raise ContractNotFoundError(f"no contracts under {folder}")
        return [self._render(pipeline, layer, f) for f in files]

    def render_text(self, text: str, source: Path) -> str:
        def substitute(match: re.Match[str]) -> str:
            key = match.group(1)
            if key not in self._tokens:
                raise ContractRenderError(f"{source}: unknown token {{{{ {key} }}}}")
            return self._tokens[key]

        return TOKEN.sub(substitute, text)

    def _render(self, pipeline: str, layer: Layer, source: Path) -> ContractSpec:
        rendered = self.render_text(source.read_text(encoding="utf-8"), source)
        document = _parse(rendered, source)
        content_hash = hashlib.sha256(rendered.encode("utf-8")).hexdigest()
        target = self._render_dir / pipeline / layer.value / content_hash / f"{source.stem}.yml"
        target.parent.mkdir(parents=True, exist_ok=True)
        _write_atomic(target, rendered)
        return ContractSpec(
            name=f"{pipeline}.{layer.value}.{source.stem}",
            pipeline=pipeline,
            layer=layer,
            source_path=source,
            rendered_path=target,
            dataset=str(document["dataset"]),
            references=tuple(dict.fromkeys(_references(document))),
        )


def _write_atomic(target: Path, text: str) -> None:
    """Readers see complete YAML even when several processes render the same content."""
    temporary_path: Path | None = None
    try:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            temporary.write(text)
        os.replace(temporary_path, target)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _references(node: object) -> list[str]:
    """Every ``valid_reference_data.dataset`` anywhere in the contract."""
    found: list[str] = []
    if isinstance(node, dict):
        ref = node.get("valid_reference_data")
        if isinstance(ref, dict) and isinstance(ref.get("dataset"), str):
            found.append(ref["dataset"])
        for value in node.values():
            found.extend(_references(value))
    elif isinstance(node, list):
        for item in node:
            found.extend(_references(item))
    return found


def _parse(text: str, source: Path) -> dict[str, Any]:
    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ContractRenderError(f"{source}: invalid YAML: {exc}") from exc
    if not isinstance(document, dict):
        raise ContractRenderError(f"{source}: a contract must be a YAML mapping")
    if not isinstance(document.get("dataset"), str):
        raise ContractRenderError(f"{source}: missing 'dataset' key")
    if not isinstance(document.get("columns"), list):
        raise ContractRenderError(f"{source}: missing 'columns' list (use [] for dataset-only contracts)")
    return document
