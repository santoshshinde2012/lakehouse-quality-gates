"""Validate caller-provided names before composing filesystem paths."""

from __future__ import annotations

from soda_lakehouse_dq.domain.errors import DataQualityError


def validate_path_component(value: str, label: str) -> str:
    """Keep a logical name within one path component on POSIX and Windows."""
    if not value or value in (".", "..") or "/" in value or "\\" in value or "\0" in value:
        raise DataQualityError(f"{label} must be a single non-empty path component")
    return value
