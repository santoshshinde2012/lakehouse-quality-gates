"""Access to local-data-lakehouse's own client, referenced by path (``DQ_LDL_REPO_PATH``), not vendored.

Single place that touches ``sys.path``: the DuckDB connection factory and the Iceberg sink both
reuse the repo's exact ATTACH / catalog logic through :func:`import_lakehouse_client`.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType

from soda_lakehouse_dq.domain.errors import DataSourceError


def import_lakehouse_client(ldl_repo_path: Path) -> ModuleType:
    src = ldl_repo_path / "src"
    if not (src / "lakehouse_client").is_dir():
        raise DataSourceError(
            f"lakehouse_client not found under {src}; "
            "set DQ_LDL_REPO_PATH to the local-data-lakehouse checkout"
        )
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    return importlib.import_module("lakehouse_client")
