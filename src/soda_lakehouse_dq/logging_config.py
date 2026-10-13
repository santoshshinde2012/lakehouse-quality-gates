"""structlog setup: pretty console locally, JSON lines in containers / Airflow (DQ_LOG_JSON=true)."""

from __future__ import annotations

import logging

import structlog


def configure_logging(level: str = "INFO", json_output: bool = False) -> None:
    numeric = logging.getLevelNamesMapping().get(level.upper(), logging.INFO)
    logging.basicConfig(level=max(numeric, logging.WARNING), format="%(message)s")  # quiet Soda's own logs
    renderer: structlog.typing.Processor = (
        structlog.processors.JSONRenderer() if json_output else structlog.dev.ConsoleRenderer(colors=False)
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric),
        cache_logger_on_first_use=False,
    )
