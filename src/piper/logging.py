"""Structured logging configuration using structlog.

Call ``configure_logging()`` once at CLI startup to set up the processor
pipeline and bind a ``run_id`` to every log event emitted during that run.

Processor pipeline (applied in order to every log event):

  1. merge_contextvars   — pulls run_id (and any other bound vars) into the event
  2. add_log_level       — adds  level="info" / "error" / …
  3. TimeStamper         — adds  timestamp="2026-03-01T02:41:55Z"
  4. JSONRenderer        — renders as a single JSON line  (format=json)
     ConsoleRenderer     — renders as coloured key=value  (format=text)

Typical usage:

    from piper.logging import configure_logging, get_logger

    run_id = configure_logging()         # call once, at CLI startup
    log = get_logger(__name__)
    log.info("ingest started", file_count=42)
    # → {"timestamp": "…", "level": "info", "run_id": "a3f7b29c",
    #    "event": "ingest started", "file_count": 42}
"""

import logging as _stdlib
import sys
import uuid

import structlog

from piper.config import Settings, get_settings


def configure_logging(settings: Settings | None = None) -> str:
    """Configure structlog for this process and return the run_id.

    Calling again reconfigures the pipeline and binds a fresh run_id.

    Args:
        settings: Pre-loaded settings; loads from ``get_settings()`` if None.

    Returns:
        run_id — 8-character hex string present on every log event this run.
    """
    if settings is None:
        settings = get_settings()

    level_int = getattr(_stdlib, settings.logging.level, _stdlib.INFO)

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]

    renderer = (
        structlog.processors.JSONRenderer()
        if settings.logging.format == "json"
        else structlog.dev.ConsoleRenderer()
    )

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(level_int),
        context_class=dict,
        # Logs go to stderr; stdout is reserved for command output.
        # cache_logger_on_first_use is intentionally False: caching captures a
        # reference to sys.stderr at first-use time, which breaks CLI test
        # runners that redirect stderr per-invocation.
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=False,
    )

    run_id = uuid.uuid4().hex[:8]
    structlog.contextvars.bind_contextvars(run_id=run_id)

    return run_id


def get_logger(name: str = "piper") -> structlog.BoundLogger:
    """Return a structlog logger.

    Pass ``__name__`` to associate the logger with the calling module::

        log = get_logger(__name__)
        log.info("loading manifest")
    """
    return structlog.get_logger(name)
