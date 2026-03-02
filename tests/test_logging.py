"""Tests for structured logging configuration."""

import structlog

from piper.config import Settings
from piper.logging import configure_logging, get_logger


def _settings(**kw) -> Settings:
    return Settings(logging=kw)


class TestConfigureLogging:
    def test_returns_eight_char_hex_run_id(self):
        run_id = configure_logging(_settings())
        assert len(run_id) == 8
        assert all(c in "0123456789abcdef" for c in run_id)

    def test_each_call_produces_unique_run_id(self):
        ids = {configure_logging(_settings()) for _ in range(10)}
        assert len(ids) == 10


class TestGetLogger:
    def test_logger_captures_events_with_run_id(self):
        """configure_logging + get_logger produce a working logger with run_id in context."""
        run_id = configure_logging(_settings())
        with structlog.testing.capture_logs() as events:
            get_logger(__name__).info("ping", x=1)
        assert events[0]["event"] == "ping"
        ctx = structlog.contextvars.get_contextvars()
        assert ctx["run_id"] == run_id
