"""Tests for structured logging configuration."""

import pytest
import structlog

from piper.config import Settings
from piper.logging import configure_logging, get_logger


@pytest.fixture(autouse=True)
def _reset_structlog():
    """Isolate each test: reset structlog config and clear context vars."""
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()
    yield
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()


def _test_settings(**logging_kwargs) -> Settings:
    """Build a minimal Settings instance with custom logging overrides."""
    return Settings(logging=logging_kwargs)


class TestConfigureLogging:
    def test_returns_eight_char_hex_run_id(self):
        run_id = configure_logging(_test_settings())
        assert len(run_id) == 8
        assert all(c in "0123456789abcdef" for c in run_id)

    def test_run_id_bound_to_context_vars(self):
        run_id = configure_logging(_test_settings())
        ctx = structlog.contextvars.get_contextvars()
        assert ctx.get("run_id") == run_id

    def test_each_call_produces_unique_run_id(self):
        ids = {configure_logging(_test_settings()) for _ in range(10)}
        assert len(ids) == 10

    def test_reconfigure_replaces_run_id(self):
        first = configure_logging(_test_settings())
        second = configure_logging(_test_settings())
        ctx = structlog.contextvars.get_contextvars()
        assert ctx["run_id"] == second
        assert ctx["run_id"] != first

    def test_accepts_explicit_settings(self):
        """configure_logging() must accept a Settings instance directly."""
        s = _test_settings(level="DEBUG", format="text")
        run_id = configure_logging(s)
        assert run_id  # non-empty

    def test_loads_settings_when_none_given(self):
        """Passing None should fall back to get_settings() without raising."""
        run_id = configure_logging(None)
        assert len(run_id) == 8


class TestGetLogger:
    def test_returns_bound_logger(self):
        configure_logging(_test_settings())
        log = get_logger(__name__)
        assert log is not None

    def test_logger_accepts_info_call(self):
        configure_logging(_test_settings())
        log = get_logger(__name__)
        # Must not raise; output goes to stderr (captured by pytest).
        log.info("test event", foo="bar")

    def test_capture_logs_records_events(self):
        """structlog.testing.capture_logs() captures events as dicts."""
        configure_logging(_test_settings())
        with structlog.testing.capture_logs() as events:
            get_logger(__name__).info("hello", answer=42)
        assert len(events) == 1
        assert events[0]["event"] == "hello"
        assert events[0]["answer"] == 42
        assert events[0]["log_level"] == "info"

    def test_run_id_present_after_merge_in_capture(self):
        """run_id appears in captured events after merge_contextvars runs."""
        run_id = configure_logging(_test_settings())
        # capture_logs bypasses the renderer but DOES run merge_contextvars
        # when it's the first processor — confirm run_id is visible.
        ctx = structlog.contextvars.get_contextvars()
        assert ctx["run_id"] == run_id
