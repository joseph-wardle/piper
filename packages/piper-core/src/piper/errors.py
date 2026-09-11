"""Failures Piper reports to a person."""


class PiperError(Exception):
    """A production failure worth reporting, as opposed to a bug."""


class ConfigError(PiperError):
    """Production configuration is missing, unreadable, or incomplete."""


class TrackerError(PiperError):
    """A tracker could not answer the question Piper asked."""
