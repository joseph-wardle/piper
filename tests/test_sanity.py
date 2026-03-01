"""Package sanity checks.

Confirms the package installs correctly and its public contract is intact.
These tests should always pass; a failure here means the build is broken.
"""

import piper


def test_version_is_declared() -> None:
    assert isinstance(piper.__version__, str)
    assert piper.__version__  # non-empty


def test_cli_app_is_importable() -> None:
    from piper.cli import app

    assert app is not None
