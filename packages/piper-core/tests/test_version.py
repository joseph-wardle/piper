from importlib.metadata import version

import piper


def test_version_matches_distribution_metadata() -> None:
    assert piper.__version__ == version("piper-core")
