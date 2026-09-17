from pathlib import PurePosixPath
from typing import Protocol

from piper.tracker import Asset


class Registry(Protocol):
    """One production's installed product versions, in the system that indexes them.

    Storage holds a version and decides its number; a registry records a
    version only after it is installed.
    """

    def register(self, asset: Asset, *, product: str, version: int, path: PurePosixPath) -> str:
        """Record an installed version and return the record's opaque id.

        ``path`` is the version's root layer. Refuses a version already
        recorded for the same asset and product.
        """
        ...
