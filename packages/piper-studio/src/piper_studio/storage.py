"""Changes every operation makes to production storage the same way."""

import os
import stat
from pathlib import Path


def make_directories(directory: Path) -> None:
    """Make ``directory`` and its missing parents, copying the nearest directory that exists."""
    missing: list[Path] = []
    existing = directory
    while not existing.is_dir():
        missing.append(existing)
        existing = existing.parent
    template = existing.stat()
    for path in reversed(missing):
        try:
            path.mkdir()
        except FileExistsError:
            if path == directory:
                raise
            continue
        os.chown(path, -1, template.st_gid)
        path.chmod(stat.S_IMODE(template.st_mode))
