"""JSONL file discovery and stability filtering.

``discover_settled_files(raw_root, settle_seconds, *, now)`` is the
single entry point.  It recursively scans ``raw_root`` for ``*.jsonl``
files and returns only those whose ``mtime`` is older than
``settle_seconds`` ago — the *settle window*.

The settle window prevents ingesting files that are still being written
by the pipeline.  A file modified within the last ``settle_seconds``
seconds may not yet contain its final events, so it is silently skipped
and will be picked up on the next run.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import NamedTuple


class FoundFile(NamedTuple):
    """Fingerprint of a discovered, settled JSONL file.

    Immutable so it is safe to use as a dict key or in a set.
    The ``(mtime, path)`` natural sort order places the oldest files
    first, which is the preferred ingest order.
    """

    path: Path
    size: int    # bytes (st_size from os.stat)
    mtime: float  # Unix timestamp (st_mtime from os.stat)


def discover_settled_files(
    raw_root: Path,
    settle_seconds: int,
    *,
    now: float | None = None,
) -> list[FoundFile]:
    """Return settled ``*.jsonl`` files under ``raw_root``, oldest-first.

    A file is *settled* if its ``mtime`` is at least ``settle_seconds``
    in the past (``mtime <= now - settle_seconds``).  Unsettled files are
    silently skipped; they will be discovered on the next run.

    Args:
        raw_root:       Root directory to scan recursively (pipeline-owned,
                        treated as read-only).  Returns an empty list if
                        the directory does not yet exist.
        settle_seconds: Minimum age in seconds for a file to be considered
                        stable.  Matches ``settings.ingest.settle_seconds``.
        now:            Override the current time as a Unix timestamp.
                        Defaults to ``time.time()``.  Pass an explicit
                        value in tests to avoid wall-clock dependence.

    Returns:
        List of :class:`FoundFile`, sorted by ``(mtime, path)`` so the
        oldest files are processed first.
    """
    if not raw_root.is_dir():
        return []

    cutoff = (now if now is not None else time.time()) - settle_seconds

    settled = []
    for path in raw_root.rglob("*.jsonl"):
        if not path.is_file():
            continue
        st = path.stat()
        if st.st_mtime <= cutoff:
            settled.append(FoundFile(path=path, size=st.st_size, mtime=st.st_mtime))

    return sorted(settled, key=lambda f: (f.mtime, f.path))
