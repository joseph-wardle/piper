"""Run lock: prevents concurrent piper ingest/backfill runs.

``RunLock(state_dir)`` is a context manager that acquires a PID-file lock
on entry and releases it on exit.  ``LockError`` is raised if another
piper process is already running.

Stale locks (from a process that crashed without releasing) are detected
by checking whether the recorded PID is still alive; if not, the lock
file is silently overwritten.

Usage::

    from piper.lock import LockError, RunLock

    try:
        with RunLock(paths.state_dir):
            # ... ingest work ...
    except LockError as exc:
        log.error("piper already running", detail=str(exc))
        raise typer.Exit(1)

Note: ``state_dir`` must already exist (created by ``piper init`` via
``paths.ensure_output_dirs()``).
"""

from __future__ import annotations

import os
from pathlib import Path

LOCK_FILE = "piper.lock"


class LockError(RuntimeError):
    """Raised when the run lock cannot be acquired."""


class RunLock:
    """PID-file lock that prevents concurrent piper runs.

    Args:
        state_dir: The piper state directory (``paths.state_dir``).
    """

    def __init__(self, state_dir: Path) -> None:
        self._lock_path = state_dir / LOCK_FILE
        self._pid = os.getpid()

    def acquire(self) -> None:
        """Write the PID lock file; raise ``LockError`` if already locked.

        Uses ``O_CREAT | O_EXCL`` for an atomic create-if-not-exists.
        If the file already exists, checks whether the owning PID is still
        alive.  A stale lock (dead process) is silently overwritten.
        """
        try:
            fd = os.open(str(self._lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            existing_pid = self._read_pid()
            if existing_pid is not None and _is_alive(existing_pid):
                raise LockError(
                    f"piper is already running (PID {existing_pid}).  Lock file: {self._lock_path}"
                ) from None
            # Stale lock from a dead process — overwrite with our PID.
            self._lock_path.write_text(str(self._pid))
            return

        # Atomic create succeeded — write PID into the open file descriptor.
        os.write(fd, str(self._pid).encode())
        os.close(fd)

    def release(self) -> None:
        """Remove the lock file if it still belongs to this process."""
        try:
            if self._read_pid() == self._pid:
                self._lock_path.unlink()
        except FileNotFoundError:
            pass  # already removed; that is fine

    def __enter__(self) -> RunLock:
        self.acquire()
        return self

    def __exit__(self, *_: object) -> None:
        self.release()

    def _read_pid(self) -> int | None:
        """Return the PID from the lock file, or None if missing or unreadable."""
        try:
            return int(self._lock_path.read_text().strip())
        except (FileNotFoundError, ValueError):
            return None


def _is_alive(pid: int) -> bool:
    """Return True if a process with *pid* is currently running."""
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False  # no such process — stale lock
    except PermissionError:
        return True  # process exists but we cannot signal it
