"""Tests for the PID-file run lock."""

import os

import pytest

from piper.lock import LOCK_FILE, LockError, RunLock


class TestAcquireAndRelease:
    def test_acquire_creates_lock_file(self, tmp_path):
        lock = RunLock(tmp_path)
        lock.acquire()
        assert (tmp_path / LOCK_FILE).is_file()
        lock.release()

    def test_lock_file_contains_current_pid(self, tmp_path):
        lock = RunLock(tmp_path)
        lock.acquire()
        pid = int((tmp_path / LOCK_FILE).read_text().strip())
        assert pid == os.getpid()
        lock.release()

    def test_release_removes_lock_file(self, tmp_path):
        lock = RunLock(tmp_path)
        lock.acquire()
        lock.release()
        assert not (tmp_path / LOCK_FILE).exists()

    def test_release_before_acquire_is_noop(self, tmp_path):
        """Calling release() without acquire() must not raise."""
        RunLock(tmp_path).release()

    def test_double_acquire_raises_lock_error(self, tmp_path):
        lock1 = RunLock(tmp_path)
        lock2 = RunLock(tmp_path)
        lock1.acquire()
        with pytest.raises(LockError):
            lock2.acquire()
        lock1.release()

    def test_lock_error_message_includes_pid(self, tmp_path):
        lock1 = RunLock(tmp_path)
        lock1.acquire()
        lock2 = RunLock(tmp_path)
        with pytest.raises(LockError, match=str(os.getpid())):
            lock2.acquire()
        lock1.release()

    def test_lock_error_message_says_already_running(self, tmp_path):
        lock1 = RunLock(tmp_path)
        lock1.acquire()
        lock2 = RunLock(tmp_path)
        with pytest.raises(LockError, match="already running"):
            lock2.acquire()
        lock1.release()


class TestContextManager:
    def test_releases_on_normal_exit(self, tmp_path):
        with RunLock(tmp_path):
            pass
        assert not (tmp_path / LOCK_FILE).exists()

    def test_releases_on_exception(self, tmp_path):
        with pytest.raises(ValueError):
            with RunLock(tmp_path):
                raise ValueError("something went wrong")
        assert not (tmp_path / LOCK_FILE).exists()

    def test_prevents_concurrent_context(self, tmp_path):
        with RunLock(tmp_path):
            with pytest.raises(LockError):
                with RunLock(tmp_path):
                    pass  # pragma: no cover


class TestStaleLock:
    def test_stale_lock_is_overwritten(self, tmp_path):
        """A lock file with a dead PID must be silently replaced."""
        # PIDs above 4 194 304 are invalid on Linux — guaranteed not alive.
        (tmp_path / LOCK_FILE).write_text("99999999")
        lock = RunLock(tmp_path)
        lock.acquire()  # must not raise
        assert int((tmp_path / LOCK_FILE).read_text()) == os.getpid()
        lock.release()

    def test_unreadable_lock_content_treated_as_stale(self, tmp_path):
        """A lock file with non-integer content is treated as stale."""
        (tmp_path / LOCK_FILE).write_text("not-a-pid")
        lock = RunLock(tmp_path)
        lock.acquire()  # must not raise
        lock.release()
