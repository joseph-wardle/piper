import stat
from pathlib import Path

import pytest

from piper_studio.storage import make_directories


def test_a_parent_another_process_is_still_making_lends_no_permissions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    asset = tmp_path / "frying_pan"
    asset.mkdir()
    asset.chmod(0o770)
    publish = asset / "publish"
    mkdir = Path.mkdir

    def made_first_elsewhere(
        self: Path, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        if self == publish and not self.exists():
            # Made, but its maker has not copied the permissions onto it yet.
            mkdir(self)
            self.chmod(0o700)
            raise FileExistsError(self)
        mkdir(self, mode, parents, exist_ok)

    monkeypatch.setattr(Path, "mkdir", made_first_elsewhere)

    make_directories(publish / "geo")

    assert stat.S_IMODE((publish / "geo").stat().st_mode) == 0o770
