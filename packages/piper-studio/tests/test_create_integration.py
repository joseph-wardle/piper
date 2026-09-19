import contextlib
import os
import shutil
import uuid
from pathlib import Path, PurePosixPath

import pytest
import shotgun_api3

from piper.errors import PiperError
from piper_shotgrid.tracker import ShotGridTracker
from piper_studio.create import CreateAssetResult, create_asset

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("PIPER_SHOTGRID_KEY"),
        reason="PIPER_SHOTGRID_KEY is not set",
    ),
]

SITE = "https://byuanimation.shotgunstudio.com"
SCRIPT = "sandwich_pipeline"
# An inactive copy of the production, kept for writes. The live one is never written.
WRITE_PROJECT = 782
# Scratch space on the same NFS mount as production storage
ROOT = Path("/groups/sandwich/04_temp")
TYPE = "Set Piece"


def test_creates_in_the_write_project_with_crew_writable_directories_and_refuses_a_repeat() -> None:
    tracker = ShotGridTracker(
        site=SITE, script=SCRIPT, key=os.environ["PIPER_SHOTGRID_KEY"], project=WRITE_PROJECT
    )
    # Unique to the run, so two runs cannot collide; its folder is new, so it
    # is started deliberately.
    run = uuid.uuid4().hex[:8]
    name = f"Piper Test {run}"
    folder = f"piper_test_{run}"

    def create(*, new_folder: bool = False) -> CreateAssetResult:
        return create_asset(
            tracker,
            root=PurePosixPath(ROOT),
            types=(TYPE,),
            name=name,
            type=TYPE,
            folder=folder,
            new_folder=new_folder,
        )

    try:
        result = create(new_folder=True)

        # The tracker only finds within its own project, so finding it proves where it went.
        assert tracker.find_assets(name) == (result.asset,)
        assert (result.asset.type, result.asset.folder) == (TYPE, folder)
        # The pipe name reads back by id as it was written, which opening work depends on.
        assert result.asset.pipe_name == f"piper_test_{run}"
        assert tracker.asset(result.asset.id) == result.asset

        group = ROOT.stat().st_gid
        for directory in (ROOT / "asset", ROOT / "asset" / folder, Path(result.directory)):
            info = directory.stat()
            assert info.st_gid == group
            assert info.st_mode & 0o070 == 0o070

        # Refused as a duplicate, not a mismatch: the type and folder read back exactly.
        with pytest.raises(PiperError, match="already exists at"):
            create()
    finally:
        remove(name, folder)


def remove(name: str, folder: str) -> None:
    """Retire the asset and remove its directories.

    ShotGrid's API can only retire, so each run leaves one retired asset in the
    write project, invisible to every find.
    """
    shotgrid = shotgun_api3.Shotgun(
        SITE, script_name=SCRIPT, api_key=os.environ["PIPER_SHOTGRID_KEY"]
    )
    project = {"type": "Project", "id": WRITE_PROJECT}
    for entity in shotgrid.find("Asset", [["project", "is", project], ["code", "is", name]]):
        shotgrid.delete("Asset", entity["id"])
    shutil.rmtree(ROOT / "asset" / folder, ignore_errors=True)
    with contextlib.suppress(OSError):
        (ROOT / "asset").rmdir()
