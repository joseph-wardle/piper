import os
import stat
from collections.abc import Callable
from pathlib import Path, PurePosixPath

import pytest

from piper.errors import PiperError, TrackerError
from piper.tracker import Asset, Tracker
from piper_studio.create import (
    CreateAssetResult,
    PartialCreateAssetError,
    UnknownFolderError,
    create_asset,
)

TYPES = ("Prop", "Set Piece", "Character")


@pytest.fixture
def root(tmp_path: Path) -> Path:
    root = tmp_path / "production"
    root.mkdir()
    return root


def create(
    tracker: Tracker,
    root: Path,
    name: str = "Toaster",
    *,
    type: str = "Prop",
    folder: str = "kitchen",
    new_folder: bool = False,
) -> CreateAssetResult:
    return create_asset(
        tracker,
        root=PurePosixPath(root),
        types=TYPES,
        name=name,
        type=type,
        folder=folder,
        new_folder=new_folder,
    )


def test_creates_the_asset_and_its_directory_named_by_slugs(tracker: Tracker, root: Path) -> None:
    result = create(tracker, root, "Mr. Yoon's Toaster", folder="Kitchen")

    assert tracker.find_assets("Toaster") == (result.asset,)
    assert (result.asset.type, result.asset.folder) == ("Prop", "kitchen")
    assert result.directory == PurePosixPath(root, "asset", "kitchen", "mr_yoons_toaster")
    assert Path(result.directory).is_dir()
    assert (result.asset_created, result.directory_created) == (True, True)


def test_an_unused_folder_is_refused_before_anything_changes(tracker: Tracker, root: Path) -> None:
    with pytest.raises(UnknownFolderError, match=r"'garage' \(folders in use: kitchen\)"):
        create(tracker, root, folder="garage")

    assert tracker.find_assets("Toaster") == ()
    assert not (root / "asset").exists()


def test_a_new_folder_is_started_when_asked(tracker: Tracker, root: Path) -> None:
    result = create(tracker, root, folder="garage", new_folder=True)

    assert result.asset.folder == "garage"
    assert Path(result.directory).is_dir()


def test_a_folder_is_one_name_not_a_path(tracker: Tracker, root: Path) -> None:
    with pytest.raises(PiperError, match="one name, not a path"):
        create(tracker, root, folder="kitchen/drawers", new_folder=True)

    assert tracker.find_assets("Toaster") == ()


def test_a_type_outside_the_production_is_refused(tracker: Tracker, root: Path) -> None:
    with pytest.raises(PiperError, match="types: Prop, Set Piece, Character"):
        create(tracker, root, type="Vehicle")

    assert tracker.find_assets("Toaster") == ()


@pytest.mark.parametrize(("name", "folder"), [("!!!", "kitchen"), ("Toaster", "!!!")])
def test_a_name_or_folder_without_letters_or_digits_is_refused(
    tracker: Tracker, root: Path, name: str, folder: str
) -> None:
    # Its slug is empty, so its directory would collapse into its parent's.
    with pytest.raises(PiperError, match="needs a letter or digit"):
        create(tracker, root, name, folder=folder, new_folder=True)

    assert len(tracker.find_assets("")) == 3
    assert not (root / "asset").exists()


@pytest.mark.parametrize(
    "make_unusable",
    [
        pytest.param(Path.rmdir, id="missing"),
        pytest.param(lambda root: root.chmod(0o500), id="unwritable"),
    ],
)
def test_a_root_the_artist_cannot_write_is_refused_before_the_tracker_changes(
    tracker: Tracker, root: Path, make_unusable: Callable[[Path], None]
) -> None:
    make_unusable(root)

    with pytest.raises(PiperError, match="is not a directory you can write to"):
        create(tracker, root)

    assert tracker.find_assets("Toaster") == ()


def test_an_asset_complete_in_both_systems_is_a_duplicate_by_slug(
    tracker: Tracker, root: Path
) -> None:
    create(tracker, root, "Toaster")

    with pytest.raises(PiperError, match="already exists at"):
        create(tracker, root, "toaster!")

    assert len(tracker.find_assets("Toaster")) == 1


def test_an_asset_missing_its_directory_is_finished(tracker: Tracker, root: Path) -> None:
    result = create(tracker, root, "Frying Pan")

    assert result.asset.id == "7701"
    assert (result.asset_created, result.directory_created) == (False, True)
    assert Path(result.directory).is_dir()


def test_a_directory_missing_its_asset_is_finished(tracker: Tracker, root: Path) -> None:
    (root / "asset" / "kitchen" / "toaster").mkdir(parents=True)

    result = create(tracker, root, "Toaster")

    assert tracker.find_assets("Toaster") == (result.asset,)
    assert (result.asset_created, result.directory_created) == (True, False)


@pytest.mark.parametrize(("type", "folder"), [("Prop", "shop"), ("Character", "kitchen")])
def test_an_existing_asset_is_never_moved_or_reclassified(
    tracker: Tracker, root: Path, type: str, folder: str
) -> None:
    with pytest.raises(
        PiperError, match=r"'Frying Pan' already exists \(type: Prop, folder: kitchen\)"
    ):
        create(tracker, root, "Frying Pan", type=type, folder=folder, new_folder=True)

    assert not (root / "asset").exists()


def test_a_tracker_failure_leaves_storage_untouched(
    tracker: Tracker, root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(name: str, *, type: str, folder: str) -> Asset:
        raise TrackerError(f"shotgrid: cannot create asset {name!r}")

    monkeypatch.setattr(tracker, "create_asset", refuse)

    with pytest.raises(TrackerError):
        create(tracker, root, new_folder=True)

    assert not (root / "asset").exists()


def test_a_storage_failure_reports_what_was_and_was_not_created(
    tracker: Tracker, root: Path
) -> None:
    assets = root / "asset"
    assets.mkdir()
    assets.chmod(0o500)

    with pytest.raises(PartialCreateAssetError) as raised:
        create(tracker, root, "Toaster")

    result = raised.value.result
    assert tracker.find_assets("Toaster") == (result.asset,)
    assert (result.asset_created, result.directory_created) == (True, False)
    message = str(raised.value)
    assert "'Toaster' is in the tracker" in message
    assert f"{result.directory} could not be created (Permission denied)" in message


def test_new_directories_copy_their_parents_mode_not_the_umask(
    tracker: Tracker, root: Path
) -> None:
    # The group is copied too; only real storage with a second group can show it.
    root.chmod(0o770)
    assets = root / "asset"
    assets.mkdir()
    assets.chmod(0o750)
    umask = os.umask(0o022)
    try:
        result = create(tracker, root)
    finally:
        os.umask(umask)

    assert stat.S_IMODE(assets.stat().st_mode) == 0o750
    for directory in (assets / "kitchen", Path(result.directory)):
        assert stat.S_IMODE(directory.stat().st_mode) == 0o750
