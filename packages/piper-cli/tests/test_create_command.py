import json
from collections.abc import Callable
from pathlib import Path

import pytest

from piper.tracker import Tracker

Run = Callable[..., int]

TOASTER = ("create", "asset", "Toaster", "--type", "Prop", "--folder", "kitchen")


def test_says_what_it_created_and_where(
    run: Run, tracker: Tracker, root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(tracker, *TOASTER) == 0

    captured = capsys.readouterr()
    assert captured.out.splitlines() == [
        "Created asset 'Toaster' (Prop, in kitchen)",
        f"Created {root / 'asset' / 'kitchen' / 'toaster'}",
    ]
    assert captured.err == ""


def test_json_carries_the_asset_its_directory_and_what_was_created(
    run: Run, tracker: Tracker, root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(tracker, *TOASTER, "--json") == 0

    assert json.loads(capsys.readouterr().out) == {
        "asset": {"id": "9003", "name": "Toaster", "type": "Prop", "folder": "kitchen"},
        "directory": str(root / "asset" / "kitchen" / "toaster"),
        "created": {"tracker": True, "directory": True},
    }


def test_an_unused_folder_names_the_flag_that_starts_it(
    run: Run, tracker: Tracker, root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    command = ("create", "asset", "Toaster", "--type", "Prop", "--folder", "garage")

    assert run(tracker, *command) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        "piper: no asset is in folder 'garage' (folders in use: kitchen); "
        "pass --new-folder to start it\n"
    )

    assert run(tracker, *command, "--new-folder") == 0

    assert (root / "asset" / "garage" / "toaster").is_dir()


def test_a_partial_create_as_json_still_writes_the_record(
    run: Run, tracker: Tracker, root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assets = root / "asset"
    assets.mkdir()
    assets.chmod(0o500)

    assert run(tracker, *TOASTER, "--json") == 1

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["asset"]["name"] == "Toaster"
    assert payload["created"] == {"tracker": True, "directory": False}
    assert "Permission denied" in payload["error"]
    assert captured.err.startswith("piper: ")
