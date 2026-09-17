import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path, PurePosixPath

import pytest

from piper.errors import RegistryError
from piper.registry import Registry
from piper.tracker import Asset, Tracker

Run = Callable[..., int]

GEO = """#usda 1.0
(
    defaultPrim = "pan"
)

def Xform "pan"
{
}
"""


@pytest.fixture
def layer(root: Path, tmp_path: Path) -> Path:
    """An exported layer, beside the directory of the asset it belongs to."""
    (root / "asset" / "kitchen" / "frying_pan").mkdir(parents=True)
    path = tmp_path / "export" / "geo.usda"
    path.parent.mkdir()
    path.write_text(GEO, encoding="utf-8")
    return path


def installed(root: Path) -> str:
    return str(root / "asset" / "kitchen" / "frying_pan" / "publish" / "geo" / "v001" / "geo.usda")


def test_says_which_version_it_installed_and_where(
    run: Run, tracker: Tracker, root: Path, layer: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(tracker, "publish", "Frying Pan", "geo", str(layer)) == 0

    captured = capsys.readouterr()
    assert captured.out.splitlines() == ["Published geo v001 of 'Frying Pan'", installed(root)]
    assert captured.err == ""


def test_json_carries_the_version_its_root_layer_and_its_record(
    run: Run, tracker: Tracker, root: Path, layer: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(tracker, "publish", "Frying Pan", "geo", str(layer), "--json") == 0

    assert json.loads(capsys.readouterr().out) == {
        "asset": {"id": "7701", "name": "Frying Pan", "type": "Prop", "folder": "kitchen"},
        "product": "geo",
        "version": 1,
        "path": installed(root),
        "record_id": "6601",
    }


def test_the_asset_is_named_exactly(
    run: Run, tracker: Tracker, layer: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(tracker, "publish", "frying pan", "geo", str(layer)) == 1

    assert capsys.readouterr().err == (
        "piper: no asset is named 'frying pan' (names containing it: 'Frying Pan')\n"
    )


def test_a_registration_failure_exits_nonzero_and_names_the_installed_layer(
    run: Run,
    tracker: Tracker,
    registry: Registry,
    root: Path,
    layer: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def refuse(asset: Asset, *, product: str, version: int, path: PurePosixPath) -> str:
        raise RegistryError("shotgrid: refused to register geo v001")

    monkeypatch.setattr(registry, "register", refuse)

    assert run(tracker, "publish", "Frying Pan", "geo", str(layer), "--json") == 1

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert (payload["path"], payload["record_id"]) == (installed(root), None)
    assert captured.err.startswith(f"piper: installed {installed(root)}, but could not register it")


def test_the_command_starts_without_loading_usd() -> None:
    # Every command pays for what `piper_cli.main` imports; only publish needs USD.
    loaded = subprocess.run(
        [sys.executable, "-c", "import sys, piper_cli.main; print('pxr' in sys.modules)"],
        capture_output=True,
        text=True,
        check=True,
    )

    assert loaded.stdout == "False\n"
