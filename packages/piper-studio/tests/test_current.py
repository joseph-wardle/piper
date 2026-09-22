"""The current layer: one per asset, moved whole, readable by anything that opens USD."""

from pathlib import Path, PurePosixPath

import pytest
from pxr import Ar, Usd
from test_compose import GEO, build, install

from piper.errors import PiperError
from piper.tracker import Asset
from piper_studio import compose
from piper_studio.current import current, current_path, make_current

PAN = Asset(id="7701", name="Frying Pan", type="Prop", folder="kitchen", pipe_name="frying_pan")


@pytest.fixture
def root(tmp_path: Path) -> PurePosixPath:
    root = tmp_path / "production"
    (root / "asset" / "kitchen" / "frying_pan").mkdir(parents=True)
    install(root, "geo", 1, GEO)
    install(root, "geo", 2, GEO)
    for version in (1, 2):
        (tmp_path / f"staged{version}").mkdir()
        build(PurePosixPath(root), tmp_path, {"geo": version}, version)
    return PurePosixPath(root)


def test_nothing_is_current_until_a_version_is_made_current(root: PurePosixPath) -> None:
    assert current(root, PAN) is None

    make_current(root, PAN, 2)

    assert current(root, PAN) == 2
    assert current_path(root, PAN) == Path(
        root, "asset/kitchen/frying_pan/publish/asset/frying_pan.usda"
    )


def test_the_current_layer_opens_as_the_asset_and_moves_whole(root: PurePosixPath) -> None:
    make_current(root, PAN, 2)
    make_current(root, PAN, 1)

    stage = Usd.Stage.Open(str(current_path(root, PAN)), Ar.DefaultResolverContext([str(root)]))
    assert stage.GetCompositionErrors() == []
    assert stage.GetDefaultPrim().GetPath() == "/frying_pan"
    assert stage.GetPrimAtPath("/frying_pan/geo/render/body")
    assert current(root, PAN) == 1
    assert [
        p.name for p in current_path(root, PAN).parent.iterdir() if p.name.startswith(".")
    ] == []
    assert compose.versions(root, PAN, compose.ASSET) == [1, 2]


def test_only_an_installed_asset_version_can_be_current(root: PurePosixPath) -> None:
    with pytest.raises(PiperError, match=r"has no asset v003 \(installed: v001, v002\)"):
        make_current(root, PAN, 3)

    assert current(root, PAN) is None


def test_a_current_layer_piper_did_not_write_is_refused_not_guessed(root: PurePosixPath) -> None:
    current_path(root, PAN).write_text("#usda 1.0\n", encoding="utf-8")

    with pytest.raises(PiperError, match="is not a current layer Piper wrote"):
        current(root, PAN)


def test_a_current_layer_that_cannot_be_replaced_is_reported(root: PurePosixPath) -> None:
    make_current(root, PAN, 1)
    current_path(root, PAN).parent.chmod(0o500)
    try:
        with pytest.raises(PiperError, match="could not write"):
            make_current(root, PAN, 2)
    finally:
        current_path(root, PAN).parent.chmod(0o700)

    assert current(root, PAN) == 1
