from dataclasses import replace
from pathlib import Path, PurePosixPath

import pytest

from piper.errors import PiperError
from piper.tracker import Asset
from piper_studio.storage import asset_directory

PAN = Asset(id="7701", name="Frying Pan", type="Prop", folder="kitchen", pipe_name="frying_pan")


def test_a_renamed_asset_keeps_the_directory_its_pipe_name_gave_it(tmp_path: Path) -> None:
    made = tmp_path / "asset" / "kitchen" / "frying_pan"
    made.mkdir(parents=True)

    assert asset_directory(PurePosixPath(tmp_path), replace(PAN, name="Cast Iron Pan")) == made


@pytest.mark.parametrize(
    ("asset", "remedy"),
    [
        pytest.param(replace(PAN, folder=None), "give it a folder in the tracker", id="folder"),
        pytest.param(replace(PAN, pipe_name=None), "`piper create asset` gives it one", id="name"),
        pytest.param(replace(PAN, pipe_name="The Pan"), "correct it in the tracker", id="typed"),
        pytest.param(replace(PAN, pipe_name="kettle"), "`piper create asset` makes it", id="made"),
    ],
)
def test_an_asset_with_no_directory_is_refused_with_what_would_give_it_one(
    tmp_path: Path, asset: Asset, remedy: str
) -> None:
    (tmp_path / "asset" / "kitchen" / "frying_pan").mkdir(parents=True)

    with pytest.raises(PiperError, match=remedy):
        asset_directory(PurePosixPath(tmp_path), asset)
