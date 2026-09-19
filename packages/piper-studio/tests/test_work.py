from dataclasses import replace
from pathlib import Path, PurePosixPath

import pytest

from piper.errors import PiperError
from piper.tracker import Asset
from piper_studio.context import Context
from piper_studio.work import prepare_work

PAN = Asset(id="7701", name="Frying Pan", type="Prop", folder="kitchen", pipe_name="frying_pan")
MODELING = Context(name="modeling", subject="asset", host="maya", extension="mb")


@pytest.fixture
def root(tmp_path: Path) -> Path:
    """A production holding the frying pan's directory."""
    root = tmp_path / "production"
    (root / "asset" / "kitchen" / "frying_pan").mkdir(parents=True)
    return root


def test_work_is_one_file_in_a_directory_made_for_its_context(root: Path) -> None:
    pan = root / "asset" / "kitchen" / "frying_pan"

    file = prepare_work(root=PurePosixPath(root), asset=PAN, context=MODELING)

    assert file == pan / "work" / "modeling" / "frying_pan.mb"
    assert file.parent.is_dir() and not file.exists()
    # The terminal prepares work before Maya starts, and Maya prepares it again.
    assert prepare_work(root=PurePosixPath(root), asset=PAN, context=MODELING) == file


def test_an_asset_with_no_directory_is_not_given_one_by_opening_its_work(root: Path) -> None:
    kettle = replace(PAN, name="Kettle", pipe_name="kettle")

    with pytest.raises(PiperError, match="has no directory at"):
        prepare_work(root=PurePosixPath(root), asset=kettle, context=MODELING)

    assert not (root / "asset" / "kitchen" / "kettle").exists()
