import re
from dataclasses import replace
from pathlib import Path, PurePosixPath

import pytest

from piper.errors import PiperError
from piper.tracker import Asset, Tracker
from piper_studio.context import Context
from piper_studio.production import Production, ShotGridConfig, Software
from piper_studio.work import prepare_work, restamp_notice, stamp, stamped_asset

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


SANDWICH = Production(
    name="sandwich",
    root=PurePosixPath("/unused"),
    types=("Prop",),
    shotgrid=ShotGridConfig(site="https://example.invalid", script="piper", project=782),
    software=Software(),
)


def production_at(root: Path) -> Production:
    return replace(SANDWICH, root=PurePosixPath(root))


def test_a_scene_at_its_work_path_carrying_its_stamp_names_its_asset(
    tracker: Tracker, root: Path
) -> None:
    production = production_at(root)
    scene = root / "asset" / "kitchen" / "frying_pan" / "work" / "modeling" / "frying_pan.mb"

    carried = stamp(production, PAN, MODELING)
    found = stamped_asset(tracker, production, carried, scene, "import it")

    assert found == PAN


@pytest.mark.parametrize(
    ("carried", "scene", "refused"),
    [
        (
            {"piper_production": None, "piper_asset_id": None, "piper_context": None},
            "asset/kitchen/frying_pan/work/modeling/frying_pan.mb",
            "this scene is not sandwich work; import it",
        ),
        (
            {"piper_production": "other", "piper_asset_id": "7701", "piper_context": "modeling"},
            "asset/kitchen/frying_pan/work/modeling/frying_pan.mb",
            "this scene is not sandwich work; import it",
        ),
        (
            {"piper_production": "sandwich", "piper_asset_id": "9", "piper_context": "modeling"},
            "asset/kitchen/frying_pan/work/modeling/frying_pan.mb",
            "this scene is work on an asset sandwich does not have (id 9)",
        ),
        (
            {"piper_production": "sandwich", "piper_asset_id": "7701", "piper_context": "modeling"},
            "elsewhere/frying_pan.mb",
            "not Frying Pan's modeling work at",
        ),
    ],
)
def test_any_other_scene_is_refused_with_the_remedy(
    tracker: Tracker, root: Path, carried: dict[str, str | None], scene: str, refused: str
) -> None:
    with pytest.raises(PiperError, match=re.escape(refused)):
        stamped_asset(tracker, production_at(root), carried, root / scene, "import it")


def test_a_file_that_was_other_work_is_told_what_it_was_and_is(tracker: Tracker) -> None:
    carried = stamp(SANDWICH, PAN, MODELING)
    lookdev = Context(name="lookdev", subject="asset", host="houdini", extension="hipnc")
    pot = replace(PAN, id="7702", name="Sauce Pot", pipe_name="sauce_pot")

    assert restamp_notice(tracker, SANDWICH, carried, pot, lookdev) == (
        "This file was modeling work on Frying Pan, in sandwich.\n\n"
        "It is now lookdev work on Sauce Pot, and has been saved that way."
    )


def test_a_file_from_another_production_is_described_without_trusting_its_id(
    tracker: Tracker,
) -> None:
    carried = {"piper_production": "other", "piper_asset_id": "7701", "piper_context": None}

    assert restamp_notice(tracker, SANDWICH, carried, PAN, MODELING) == (
        "This file was unnamed work on an asset this production does not have, in other.\n\n"
        "It is now modeling work on Frying Pan, and has been saved that way."
    )


def test_a_file_that_carried_nothing_is_new_work_and_told_nothing(tracker: Tracker) -> None:
    carried = {"piper_production": None, "piper_asset_id": None, "piper_context": None}

    assert restamp_notice(tracker, SANDWICH, carried, PAN, MODELING) is None
