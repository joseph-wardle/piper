from pathlib import PurePosixPath

import pytest

from piper_studio.layout import asset_root, slug


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("Frying Pan", "frying_pan"),
        ("Mr. Yoon's Shop Exterior", "mr_yoons_shop_exterior"),
        ("Mr. Yoon\u2019s Shop", "mr_yoons_shop"),
        ("  --Hero   Prop--  ", "hero_prop"),
        ("SQ010 / Kitchen", "sq010_kitchen"),
        ("!!!", ""),
        # A folder is stored as its slug and slugged again when read back.
        ("mr_yoons_shop", "mr_yoons_shop"),
    ],
)
def test_a_slug_keeps_letters_and_digits_and_joins_the_rest(name: str, expected: str) -> None:
    assert slug(name) == expected


def test_an_asset_lives_in_its_folder_under_the_production_root() -> None:
    directory = asset_root(PurePosixPath("/production"), "Kitchen", "Frying Pan")

    assert directory == PurePosixPath("/production/asset/kitchen/frying_pan")


def test_no_folder_or_name_reaches_outside_the_root() -> None:
    directory = asset_root(PurePosixPath("/production"), "../../etc", "../passwd")

    assert directory == PurePosixPath("/production/asset/etc/passwd")
