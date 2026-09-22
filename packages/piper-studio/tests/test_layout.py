import os
from pathlib import PurePosixPath

import pytest
from pxr import Sdf

from piper_studio.layout import (
    asset_root,
    slug,
    usable_pipe_name,
    version_directory,
    version_name,
    version_number,
)


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


@pytest.mark.parametrize(
    "pipe_name", ["frying_pan", "3d_printer", "sq010", "The Pan", "", "_pan", "pan_3d"]
)
def test_a_usable_pipe_name_is_a_slug_that_can_name_a_prim(pipe_name: str) -> None:
    assert usable_pipe_name(pipe_name) == (
        slug(pipe_name) == pipe_name and Sdf.Path.IsValidIdentifier(pipe_name)
    )


def test_an_asset_lives_in_its_folder_under_the_production_root() -> None:
    directory = asset_root(PurePosixPath("/production"), "Kitchen", "Frying Pan")

    assert directory == PurePosixPath("/production/asset/kitchen/frying_pan")


def test_no_folder_or_name_reaches_outside_the_root() -> None:
    directory = asset_root(PurePosixPath("/production"), "../../etc", "../passwd")

    assert directory == PurePosixPath("/production/asset/etc/passwd")


@pytest.mark.parametrize(
    ("name", "number"),
    [
        ("v001", 1),
        ("v042", 42),
        ("v1000", 1000),
        ("v0007", 7),
        ("v01", None),
        ("V001", None),
        ("v001a", None),
        ("v\u0661\u0662\u0663", None),
        (".tmp_v001", None),
    ],
)
def test_a_version_is_v_and_at_least_three_digits(name: str, number: int | None) -> None:
    assert version_number(name) == number


def test_a_version_number_is_spelled_with_at_least_three_digits() -> None:
    assert [version_name(number) for number in (1, 42, 1000)] == ["v001", "v042", "v1000"]


@pytest.mark.parametrize(
    ("path", "version"),
    [
        (
            "asset/kitchen/frying_pan/publish/geo/v004/geo.usd",
            "asset/kitchen/frying_pan/publish/geo/v004",
        ),
        (
            "asset/kitchen/frying_pan/publish/geo/v004/src/pan.ma",
            "asset/kitchen/frying_pan/publish/geo/v004",
        ),
        ("asset/kitchen/frying_pan/publish/geo/v004", None),
        ("asset/kitchen/frying_pan/publish/geo/.tmp_1a2b/geo.usd", None),
        ("asset/kitchen/frying_pan/work/geo/v004/geo.usd", None),
        ("../elsewhere/asset/kitchen/frying_pan/publish/geo/v004/geo.usd", None),
    ],
)
def test_a_path_inside_a_version_names_that_version(path: str, version: str | None) -> None:
    root = PurePosixPath("/production/shows/pan")

    found = version_directory(root, PurePosixPath(os.path.normpath(root / path)))

    assert found == (root / version if version else None)
