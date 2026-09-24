import json
from pathlib import Path

import pytest

from piper.errors import PiperError
from piper_studio.textures import convert


@pytest.fixture
def export(tmp_path: Path) -> Path:
    """A Painter export of one slot: a colour map, a data map, and the preview beside them."""
    export = tmp_path / "export"
    export.mkdir()
    for name in ("body_BaseColor.1001.png", "body_Normal.1001.png", "body_BaseColor.1001.jpg"):
        (export / name).touch()
    return export


def calls(renderman: Path) -> list[list[str]]:
    try:
        lines = (renderman / "bin" / "calls.json").read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return []
    return [json.loads(line) for line in lines]


def test_colour_maps_become_acescg_half_exr_and_data_maps_tiff_as_exported_beside_the_png(
    export: Path, renderman: Path
) -> None:
    written = convert(export, renderman=renderman)

    assert written == [export / "body_BaseColor.1001.tex", export / "body_Normal.1001.tex"]
    assert calls(renderman) == [
        [
            "--colorconfig",
            str(renderman / "lib" / "ocio" / "ACES-1.3" / "config.ocio"),
            str(export / "body_BaseColor.1001.png"),
            "--colorconvert",
            "sRGB - Texture",
            "ACEScg",
            "-d",
            "half",
            "--compression",
            "zip",
            "--planarconfig",
            "separate",
            "-otex:fileformatname=exr:wrap=clamp:resize=1:prman_options=1",
            str(export / "body_BaseColor.1001.tex"),
        ],
        [
            str(export / "body_Normal.1001.png"),
            "--compression",
            "zip",
            "--planarconfig",
            "separate",
            "-otex:fileformatname=tiff:wrap=clamp:resize=1:prman_options=1",
            str(export / "body_Normal.1001.tex"),
        ],
    ]
    assert sorted(path.name for path in export.iterdir()) == [
        "body_BaseColor.1001.jpg",
        "body_BaseColor.1001.png",
        "body_BaseColor.1001.tex",
        "body_Normal.1001.png",
        "body_Normal.1001.tex",
    ]


def test_a_directory_is_refused_whole_before_anything_is_converted(
    export: Path, renderman: Path, tmp_path: Path
) -> None:
    with pytest.raises(PiperError, match="holds no PNG"):
        convert(tmp_path, renderman=renderman)

    (export / "thumbnail.png").touch()
    with pytest.raises(PiperError, match=r"named <slot>_<map>\.<udim>\.png"):
        convert(export, renderman=renderman)
    (export / "thumbnail.png").unlink()

    (export / "body_Roughness.1001.png").touch()
    with pytest.raises(PiperError) as refusal:
        convert(export, renderman=renderman)
    assert str(refusal.value) == (
        f"cannot convert {export / 'body_Roughness.1001.png'}: 'Roughness' is not a map "
        "Piper knows (BaseColor, Emissive, Metallic, SpecularRoughness, Normal, "
        "Displacement, Presence)"
    )
    (export / "body_Roughness.1001.png").unlink()

    with pytest.raises(PiperError) as refusal:
        convert(export, renderman=tmp_path / "nowhere")
    assert str(refusal.value) == (
        f"cannot convert {export}: RenderMan 27.3 is not installed at {tmp_path / 'nowhere'}; "
        "set RMANTREE to where it is"
    )

    assert calls(renderman) == []


def test_a_conversion_oiiotool_complained_about_is_refused_and_its_texture_removed(
    tmp_path: Path, renderman: Path
) -> None:
    export = tmp_path / "export"
    export.mkdir()
    for name in ("arm_BaseColor.1001.png", "body_broken_Normal.1001.png", "leg_Normal.1001.png"):
        (export / name).touch()
    (export / "leg_Normal.1001.tex").write_text("from an earlier run", encoding="utf-8")

    with pytest.raises(PiperError) as refusal:
        convert(export, renderman=renderman)

    assert str(refusal.value) == (
        f"cannot convert {export / 'body_broken_Normal.1001.png'}: rmanoiiotool failed\n"
        "libpng error: IDAT: Read error: hit end of file"
    )
    # Exactly the textures this run made, in name order: the earlier run's is gone too.
    assert sorted(path.name for path in export.glob("*.tex")) == ["arm_BaseColor.1001.tex"]
