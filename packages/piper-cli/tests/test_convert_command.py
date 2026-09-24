import sys
from pathlib import Path

import pytest

from piper_cli import main as cli

_STAND_IN = f"""\
#!{sys.executable}
import sys
from pathlib import Path
Path(sys.argv[-1]).touch()
"""


def test_convert_needs_no_production_and_finds_renderman_through_rmantree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    oiiotool = tmp_path / "RenderManProServer" / "bin" / "rmanoiiotool"
    oiiotool.parent.mkdir(parents=True)
    oiiotool.write_text(_STAND_IN, encoding="utf-8")
    oiiotool.chmod(0o755)
    monkeypatch.setenv("RMANTREE", str(oiiotool.parents[1]))
    export = tmp_path / "export"
    export.mkdir()
    (export / "body_BaseColor.1001.png").touch()
    (export / "body_Normal.1001.png").touch()

    assert cli.main(("convert", str(export))) == 0

    assert capsys.readouterr().out == f"Converted 2 textures in {export}\n"
    assert (export / "body_BaseColor.1001.tex").is_file()
    assert (export / "body_Normal.1001.tex").is_file()
