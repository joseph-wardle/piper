import sys
from pathlib import Path

import pytest

# Stands in for rmanoiiotool: records its arguments and writes the texture asked
# for. A "broken" PNG gets what a truncated one gets from the real tool: a libpng
# error on stderr, a texture written anyway, and exit 0.
_STAND_IN = f"""\
#!{sys.executable}
import json, sys
from pathlib import Path
with Path(__file__).with_name("calls.json").open("a") as log:
    log.write(json.dumps(sys.argv[1:]) + "\\n")
Path(sys.argv[-1]).touch()
if any("broken" in argument for argument in sys.argv):
    print("libpng error: IDAT: Read error: hit end of file", file=sys.stderr)
"""


@pytest.fixture
def renderman(tmp_path: Path) -> Path:
    """A RenderMan install whose oiiotool is the stand-in."""
    install = tmp_path / "RenderManProServer"
    oiiotool = install / "bin" / "rmanoiiotool"
    oiiotool.parent.mkdir(parents=True)
    oiiotool.write_text(_STAND_IN, encoding="utf-8")
    oiiotool.chmod(0o755)
    return install
