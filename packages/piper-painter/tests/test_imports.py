import subprocess
import sys

# What Painter imports, and Painter can load no USD and has no `substance_painter` outside itself.
_INSIDE_PAINTER = """
import sys
sys.modules["pxr"] = None
import piper_studio.context, piper_studio.layout, piper_studio.launch, piper_studio.production
import piper_studio.profile, piper_studio.storage, piper_studio.textures, piper_studio.tracker
import piper_studio.work
import piper_painter.command, piper_painter.export
"""


def test_what_painter_imports_needs_no_usd() -> None:
    ran = subprocess.run(
        [sys.executable, "-c", _INSIDE_PAINTER], capture_output=True, text=True, check=False
    )

    assert ran.returncode == 0, ran.stderr
