"""Running ``piper`` from Painter, the one host that reaches an operation out of process.

Painter loads its own USD library at startup, which any ``pxr`` then binds to
and fails, so whatever needs USD runs in the command line's interpreter.
"""

import json
import os
import subprocess

from piper.errors import PiperError
from piper_studio.launch import PYTHON_ENV


def run(*arguments: str) -> str:
    """Run ``piper`` with ``arguments`` and return what it printed; refuse with what it refused."""
    python = os.environ.get(PYTHON_ENV)
    if not python:
        raise PiperError(
            f"{PYTHON_ENV} is not set, so Painter cannot find piper; "
            "start Painter with `piper launch painter`"
        )
    # Painter's own path holds the plugin's packages; the command has its own.
    environment = {name: value for name, value in os.environ.items() if name != "PYTHONPATH"}
    ran = subprocess.run(
        [python, "-m", "piper_cli", *arguments],
        capture_output=True,
        text=True,
        env=environment,
        check=False,
        # Windows would otherwise open a console window for it.
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    if ran.returncode != 0:
        said = ran.stderr.strip() or ran.stdout.strip() or f"exited with status {ran.returncode}"
        raise PiperError(said.removeprefix("piper: "))
    return ran.stdout.strip()


def query(*arguments: str) -> dict[str, object]:
    """Run ``piper`` for its JSON result."""
    return json.loads(run(*arguments, "--json"))
