"""Opens the work ``piper open`` named, once Houdini's interface is up."""

import os

from piper_studio.launch import WORK_ENV

# Popped, so that a Houdini started from this one does not open it again.
work = os.environ.pop(WORK_ENV, "")
if work:
    from piper_houdini import ui

    ui.open_launched_work(*work.split())
