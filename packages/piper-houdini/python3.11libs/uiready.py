"""Opens the work ``piper open`` named, once Houdini's interface is up."""

import os

work = os.environ.pop("PIPER_OPEN", "")
if work:
    from piper_houdini import ui

    ui.open_launched_work(*work.split())
