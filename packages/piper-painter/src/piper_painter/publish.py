"""Publishing texturing work from Painter: the export, and the command line that installs it."""

from pathlib import Path

from substance_painter import export as painter_export
from substance_painter import project, textureset
from substance_painter.exception import ProjectError

from piper.errors import PiperError
from piper.tracker import Asset, Tracker
from piper_painter import command, export
from piper_painter.work import open_file, scene_stamp
from piper_studio import textures
from piper_studio.production import Production
from piper_studio.work import stamped_asset

_REMEDY = "open the asset's work with Piper: Open Work…"


def scene_asset(tracker: Tracker, production: Production) -> Asset:
    """The asset the open project is work on, refusing a project that is not that work's file."""
    if not project.is_open():
        raise PiperError(f"no project is open; {_REMEDY}")
    return stamped_asset(tracker, production, scene_stamp(), scene_file(), _REMEDY)


def scene_file() -> Path:
    """The open project's file, refusing a project never saved."""
    path = open_file()
    if path is None:
        raise PiperError(f"this project has never been saved, so it is no asset's work; {_REMEDY}")
    return path


def texture_sets() -> list[str]:
    """The project's texture sets, each a slot of the model."""
    return [texture_set.name() for texture_set in textureset.all_texture_sets()]


def export_textures(directory: Path, sets: list[str]) -> list[str]:
    """Export every map of every set into ``directory``, the PNGs then the previews.

    Returns what Painter warned about.
    """
    warnings = []
    for preview in (False, True):
        try:
            result = painter_export.export_project_textures(
                export.config(directory, sets, preview=preview)
            )
        except (ProjectError, ValueError) as exc:
            raise PiperError(f"Painter could not export the textures ({exc})") from exc
        if result.status == painter_export.ExportStatus.Warning:
            warnings.append(str(result.message).strip())
        elif result.status != painter_export.ExportStatus.Success:
            raise PiperError(f"Painter did not export the textures ({result.message})")
    # Painter reports success when a stack has no channel for a map, and exports nothing.
    missing = export.missing_maps(directory, sets)
    if missing:
        warnings.append(
            "No map was exported for these texture sets; Painter exports a map only from a "
            "stack with its channel:\n"
            + "\n".join(f"{name}: {', '.join(lacking)}" for name, lacking in missing.items())
        )
    return warnings


def publish_work(asset: Asset, directory: Path, scene: Path) -> str:
    """Publish the export in ``directory`` as ``asset``'s textures, with ``scene`` as its source."""
    return command.run(
        "publish", asset.name, textures.PRODUCT, str(directory), "--source", str(scene)
    )
