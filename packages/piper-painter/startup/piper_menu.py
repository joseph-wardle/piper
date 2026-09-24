"""Loads Piper's menu items when Painter starts, from the ``startup`` folder Painter reads."""

from piper_painter import ui


def start_plugin() -> None:
    ui.start()


def close_plugin() -> None:
    ui.stop()
