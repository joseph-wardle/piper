from __future__ import annotations

from piper.commands.base import Command
from piper.commands.doctor import DoctorCommand
from piper.commands.goto import GotoCommand
from piper.commands.init import InitCommand
from piper.commands.path import PathCommand
from piper.commands.run import RunCommand


def built_in_commands() -> list[Command]:
    return [
        PathCommand(),
        GotoCommand(),
        RunCommand(),
        InitCommand(),
        DoctorCommand(),
    ]


__all__ = ["built_in_commands", "Command"]
