from __future__ import annotations

from argparse import ArgumentParser, Namespace
from collections.abc import Mapping
from pathlib import Path

from piper.config import get_user_config_path, load_user_config, resolve_context
from piper.errors import ConfigError, PiperError
from piper.models import ResolvedContext


def _status(level: str, message: str) -> str:
    return f"{level:<5} {message}"


class DoctorCommand:
    name = "doctor"
    help = "Validate config, show resolution, shell integration, and scripts."
    requires_context = False

    def configure(self, parser: ArgumentParser) -> None:
        del parser

    def run(
        self,
        args: Namespace,
        *,
        context: ResolvedContext | None,
        environ: Mapping[str, str],
        cwd: Path,
    ) -> int:
        del context

        blocking_errors = False
        user_config_path = get_user_config_path(environ)

        if user_config_path.exists():
            print(_status("OK", f"user config found: {user_config_path}"))
        else:
            print(_status("WARN", f"user config missing: {user_config_path}"))

        try:
            load_user_config(user_config_path)
            print(_status("OK", "user config parse: valid"))
        except ConfigError as exc:
            print(_status("ERROR", f"user config parse: {exc}"))
            blocking_errors = True

        resolved_context: ResolvedContext | None = None
        try:
            resolved_context = resolve_context(
                args.show,
                cwd=cwd,
                environ=environ,
                user_config_path=user_config_path,
            )
            print(
                _status(
                    "OK",
                    f"show resolved: {resolved_context.show.name} ({resolved_context.show.root})",
                )
            )
        except PiperError as exc:
            print(_status("ERROR", f"show resolution: {exc}"))
            blocking_errors = True

        if resolved_context is not None:
            show_config_path = resolved_context.show.show_config_path
            if show_config_path.exists():
                print(_status("OK", f"show config found: {show_config_path}"))
            else:
                print(_status("WARN", f"show config missing: {show_config_path}"))

            if resolved_context.show.goto_templates:
                print(
                    _status(
                        "OK",
                        f"goto templates loaded: {', '.join(sorted(resolved_context.show.goto_templates))}",
                    )
                )
            else:
                print(_status("ERROR", "goto templates missing"))
                blocking_errors = True

            if not resolved_context.show.script_dirs:
                print(_status("WARN", "no script directories configured"))
            else:
                for script_dir in resolved_context.show.script_dirs:
                    if script_dir.is_dir():
                        print(_status("OK", f"script dir exists: {script_dir}"))
                    else:
                        print(_status("WARN", f"script dir missing: {script_dir}"))

        if environ.get("PIPER_SHELL_INTEGRATION") == "1":
            print(_status("OK", "shell integration marker present"))
        else:
            print(
                _status(
                    "WARN",
                    "shell integration marker missing (PIPER_SHELL_INTEGRATION=1)",
                )
            )

        return 1 if blocking_errors else 0
