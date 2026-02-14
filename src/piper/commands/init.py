from __future__ import annotations

from argparse import ArgumentParser, Namespace
from collections.abc import Mapping
from pathlib import Path

from piper.models import ResolvedContext

_INIT_TEMPLATE = """# piper shell integration (__SHELL__)
export PIPER_SHELL_INTEGRATION=1

_piper_goto() {
  local dest
  dest="$(command piper path "$@")" || return $?
  builtin cd -- "$dest" || return $?
}

piper() {
  if [ "$1" = "goto" ]; then
    shift
    _piper_goto "$@"
    return $?
  else
    command piper "$@"
  fi
}
"""


class InitCommand:
    name = "init"
    help = "Print shell integration snippet for bash/zsh."
    requires_context = False

    def configure(self, parser: ArgumentParser) -> None:
        parser.add_argument("shell", choices=["bash", "zsh"])

    def run(
        self,
        args: Namespace,
        *,
        context: ResolvedContext | None,
        environ: Mapping[str, str],
        cwd: Path,
    ) -> int:
        del context, environ, cwd
        print(_INIT_TEMPLATE.replace("__SHELL__", args.shell))
        return 0
