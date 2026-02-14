from __future__ import annotations

import io
import json
import tempfile
import textwrap
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from piper.cli import main


class CLITests(unittest.TestCase):
    def _write(self, path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(content).strip() + "\n", encoding="utf-8")

    def _invoke(
        self,
        argv: list[str],
        *,
        env: dict[str, str],
        cwd: Path,
    ) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()

        with redirect_stdout(stdout), redirect_stderr(stderr):
            exit_code = main(argv, environ=env, cwd=cwd)

        return exit_code, stdout.getvalue(), stderr.getvalue()

    def _build_test_layout(self, root: Path) -> tuple[dict[str, str], Path, Path, Path]:
        xdg_home = root / "xdg"
        show_root = root / "groups" / "bobo"

        shot_dir = show_root / "production" / "shot" / "F_160"
        shot_dir.mkdir(parents=True)
        (show_root / "production" / "set" / "Forest_layout").mkdir(parents=True)

        scripts_dir = show_root / "pipeline" / "piper" / "scripts"
        scripts_dir.mkdir(parents=True)

        user_config_path = xdg_home / "piper" / "config.toml"
        self._write(
            user_config_path,
            f"""
            default_show = "bobo"

            [shows]
            bobo = "{show_root}"
            """,
        )

        show_config_path = show_root / "pipeline" / "piper.toml"
        self._write(
            show_config_path,
            f"""
            [scripts]
            dirs = ["{scripts_dir}"]

            [goto]
            shot = "{{root}}/production/shot/{{id}}"
            asset = "{{root}}/production/asset/{{id}}"
            environment = "{{root}}/production/set/{{id}}"
            """,
        )

        workdir = root / "work"
        workdir.mkdir(parents=True)

        env = {"XDG_CONFIG_HOME": str(xdg_home)}
        return env, workdir, show_root, scripts_dir

    def test_path_and_goto_print_resolved_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            env, workdir, show_root, _scripts_dir = self._build_test_layout(root)

            expected = str(show_root / "production" / "shot" / "F_160")

            code, out, err = self._invoke(
                ["path", "shot", "F_160"],
                env=env,
                cwd=workdir,
            )
            self.assertEqual(code, 0)
            self.assertEqual(out.strip(), expected)
            self.assertEqual(err, "")

            code, out, err = self._invoke(
                ["goto", "shot", "F_160"],
                env=env,
                cwd=workdir,
            )
            self.assertEqual(code, 0)
            self.assertEqual(out.strip(), expected)
            self.assertEqual(err, "")

    def test_path_output_is_machine_clean(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            env, workdir, show_root, _scripts_dir = self._build_test_layout(root)

            expected = f"{show_root / 'production' / 'shot' / 'F_160'}\n"
            code, out, err = self._invoke(
                ["path", "shot", "F_160"], env=env, cwd=workdir
            )

            self.assertEqual(code, 0)
            self.assertEqual(out, expected)
            self.assertEqual(err, "")

    def test_run_executes_script_in_invocation_cwd(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            env, workdir, _show_root, scripts_dir = self._build_test_layout(root)

            script_path = scripts_dir / "rm_generate_report.py"
            self._write(
                script_path,
                """
                import json
                import pathlib
                import sys

                payload = {
                    "cwd": str(pathlib.Path.cwd()),
                    "args": sys.argv[1:],
                }
                pathlib.Path("run_output.json").write_text(json.dumps(payload), encoding="utf-8")
                """,
            )

            code, out, err = self._invoke(
                ["run", "rm_generate_report", "--", "--today"],
                env=env,
                cwd=workdir,
            )
            self.assertEqual(code, 0)
            self.assertEqual(out, "")
            self.assertEqual(err, "")

            payload = json.loads(
                (workdir / "run_output.json").read_text(encoding="utf-8")
            )
            self.assertEqual(payload["cwd"], str(workdir))
            self.assertEqual(payload["args"], ["--today"])

    def test_run_list_prints_sorted_names(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            env, workdir, _show_root, scripts_dir = self._build_test_layout(root)

            self._write(scripts_dir / "zeta.py", "print('zeta')")
            self._write(scripts_dir / "alpha", "print('alpha')")
            self._write(scripts_dir / "notes.txt", "no")

            code, out, err = self._invoke(["run", "--list"], env=env, cwd=workdir)
            self.assertEqual(code, 0)
            self.assertEqual(out.splitlines(), ["alpha", "zeta"])
            self.assertEqual(err, "")

    def test_init_prints_shell_snippet(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            env, workdir, _show_root, _scripts_dir = self._build_test_layout(root)

            code, out, err = self._invoke(["init", "bash"], env=env, cwd=workdir)
            self.assertEqual(code, 0)
            self.assertIn("# piper shell integration (bash)", out)
            self.assertIn("command piper path", out)
            self.assertIn("builtin cd --", out)
            self.assertEqual(err, "")

            code, out, err = self._invoke(["init", "zsh"], env=env, cwd=workdir)
            self.assertEqual(code, 0)
            self.assertIn("# piper shell integration (zsh)", out)
            self.assertIn("command piper path", out)
            self.assertIn("builtin cd --", out)
            self.assertEqual(err, "")

    def test_doctor_returns_non_zero_when_show_resolution_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            env = {"XDG_CONFIG_HOME": str(root / "xdg")}

            code, out, err = self._invoke(["doctor"], env=env, cwd=root)
            self.assertEqual(code, 1)
            self.assertIn("show resolution", out)
            self.assertEqual(err, "")


if __name__ == "__main__":
    unittest.main()
