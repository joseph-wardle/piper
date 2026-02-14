from __future__ import annotations

import os
import tempfile
import textwrap
import unittest
from pathlib import Path

from piper.config import resolve_context
from piper.errors import ShowResolutionError


class ConfigResolutionTests(unittest.TestCase):
    def _write(self, path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(content).strip() + "\n", encoding="utf-8")

    def test_show_selection_precedence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            show_a = root / "groups" / "a"
            show_b = root / "groups" / "b"
            show_a.mkdir(parents=True)
            show_b.mkdir(parents=True)

            user_config_path = root / "config.toml"
            self._write(
                user_config_path,
                f"""
                default_show = "a"

                [shows]
                a = "{show_a}"
                b = "{show_b}"
                """,
            )

            cwd_in_b = show_b / "work" / "lighting"
            cwd_in_b.mkdir(parents=True)
            cwd_outside = root / "work" / "outside"
            cwd_outside.mkdir(parents=True)

            context = resolve_context(
                "a",
                cwd=cwd_in_b,
                environ={"PIPER_SHOW": "b"},
                user_config_path=user_config_path,
            )
            self.assertEqual(context.show.name, "a")

            context = resolve_context(
                None,
                cwd=cwd_in_b,
                environ={"PIPER_SHOW": "a"},
                user_config_path=user_config_path,
            )
            self.assertEqual(context.show.name, "a")

            context = resolve_context(
                None,
                cwd=cwd_in_b,
                environ={},
                user_config_path=user_config_path,
            )
            self.assertEqual(context.show.name, "b")

            context = resolve_context(
                None,
                cwd=cwd_outside,
                environ={},
                user_config_path=user_config_path,
            )
            self.assertEqual(context.show.name, "a")

    def test_unknown_show_raises(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            show_root = root / "groups" / "bobo"
            show_root.mkdir(parents=True)

            user_config_path = root / "config.toml"
            self._write(
                user_config_path,
                f"""
                [shows]
                bobo = "{show_root}"
                """,
            )

            with self.assertRaises(ShowResolutionError):
                resolve_context(
                    "unknown",
                    cwd=root,
                    environ={},
                    user_config_path=user_config_path,
                )

    def test_script_dir_precedence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            show_root = root / "groups" / "bobo"
            show_root.mkdir(parents=True)
            user_scripts = root / "user_scripts"
            show_scripts = root / "show_scripts"
            env_scripts_a = root / "env_scripts_a"
            env_scripts_b = root / "env_scripts_b"
            user_scripts.mkdir(parents=True)
            show_scripts.mkdir(parents=True)
            env_scripts_a.mkdir(parents=True)
            env_scripts_b.mkdir(parents=True)

            user_config_path = root / "config.toml"
            self._write(
                user_config_path,
                f"""
                default_show = "bobo"

                [shows]
                bobo = "{show_root}"

                [scripts]
                dirs = ["{user_scripts}"]
                """,
            )

            show_config_path = show_root / "pipeline" / "piper.toml"
            self._write(
                show_config_path,
                f"""
                [scripts]
                dirs = ["{show_scripts}"]
                """,
            )

            context = resolve_context(
                None,
                cwd=root,
                environ={
                    "PIPER_SCRIPT_DIRS": os.pathsep.join(
                        [str(env_scripts_a), str(env_scripts_b), str(env_scripts_a)]
                    )
                },
                user_config_path=user_config_path,
            )
            self.assertEqual(context.show.script_dirs, (env_scripts_a, env_scripts_b))

            context = resolve_context(
                None,
                cwd=root,
                environ={},
                user_config_path=user_config_path,
            )
            self.assertEqual(context.show.script_dirs, (show_scripts,))

            self._write(
                show_config_path, '[goto]\nshot = "{root}/production/shot/{id}"'
            )
            context = resolve_context(
                None,
                cwd=root,
                environ={},
                user_config_path=user_config_path,
            )
            self.assertEqual(context.show.script_dirs, (user_scripts,))

            self._write(
                user_config_path,
                f"""
                default_show = "bobo"

                [shows]
                bobo = "{show_root}"
                """,
            )
            context = resolve_context(
                None,
                cwd=root,
                environ={},
                user_config_path=user_config_path,
            )
            project_root = Path(__file__).resolve().parents[1]
            self.assertEqual(context.show.script_dirs, (project_root / "scripts",))

    def test_goto_template_precedence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            show_root = root / "groups" / "bobo"
            show_root.mkdir(parents=True)

            user_config_path = root / "config.toml"
            self._write(
                user_config_path,
                f"""
                default_show = "bobo"

                [shows]
                bobo = "{show_root}"

                [goto]
                shot = "{{root}}/user/{{id}}"
                """,
            )

            show_config_path = show_root / "pipeline" / "piper.toml"
            self._write(show_config_path, '[goto]\nshot = "{root}/show/{id}"')

            context = resolve_context(
                None,
                cwd=root,
                environ={"PIPER_GOTO_SHOT": "{root}/env/{id}"},
                user_config_path=user_config_path,
            )
            self.assertEqual(context.show.goto_templates["shot"], ("{root}/env/{id}",))

            context = resolve_context(
                None,
                cwd=root,
                environ={},
                user_config_path=user_config_path,
            )
            self.assertEqual(
                context.show.goto_templates["shot"],
                ("{root}/show/{id}",),
            )

            self._write(show_config_path, "")
            context = resolve_context(
                None,
                cwd=root,
                environ={},
                user_config_path=user_config_path,
            )
            self.assertEqual(context.show.goto_templates["shot"], ("{root}/user/{id}",))

            self._write(
                user_config_path,
                f"""
                default_show = "bobo"

                [shows]
                bobo = "{show_root}"
                """,
            )
            context = resolve_context(
                None,
                cwd=root,
                environ={},
                user_config_path=user_config_path,
            )
            self.assertEqual(
                context.show.goto_templates["shot"],
                ("{root}/production/shot/{id}",),
            )

    def test_show_config_keeps_array_templates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            show_root = root / "groups" / "bobo"
            show_root.mkdir(parents=True)

            user_config_path = root / "config.toml"
            self._write(
                user_config_path,
                f"""
                default_show = "bobo"

                [shows]
                bobo = "{show_root}"
                """,
            )

            self._write(
                show_root / "pipeline" / "piper.toml",
                """
                [goto]
                asset = [
                  "{root}/production/asset/{id}",
                  "{root}/production/asset/*/{id}",
                  "{root}/production/asset/*/*/{id}",
                ]
                """,
            )

            context = resolve_context(
                None,
                cwd=root,
                environ={},
                user_config_path=user_config_path,
            )
            self.assertEqual(
                context.show.goto_templates["asset"],
                (
                    "{root}/production/asset/{id}",
                    "{root}/production/asset/*/{id}",
                    "{root}/production/asset/*/*/{id}",
                ),
            )


if __name__ == "__main__":
    unittest.main()
