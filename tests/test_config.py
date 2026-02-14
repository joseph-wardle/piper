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

    def test_resolve_context_uses_default_show_and_show_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            show_root = root / "groups" / "bobo"
            (show_root / "production" / "shot" / "F_160").mkdir(parents=True)
            scripts_dir = show_root / "pipeline" / "piper" / "scripts"
            scripts_dir.mkdir(parents=True)

            user_config_path = root / "xdg" / "piper" / "config.toml"
            self._write(
                user_config_path,
                f"""
                default_show = "bobo"

                [shows]
                bobo = "{show_root}"

                [goto]
                shot = "{{root}}/production/shot/from_user_{{id}}"
                """,
            )

            show_config_path = show_root / "pipeline" / "piper.toml"
            self._write(
                show_config_path,
                """
                [goto]
                shot = "{root}/production/shot/{id}"

                [scripts]
                dirs = ["scripts"]
                """,
            )

            context = resolve_context(
                None,
                cwd=root,
                environ={},
                user_config_path=user_config_path,
            )

            self.assertEqual(context.show.name, "bobo")
            self.assertEqual(
                context.show.goto_templates["shot"],
                ("{root}/production/shot/{id}",),
            )
            self.assertEqual(
                context.show.script_dirs, (show_root / "pipeline" / "scripts",)
            )

    def test_resolve_context_infers_show_from_cwd(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            show_a = root / "shows" / "a"
            show_b = root / "shows" / "b"
            (show_a / "pipeline").mkdir(parents=True)
            (show_b / "pipeline").mkdir(parents=True)

            user_config_path = root / "config.toml"
            self._write(
                user_config_path,
                f"""
                [shows]
                a = "{show_a}"
                b = "{show_b}"
                """,
            )

            cwd = show_b / "work" / "lighting"
            cwd.mkdir(parents=True)

            context = resolve_context(
                None,
                cwd=cwd,
                environ={},
                user_config_path=user_config_path,
            )

            self.assertEqual(context.show.name, "b")

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

    def test_default_script_dir_falls_back_to_project_scripts(self) -> None:
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

            context = resolve_context(
                None,
                cwd=root,
                environ={},
                user_config_path=user_config_path,
            )

            project_root = Path(__file__).resolve().parents[1]
            self.assertEqual(context.show.script_dirs, (project_root / "scripts",))

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

            show_config_path = show_root / "pipeline" / "piper.toml"
            self._write(
                show_config_path,
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

    def test_cli_show_flag_has_priority_over_environment_show(self) -> None:
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
                [shows]
                a = "{show_a}"
                b = "{show_b}"
                """,
            )

            context = resolve_context(
                "a",
                cwd=root,
                environ={"PIPER_SHOW": "b"},
                user_config_path=user_config_path,
            )
            self.assertEqual(context.show.name, "a")

    def test_environment_script_dirs_override_show_and_user_config(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            show_root = root / "groups" / "bobo"
            show_root.mkdir(parents=True)
            env_scripts_a = root / "env_scripts_a"
            env_scripts_a.mkdir(parents=True)
            env_scripts_b = root / "env_scripts_b"
            env_scripts_b.mkdir(parents=True)

            user_scripts = root / "user_scripts"
            user_scripts.mkdir(parents=True)
            show_scripts = show_root / "pipeline" / "show_scripts"
            show_scripts.mkdir(parents=True)

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
            self._write(
                show_root / "pipeline" / "piper.toml",
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

    def test_environment_goto_overrides_show_user_and_defaults(self) -> None:
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
            self._write(
                show_root / "pipeline" / "piper.toml",
                """
                [goto]
                shot = "{root}/show/{id}"
                """,
            )

            context = resolve_context(
                None,
                cwd=root,
                environ={"PIPER_GOTO_SHOT": "{root}/env/{id}"},
                user_config_path=user_config_path,
            )
            self.assertEqual(context.show.goto_templates["shot"], ("{root}/env/{id}",))

    def test_user_goto_overrides_built_in_defaults(self) -> None:
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
                environment = "{{root}}/custom/set/{{id}}"
                """,
            )

            context = resolve_context(
                None,
                cwd=root,
                environ={},
                user_config_path=user_config_path,
            )
            self.assertEqual(
                context.show.goto_templates["environment"],
                ("{root}/custom/set/{id}",),
            )


if __name__ == "__main__":
    unittest.main()
