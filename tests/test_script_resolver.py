from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from piper.errors import ScriptResolutionError
from piper.models import ShowConfig
from piper.resolvers.scripts import (
    list_available_scripts,
    resolve_script_path,
    validate_script_name,
)


class ScriptResolverTests(unittest.TestCase):
    def _make_show(self, root: Path, script_dirs: tuple[Path, ...]) -> ShowConfig:
        return ShowConfig(
            name="bobo",
            root=root,
            goto_templates={},
            script_dirs=script_dirs,
            user_config_path=root / "user.toml",
            show_config_path=root / "show.toml",
        )

    def test_validate_script_name_rejects_paths(self) -> None:
        with self.assertRaises(ScriptResolutionError):
            validate_script_name("../bad")

        with self.assertRaises(ScriptResolutionError):
            validate_script_name("folder/script")

        with self.assertRaises(ScriptResolutionError):
            validate_script_name(r"folder\\script")

        with self.assertRaises(ScriptResolutionError):
            validate_script_name("/tmp/script")

        with self.assertRaises(ScriptResolutionError):
            validate_script_name(r"C:\\temp\\script")

    def test_resolve_script_path_supports_py_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            scripts = root / "scripts"
            scripts.mkdir(parents=True)
            target = scripts / "rm_generate_report.py"
            target.write_text("print('ok')\n", encoding="utf-8")

            show = self._make_show(root, (scripts,))

            resolved = resolve_script_path(show, "rm_generate_report")
            self.assertEqual(resolved, target)

    def test_resolve_script_path_prefers_exact_name_over_py_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            scripts = root / "scripts"
            scripts.mkdir(parents=True)

            exact = scripts / "rm_generate_report"
            exact.write_text("print('exact')\n", encoding="utf-8")
            fallback = scripts / "rm_generate_report.py"
            fallback.write_text("print('py')\n", encoding="utf-8")

            show = self._make_show(root, (scripts,))
            resolved = resolve_script_path(show, "rm_generate_report")

            self.assertEqual(resolved, exact)

    def test_list_available_scripts_is_sorted_unique(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            a = root / "a"
            b = root / "b"
            a.mkdir(parents=True)
            b.mkdir(parents=True)

            (a / "alpha.py").write_text("", encoding="utf-8")
            (a / "gamma").write_text("", encoding="utf-8")
            (b / "alpha").write_text("", encoding="utf-8")
            (b / "beta.py").write_text("", encoding="utf-8")
            (b / "notes.txt").write_text("", encoding="utf-8")

            show = self._make_show(root, (a, b))

            names = list_available_scripts(show)
            self.assertEqual(names, ["alpha", "beta", "gamma"])


if __name__ == "__main__":
    unittest.main()
