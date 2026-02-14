from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from piper.errors import ConfigError, PathResolutionError
from piper.models import ShowConfig
from piper.resolvers.paths import UnknownKindError, resolve_existing_path


class PathResolverTests(unittest.TestCase):
    def _make_show(
        self, root: Path, templates: dict[str, tuple[str, ...]]
    ) -> ShowConfig:
        return ShowConfig(
            name="bobo",
            root=root,
            goto_templates=templates,
            script_dirs=(),
            user_config_path=root / "user.toml",
            show_config_path=root / "show.toml",
        )

    def test_resolve_existing_path_picks_first_existing_candidate_per_kind(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            shot_target = root / "production" / "shot" / "F_160"
            shot_target.mkdir(parents=True)
            asset_target = root / "production" / "asset" / "fence_door"
            asset_target.mkdir(parents=True)
            environment_target = root / "production" / "set" / "Forest_layout"
            environment_target.mkdir(parents=True)

            show = self._make_show(
                root,
                {
                    "shot": (
                        "{root}/production/shot/missing/{id}",
                        "{root}/production/shot/{id}",
                    ),
                    "asset": (
                        "{root}/production/asset/missing/{id}",
                        "{root}/production/asset/{id}",
                    ),
                    "environment": (
                        "{root}/production/missing/{id}",
                        "{root}/production/set/{id}",
                    ),
                },
            )

            self.assertEqual(
                resolve_existing_path(show, "shot", "F_160"),
                shot_target,
            )
            self.assertEqual(
                resolve_existing_path(show, "asset", "fence_door"),
                asset_target,
            )
            self.assertEqual(
                resolve_existing_path(show, "environment", "Forest_layout"),
                environment_target,
            )

    def test_unknown_kind_raises(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            show = self._make_show(root, {"shot": ("{root}/production/shot/{id}",)})

            with self.assertRaises(UnknownKindError):
                resolve_existing_path(show, "asset", "fence_door")

    def test_glob_template_resolves_nested_asset(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            target = (
                root / "production" / "asset" / "environment" / "Forest" / "fence_door"
            )
            target.mkdir(parents=True)

            show = self._make_show(
                root,
                {"asset": ("{root}/production/asset/environment/*/{id}",)},
            )

            resolved = resolve_existing_path(show, "asset", "fence_door")
            self.assertEqual(resolved, target)

    def test_glob_template_is_deterministic_with_multiple_matches(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            first = root / "production" / "asset" / "rigged" / "bee"
            second = root / "production" / "asset" / "test" / "bee"
            first.mkdir(parents=True)
            second.mkdir(parents=True)

            show = self._make_show(
                root,
                {"asset": ("{root}/production/asset/*/{id}",)},
            )

            resolved = resolve_existing_path(show, "asset", "bee")
            self.assertEqual(resolved, first)

    def test_kind_is_dynamic_from_config(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            target = root / "production" / "sequence" / "A"
            target.mkdir(parents=True)

            show = self._make_show(
                root,
                {"sequence": ("{root}/production/sequence/{id}",)},
            )

            resolved = resolve_existing_path(show, "sequence", "A")
            self.assertEqual(resolved, target)

    def test_invalid_template_placeholder_raises_clear_config_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            show = self._make_show(
                root,
                {"shot": ("{root}/production/{unknown}/{id}",)},
            )

            with self.assertRaises(ConfigError) as ctx:
                resolve_existing_path(show, "shot", "F_160")

            self.assertIn("unknown placeholder", str(ctx.exception))

    def test_missing_path_error_includes_attempted_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            show = self._make_show(
                root,
                {
                    "shot": (
                        "{root}/production/shot/{id}",
                        "{root}/production/shot/archive/{id}",
                    )
                },
            )

            with self.assertRaises(PathResolutionError) as ctx:
                resolve_existing_path(show, "shot", "MISSING")

            first = root / "production" / "shot" / "MISSING"
            second = root / "production" / "shot" / "archive" / "MISSING"
            self.assertEqual(ctx.exception.kind, "shot")
            self.assertEqual(ctx.exception.identifier, "MISSING")
            self.assertEqual(ctx.exception.candidates, (first, second))
            self.assertIn(str(first), str(ctx.exception))
            self.assertIn(str(second), str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
