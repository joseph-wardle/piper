from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from piper.errors import PathResolutionError
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

    def test_resolve_existing_path_picks_first_existing_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            target = root / "production" / "set" / "Forest_layout"
            target.mkdir(parents=True)

            show = self._make_show(
                root,
                {
                    "environment": (
                        "{root}/production/missing/{id}",
                        "{root}/production/set/{id}",
                    )
                },
            )

            resolved = resolve_existing_path(show, "environment", "Forest_layout")

            self.assertEqual(resolved, target)

    def test_unknown_kind_raises(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            show = self._make_show(root, {"shot": ("{root}/production/shot/{id}",)})

            with self.assertRaises(UnknownKindError):
                resolve_existing_path(show, "asset", "fence_door")

    def test_missing_candidates_raise_resolution_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            show = self._make_show(root, {"shot": ("{root}/production/shot/{id}",)})

            with self.assertRaises(PathResolutionError) as ctx:
                resolve_existing_path(show, "shot", "F_999")

            self.assertEqual(ctx.exception.kind, "shot")
            self.assertEqual(ctx.exception.identifier, "F_999")
            self.assertEqual(len(ctx.exception.candidates), 1)

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


if __name__ == "__main__":
    unittest.main()
