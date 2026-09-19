import xmlrpc.client
from pathlib import PurePosixPath
from typing import Any

import pytest
import shotgun_api3

from piper.errors import RegistryError
from piper.tracker import Asset
from piper_shotgrid.registry import ShotGridRegistry

PAN = Asset(id="1234", name="Frying Pan", type="Prop", folder="kitchen", pipe_name="frying_pan")
PATH = PurePosixPath("/production/asset/kitchen/frying_pan/publish/geo/v004/geo.usd")


class CreateFails:
    """Stands in for the site: it holds no record yet, and the create fails."""

    def __init__(self, failure: Exception) -> None:
        self.failure = failure

    def find_one(self, entity_type: str, filters: list[Any], fields: list[str]) -> None:
        return None

    def create(self, entity_type: str, data: dict[str, Any], fields: list[str]) -> None:
        raise self.failure


@pytest.mark.parametrize(
    ("failure", "reported"),
    [
        pytest.param(
            shotgun_api3.Fault("API create() CRUD ERROR #6"),
            "refused to register geo v004 of 'Frying Pan' in project 782",
            id="site-refused",
        ),
        pytest.param(
            TimeoutError("timed out"),
            "registering geo v004 of 'Frying Pan' in project 782 on https://sandwich.invalid "
            "may or may not have succeeded",
            id="no-answer",
        ),
        pytest.param(
            xmlrpc.client.ProtocolError("sandwich.invalid", 504, "Gateway Timeout", {}),
            "may or may not have succeeded",
            id="gateway",
        ),
    ],
)
def test_a_failed_create_says_whether_the_record_may_exist(
    failure: Exception, reported: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = ShotGridRegistry(
        site="https://sandwich.invalid",
        script="sandwich_pipeline",
        key="not-a-real-key",
        project=782,
    )
    monkeypatch.setattr(registry, "_shotgrid", CreateFails(failure))

    with pytest.raises(RegistryError, match=reported):
        registry.register(PAN, product="geo", version=4, path=PATH)
