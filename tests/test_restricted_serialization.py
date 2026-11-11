from __future__ import annotations

import json
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from apscheduler import DeserializationError
from apscheduler.serializers.json import JSONSerializer
from apscheduler.triggers.combining import AndTrigger, OrTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

import sideeffect_types
from sideeffect_types import AllowedHost, EvilType

MAGIC_KEY = "_apscheduler_json"
EVIL_REF = "sideeffect_types:EvilType"
HOST_REF = "sideeffect_types:AllowedHost"
UNIMPORTED_REF = "unimported_evil_module:UnimportedEvil"


@pytest.fixture(autouse=True)
def clear_construction_log():
    sideeffect_types.construction_log.clear()
    yield
    sideeffect_types.construction_log.clear()


def make_marker(ref: str, state: object) -> str:
    return json.dumps({MAGIC_KEY: [ref, state]})


@pytest.fixture
def restricted_serializer() -> JSONSerializer:
    return JSONSerializer(restricted=True, allowed_types=(AllowedHost,))


def test_unregistered_type_is_rejected_without_side_effects(
    restricted_serializer: JSONSerializer,
) -> None:
    payload = make_marker(EVIL_REF, {}).encode("utf-8")
    with pytest.raises(DeserializationError, match="not in the serializer"):
        restricted_serializer.deserialize(payload)

    # Neither the class nor its state hook ran.
    assert sideeffect_types.construction_log == []


def test_unregistered_module_is_never_imported(
    restricted_serializer: JSONSerializer,
) -> None:
    sys.modules.pop("unimported_evil_module", None)
    payload = make_marker(UNIMPORTED_REF, {}).encode("utf-8")
    with pytest.raises(DeserializationError):
        restricted_serializer.deserialize(payload)

    assert "unimported_evil_module" not in sys.modules
    assert sideeffect_types.construction_log == []


def test_nested_unregistered_type_cannot_bypass(
    restricted_serializer: JSONSerializer,
) -> None:
    # An object marker nested inside another marker's state. JSON object hooks run
    # bottom-up, so the inner reference is resolved first and must be rejected.
    payload = json.dumps(
        {
            MAGIC_KEY: [
                HOST_REF,
                {"child": {MAGIC_KEY: [EVIL_REF, {}]}},
            ]
        }
    ).encode("utf-8")
    with pytest.raises(DeserializationError, match="EvilType"):
        restricted_serializer.deserialize(payload)

    assert sideeffect_types.construction_log == []


def test_nested_unregistered_type_inside_trusted_trigger(
    restricted_serializer: JSONSerializer,
) -> None:
    # A tampered AndTrigger whose inner trigger points at an unregistered type. The
    # context-wide registry must reject it while AndTrigger.__setstate__ runs.
    serializer = JSONSerializer(
        restricted=True, allowed_types=(AllowedHost, AndTrigger, DateTrigger)
    )
    and_trigger = AndTrigger(
        [DateTrigger(datetime(2020, 1, 1, tzinfo=ZoneInfo("UTC")))]
    )
    payload = json.loads(serializer.serialize(and_trigger))
    inner_ref = payload[MAGIC_KEY][1]["triggers"][0][0]
    payload[MAGIC_KEY][1]["triggers"][0][0] = EVIL_REF
    assert inner_ref != EVIL_REF

    with pytest.raises(DeserializationError, match="EvilType"):
        serializer.deserialize(json.dumps(payload).encode("utf-8"))

    assert sideeffect_types.construction_log == []


def test_registered_type_is_restored(
    restricted_serializer: JSONSerializer,
) -> None:
    payload = restricted_serializer.serialize(AllowedHost(child="hello"))
    restored = restricted_serializer.deserialize(payload)
    assert isinstance(restored, AllowedHost)
    assert restored.child == "hello"
    assert sideeffect_types.construction_log == ["AllowedHost.__setstate__"]


def test_factory_is_used_instead_of_importing(
    restricted_serializer: JSONSerializer,
) -> None:
    factory_calls: list[object] = []

    def factory(state: object) -> str:
        factory_calls.append(state)
        return "factory-result"

    serializer = JSONSerializer(
        restricted=True, factories={"some.external:Type": factory}
    )
    payload = make_marker("some.external:Type", {"x": 1}).encode("utf-8")
    assert serializer.deserialize(payload) == "factory-result"
    assert factory_calls == [{"x": 1}]
    assert sideeffect_types.construction_log == []


def test_registered_factory_blocks_default_construction(
    restricted_serializer: JSONSerializer,
) -> None:
    serializer = JSONSerializer(
        restricted=True,
        factories={EVIL_REF: lambda state: "rebuilt"},
    )
    payload = make_marker(EVIL_REF, {}).encode("utf-8")
    assert serializer.deserialize(payload) == "rebuilt"
    assert sideeffect_types.construction_log == []


def test_restricted_mode_does_not_fall_back_to_import() -> None:
    serializer = JSONSerializer(restricted=True)
    payload = make_marker(EVIL_REF, {}).encode("utf-8")
    with pytest.raises(DeserializationError):
        serializer.deserialize(payload)

    assert sideeffect_types.construction_log == []


def test_plain_dict_with_magic_key_conflict_is_rejected() -> None:
    serializer = JSONSerializer(restricted=True, allowed_types=(AllowedHost,))
    payloads = [
        # Marker plus an ordinary key: ambiguous, must not be treated as an object.
        json.dumps({MAGIC_KEY: [HOST_REF, {}], "other": 1}),
        # Magic key holding a plain value rather than a [ref, state] marker.
        json.dumps({MAGIC_KEY: "not a marker"}),
        # Malformed marker shape.
        json.dumps({MAGIC_KEY: [HOST_REF]}),
        json.dumps({MAGIC_KEY: [123, {}]}),
    ]
    for raw in payloads:
        with pytest.raises(DeserializationError, match="Malformed serialized object"):
            serializer.deserialize(raw.encode("utf-8"))

    assert sideeffect_types.construction_log == []


def test_conflict_is_rejected_even_in_trusted_mode() -> None:
    serializer = JSONSerializer()
    payload = json.dumps({MAGIC_KEY: [HOST_REF, {}], "other": 1}).encode("utf-8")
    with pytest.raises(DeserializationError, match="Malformed serialized object"):
        serializer.deserialize(payload)

    assert sideeffect_types.construction_log == []


def test_plain_dict_without_magic_key_round_trips(
    restricted_serializer: JSONSerializer,
) -> None:
    data = {"ordinary": "value", "nested": {"a": [1, 2]}}
    assert restricted_serializer.deserialize(
        restricted_serializer.serialize(data)
    ) == data


def test_duplicate_registration_is_rejected() -> None:
    with pytest.raises(ValueError, match="Duplicate registration"):
        JSONSerializer(
            restricted=True,
            allowed_types=(AllowedHost,),
            factories={HOST_REF: lambda state: None},
        )


def test_non_class_allowed_type_is_rejected() -> None:
    with pytest.raises(TypeError, match="must be classes"):
        JSONSerializer(
            restricted=True, allowed_types=(object(),)  # type: ignore[arg-type]
        )


def test_registration_without_restricted_flag_is_rejected() -> None:
    with pytest.raises(ValueError, match="only take effect when restricted=True"):
        JSONSerializer(allowed_types=(AllowedHost,))
    with pytest.raises(ValueError, match="only take effect when restricted=True"):
        JSONSerializer(factories={HOST_REF: lambda state: None})


def test_trusted_mode_preserves_legacy_behavior() -> None:
    serializer = JSONSerializer()
    assert serializer.restricted is False
    payload = serializer.serialize(EvilType())
    # Serialization does not construct anything; clear the __init__ entry produced by
    # creating the source object above so only deserialization effects remain.
    sideeffect_types.construction_log.clear()
    restored = serializer.deserialize(payload)
    assert isinstance(restored, EvilType)
    assert sideeffect_types.construction_log == ["EvilType.__setstate__"]


@pytest.mark.parametrize(
    "trigger",
    [
        pytest.param(
            DateTrigger(datetime(2020, 5, 14, tzinfo=ZoneInfo("UTC"))), id="date"
        ),
        pytest.param(
            IntervalTrigger(
                seconds=30, start_time=datetime(2020, 5, 14, tzinfo=ZoneInfo("UTC"))
            ),
            id="interval",
        ),
        pytest.param(
            AndTrigger(
                [
                    DateTrigger(datetime(2020, 5, 14, tzinfo=ZoneInfo("UTC"))),
                    IntervalTrigger(
                        hours=1,
                        start_time=datetime(2020, 5, 14, tzinfo=ZoneInfo("UTC")),
                    ),
                ]
            ),
            id="and",
        ),
        pytest.param(
            OrTrigger(
                [
                    DateTrigger(datetime(2020, 5, 14, tzinfo=ZoneInfo("UTC"))),
                    IntervalTrigger(
                        hours=1,
                        start_time=datetime(2020, 5, 14, tzinfo=ZoneInfo("UTC")),
                    ),
                ]
            ),
            id="or",
        ),
    ],
)
def test_standard_triggers_remain_recoverable(trigger) -> None:
    serializer = JSONSerializer(
        restricted=True,
        allowed_types=(
            DateTrigger,
            IntervalTrigger,
            AndTrigger,
            OrTrigger,
        ),
    )
    restored = serializer.deserialize(serializer.serialize(trigger))
    assert restored == trigger
    assert type(restored) is type(trigger)
