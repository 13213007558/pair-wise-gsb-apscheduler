from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from datetime import date, timedelta, tzinfo
from enum import Enum
from json import dumps, loads
from typing import Any
from uuid import UUID

import attrs

from .. import DeserializationError, SerializationError
from .._marshalling import (
    build_type_registry,
    marshal_object,
    marshal_timezone,
    restricted_unmarshalling,
    unmarshal_object,
)
from ..abc import Serializer


@attrs.define(kw_only=True, eq=False)
class JSONSerializer(Serializer):
    """
    Serializes objects using JSON.

    Can serialize types not normally CBOR serializable, if they implement
    ``__getstate__()`` and ``__setstate__()``. These objects are serialized into dicts
    that contain the necessary information for deserialization in ``magic_key``.

    By default, the serializer restores any type reference found in the serialized
    data by importing the module it points to. This is only safe for data stores whose
    contents you fully trust. When serializing data that passes through another service
    and may have been tampered with, set ``restricted`` to ``True`` and register the
    types (or factory callables) that are allowed to be restored. In restricted mode a
    reference that was not registered is rejected with
    :class:`~apscheduler.DeserializationError` before any module import or object
    construction, including for objects nested inside another object's state. The
    serializer never falls back to unrestricted restoration in that case.

    :param magic_key: name of a specially handled dict key that indicates that a dict
        contains a serialized instance of an arbitrary type
    :param restricted: if ``True``, only restore types registered via
        ``allowed_types`` and ``factories`` instead of importing arbitrary references
    :param allowed_types: classes that may be restored; each is registered under its
        ``"module:qualname"`` reference and rebuilt with ``__new__`` / ``__setstate__``
    :param factories: mapping of ``"module:qualname"`` references to callables that
        rebuild the object from its marshalled state
    :param dump_options: keyword arguments passed to :func:`json.dumps`
    :param load_options: keyword arguments passed to :func:`json.loads`
    """

    magic_key: str = "_apscheduler_json"
    restricted: bool = False
    allowed_types: Iterable[type] = attrs.field(factory=tuple)
    factories: Mapping[str, Callable[..., Any]] = attrs.field(factory=dict)
    dump_options: dict[str, Any] = attrs.field(factory=dict)
    load_options: dict[str, Any] = attrs.field(factory=dict)
    _registry: dict[str, Callable[..., Any]] = attrs.field(
        init=False, factory=dict, repr=False
    )

    def __attrs_post_init__(self):
        if self.restricted:
            self._registry = build_type_registry(self.allowed_types, self.factories)
        elif self.allowed_types or self.factories:
            raise ValueError(
                "allowed_types and factories only take effect when restricted=True; "
                "set restricted=True to enable restricted deserialization"
            )

        self.dump_options["default"] = self._default_hook
        self.load_options["object_hook"] = self._object_hook

    def _default_hook(self, obj):
        if isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, timedelta):
            return obj.total_seconds()
        elif isinstance(obj, tzinfo):
            return marshal_timezone(obj)
        elif isinstance(obj, UUID):
            return str(obj)
        elif isinstance(obj, Enum):
            return obj.name
        elif hasattr(obj, "__getstate__"):
            cls_ref, state = marshal_object(obj)
            return {self.magic_key: [cls_ref, state]}

        raise TypeError(
            f"Object of type {obj.__class__.__name__!r} is not JSON serializable"
        )

    def _object_hook(self, obj_state: dict[str, Any]):
        if self.magic_key not in obj_state:
            return obj_state

        extra_keys = obj_state.keys() - {self.magic_key}
        marker = obj_state[self.magic_key]
        if extra_keys or not (
            isinstance(marker, list)
            and len(marker) == 2
            and isinstance(marker[0], str)
        ):
            # A plain dict that merely contains the magic key, or a malformed marker,
            # must never be interpreted as a serialized object.
            raise DeserializationError(
                f"Malformed serialized object marker under key {self.magic_key!r}"
            )

        ref, state = marker
        if self.restricted:
            with restricted_unmarshalling(self._registry):
                return unmarshal_object(ref, state)

        return unmarshal_object(ref, state)

    def serialize(self, obj: object) -> bytes:
        try:
            return dumps(obj, ensure_ascii=False, **self.dump_options).encode("utf-8")
        except Exception as exc:
            raise SerializationError from exc

    def deserialize(self, serialized: bytes):
        try:
            return loads(serialized, **self.load_options)
        except DeserializationError:
            raise
        except Exception as exc:
            raise DeserializationError(str(exc)) from exc
