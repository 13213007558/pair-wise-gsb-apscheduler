from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import tzinfo
from functools import partial
from inspect import isclass, ismethod, ismethoddescriptor
from typing import Any
from zoneinfo import ZoneInfo

from ._exceptions import DeserializationError, SerializationError

#: When this variable holds a registry, ``unmarshal_object`` operates in the
#: restricted mode and only restores references found in the registry, without
#: ever importing the module named in the reference.
_unmarshal_registry: ContextVar[Mapping[str, Callable[..., Any]] | None] = (
    ContextVar("unmarshal_registry", default=None)
)


def marshal_object(obj) -> tuple[str, Any]:
    return (
        f"{obj.__class__.__module__}:{obj.__class__.__qualname__}",
        obj.__getstate__(),
    )


def build_type_registry(
    allowed_types: Iterable[type] = (),
    factories: Mapping[str, Callable[..., Any]] | None = None,
) -> dict[str, Callable[..., Any]]:
    """
    Build a mapping of ``"module:qualname"`` references to their restore callables.

    Each class in ``allowed_types`` is registered under its own reference and restored
    using the default mechanism (``cls.__new__`` followed by ``__setstate__``).
    Each entry in ``factories`` maps a reference to a callable that receives the
    marshalled state and returns the restored object.

    :raises ValueError: if two entries resolve to the same reference, or a class in
        ``allowed_types`` lacks the ``__module__`` / ``__qualname__`` attributes

    """
    registry: dict[str, Callable[..., Any]] = {}
    for cls in allowed_types:
        if not isinstance(cls, type):
            raise TypeError(f"allowed_types entries must be classes, got {cls!r}")
        if not hasattr(cls, "__module__") or not hasattr(cls, "__qualname__"):
            raise ValueError(f"Class {cls!r} has no module or qualname")

        ref = f"{cls.__module__}:{cls.__qualname__}"
        if ref in registry:
            raise ValueError(f"Duplicate registration for reference {ref!r}")

        registry[ref] = cls

    for ref, factory in (factories or {}).items():
        if not isinstance(ref, str) or ":" not in ref:
            raise ValueError(f"Invalid reference used as factory key: {ref!r}")
        if not callable(factory):
            raise TypeError(f"Factory for {ref!r} is not callable")
        if ref in registry:
            raise ValueError(f"Duplicate registration for reference {ref!r}")

        registry[ref] = factory

    return registry


@contextmanager
def restricted_unmarshalling(
    registry: Mapping[str, Callable[..., Any]],
) -> Iterator[None]:
    """
    Run the enclosed code with ``unmarshal_object`` restricted to ``registry``.

    Any reference not present in the registry is rejected with
    :class:`~apscheduler.DeserializationError` before its module is imported or any
    object is constructed. The restriction also applies to objects restored
    transitively while another object's ``__setstate__`` runs, so nested objects cannot
    bypass it.

    """
    token = _unmarshal_registry.set(dict(registry))
    try:
        yield
    finally:
        _unmarshal_registry.reset(token)


def unmarshal_object(ref: str, state: Any) -> Any:
    registry = _unmarshal_registry.get()
    if registry is not None:
        restore = registry.get(ref)
        if restore is None:
            raise DeserializationError(
                f"Refusing to restore unregistered type reference {ref!r}: the "
                f"reference is not in the serializer's allowed types"
            )

        if isinstance(restore, type):
            instance = restore.__new__(restore)
            instance.__setstate__(state)
            return instance

        return restore(state)

    cls = callable_from_ref(ref)
    if not isinstance(cls, type):
        raise TypeError(f"{ref} is not a class")

    instance = cls.__new__(cls)
    instance.__setstate__(state)
    return instance


def marshal_timezone(value: tzinfo) -> str:
    if isinstance(value, ZoneInfo):
        return value.key
    elif hasattr(value, "zone"):  # pytz timezones
        return value.zone

    raise SerializationError(
        f"Unserializable time zone: {value!r}\n"
        f"Only time zones from the zoneinfo or pytz modules can be serialized."
    )


def unmarshal_timezone(value: str) -> ZoneInfo:
    return ZoneInfo(value)


def callable_to_ref(func: Callable) -> str:
    """
    Return a reference to the given callable.

    :raises SerializationError: if the given object is not callable, is a partial(),
        bound method, lambda or local function or does not have the ``__module__`` and
        ``__qualname__`` attributes

    """
    if isinstance(func, partial):
        raise SerializationError("Cannot create a reference to a partial()")

    if ismethod(func):
        if not isclass(func.__self__):
            raise SerializationError("Cannot create a reference to an instance method")

        return f"{func.__module__}:{func.__self__.__qualname__}.{func.__name__}"

    if ismethoddescriptor(func):
        return f"{func.__objclass__.__module__}:{func.__qualname__}"

    if not hasattr(func, "__module__"):
        raise SerializationError("Callable has no __module__ attribute")

    if not hasattr(func, "__qualname__"):
        raise SerializationError("Callable has no __qualname__ attribute")

    if "<lambda>" in func.__qualname__:
        raise SerializationError("Cannot create a reference to a lambda")

    if "<locals>" in func.__qualname__:
        raise SerializationError("Cannot create a reference to a nested function")

    return f"{func.__module__}:{func.__qualname__}"


def callable_from_ref(ref: str) -> Callable:
    """
    Return the callable pointed to by ``ref``.

    :raises DeserializationError: if the reference could not be resolved or the looked
        up object is not callable

    """
    if ":" not in ref:
        raise ValueError(f"Invalid reference: {ref}")

    modulename, rest = ref.split(":", 1)
    try:
        obj = __import__(modulename, fromlist=[rest])
    except ImportError as exc:
        raise LookupError(
            f"Error resolving reference {ref!r}: could not import module"
        ) from exc

    try:
        for name in rest.split("."):
            obj = getattr(obj, name)
    except Exception as exc:
        raise DeserializationError(
            f"Error resolving reference {ref!r}: error looking up object"
        ) from exc

    if not callable(obj):
        raise DeserializationError(
            f"{ref!r} points to an object of type "
            f"{obj.__class__.__qualname__} which is not callable"
        )

    return obj
