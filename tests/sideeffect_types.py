from __future__ import annotations

from typing import Any

# Entries are appended when a type is actually constructed during deserialization.
# Restricted-deserialization tests assert that this list stays empty on rejection.
construction_log: list[str] = []


class EvilType:
    """An unregistered type whose construction would prove code execution."""

    def __init__(self) -> None:
        construction_log.append("EvilType.__init__")

    def __getstate__(self) -> dict[str, Any]:
        return {}

    def __setstate__(self, state: dict[str, Any]) -> None:
        construction_log.append("EvilType.__setstate__")


class AllowedHost:
    """A registered type that may legitimately carry nested serialized state."""

    def __init__(self, child: Any = None) -> None:
        self.child = child

    def __getstate__(self) -> dict[str, Any]:
        return {"child": self.child}

    def __setstate__(self, state: dict[str, Any]) -> None:
        construction_log.append("AllowedHost.__setstate__")
        self.child = state["child"]
