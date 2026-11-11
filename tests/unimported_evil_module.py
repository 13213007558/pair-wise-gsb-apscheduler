from __future__ import annotations

from typing import Any


class UnimportedEvil:
    """Lives in a module that restricted deserialization must never import."""

    def __getstate__(self) -> dict[str, Any]:
        return {}

    def __setstate__(self, state: dict[str, Any]) -> None:
        pass
