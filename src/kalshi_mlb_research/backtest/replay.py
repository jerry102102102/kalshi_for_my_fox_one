from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class ReplayItem:
    timestamp: datetime
    kind: str
    payload: dict[str, Any]


class ReplayEngine:
    def __init__(self, items: list[ReplayItem]) -> None:
        self.items = sorted(items, key=lambda item: (item.timestamp, item.kind))

    def run(self, handler: Callable[[ReplayItem, dict[str, Any]], None]) -> dict[str, Any]:
        state: dict[str, Any] = {"processed": 0, "by_kind": {}}
        for item in self.items:
            handler(item, state)
            state["processed"] += 1
            state["by_kind"][item.kind] = state["by_kind"].get(item.kind, 0) + 1
        return state

