from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

from kalshi_mlb_research.backtest.replay import ReplayItem


class LatencySimulator:
    def __init__(self, latency_ms: int) -> None:
        if latency_ms < 0:
            raise ValueError("latency_ms must be non-negative")
        self.latency_ms = latency_ms

    def apply(self, items: list[ReplayItem]) -> list[ReplayItem]:
        delta = timedelta(milliseconds=self.latency_ms)
        return [replace(item, timestamp=item.timestamp + delta) for item in items]

