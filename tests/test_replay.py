from __future__ import annotations

from datetime import datetime, timezone

from kalshi_mlb_research.backtest.latency_simulator import LatencySimulator
from kalshi_mlb_research.backtest.replay import ReplayEngine, ReplayItem


def test_replay_is_deterministic() -> None:
    items = [
        ReplayItem(datetime(2026, 7, 8, 12, 0, 1, tzinfo=timezone.utc), "b", {}),
        ReplayItem(datetime(2026, 7, 8, 12, 0, 0, tzinfo=timezone.utc), "a", {}),
    ]

    def handler(item: ReplayItem, state: dict) -> None:
        state.setdefault("seen", []).append(item.kind)

    first = ReplayEngine(items).run(handler)
    second = ReplayEngine(items).run(handler)

    assert first == second
    assert first["seen"] == ["a", "b"]


def test_latency_simulator_shifts_items() -> None:
    item = ReplayItem(datetime(2026, 7, 8, 12, 0, 0, tzinfo=timezone.utc), "a", {})
    shifted = LatencySimulator(1000).apply([item])[0]

    assert shifted.timestamp.second == 1

