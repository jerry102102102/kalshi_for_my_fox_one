from __future__ import annotations

from decimal import Decimal

from kalshi_mlb_research.config import Settings
from kalshi_mlb_research.execution.risk_manager import RiskManager


def test_position_limit_reduces_or_blocks_size() -> None:
    manager = RiskManager(Settings(max_contracts_per_trade=5, max_position_per_market=7))

    decision = manager.check_order("TEST", 10, {"TEST": 5})

    assert decision.allowed is True
    assert decision.size == 2


def test_daily_loss_blocks_orders() -> None:
    manager = RiskManager(Settings(max_daily_loss_usd=Decimal("25")))

    decision = manager.check_order("TEST", 1, {}, realized_pnl=Decimal("-25"))

    assert decision.allowed is False
    assert decision.reason == "RISK_LIMIT_REACHED"

