from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from kalshi_mlb_research.config import Settings, load_settings


@dataclass(frozen=True)
class RiskDecision:
    allowed: bool
    size: int
    reason: str | None = None


@dataclass
class RiskState:
    positions: dict[str, int]
    realized_pnl: Decimal = Decimal("0")


class RiskManager:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or load_settings()

    def check_order(
        self,
        ticker: str,
        requested_size: int,
        positions: dict[str, int] | None = None,
        realized_pnl: Decimal = Decimal("0"),
    ) -> RiskDecision:
        positions = positions or {}
        if requested_size <= 0:
            return RiskDecision(False, 0, "INVALID_SIZE")
        if realized_pnl <= -self.settings.max_daily_loss_usd:
            return RiskDecision(False, 0, "RISK_LIMIT_REACHED")
        if len([value for value in positions.values() if value != 0]) >= self.settings.max_open_markets:
            if positions.get(ticker, 0) == 0:
                return RiskDecision(False, 0, "RISK_LIMIT_REACHED")
        current_abs = abs(positions.get(ticker, 0))
        capacity = max(0, self.settings.max_position_per_market - current_abs)
        size = min(requested_size, self.settings.max_contracts_per_trade, capacity)
        if size <= 0:
            return RiskDecision(False, 0, "POSITION_LIMIT_REACHED")
        return RiskDecision(True, size)

