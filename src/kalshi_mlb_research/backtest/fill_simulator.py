from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from kalshi_mlb_research.execution.fee_model import estimate_taker_fee
from kalshi_mlb_research.kalshi.orderbook import vwap_buy_yes, vwap_sell_yes
from kalshi_mlb_research.kalshi.schemas import NormalizedOrderBook


@dataclass(frozen=True)
class SimulatedFill:
    side: Literal["BUY_YES", "SELL_YES"]
    size_requested: int
    size_filled: int
    price: Decimal | None
    fee: Decimal
    reason: str | None = None


class FillSimulator:
    def taker_fill(
        self,
        book: NormalizedOrderBook,
        side: Literal["BUY_YES", "SELL_YES"],
        size: int,
    ) -> SimulatedFill:
        price = vwap_buy_yes(book, size) if side == "BUY_YES" else vwap_sell_yes(book, size)
        if price is None:
            return SimulatedFill(side, size, 0, None, Decimal("0"), "INSUFFICIENT_DEPTH")
        return SimulatedFill(side, size, size, price, estimate_taker_fee(price, size), None)

