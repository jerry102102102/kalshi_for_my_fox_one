from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from kalshi_mlb_research.kalshi.orderbook import available_depth, vwap_buy_yes, vwap_sell_yes
from kalshi_mlb_research.kalshi.schemas import NormalizedOrderBook


@dataclass(frozen=True)
class ExecutablePrice:
    side: str
    size: int
    vwap: Decimal | None
    depth_available: int
    best_price: Decimal | None


def executable_buy_yes(book: NormalizedOrderBook, size: int) -> ExecutablePrice:
    return ExecutablePrice(
        side="BUY_YES",
        size=size,
        vwap=vwap_buy_yes(book, size),
        depth_available=available_depth(book.yes_ask_levels),
        best_price=book.yes_best_ask,
    )


def executable_sell_yes(book: NormalizedOrderBook, size: int) -> ExecutablePrice:
    return ExecutablePrice(
        side="SELL_YES",
        size=size,
        vwap=vwap_sell_yes(book, size),
        depth_available=available_depth(book.yes_bid_levels),
        best_price=book.yes_best_bid,
    )

