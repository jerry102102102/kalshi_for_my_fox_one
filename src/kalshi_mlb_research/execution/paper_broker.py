from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Literal

from kalshi_mlb_research.config import Settings, load_settings
from kalshi_mlb_research.exceptions import LiveTradingDisabledError
from kalshi_mlb_research.execution.fee_model import estimate_taker_fee
from kalshi_mlb_research.kalshi.orderbook import vwap_buy_yes, vwap_sell_yes
from kalshi_mlb_research.kalshi.schemas import NormalizedOrderBook
from kalshi_mlb_research.time_utils import utc_now

OrderSide = Literal["BUY_YES", "SELL_YES"]


@dataclass(frozen=True)
class PaperOrder:
    order_id: str
    ticker: str
    side: OrderSide
    size: int
    reason: str
    created_at_utc: datetime


@dataclass(frozen=True)
class PaperFill:
    fill_id: str
    order_id: str
    ticker: str
    side: OrderSide
    size: int
    price: Decimal
    fee: Decimal
    filled_at_utc: datetime


@dataclass
class PaperPosition:
    ticker: str
    yes_contracts: int = 0
    cash: Decimal = Decimal("0")
    realized_fees: Decimal = Decimal("0")

    def apply_fill(self, fill: PaperFill) -> None:
        notional = fill.price * fill.size
        if fill.side == "BUY_YES":
            self.yes_contracts += fill.size
            self.cash -= notional + fill.fee
        else:
            self.yes_contracts -= fill.size
            self.cash += notional - fill.fee
        self.realized_fees += fill.fee


@dataclass
class PaperBroker:
    settings: Settings | None = None
    positions: dict[str, PaperPosition] = field(default_factory=dict)
    orders: list[PaperOrder] = field(default_factory=list)
    fills: list[PaperFill] = field(default_factory=list)
    skip_log: list[dict] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.settings = self.settings or load_settings()

    def create_order(self, ticker: str, side: OrderSide, size: int, reason: str) -> PaperOrder:
        order = PaperOrder(
            order_id=str(uuid.uuid4()),
            ticker=ticker,
            side=side,
            size=size,
            reason=reason,
            created_at_utc=utc_now(),
        )
        self.orders.append(order)
        return order

    def simulate_fill(self, order: PaperOrder, book: NormalizedOrderBook) -> PaperFill | None:
        price = vwap_buy_yes(book, order.size) if order.side == "BUY_YES" else vwap_sell_yes(book, order.size)
        if price is None:
            self.log_skip(order.ticker, "INSUFFICIENT_DEPTH", {"order_id": order.order_id})
            return None
        fee = estimate_taker_fee(price, order.size)
        fill = PaperFill(
            fill_id=str(uuid.uuid4()),
            order_id=order.order_id,
            ticker=order.ticker,
            side=order.side,
            size=order.size,
            price=price,
            fee=fee,
            filled_at_utc=utc_now(),
        )
        self.fills.append(fill)
        position = self.positions.setdefault(order.ticker, PaperPosition(order.ticker))
        position.apply_fill(fill)
        return fill

    def log_skip(self, ticker: str, reason: str, context: dict | None = None) -> None:
        self.skip_log.append(
            {"ticker": ticker, "reason": reason, "context": context or {}, "observed_at_utc": utc_now().isoformat()}
        )

    def equity(self, marks: dict[str, Decimal] | None = None) -> Decimal:
        marks = marks or {}
        total = Decimal("0")
        for ticker, position in self.positions.items():
            total += position.cash + marks.get(ticker, Decimal("0")) * position.yes_contracts
        return total


class LiveTradingExecutor:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or load_settings()

    def place_live_order(self, *_args: object, **_kwargs: object) -> None:
        if not self.settings.enable_live_trading:
            raise LiveTradingDisabledError("Live trading is disabled. Set ENABLE_LIVE_TRADING=true explicitly.")
        raise NotImplementedError("Live order placement is intentionally not implemented in the research MVP")

