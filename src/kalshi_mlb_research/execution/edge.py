from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from kalshi_mlb_research.config import Settings, load_settings
from kalshi_mlb_research.execution.executable_price import executable_buy_yes, executable_sell_yes
from kalshi_mlb_research.execution.fee_model import fee_per_contract
from kalshi_mlb_research.kalshi.schemas import NormalizedOrderBook

Decision = Literal["BUY_YES", "SELL_YES", "HOLD"]


@dataclass(frozen=True)
class EdgeResult:
    decision: Decision
    skip_reason: str | None
    model_prob: Decimal
    buy_yes_vwap: Decimal | None
    sell_yes_vwap: Decimal | None
    yes_best_bid: Decimal | None
    yes_best_ask: Decimal | None
    spread: Decimal | None
    estimated_fee_per_contract: Decimal | None
    safety_margin: Decimal
    net_edge_buy_yes: Decimal | None
    net_edge_sell_yes: Decimal | None

    def as_dict(self) -> dict:
        return {
            "decision": self.decision,
            "skip_reason": self.skip_reason,
            "model_prob": str(self.model_prob),
            "buy_yes_vwap": str(self.buy_yes_vwap) if self.buy_yes_vwap is not None else None,
            "sell_yes_vwap": str(self.sell_yes_vwap) if self.sell_yes_vwap is not None else None,
            "yes_best_bid": str(self.yes_best_bid) if self.yes_best_bid is not None else None,
            "yes_best_ask": str(self.yes_best_ask) if self.yes_best_ask is not None else None,
            "spread": str(self.spread) if self.spread is not None else None,
            "estimated_fee_per_contract": (
                str(self.estimated_fee_per_contract) if self.estimated_fee_per_contract is not None else None
            ),
            "safety_margin": str(self.safety_margin),
            "net_edge_buy_yes": str(self.net_edge_buy_yes) if self.net_edge_buy_yes is not None else None,
            "net_edge_sell_yes": str(self.net_edge_sell_yes) if self.net_edge_sell_yes is not None else None,
        }


def evaluate_yes_edge(
    book: NormalizedOrderBook,
    model_prob: float | Decimal,
    size: int,
    model_uncertainty_width: float | Decimal = Decimal("0"),
    settings: Settings | None = None,
) -> EdgeResult:
    settings = settings or load_settings()
    model_p = Decimal(str(model_prob)).quantize(Decimal("0.0001"))
    uncertainty = Decimal(str(model_uncertainty_width))
    buy = executable_buy_yes(book, size)
    sell = executable_sell_yes(book, size)

    if book.yes_spread is None:
        return _hold("INSUFFICIENT_DEPTH", model_p, buy.vwap, sell.vwap, book, None, settings, None, None)
    if book.yes_spread > settings.max_spread:
        return _hold("SPREAD_TOO_WIDE", model_p, buy.vwap, sell.vwap, book, None, settings, None, None)
    if uncertainty > settings.max_model_uncertainty_width:
        return _hold("MODEL_UNCERTAINTY_TOO_HIGH", model_p, buy.vwap, sell.vwap, book, None, settings, None, None)
    if buy.vwap is None or sell.vwap is None:
        return _hold("INSUFFICIENT_DEPTH", model_p, buy.vwap, sell.vwap, book, None, settings, None, None)
    if buy.depth_available < settings.min_book_depth or sell.depth_available < settings.min_book_depth:
        return _hold("INSUFFICIENT_DEPTH", model_p, buy.vwap, sell.vwap, book, None, settings, None, None)

    buy_fee = fee_per_contract(buy.vwap)
    sell_fee = fee_per_contract(sell.vwap)
    buy_edge = model_p - buy.vwap - buy_fee - settings.slippage_buffer - settings.safety_margin
    sell_edge = sell.vwap - model_p - sell_fee - settings.slippage_buffer - settings.safety_margin
    if buy_edge > 0 and buy_edge >= sell_edge:
        decision: Decision = "BUY_YES"
        reason = None
    elif sell_edge > 0:
        decision = "SELL_YES"
        reason = None
    else:
        decision = "HOLD"
        reason = "EDGE_TOO_SMALL"

    return EdgeResult(
        decision=decision,
        skip_reason=reason,
        model_prob=model_p,
        buy_yes_vwap=buy.vwap,
        sell_yes_vwap=sell.vwap,
        yes_best_bid=book.yes_best_bid,
        yes_best_ask=book.yes_best_ask,
        spread=book.yes_spread,
        estimated_fee_per_contract=buy_fee if decision != "SELL_YES" else sell_fee,
        safety_margin=settings.safety_margin,
        net_edge_buy_yes=buy_edge,
        net_edge_sell_yes=sell_edge,
    )


def _hold(
    reason: str,
    model_p: Decimal,
    buy_vwap: Decimal | None,
    sell_vwap: Decimal | None,
    book: NormalizedOrderBook,
    fee: Decimal | None,
    settings: Settings,
    buy_edge: Decimal | None,
    sell_edge: Decimal | None,
) -> EdgeResult:
    return EdgeResult(
        decision="HOLD",
        skip_reason=reason,
        model_prob=model_p,
        buy_yes_vwap=buy_vwap,
        sell_yes_vwap=sell_vwap,
        yes_best_bid=book.yes_best_bid,
        yes_best_ask=book.yes_best_ask,
        spread=book.yes_spread,
        estimated_fee_per_contract=fee,
        safety_margin=settings.safety_margin,
        net_edge_buy_yes=buy_edge,
        net_edge_sell_yes=sell_edge,
    )

