from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class PriceLevel:
    price: Decimal
    size: int

    def __post_init__(self) -> None:
        if self.price < 0 or self.price > 1:
            raise ValueError("price must be in [0, 1] dollars")
        if self.size < 0:
            raise ValueError("size must be non-negative")

    def as_dict(self) -> dict:
        return {"price": str(self.price), "size": self.size}


@dataclass(frozen=True)
class RawKalshiOrderBook:
    ticker: str
    observed_at_utc: datetime
    yes_bids: list[PriceLevel]
    no_bids: list[PriceLevel]
    raw_payload: dict = field(default_factory=dict)


@dataclass(frozen=True)
class NormalizedOrderBook:
    ticker: str
    observed_at_utc: datetime
    yes_best_bid: Decimal | None
    yes_best_ask: Decimal | None
    no_best_bid: Decimal | None
    no_best_ask: Decimal | None
    yes_spread: Decimal | None
    no_spread: Decimal | None
    yes_bid_levels: list[PriceLevel]
    yes_ask_levels: list[PriceLevel]
    no_bid_levels: list[PriceLevel]
    no_ask_levels: list[PriceLevel]

    @property
    def yes_mid(self) -> Decimal | None:
        if self.yes_best_bid is None or self.yes_best_ask is None:
            return None
        return (self.yes_best_bid + self.yes_best_ask) / Decimal("2")

    def as_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "observed_at_utc": self.observed_at_utc.isoformat(),
            "yes_best_bid": str(self.yes_best_bid) if self.yes_best_bid is not None else None,
            "yes_best_ask": str(self.yes_best_ask) if self.yes_best_ask is not None else None,
            "no_best_bid": str(self.no_best_bid) if self.no_best_bid is not None else None,
            "no_best_ask": str(self.no_best_ask) if self.no_best_ask is not None else None,
            "yes_spread": str(self.yes_spread) if self.yes_spread is not None else None,
            "no_spread": str(self.no_spread) if self.no_spread is not None else None,
            "yes_bid_levels": [level.as_dict() for level in self.yes_bid_levels],
            "yes_ask_levels": [level.as_dict() for level in self.yes_ask_levels],
            "no_bid_levels": [level.as_dict() for level in self.no_bid_levels],
            "no_ask_levels": [level.as_dict() for level in self.no_ask_levels],
        }

