from __future__ import annotations

from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable, Sequence

from kalshi_mlb_research.kalshi.schemas import NormalizedOrderBook, PriceLevel, RawKalshiOrderBook
from kalshi_mlb_research.time_utils import utc_now

ONE = Decimal("1")
CENT = Decimal("0.01")


def _to_decimal_price(value: object) -> Decimal:
    price = Decimal(str(value))
    if price > ONE:
        price = price / Decimal("100")
    return price.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def _to_size(value: object) -> int:
    return int(Decimal(str(value)).to_integral_value(rounding=ROUND_HALF_UP))


def _levels_from_any(raw_levels: object) -> list[PriceLevel]:
    if raw_levels is None:
        return []
    if isinstance(raw_levels, dict):
        iterator: Iterable[Sequence[object]] = raw_levels.items()
    else:
        iterator = raw_levels  # type: ignore[assignment]

    levels: list[PriceLevel] = []
    for raw in iterator:
        if isinstance(raw, dict):
            price = raw.get("price") or raw.get("price_dollars") or raw.get("price_cents")
            size = raw.get("size") or raw.get("quantity") or raw.get("count") or raw.get("contracts")
        else:
            price, size = raw[0], raw[1]
        level = PriceLevel(_to_decimal_price(price), _to_size(size))
        if level.size > 0:
            levels.append(level)
    return levels


def _extract_book(payload: dict) -> dict:
    if "orderbook_fp" in payload:
        return payload["orderbook_fp"]
    if "orderbook" in payload:
        return payload["orderbook"]
    return payload


def parse_raw_orderbook(
    ticker: str,
    payload: dict,
    observed_at_utc: datetime | None = None,
) -> RawKalshiOrderBook:
    book = _extract_book(payload)
    yes_raw = (
        book.get("yes_dollars")
        or book.get("yes")
        or book.get("yes_bids")
        or book.get("yes_cents")
        or []
    )
    no_raw = (
        book.get("no_dollars")
        or book.get("no")
        or book.get("no_bids")
        or book.get("no_cents")
        or []
    )
    return RawKalshiOrderBook(
        ticker=ticker,
        observed_at_utc=observed_at_utc or utc_now(),
        yes_bids=sorted(_levels_from_any(yes_raw), key=lambda level: level.price, reverse=True),
        no_bids=sorted(_levels_from_any(no_raw), key=lambda level: level.price, reverse=True),
        raw_payload=payload,
    )


def _ask_levels_from_opposite_bids(opposite_bids: list[PriceLevel]) -> list[PriceLevel]:
    asks = [PriceLevel((ONE - level.price).quantize(Decimal("0.0001")), level.size) for level in opposite_bids]
    return sorted(asks, key=lambda level: level.price)


def _spread(best_bid: Decimal | None, best_ask: Decimal | None) -> Decimal | None:
    if best_bid is None or best_ask is None:
        return None
    return (best_ask - best_bid).quantize(Decimal("0.0001"))


class OrderBookNormalizer:
    def normalize(self, raw: RawKalshiOrderBook) -> NormalizedOrderBook:
        yes_bids = sorted(raw.yes_bids, key=lambda level: level.price, reverse=True)
        no_bids = sorted(raw.no_bids, key=lambda level: level.price, reverse=True)
        yes_asks = _ask_levels_from_opposite_bids(no_bids)
        no_asks = _ask_levels_from_opposite_bids(yes_bids)

        yes_best_bid = yes_bids[0].price if yes_bids else None
        yes_best_ask = yes_asks[0].price if yes_asks else None
        no_best_bid = no_bids[0].price if no_bids else None
        no_best_ask = no_asks[0].price if no_asks else None

        return NormalizedOrderBook(
            ticker=raw.ticker,
            observed_at_utc=raw.observed_at_utc,
            yes_best_bid=yes_best_bid,
            yes_best_ask=yes_best_ask,
            no_best_bid=no_best_bid,
            no_best_ask=no_best_ask,
            yes_spread=_spread(yes_best_bid, yes_best_ask),
            no_spread=_spread(no_best_bid, no_best_ask),
            yes_bid_levels=yes_bids,
            yes_ask_levels=yes_asks,
            no_bid_levels=no_bids,
            no_ask_levels=no_asks,
        )

    def from_payload(
        self,
        ticker: str,
        payload: dict,
        observed_at_utc: datetime | None = None,
    ) -> NormalizedOrderBook:
        return self.normalize(parse_raw_orderbook(ticker, payload, observed_at_utc))


def available_depth(levels: list[PriceLevel], max_price: Decimal | None = None) -> int:
    total = 0
    for level in levels:
        if max_price is not None and level.price > max_price:
            break
        total += level.size
    return total


def vwap(levels: list[PriceLevel], size: int) -> Decimal | None:
    if size <= 0:
        raise ValueError("size must be positive")
    remaining = size
    total = Decimal("0")
    for level in levels:
        take = min(remaining, level.size)
        total += level.price * take
        remaining -= take
        if remaining == 0:
            return (total / Decimal(size)).quantize(Decimal("0.0001"))
    return None


def vwap_buy_yes(book: NormalizedOrderBook, size: int) -> Decimal | None:
    return vwap(book.yes_ask_levels, size)


def vwap_sell_yes(book: NormalizedOrderBook, size: int) -> Decimal | None:
    return vwap(book.yes_bid_levels, size)

