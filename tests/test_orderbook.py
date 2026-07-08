from __future__ import annotations

from decimal import Decimal

from kalshi_mlb_research.kalshi.orderbook import OrderBookNormalizer, vwap_buy_yes, vwap_sell_yes


def test_yes_no_bid_complement_normalization() -> None:
    book = OrderBookNormalizer().from_payload(
        "TEST",
        {"orderbook_fp": {"yes_dollars": [["0.42", "10"]], "no_dollars": [["0.55", "10"]]}},
    )

    assert book.yes_best_bid == Decimal("0.4200")
    assert book.yes_best_ask == Decimal("0.4500")
    assert book.no_best_bid == Decimal("0.5500")
    assert book.no_best_ask == Decimal("0.5800")
    assert book.yes_spread == Decimal("0.0300")


def test_vwap_crosses_levels_and_depth_insufficient_returns_none() -> None:
    book = OrderBookNormalizer().from_payload(
        "TEST",
        {
            "orderbook_fp": {
                "yes_dollars": [["0.40", "2"], ["0.39", "10"]],
                "no_dollars": [["0.55", "2"], ["0.54", "3"]],
            }
        },
    )

    assert vwap_buy_yes(book, 5) == Decimal("0.4560")
    assert vwap_sell_yes(book, 3) == Decimal("0.3967")
    assert vwap_buy_yes(book, 6) is None

