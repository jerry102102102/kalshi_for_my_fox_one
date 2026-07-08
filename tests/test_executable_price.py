from __future__ import annotations

from decimal import Decimal

from kalshi_mlb_research.execution.executable_price import executable_buy_yes, executable_sell_yes
from kalshi_mlb_research.kalshi.orderbook import OrderBookNormalizer


def test_executable_price_uses_vwap_depth() -> None:
    book = OrderBookNormalizer().from_payload(
        "TEST",
        {
            "orderbook_fp": {
                "yes_dollars": [["0.51", "4"], ["0.50", "6"]],
                "no_dollars": [["0.47", "3"], ["0.46", "7"]],
            }
        },
    )

    buy = executable_buy_yes(book, 5)
    sell = executable_sell_yes(book, 5)

    assert buy.vwap == Decimal("0.5340")
    assert sell.vwap == Decimal("0.5080")
    assert buy.depth_available == 10
    assert sell.depth_available == 10
