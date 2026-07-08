from __future__ import annotations

from decimal import Decimal

from kalshi_mlb_research.config import Settings
from kalshi_mlb_research.exceptions import LiveTradingDisabledError
from kalshi_mlb_research.execution.paper_broker import LiveTradingExecutor, PaperBroker
from kalshi_mlb_research.kalshi.orderbook import OrderBookNormalizer


def test_paper_order_fill_updates_position_and_cash() -> None:
    book = OrderBookNormalizer().from_payload(
        "TEST",
        {"orderbook_fp": {"yes_dollars": [["0.45", "10"]], "no_dollars": [["0.55", "10"]]}},
    )
    broker = PaperBroker(settings=Settings())

    order = broker.create_order("TEST", "BUY_YES", 5, "unit test")
    fill = broker.simulate_fill(order, book)

    assert fill is not None
    assert fill.price == Decimal("0.4500")
    assert broker.positions["TEST"].yes_contracts == 5
    assert broker.positions["TEST"].cash < 0


def test_live_trading_guard_defaults_to_disabled() -> None:
    executor = LiveTradingExecutor(settings=Settings(enable_live_trading=False))

    try:
        executor.place_live_order()
    except LiveTradingDisabledError:
        pass
    else:
        raise AssertionError("live trading should be disabled by default")

