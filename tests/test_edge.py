from __future__ import annotations

from decimal import Decimal

from kalshi_mlb_research.config import Settings
from kalshi_mlb_research.execution.edge import evaluate_yes_edge
from kalshi_mlb_research.kalshi.orderbook import OrderBookNormalizer


def test_spread_too_wide_skips_trade() -> None:
    settings = Settings(max_spread=Decimal("0.03"))
    book = OrderBookNormalizer().from_payload(
        "TEST",
        {"orderbook_fp": {"yes_dollars": [["0.40", "10"]], "no_dollars": [["0.50", "10"]]}},
    )

    result = evaluate_yes_edge(book, 0.70, 5, settings=settings)

    assert result.decision == "HOLD"
    assert result.skip_reason == "SPREAD_TOO_WIDE"


def test_positive_buy_edge_uses_executable_ask_fee_and_margin() -> None:
    settings = Settings(safety_margin=Decimal("0.01"), slippage_buffer=Decimal("0.00"), min_book_depth=1)
    book = OrderBookNormalizer().from_payload(
        "TEST",
        {"orderbook_fp": {"yes_dollars": [["0.50", "10"]], "no_dollars": [["0.55", "10"]]}},
    )

    result = evaluate_yes_edge(book, 0.60, 5, settings=settings)

    assert result.decision == "BUY_YES"
    assert result.buy_yes_vwap == Decimal("0.4500")
    assert result.net_edge_buy_yes is not None
    assert result.net_edge_buy_yes > 0


def test_model_uncertainty_too_high_skips() -> None:
    settings = Settings(max_model_uncertainty_width=Decimal("0.10"), min_book_depth=1)
    book = OrderBookNormalizer().from_payload(
        "TEST",
        {"orderbook_fp": {"yes_dollars": [["0.50", "10"]], "no_dollars": [["0.50", "10"]]}},
    )

    result = evaluate_yes_edge(book, 0.60, 5, model_uncertainty_width=Decimal("0.20"), settings=settings)

    assert result.decision == "HOLD"
    assert result.skip_reason == "MODEL_UNCERTAINTY_TOO_HIGH"

