from __future__ import annotations

from decimal import Decimal

from kalshi_mlb_research.backtest.metrics import max_drawdown
from kalshi_mlb_research.reports.common import markdown_table


def build_pnl_report(trades: list[dict], equity_curve: list[Decimal] | None = None) -> str:
    equity_curve = equity_curve or []
    rows = [
        {"metric": "total paper trades", "value": len(trades)},
        {"metric": "max drawdown", "value": max_drawdown(equity_curve)},
    ]
    return "# Paper PnL Report\n\n" + markdown_table(rows)

