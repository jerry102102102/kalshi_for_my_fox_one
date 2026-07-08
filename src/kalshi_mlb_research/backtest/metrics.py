from __future__ import annotations

from decimal import Decimal


def max_drawdown(equity_curve: list[Decimal]) -> Decimal:
    if not equity_curve:
        return Decimal("0")
    peak = equity_curve[0]
    worst = Decimal("0")
    for value in equity_curve:
        peak = max(peak, value)
        worst = min(worst, value - peak)
    return abs(worst)


def fill_ratio(requested: int, filled: int) -> float:
    if requested <= 0:
        return 0.0
    return filled / requested

