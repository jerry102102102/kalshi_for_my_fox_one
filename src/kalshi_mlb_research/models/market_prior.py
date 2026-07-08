from __future__ import annotations


def market_price_to_prior(yes_mid: float | None, fallback: float = 0.5) -> float:
    if yes_mid is None:
        return fallback
    return min(0.99, max(0.01, float(yes_mid)))

