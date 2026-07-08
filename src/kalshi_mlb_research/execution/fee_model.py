from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP


def estimate_taker_fee(price: Decimal, contracts: int) -> Decimal:
    if contracts <= 0:
        raise ValueError("contracts must be positive")
    # Placeholder until the exact market fee schedule is plugged in.
    per_contract = max(Decimal("0.01"), (price * (Decimal("1") - price) * Decimal("0.02")))
    return (per_contract * contracts).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def fee_per_contract(price: Decimal) -> Decimal:
    return (estimate_taker_fee(price, 1)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

