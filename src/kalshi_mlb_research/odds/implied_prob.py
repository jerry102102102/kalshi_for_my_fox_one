from __future__ import annotations


def american_to_implied_probability(odds: int) -> float:
    if odds == 0:
        raise ValueError("American odds cannot be 0")
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return (-odds) / ((-odds) + 100.0)


def no_vig_probabilities(home_american: int, away_american: int) -> tuple[float, float]:
    home = american_to_implied_probability(home_american)
    away = american_to_implied_probability(away_american)
    total = home + away
    if total <= 0:
        raise ValueError("implied probability total must be positive")
    return home / total, away / total

