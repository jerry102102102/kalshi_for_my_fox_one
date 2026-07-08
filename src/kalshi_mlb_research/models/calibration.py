from __future__ import annotations


def brier_score(predictions: list[float], outcomes: list[int]) -> float:
    if len(predictions) != len(outcomes):
        raise ValueError("predictions and outcomes must have same length")
    if not predictions:
        return 0.0
    return sum((p - o) ** 2 for p, o in zip(predictions, outcomes)) / len(predictions)

