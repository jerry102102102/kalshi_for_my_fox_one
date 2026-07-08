from __future__ import annotations

from kalshi_mlb_research.models.calibration import brier_score
from kalshi_mlb_research.reports.common import markdown_table


def build_calibration_report(predictions: list[float], outcomes: list[int]) -> str:
    return "# Calibration Report\n\n" + markdown_table([{"metric": "brier_score", "value": brier_score(predictions, outcomes)}])

