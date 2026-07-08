from __future__ import annotations

from kalshi_mlb_research.reports.common import markdown_table


def build_data_quality_report(rows: list[dict]) -> str:
    return "# Data Quality Report\n\n" + markdown_table(rows)

