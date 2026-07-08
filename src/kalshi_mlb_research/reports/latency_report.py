from __future__ import annotations

from kalshi_mlb_research.reports.common import markdown_table


def build_latency_report(rows: list[dict]) -> str:
    return "# Market Reaction / Latency Report\n\n" + markdown_table(rows)

