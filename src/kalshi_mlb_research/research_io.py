from __future__ import annotations

import csv
import json
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from kalshi_mlb_research.kalshi.schemas import NormalizedOrderBook, PriceLevel
from kalshi_mlb_research.time_utils import parse_iso_datetime


def jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [jsonable(item) for item in value]
    return value


def dumps(value: Any) -> str:
    return json.dumps(jsonable(value), sort_keys=True, default=str)


def decimal_to_float(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def book_depth(levels: list[PriceLevel]) -> int:
    return sum(level.size for level in levels)


def report_dir(base_dir: Path, target_date: date) -> Path:
    path = base_dir / target_date.isoformat()
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_csv(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = sorted({key for row in rows for key in row.keys()})
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column) for column in columns})
    return path


def write_markdown_table(path: Path, title: str, rows: list[dict[str, Any]], note: str | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        columns = list(rows[0].keys())
        lines = [
            f"# {title}",
            "",
            "| " + " | ".join(columns) + " |",
            "| " + " | ".join("---" for _ in columns) + " |",
        ]
        for row in rows:
            lines.append("| " + " | ".join(str(row.get(column, "")) for column in columns) + " |")
    else:
        lines = [f"# {title}", "", note or "No rows."]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def book_from_normalized_json(payload: str | dict) -> NormalizedOrderBook:
    data = json.loads(payload) if isinstance(payload, str) else payload

    def levels(name: str) -> list[PriceLevel]:
        return [PriceLevel(Decimal(str(level["price"])), int(level["size"])) for level in data.get(name, [])]

    def dec(name: str) -> Decimal | None:
        value = data.get(name)
        return Decimal(str(value)) if value is not None else None

    observed = parse_iso_datetime(data.get("observed_at_utc")) or datetime.fromisoformat(data["observed_at_utc"])
    return NormalizedOrderBook(
        ticker=data["ticker"],
        observed_at_utc=observed,
        yes_best_bid=dec("yes_best_bid"),
        yes_best_ask=dec("yes_best_ask"),
        no_best_bid=dec("no_best_bid"),
        no_best_ask=dec("no_best_ask"),
        yes_spread=dec("yes_spread"),
        no_spread=dec("no_spread"),
        yes_bid_levels=levels("yes_bid_levels"),
        yes_ask_levels=levels("yes_ask_levels"),
        no_bid_levels=levels("no_bid_levels"),
        no_ask_levels=levels("no_ask_levels"),
    )


def yes_mid_from_row(row: dict[str, Any]) -> float | None:
    bid = row.get("yes_best_bid")
    ask = row.get("yes_best_ask")
    if bid is None or ask is None:
        return None
    return (float(bid) + float(ask)) / 2.0

