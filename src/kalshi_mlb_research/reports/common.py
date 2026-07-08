from __future__ import annotations

from pathlib import Path


def markdown_table(rows: list[dict]) -> str:
    if not rows:
        return "_No rows._\n"
    columns = list(rows[0].keys())
    header = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join("---" for _ in columns) + " |"
    body = ["| " + " | ".join(str(row.get(column, "")) for column in columns) + " |" for row in rows]
    return "\n".join([header, divider, *body]) + "\n"


def write_report(path: Path, title: str, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"# {title}\n\n{markdown_table(rows)}", encoding="utf-8")
    return path

