from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from kalshi_mlb_research.config import Settings, load_settings


@dataclass
class ParquetWriter:
    settings: Settings | None = None

    def __post_init__(self) -> None:
        self.settings = self.settings or load_settings()
        self.settings.parquet_dir.mkdir(parents=True, exist_ok=True)

    def export_table(self, duckdb_path: Path, table: str, output_name: str | None = None) -> Path:
        output = self.settings.parquet_dir / (output_name or f"{table}.parquet")
        conn = duckdb.connect(str(duckdb_path), read_only=True)
        try:
            conn.execute(f"COPY {table} TO ? (FORMAT PARQUET)", [str(output)])
        finally:
            conn.close()
        return output

