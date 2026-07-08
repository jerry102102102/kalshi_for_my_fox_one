from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

from kalshi_mlb_research.config import Settings, load_settings
from kalshi_mlb_research.time_utils import utc_now


@dataclass
class DuckDBStore:
    path: Path | None = None
    settings: Settings | None = None

    def __post_init__(self) -> None:
        self.settings = self.settings or load_settings()
        self.path = self.path or self.settings.duckdb_path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.path))
        self.init_schema()

    def close(self) -> None:
        self.conn.close()

    def init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kalshi_orderbook_snapshots (
              observed_at_utc TIMESTAMP,
              ticker VARCHAR,
              source VARCHAR,
              source_mode VARCHAR,
              raw_payload VARCHAR,
              normalized_book VARCHAR,
              yes_best_bid DOUBLE,
              yes_best_ask DOUBLE,
              yes_spread DOUBLE,
              yes_bid_depth INTEGER,
              yes_ask_depth INTEGER
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kalshi_ws_raw (
              observed_at_utc TIMESTAMP,
              ticker VARCHAR,
              channel VARCHAR,
              source VARCHAR,
              raw_payload VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mlb_game_states (
              observed_at_utc TIMESTAMP,
              game_pk VARCHAR,
              game_date DATE,
              home_team VARCHAR,
              away_team VARCHAR,
              status VARCHAR,
              inning INTEGER,
              half_inning VARCHAR,
              outs INTEGER,
              balls INTEGER,
              strikes INTEGER,
              runner_on_first BOOLEAN,
              runner_on_second BOOLEAN,
              runner_on_third BOOLEAN,
              home_score INTEGER,
              away_score INTEGER,
              batter_id VARCHAR,
              pitcher_id VARCHAR,
              last_play_type VARCHAR,
              last_play_description VARCHAR,
              play_index INTEGER,
              event_type VARCHAR,
              description VARCHAR,
              home_final_score INTEGER,
              away_final_score INTEGER,
              home_win_label INTEGER,
              raw_payload VARCHAR,
              state VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mlb_schedule (
              game_date DATE,
              game_pk VARCHAR,
              game_time_utc TIMESTAMP,
              home_team VARCHAR,
              away_team VARCHAR,
              status VARCHAR,
              raw_payload VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mlb_games (
              game_date DATE,
              game_pk VARCHAR,
              game_time_utc TIMESTAMP,
              home_team VARCHAR,
              away_team VARCHAR,
              status VARCHAR,
              home_final_score INTEGER,
              away_final_score INTEGER,
              home_win_label INTEGER,
              raw_payload VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mlb_plays (
              observed_at_utc TIMESTAMP,
              game_date DATE,
              game_pk VARCHAR,
              play_index INTEGER,
              inning INTEGER,
              half_inning VARCHAR,
              outs INTEGER,
              balls INTEGER,
              strikes INTEGER,
              runner_on_first BOOLEAN,
              runner_on_second BOOLEAN,
              runner_on_third BOOLEAN,
              home_score INTEGER,
              away_score INTEGER,
              batter_id VARCHAR,
              pitcher_id VARCHAR,
              event_type VARCHAR,
              description VARCHAR,
              home_final_score INTEGER,
              away_final_score INTEGER,
              home_win_label INTEGER,
              raw_payload VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS mlb_final_results (
              game_date DATE,
              game_pk VARCHAR,
              home_team VARCHAR,
              away_team VARCHAR,
              home_final_score INTEGER,
              away_final_score INTEGER,
              home_win_label INTEGER,
              raw_payload VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sports_events (
              observed_at_utc TIMESTAMP,
              game_pk VARCHAR,
              source_event_time_utc TIMESTAMP,
              event_type VARCHAR,
              before_state VARCHAR,
              after_state VARCHAR,
              raw_payload VARCHAR,
              event VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kalshi_markets (
              ticker VARCHAR,
              title VARCHAR,
              event_title VARCHAR,
              series_ticker VARCHAR,
              category VARCHAR,
              status VARCHAR,
              market_date DATE,
              open_time TIMESTAMP,
              close_time TIMESTAMP,
              source VARCHAR,
              raw_payload VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kalshi_market_game_candidates (
              ticker VARCHAR,
              title VARCHAR,
              event_title VARCHAR,
              series_ticker VARCHAR,
              market_date DATE,
              candidate_game_pk VARCHAR,
              home_team VARCHAR,
              away_team VARCHAR,
              match_score DOUBLE,
              match_reason VARCHAR,
              requires_manual_review BOOLEAN,
              raw_payload VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kalshi_market_candles (
              observed_at_utc TIMESTAMP,
              ticker VARCHAR,
              end_period_ts BIGINT,
              period_interval INTEGER,
              yes_bid_close DOUBLE,
              yes_ask_close DOUBLE,
              source VARCHAR,
              raw_payload VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kalshi_trades (
              observed_at_utc TIMESTAMP,
              trade_id VARCHAR,
              ticker VARCHAR,
              yes_price DOUBLE,
              count INTEGER,
              source VARCHAR,
              raw_payload VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS candle_trades (
              observed_at_utc TIMESTAMP,
              run_id VARCHAR,
              game_pk VARCHAR,
              ticker VARCHAR,
              side VARCHAR,
              price DOUBLE,
              model_prob DOUBLE,
              edge DOUBLE,
              fee DOUBLE,
              slippage DOUBLE,
              pnl DOUBLE,
              event_type VARCHAR,
              raw_payload VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS odds_snapshots (
              observed_at_utc TIMESTAMP,
              event_id VARCHAR,
              snapshot VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pregame_priors (
              observed_at_utc TIMESTAMP,
              event_id VARCHAR,
              game_pk VARCHAR,
              home_team VARCHAR,
              away_team VARCHAR,
              home_no_vig_prior DOUBLE,
              away_no_vig_prior DOUBLE,
              bookmaker_count INTEGER,
              home_moneyline_range VARCHAR,
              away_moneyline_range VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS model_predictions (
              observed_at_utc TIMESTAMP,
              game_pk VARCHAR,
              market_ticker VARCHAR,
              prediction VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_game_mappings (
              created_at_utc TIMESTAMP,
              game_pk VARCHAR,
              kalshi_ticker VARCHAR,
              home_team VARCHAR,
              away_team VARCHAR,
              market_title VARCHAR,
              market_type VARCHAR,
              settlement_notes VARCHAR,
              created_by VARCHAR,
              mapping_valid BOOLEAN,
              warning VARCHAR,
              mapping VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_orders (
              observed_at_utc TIMESTAMP,
              run_id VARCHAR,
              order_id VARCHAR,
              game_pk VARCHAR,
              ticker VARCHAR,
              side VARCHAR,
              size INTEGER,
              reason VARCHAR,
              payload VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_fills (
              observed_at_utc TIMESTAMP,
              run_id VARCHAR,
              fill_id VARCHAR,
              order_id VARCHAR,
              game_pk VARCHAR,
              ticker VARCHAR,
              side VARCHAR,
              size INTEGER,
              price DOUBLE,
              fee DOUBLE,
              payload VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_positions (
              observed_at_utc TIMESTAMP,
              run_id VARCHAR,
              ticker VARCHAR,
              yes_contracts INTEGER,
              cash DOUBLE,
              realized_fees DOUBLE,
              payload VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_equity (
              observed_at_utc TIMESTAMP,
              run_id VARCHAR,
              equity DOUBLE,
              payload VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_trades (
              observed_at_utc TIMESTAMP,
              run_id VARCHAR,
              ticker VARCHAR,
              payload VARCHAR
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS skip_log (
              observed_at_utc TIMESTAMP,
              run_id VARCHAR,
              game_pk VARCHAR,
              ticker VARCHAR,
              reason VARCHAR,
              payload VARCHAR
            );
            """
        )
        self._ensure_columns()

    def _ensure_columns(self) -> None:
        expected = {
            "kalshi_orderbook_snapshots": {
                "source": "VARCHAR",
                "source_mode": "VARCHAR",
                "yes_best_bid": "DOUBLE",
                "yes_best_ask": "DOUBLE",
                "yes_spread": "DOUBLE",
                "yes_bid_depth": "INTEGER",
                "yes_ask_depth": "INTEGER",
            },
            "kalshi_ws_raw": {"source": "VARCHAR"},
            "mlb_game_states": {
                "game_date": "DATE",
                "play_index": "INTEGER",
                "home_team": "VARCHAR",
                "away_team": "VARCHAR",
                "status": "VARCHAR",
                "inning": "INTEGER",
                "half_inning": "VARCHAR",
                "outs": "INTEGER",
                "balls": "INTEGER",
                "strikes": "INTEGER",
                "runner_on_first": "BOOLEAN",
                "runner_on_second": "BOOLEAN",
                "runner_on_third": "BOOLEAN",
                "home_score": "INTEGER",
                "away_score": "INTEGER",
                "batter_id": "VARCHAR",
                "pitcher_id": "VARCHAR",
                "last_play_type": "VARCHAR",
                "last_play_description": "VARCHAR",
                "event_type": "VARCHAR",
                "description": "VARCHAR",
                "home_final_score": "INTEGER",
                "away_final_score": "INTEGER",
                "home_win_label": "INTEGER",
                "raw_payload": "VARCHAR",
            },
            "sports_events": {
                "source_event_time_utc": "TIMESTAMP",
                "before_state": "VARCHAR",
                "after_state": "VARCHAR",
                "raw_payload": "VARCHAR",
            },
            "market_game_mappings": {
                "home_team": "VARCHAR",
                "away_team": "VARCHAR",
                "market_title": "VARCHAR",
                "market_type": "VARCHAR",
                "settlement_notes": "VARCHAR",
                "created_by": "VARCHAR",
                "mapping_valid": "BOOLEAN",
                "warning": "VARCHAR",
            },
            "skip_log": {"game_pk": "VARCHAR"},
        }
        for table, columns in expected.items():
            existing = {column[1] for column in self.conn.execute(f"PRAGMA table_info('{table}')").fetchall()}
            for name, kind in columns.items():
                if name not in existing:
                    self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {kind}")

    def append_json(self, table: str, row: dict[str, Any]) -> None:
        serializable = _to_jsonable(row)
        payload = json.dumps(serializable, default=str, sort_keys=True)
        observed = row.get("observed_at_utc") or row.get("created_at_utc") or utc_now()
        if isinstance(observed, str):
            observed_value = observed
        elif isinstance(observed, datetime):
            observed_value = observed.isoformat()
        else:
            observed_value = str(observed)
        self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table} (observed_at_utc TIMESTAMP, payload VARCHAR);"
        )
        table_info = self.conn.execute(f"PRAGMA table_info('{table}')").fetchall()
        columns = [column[1] for column in table_info]
        values: list[Any] = []
        for column in columns:
            if column == "observed_at_utc":
                values.append(observed_value)
            elif column == "created_at_utc":
                values.append(observed_value)
            elif column in row:
                value = row[column]
                if value is None or isinstance(value, (str, int, float, bool)):
                    values.append(value)
                elif isinstance(value, datetime):
                    values.append(value.isoformat())
                elif isinstance(value, date):
                    values.append(value.isoformat())
                elif isinstance(value, Decimal):
                    values.append(float(value))
                else:
                    values.append(json.dumps(_to_jsonable(value), default=str, sort_keys=True))
            elif column == "payload":
                values.append(payload)
            else:
                values.append(None)
        placeholders = ", ".join("?" for _ in columns)
        column_sql = ", ".join(columns)
        self.conn.execute(f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})", values)

    def fetch_all(self, query: str, params: list[Any] | None = None) -> list[dict]:
        result = self.conn.execute(query, params or []).fetchall()
        columns = [desc[0] for desc in self.conn.description]
        return [dict(zip(columns, row)) for row in result]


def _to_jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return {key: _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    return value
