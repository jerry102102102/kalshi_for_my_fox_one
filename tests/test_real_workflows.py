from __future__ import annotations

import csv
import json
from datetime import timedelta

from typer.testing import CliRunner

from kalshi_mlb_research.cli import (
    _run_paper_replay,
    _store_kalshi_snapshot,
    _store_mlb_state,
    _store_sports_event,
    app,
)
from kalshi_mlb_research.config import Settings
from kalshi_mlb_research.kalshi.auth import KalshiAuth, KalshiAuthError
from kalshi_mlb_research.kalshi.orderbook import OrderBookNormalizer
from kalshi_mlb_research.mlb.event_normalizer import SportsEventNormalizer
from kalshi_mlb_research.mlb.state_parser import MLBStateParser
from kalshi_mlb_research.research_io import book_depth, decimal_to_float
from kalshi_mlb_research.sample_data import sample_mlb_live_payload, sample_orderbook_payload
from kalshi_mlb_research.storage.duckdb_store import DuckDBStore
from kalshi_mlb_research.time_utils import utc_now


def _configure_tmp(monkeypatch, tmp_path):
    db_path = tmp_path / "research.duckdb"
    reports_dir = tmp_path / "reports"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("REPORTS_DIR", str(reports_dir))
    return db_path, reports_dir


def _store_book_at(store: DuckDBStore, ticker: str, observed_at, payload: dict | None = None) -> None:
    payload = payload or sample_orderbook_payload()
    book = OrderBookNormalizer().from_payload(ticker, payload, observed_at_utc=observed_at)
    store.append_json(
        "kalshi_orderbook_snapshots",
        {
            "observed_at_utc": book.observed_at_utc,
            "ticker": ticker,
            "source": "test",
            "source_mode": "polling",
            "raw_payload": payload,
            "normalized_book": book.as_dict(),
            "yes_best_bid": decimal_to_float(book.yes_best_bid),
            "yes_best_ask": decimal_to_float(book.yes_best_ask),
            "yes_spread": decimal_to_float(book.yes_spread),
            "yes_bid_depth": book_depth(book.yes_bid_levels),
            "yes_ask_depth": book_depth(book.yes_ask_levels),
        },
    )


def _seed_mapping_state_and_book(monkeypatch, tmp_path):
    db_path, reports_dir = _configure_tmp(monkeypatch, tmp_path)
    store = DuckDBStore(path=db_path)
    target_date = utc_now().date()
    state = MLBStateParser().parse(sample_mlb_live_payload("123"))
    event = SportsEventNormalizer().normalize(None, state)
    _store_mlb_state(store, state, target_date)
    _store_sports_event(store, event)
    _store_book_at(store, "KXTEST", state.observed_at_utc - timedelta(seconds=1))
    store.append_json(
        "market_game_mappings",
        {
            "created_at_utc": utc_now(),
            "game_pk": "123",
            "kalshi_ticker": "KXTEST",
            "home_team": state.home_team,
            "away_team": state.away_team,
            "market_title": "New York Yankees vs Boston Red Sox",
            "market_type": "GAME_WINNER",
            "settlement_notes": "",
            "created_by": "manual",
            "mapping_valid": True,
            "warning": None,
            "mapping": {"game_pk": "123", "ticker": "KXTEST"},
        },
    )
    store.close()
    return target_date, reports_dir


def test_kalshi_auth_missing_credentials_error() -> None:
    auth = KalshiAuth(Settings(kalshi_api_key_id=None, kalshi_private_key_path=None))

    try:
        auth.require_credentials()
    except KalshiAuthError as exc:
        assert "Missing KALSHI_API_KEY_ID" in str(exc)
    else:
        raise AssertionError("missing credentials should fail clearly")


def test_manual_market_mapping_persistence(monkeypatch, tmp_path) -> None:
    _configure_tmp(monkeypatch, tmp_path)
    state = MLBStateParser().parse(sample_mlb_live_payload("123"))

    monkeypatch.setattr("kalshi_mlb_research.cli._fetch_state", lambda _game_pk: state)

    class FakeKalshiRestClient:
        def get_market(self, _ticker):
            return {"title": "New York Yankees vs Boston Red Sox", "rules_primary": "Most runs wins"}

        def close(self):
            pass

    monkeypatch.setattr("kalshi_mlb_research.cli.KalshiRestClient", FakeKalshiRestClient)
    result = CliRunner().invoke(app, ["map-market", "--game-pk", "123", "--ticker", "KXTEST", "--manual"])

    assert result.exit_code == 0, result.output
    store = DuckDBStore()
    rows = store.fetch_all("SELECT * FROM market_game_mappings WHERE game_pk='123'")
    store.close()
    assert rows[0]["kalshi_ticker"] == "KXTEST"
    assert rows[0]["created_by"] == "manual"


def test_report_data_quality_with_stored_rows(monkeypatch, tmp_path) -> None:
    db_path, reports_dir = _configure_tmp(monkeypatch, tmp_path)
    store = DuckDBStore(path=db_path)
    target_date = utc_now().date()
    _store_kalshi_snapshot(store, "KXTEST", sample_orderbook_payload(), "test", "polling")
    state = MLBStateParser().parse(sample_mlb_live_payload("123"))
    _store_mlb_state(store, state, target_date)
    store.close()

    result = CliRunner().invoke(app, ["report-data-quality", "--date", target_date.isoformat()])

    assert result.exit_code == 0, result.output
    csv_path = reports_dir / target_date.isoformat() / "data_quality.csv"
    assert csv_path.exists()
    rows = list(csv.DictReader(csv_path.open()))
    metrics = {row["metric"]: row["value"] for row in rows}
    assert metrics["kalshi_snapshot_count"] == "1"
    assert metrics["mlb_state_count"] == "1"


def test_latency_alignment_around_event_timestamp(monkeypatch, tmp_path) -> None:
    target_date, reports_dir = _seed_mapping_state_and_book(monkeypatch, tmp_path)
    store = DuckDBStore()
    state_row = store.fetch_all("SELECT observed_at_utc FROM mlb_game_states LIMIT 1")[0]
    _store_book_at(store, "KXTEST", state_row["observed_at_utc"] + timedelta(seconds=1))
    store.close()

    result = CliRunner().invoke(app, ["report-latency", "--date", target_date.isoformat()])

    assert result.exit_code == 0, result.output
    csv_path = reports_dir / target_date.isoformat() / "latency_events.csv"
    assert csv_path.exists()
    rows = list(csv.DictReader(csv_path.open()))
    assert rows
    assert "price_change_1s" in rows[0]


def test_edge_report_uses_vwap_not_mid_price(monkeypatch, tmp_path) -> None:
    target_date, reports_dir = _seed_mapping_state_and_book(monkeypatch, tmp_path)

    result = CliRunner().invoke(app, ["report-edge", "--date", target_date.isoformat()])

    assert result.exit_code == 0, result.output
    csv_path = reports_dir / target_date.isoformat() / "edge_samples.csv"
    rows = list(csv.DictReader(csv_path.open()))
    assert rows
    assert rows[0]["buy_yes_vwap_size_1"] == "0.4500"
    assert rows[0]["market_yes_best_bid"] == "0.4200"


def test_replay_deterministic_with_latency(monkeypatch, tmp_path) -> None:
    target_date, _reports_dir = _seed_mapping_state_and_book(monkeypatch, tmp_path)

    first = _run_paper_replay(target_date, latency_ms=1000)
    second = _run_paper_replay(target_date, latency_ms=1000)

    comparable = ["latency_ms", "trade_count", "fill_count", "skip_count", "gross_pnl", "estimated_fees"]
    assert {key: first[key] for key in comparable} == {key: second[key] for key in comparable}

