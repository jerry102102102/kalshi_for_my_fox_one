from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import asdict
from datetime import date as Date
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Annotated, Any, Optional

import httpx
import typer

from kalshi_mlb_research.config import load_settings
from kalshi_mlb_research.exceptions import ExternalServiceError
from kalshi_mlb_research.execution.edge import evaluate_yes_edge
from kalshi_mlb_research.execution.paper_broker import PaperBroker
from kalshi_mlb_research.execution.risk_manager import RiskManager
from kalshi_mlb_research.kalshi.auth import KalshiAuthError
from kalshi_mlb_research.kalshi.market_discovery import discover_candidate_markets
from kalshi_mlb_research.kalshi.orderbook import OrderBookNormalizer
from kalshi_mlb_research.kalshi.rest_client import KalshiRestClient
from kalshi_mlb_research.kalshi.ws_client import KalshiWebSocketClient
from kalshi_mlb_research.mlb.client import MLBClient
from kalshi_mlb_research.mlb.event_normalizer import SportsEventNormalizer
from kalshi_mlb_research.mlb.schemas import MLBGameState
from kalshi_mlb_research.mlb.state_parser import MLBStateParser
from kalshi_mlb_research.mlb.team_mapping import team_similarity
from kalshi_mlb_research.models.baseline_mlb_wp import MLBWinProbabilityModel
from kalshi_mlb_research.research_io import (
    book_depth,
    book_from_normalized_json,
    decimal_to_float,
    dumps,
    report_dir,
    write_csv,
    write_markdown_table,
    yes_mid_from_row,
)
from kalshi_mlb_research.storage.duckdb_store import DuckDBStore
from kalshi_mlb_research.time_utils import ensure_utc, parse_date_arg, parse_iso_datetime, utc_now

app = typer.Typer(help="Kalshi MLB research CLI.")


def _echo_json(payload: object) -> None:
    typer.echo(json.dumps(payload, indent=2, default=str, sort_keys=True))


def _date_sql(column: str = "observed_at_utc") -> str:
    return f"CAST({column} AS DATE) = ?"


def _active_game(status: str | None) -> bool:
    normalized = (status or "").lower()
    if not normalized:
        return False
    inactive = ("scheduled", "pre-game", "warmup", "final", "game over", "postponed", "cancelled")
    return not any(term in normalized for term in inactive)


def _store_kalshi_snapshot(
    store: DuckDBStore,
    ticker: str,
    payload: dict,
    source: str,
    source_mode: str,
) -> None:
    book = OrderBookNormalizer().from_payload(ticker, payload)
    store.append_json(
        "kalshi_orderbook_snapshots",
        {
            "observed_at_utc": book.observed_at_utc,
            "ticker": ticker,
            "source": source,
            "source_mode": source_mode,
            "raw_payload": payload,
            "normalized_book": book.as_dict(),
            "yes_best_bid": decimal_to_float(book.yes_best_bid),
            "yes_best_ask": decimal_to_float(book.yes_best_ask),
            "yes_spread": decimal_to_float(book.yes_spread),
            "yes_bid_depth": book_depth(book.yes_bid_levels),
            "yes_ask_depth": book_depth(book.yes_ask_levels),
        },
    )


def _latest_book_row(store: DuckDBStore, ticker: str) -> dict[str, Any] | None:
    rows = store.fetch_all(
        """
        SELECT * FROM kalshi_orderbook_snapshots
        WHERE ticker = ?
        ORDER BY observed_at_utc DESC
        LIMIT 1
        """,
        [ticker],
    )
    return rows[0] if rows else None


def _book_at_or_after(store: DuckDBStore, ticker: str, observed_at: datetime) -> dict[str, Any] | None:
    rows = store.fetch_all(
        """
        SELECT * FROM kalshi_orderbook_snapshots
        WHERE ticker = ? AND observed_at_utc >= ?
        ORDER BY observed_at_utc ASC
        LIMIT 1
        """,
        [ticker, observed_at.isoformat()],
    )
    return rows[0] if rows else None


def _book_at_or_before(store: DuckDBStore, ticker: str, observed_at: datetime) -> dict[str, Any] | None:
    rows = store.fetch_all(
        """
        SELECT * FROM kalshi_orderbook_snapshots
        WHERE ticker = ? AND observed_at_utc <= ?
        ORDER BY observed_at_utc DESC
        LIMIT 1
        """,
        [ticker, observed_at.isoformat()],
    )
    return rows[0] if rows else None


def _nearest_book(store: DuckDBStore, ticker: str, observed_at: datetime, max_seconds: int = 45) -> dict[str, Any] | None:
    start = observed_at - timedelta(seconds=max_seconds)
    end = observed_at + timedelta(seconds=max_seconds)
    rows = store.fetch_all(
        """
        SELECT * FROM kalshi_orderbook_snapshots
        WHERE ticker = ? AND observed_at_utc BETWEEN ? AND ?
        """,
        [ticker, start.isoformat(), end.isoformat()],
    )
    if not rows:
        return None
    return min(rows, key=lambda row: abs((ensure_utc(row["observed_at_utc"]) - observed_at).total_seconds()))


def _store_mlb_schedule(store: DuckDBStore, target_date: Date, games: list[dict]) -> None:
    for game in games:
        raw = game.get("raw", {})
        store.append_json(
            "mlb_schedule",
            {
                "game_date": target_date.isoformat(),
                "game_pk": str(game.get("game_pk")),
                "game_time_utc": game.get("game_date"),
                "home_team": game.get("home_team"),
                "away_team": game.get("away_team"),
                "status": game.get("status"),
                "raw_payload": raw,
            },
        )


def _store_mlb_state(store: DuckDBStore, state: MLBGameState, target_date: Date) -> None:
    store.append_json(
        "mlb_game_states",
        {
            "observed_at_utc": state.observed_at_utc,
            "game_pk": state.game_pk,
            "game_date": target_date.isoformat(),
            "home_team": state.home_team,
            "away_team": state.away_team,
            "status": state.status,
            "inning": state.inning,
            "half_inning": state.half_inning,
            "outs": state.outs,
            "balls": state.balls,
            "strikes": state.strikes,
            "runner_on_first": state.runner_on_first,
            "runner_on_second": state.runner_on_second,
            "runner_on_third": state.runner_on_third,
            "home_score": state.home_score,
            "away_score": state.away_score,
            "batter_id": state.batter_id,
            "pitcher_id": state.pitcher_id,
            "last_play_type": state.last_play_type,
            "last_play_description": state.last_play_description,
            "raw_payload": state.raw_payload,
            "state": state,
        },
    )


def _store_sports_event(store: DuckDBStore, event: object) -> None:
    before = getattr(event, "before_state")
    after = getattr(event, "after_state")
    store.append_json(
        "sports_events",
        {
            "observed_at_utc": getattr(event, "observed_at_utc"),
            "game_pk": getattr(event, "game_pk"),
            "source_event_time_utc": getattr(event, "source_event_time_utc"),
            "event_type": getattr(event, "event_type"),
            "before_state": before,
            "after_state": after,
            "raw_payload": getattr(event, "raw_payload"),
            "event": event,
        },
    )


def _state_from_stored_json(payload: str) -> MLBGameState:
    data = json.loads(payload)
    return MLBGameState(
        game_pk=str(data["game_pk"]),
        observed_at_utc=parse_iso_datetime(data["observed_at_utc"]) or utc_now(),
        source_event_time_utc=parse_iso_datetime(data.get("source_event_time_utc")),
        status=data.get("status") or "",
        home_team=data.get("home_team") or "",
        away_team=data.get("away_team") or "",
        inning=int(data.get("inning") or 1),
        half_inning=data.get("half_inning") or "top",
        home_score=int(data.get("home_score") or 0),
        away_score=int(data.get("away_score") or 0),
        outs=int(data.get("outs") or 0),
        balls=int(data.get("balls") or 0),
        strikes=int(data.get("strikes") or 0),
        runner_on_first=bool(data.get("runner_on_first")),
        runner_on_second=bool(data.get("runner_on_second")),
        runner_on_third=bool(data.get("runner_on_third")),
        batter_id=data.get("batter_id"),
        pitcher_id=data.get("pitcher_id"),
        last_play_type=data.get("last_play_type"),
        last_play_description=data.get("last_play_description"),
        raw_payload=data.get("raw_payload") or {},
    )


def _fetch_state(game_pk: str) -> MLBGameState:
    client = MLBClient()
    try:
        return MLBStateParser().parse(client.live_game(game_pk))
    finally:
        client.close()


@app.command("check-kalshi-auth")
def check_kalshi_auth() -> None:
    client = KalshiRestClient()
    try:
        result = client.check_auth()
    except httpx.HTTPError as exc:
        result = {
            "ok": False,
            "environment": load_settings().kalshi_env,
            "account_endpoint_reachable": False,
            "market_data_endpoint_reachable": False,
            "reason": f"Network error: {exc}",
        }
    finally:
        client.close()
    if result["ok"]:
        typer.echo("Kalshi auth: OK")
    else:
        typer.echo("Kalshi auth: FAILED")
    typer.echo(f"environment: {result['environment']}")
    typer.echo(f"account endpoint reachable: {'yes' if result['account_endpoint_reachable'] else 'no'}")
    typer.echo(f"market data endpoint reachable: {'yes' if result['market_data_endpoint_reachable'] else 'no'}")
    if result.get("reason"):
        typer.echo(f"reason: {result['reason']}")


@app.command("discover-markets")
def discover_markets(query: str = "mlb", status: str = "open", limit: int = 100) -> None:
    client = KalshiRestClient()
    try:
        markets = discover_candidate_markets(client.list_markets(query=query, status=status, limit=limit), query=query)
    except Exception as exc:
        typer.echo(f"Kalshi discovery failed: {exc}", err=True)
        raise typer.Exit(1)
    finally:
        client.close()
    _echo_json(
        [
            {
                "ticker": market.get("ticker"),
                "title": market.get("title") or market.get("event_title"),
                "status": market.get("status"),
            }
            for market in markets
        ]
    )


async def _record_kalshi_websocket(
    tickers: list[str],
    duration: int,
    raw: bool,
    snapshot_interval: int,
    store_path: str,
) -> int:
    store = DuckDBStore(path=load_settings().duckdb_path if not store_path else load_settings().duckdb_path)
    rest = KalshiRestClient()
    ws = KalshiWebSocketClient()
    snapshot_count = 0
    last_snapshot = 0.0

    async def handler(message: dict) -> None:
        nonlocal snapshot_count, last_snapshot
        ticker = str(message.get("msg", {}).get("market_ticker") or message.get("market_ticker") or "")
        channel = str(message.get("type") or message.get("channel") or "")
        store.append_json(
            "kalshi_ws_raw",
            {
                "observed_at_utc": utc_now(),
                "ticker": ticker,
                "channel": channel,
                "source": "websocket",
                "raw_payload": message if raw else {"type": channel, "market_ticker": ticker},
            },
        )
        now = time.time()
        if now - last_snapshot >= snapshot_interval:
            for ticker_item in tickers:
                payload = rest.get_orderbook(ticker_item)
                _store_kalshi_snapshot(store, ticker_item, payload, "websocket_refresh", "websocket")
                snapshot_count += 1
            last_snapshot = now

    try:
        await ws.record_for(["orderbook_delta", "trade", "market_lifecycle_v2"], tickers, duration, handler)
    except asyncio.TimeoutError:
        pass
    finally:
        rest.close()
        store.close()
    return snapshot_count


@app.command("record-kalshi")
def record_kalshi(
    tickers: Annotated[str, typer.Option("--tickers")],
    duration: int = 300,
    raw: bool = True,
    book_snapshots_interval: int = typer.Option(30, "--book-snapshots-interval"),
) -> None:
    ticker_list = [ticker.strip() for ticker in tickers.split(",") if ticker.strip()]
    if not ticker_list:
        typer.echo("No tickers provided", err=True)
        raise typer.Exit(1)

    store = DuckDBStore()
    client = KalshiRestClient()
    snapshot_count = 0
    raw_message_count = 0
    source_mode = "polling"
    blockers: list[str] = []

    try:
        for ticker in ticker_list:
            try:
                market = client.get_market(ticker)
                if market.get("status") and str(market.get("status")).lower() not in {"open", "active", "initialized"}:
                    blockers.append(f"{ticker}: market status is {market.get('status')}")
                payload = client.get_orderbook(ticker)
                _store_kalshi_snapshot(store, ticker, payload, "rest_initial_snapshot", source_mode)
                snapshot_count += 1
            except Exception as exc:
                blockers.append(f"{ticker}: {exc}")
        if blockers:
            _echo_json({"recorded_snapshots": snapshot_count, "source_mode": source_mode, "blocking_reasons": blockers})
            raise typer.Exit(1)
    finally:
        client.close()
        store.close()

    settings = load_settings()
    can_try_ws = bool(settings.kalshi_api_key_id and settings.kalshi_private_key_path)
    if can_try_ws and duration > 0:
        try:
            source_mode = "websocket"
            snapshot_count += asyncio.run(
                _record_kalshi_websocket(ticker_list, duration, raw, max(1, book_snapshots_interval), "")
            )
        except Exception as exc:
            source_mode = "polling"
            blockers.append(f"websocket unavailable; fell back to REST polling: {exc}")
    elif duration > 0:
        blockers.append("websocket unavailable; missing Kalshi credentials, using REST polling")

    if source_mode == "polling" and duration > 0:
        store = DuckDBStore()
        client = KalshiRestClient()
        deadline = time.time() + duration
        try:
            while time.time() < deadline:
                for ticker in ticker_list:
                    payload = client.get_orderbook(ticker)
                    _store_kalshi_snapshot(store, ticker, payload, "rest_poll", "polling")
                    snapshot_count += 1
                time.sleep(max(1, book_snapshots_interval))
        except Exception as exc:
            blockers.append(f"polling stopped: {exc}")
        finally:
            client.close()
            store.close()

    _echo_json(
        {
            "recorded_snapshots": snapshot_count,
            "raw_message_count": raw_message_count,
            "tickers": ticker_list,
            "source_mode": source_mode,
            "blocking_reasons": blockers,
        }
    )


@app.command("inspect-book")
def inspect_book(ticker: Annotated[str, typer.Option("--ticker")]) -> None:
    store = DuckDBStore()
    try:
        latest = _latest_book_row(store, ticker)
        if not latest:
            _echo_json({"ticker": ticker, "blocking_reason": "No stored Kalshi snapshots for ticker"})
            raise typer.Exit(1)
        counts = store.fetch_all(
            """
            SELECT
              COUNT(*) AS snapshot_count,
              MAX(source_mode) AS source_mode,
              MAX(observed_at_utc) AS last_observed_at_utc
            FROM kalshi_orderbook_snapshots
            WHERE ticker = ?
            """,
            [ticker],
        )[0]
        raw_count = store.fetch_all("SELECT COUNT(*) AS raw_message_count FROM kalshi_ws_raw WHERE ticker = ?", [ticker])[0]
    finally:
        store.close()
    _echo_json(
        {
            "ticker": ticker,
            "last_observed_at_utc": counts["last_observed_at_utc"],
            "yes_best_bid": latest["yes_best_bid"],
            "yes_best_ask": latest["yes_best_ask"],
            "yes_spread": latest["yes_spread"],
            "yes_bid_depth": latest["yes_bid_depth"],
            "yes_ask_depth": latest["yes_ask_depth"],
            "snapshot_count": counts["snapshot_count"],
            "raw_message_count": raw_count["raw_message_count"],
            "source_mode": counts["source_mode"],
        }
    )


@app.command("record-mlb")
def record_mlb(
    date: str = "today",
    duration: int = 300,
    game_pk: Annotated[Optional[str], typer.Option("--game-pk")] = None,
    poll_interval: int = 15,
) -> None:
    target_date = parse_date_arg(date)
    store = DuckDBStore()
    client = MLBClient()
    parser = MLBStateParser()
    event_normalizer = SportsEventNormalizer()
    previous: dict[str, MLBGameState] = {}
    state_count = 0
    event_count = 0
    try:
        games = client.schedule(target_date)
        if game_pk:
            games = [game for game in games if str(game.get("game_pk")) == str(game_pk)]
        _store_mlb_schedule(store, target_date, games)
        live_games = [game for game in games if _active_game(str(game.get("status")))]
        if not live_games:
            next_game = min(
                (game for game in games if game.get("game_date")),
                key=lambda game: game.get("game_date"),
                default=None,
            )
            _echo_json(
                {
                    "date": target_date,
                    "schedule": [
                        {
                            "game_pk": game.get("game_pk"),
                            "home_team": game.get("home_team"),
                            "away_team": game.get("away_team"),
                            "status": game.get("status"),
                            "game_time_utc": game.get("game_date"),
                        }
                        for game in games
                    ],
                    "recorded_states": 0,
                    "recorded_events": 0,
                    "blocking_reason": "no live games currently",
                    "next_scheduled_game_time": next_game.get("game_date") if next_game else None,
                }
            )
            return

        deadline = time.time() + max(0, duration)
        while True:
            for game in live_games:
                payload = client.live_game(str(game["game_pk"]))
                state = parser.parse(payload)
                _store_mlb_state(store, state, target_date)
                event = event_normalizer.normalize(previous.get(state.game_pk), state)
                _store_sports_event(store, event)
                previous[state.game_pk] = state
                state_count += 1
                event_count += 1
            if duration <= 0 or time.time() >= deadline:
                break
            time.sleep(max(1, poll_interval))
    except Exception as exc:
        _echo_json({"date": target_date, "recorded_states": state_count, "blocking_reason": str(exc)})
        raise typer.Exit(1)
    finally:
        client.close()
        store.close()
    _echo_json({"date": target_date, "recorded_states": state_count, "recorded_events": event_count})


@app.command("inspect-mlb-games")
def inspect_mlb_games(date: str = "today") -> None:
    target_date = parse_date_arg(date)
    store = DuckDBStore()
    try:
        rows = store.fetch_all(
            """
            WITH state_counts AS (
              SELECT game_pk, COUNT(*) AS state_count, MAX(observed_at_utc) AS last_observed_at_utc,
                     MAX(inning) AS inning, MAX(home_score) AS home_score, MAX(away_score) AS away_score,
                     ANY_VALUE(status) AS status
              FROM mlb_game_states
              GROUP BY game_pk
            ),
            event_counts AS (
              SELECT game_pk, COUNT(*) AS event_count
              FROM sports_events
              GROUP BY game_pk
            )
            SELECT
              s.game_pk,
              s.home_team,
              s.away_team,
              COALESCE(sc.status, s.status) AS status,
              sc.inning,
              sc.home_score,
              sc.away_score,
              sc.last_observed_at_utc,
              COALESCE(sc.state_count, 0) AS state_count,
              COALESCE(ec.event_count, 0) AS event_count
            FROM mlb_schedule s
            LEFT JOIN state_counts sc ON s.game_pk = sc.game_pk
            LEFT JOIN event_counts ec ON s.game_pk = ec.game_pk
            WHERE s.game_date = ?
            ORDER BY s.game_pk
            """,
            [target_date.isoformat()],
        )
    finally:
        store.close()
    _echo_json(rows)


@app.command("inspect-mlb-game")
def inspect_mlb_game(game_pk: Annotated[str, typer.Option("--game-pk")]) -> None:
    state = _fetch_state(game_pk)
    _echo_json(asdict(state))


@app.command("map-market")
def map_market(
    game_pk: Annotated[str, typer.Option("--game-pk")],
    ticker: Annotated[str, typer.Option("--ticker")],
    market_type: str = typer.Option("GAME_WINNER", "--market-type"),
    manual: bool = typer.Option(False, "--manual"),
) -> None:
    if not manual:
        typer.echo("Only manual mapping is supported for reliable first-pass research. Pass --manual.", err=True)
        raise typer.Exit(1)
    state = _fetch_state(game_pk)
    client = KalshiRestClient()
    try:
        market = client.get_market(ticker)
    finally:
        client.close()
    market_title = str(market.get("title") or market.get("event_title") or "")
    confidence = max(team_similarity(state.home_team, market_title), team_similarity(state.away_team, market_title))
    warning = None
    mapping_valid = confidence > 0.25 or state.home_team.lower() in market_title.lower() or state.away_team.lower() in market_title.lower()
    if not mapping_valid:
        warning = "Ticker title does not appear to match either MLB team; manual mapping stored anyway."
    store = DuckDBStore()
    try:
        store.append_json(
            "market_game_mappings",
            {
                "created_at_utc": utc_now(),
                "game_pk": game_pk,
                "kalshi_ticker": ticker,
                "home_team": state.home_team,
                "away_team": state.away_team,
                "market_title": market_title,
                "market_type": market_type,
                "settlement_notes": market.get("rules_primary") or market.get("subtitle") or "",
                "created_by": "manual",
                "mapping_valid": mapping_valid,
                "warning": warning,
                "mapping": {
                    "game_pk": game_pk,
                    "ticker": ticker,
                    "market": market,
                    "state_observed_at_utc": state.observed_at_utc,
                },
            },
        )
    finally:
        store.close()
    _echo_json({"game_pk": game_pk, "ticker": ticker, "mapping_valid": mapping_valid, "warning": warning})


@app.command("inspect-mapping")
def inspect_mapping(date: str = "today") -> None:
    target_date = parse_date_arg(date)
    store = DuckDBStore()
    try:
        rows = store.fetch_all(
            """
            SELECT game_pk, home_team, away_team, kalshi_ticker AS ticker, market_title,
                   market_type, created_by, mapping_valid, warning
            FROM market_game_mappings
            WHERE CAST(created_at_utc AS DATE) = ?
            ORDER BY created_at_utc DESC
            """,
            [target_date.isoformat()],
        )
    finally:
        store.close()
    _echo_json(rows)


@app.command("predict-mlb")
def predict_mlb(
    game_pk: Annotated[str, typer.Option("--game-pk")],
    pregame_home_prior: Optional[float] = None,
) -> None:
    state = _fetch_state(game_pk)
    prediction = MLBWinProbabilityModel().predict(state, pregame_home_prior=pregame_home_prior)
    _echo_json(asdict(prediction))


@app.command("inspect-edge")
def inspect_edge(
    game_pk: Annotated[str, typer.Option("--game-pk")],
    ticker: Annotated[str, typer.Option("--ticker")],
    size: int = 5,
    pregame_home_prior: Optional[float] = None,
) -> None:
    state = _fetch_state(game_pk)
    store = DuckDBStore()
    try:
        latest = _latest_book_row(store, ticker)
    finally:
        store.close()
    if not latest:
        _echo_json({"blocking_reason": f"No stored Kalshi book snapshots for {ticker}. Run record-kalshi first."})
        raise typer.Exit(1)
    book = book_from_normalized_json(latest["normalized_book"])
    prediction = MLBWinProbabilityModel().predict(state, pregame_home_prior=pregame_home_prior, market_ticker=ticker)
    uncertainty = Decimal(str(prediction.home_win_p_high - prediction.home_win_p_low))
    _echo_json(evaluate_yes_edge(book, prediction.home_win_p_mid, size=size, model_uncertainty_width=uncertainty).as_dict())


def _report_paths(target_date: Date, stem: str) -> tuple[Any, Any]:
    directory = report_dir(load_settings().reports_dir, target_date)
    return directory / f"{stem}.md", directory / f"{stem}.csv"


@app.command("report-data-quality")
def report_data_quality(date: str = "today") -> None:
    target_date = parse_date_arg(date)
    store = DuckDBStore()
    try:
        summary = store.fetch_all(
            """
            SELECT
              (SELECT COUNT(DISTINCT ticker) FROM kalshi_orderbook_snapshots WHERE CAST(observed_at_utc AS DATE)=?) AS kalshi_ticker_count,
              (SELECT COUNT(*) FROM kalshi_orderbook_snapshots WHERE CAST(observed_at_utc AS DATE)=?) AS kalshi_snapshot_count,
              (SELECT COUNT(*) FROM kalshi_ws_raw WHERE CAST(observed_at_utc AS DATE)=?) AS kalshi_raw_websocket_message_count,
              (SELECT COUNT(DISTINCT game_pk) FROM mlb_schedule WHERE game_date=?) AS mlb_game_count,
              (SELECT COUNT(*) FROM mlb_game_states WHERE game_date=?) AS mlb_state_count,
              (SELECT COUNT(*) FROM sports_events WHERE CAST(observed_at_utc AS DATE)=?) AS mlb_event_count,
              (SELECT COUNT(*) FROM market_game_mappings WHERE CAST(created_at_utc AS DATE)=?) AS mapping_count,
              (SELECT COUNT(*) FROM kalshi_orderbook_snapshots WHERE observed_at_utc IS NULL)
              + (SELECT COUNT(*) FROM mlb_game_states WHERE observed_at_utc IS NULL)
              + (SELECT COUNT(*) FROM sports_events WHERE observed_at_utc IS NULL) AS missing_timestamp_count,
              (SELECT MIN(observed_at_utc) FROM kalshi_orderbook_snapshots WHERE CAST(observed_at_utc AS DATE)=?) AS first_kalshi_observed_at,
              (SELECT MAX(observed_at_utc) FROM kalshi_orderbook_snapshots WHERE CAST(observed_at_utc AS DATE)=?) AS last_kalshi_observed_at,
              (SELECT MIN(observed_at_utc) FROM mlb_game_states WHERE game_date=?) AS first_mlb_observed_at,
              (SELECT MAX(observed_at_utc) FROM mlb_game_states WHERE game_date=?) AS last_mlb_observed_at
            """,
            [target_date.isoformat()] * 11,
        )[0]
        gap_rows = store.fetch_all(
            """
            WITH gaps AS (
              SELECT observed_at_utc,
                     observed_at_utc - LAG(observed_at_utc) OVER (PARTITION BY ticker ORDER BY observed_at_utc) AS gap
              FROM kalshi_orderbook_snapshots
              WHERE CAST(observed_at_utc AS DATE)=?
            )
            SELECT
              COUNT(gap) AS gap_count,
              SUM(CASE WHEN EXTRACT(EPOCH FROM gap) * 1000 > ? THEN 1 ELSE 0 END) AS stale_gap_count
            FROM gaps
            """,
            [target_date.isoformat(), load_settings().max_data_staleness_ms],
        )[0]
    finally:
        store.close()
    gap_count = int(gap_rows.get("gap_count") or 0)
    stale_count = int(gap_rows.get("stale_gap_count") or 0)
    summary["stale_data_ratio"] = (stale_count / gap_count) if gap_count else 0.0
    summary["time_coverage"] = {
        "kalshi": [summary.pop("first_kalshi_observed_at"), summary.pop("last_kalshi_observed_at")],
        "mlb": [summary.pop("first_mlb_observed_at"), summary.pop("last_mlb_observed_at")],
    }
    rows = [{"metric": key, "value": value} for key, value in summary.items()]
    md_path, csv_path = _report_paths(target_date, "data_quality")
    write_markdown_table(md_path, "Data Quality Report", rows)
    write_csv(csv_path, rows)
    _echo_json({"markdown": str(md_path), "csv": str(csv_path), "rows": rows})


@app.command("report-latency")
def report_latency(date: str = "today") -> None:
    target_date = parse_date_arg(date)
    store = DuckDBStore()
    reasons: list[str] = []
    rows: list[dict[str, Any]] = []
    offsets = [-30, -10, -5, -1, 0, 1, 5, 10, 30]
    try:
        mappings = store.fetch_all("SELECT * FROM market_game_mappings")
        if not mappings:
            reasons.append("no mapped markets")
        for mapping in mappings:
            events = store.fetch_all(
                "SELECT * FROM sports_events WHERE game_pk=? AND CAST(observed_at_utc AS DATE)=? ORDER BY observed_at_utc",
                [mapping["game_pk"], target_date.isoformat()],
            )
            if not events:
                continue
            for event in events:
                event_time = ensure_utc(event["observed_at_utc"])
                points: dict[int, dict[str, Any] | None] = {}
                for offset in offsets:
                    points[offset] = _nearest_book(
                        store,
                        mapping["kalshi_ticker"],
                        event_time + timedelta(seconds=offset),
                        max_seconds=35,
                    )
                if not any(points.values()):
                    continue
                base_mid = yes_mid_from_row(points[0]) if points[0] else None
                row = {
                    "observed_at_utc": event_time.isoformat(),
                    "game_pk": mapping["game_pk"],
                    "ticker": mapping["kalshi_ticker"],
                    "event_type": event["event_type"],
                    "yes_mid": base_mid,
                    "yes_best_bid": points[0].get("yes_best_bid") if points[0] else None,
                    "yes_best_ask": points[0].get("yes_best_ask") if points[0] else None,
                    "spread": points[0].get("yes_spread") if points[0] else None,
                    "depth": (
                        (points[0].get("yes_bid_depth") or 0) + (points[0].get("yes_ask_depth") or 0)
                        if points[0]
                        else None
                    ),
                }
                for seconds in [1, 5, 10, 30]:
                    future_mid = yes_mid_from_row(points[seconds]) if points[seconds] else None
                    row[f"price_change_{seconds}s"] = (
                        future_mid - base_mid if future_mid is not None and base_mid is not None else None
                    )
                rows.append(row)
        if mappings and not rows:
            reasons.append("no MLB events" if not any(store.fetch_all("SELECT 1 FROM sports_events LIMIT 1")) else "no Kalshi book snapshots around event windows")
    finally:
        store.close()
    md_path, csv_path = _report_paths(target_date, "latency_report")
    if not rows:
        note = "Insufficient data for latency analysis\n\nreason:\n" + "\n".join(f"- {reason}" for reason in reasons)
        write_markdown_table(md_path, "Latency Report", [], note=note)
    else:
        write_markdown_table(md_path, "Latency Report", rows)
    write_csv(csv_path.with_name("latency_events.csv"), rows)
    _echo_json({"markdown": str(md_path), "csv": str(csv_path.with_name("latency_events.csv")), "rows": len(rows), "reasons": reasons})


def _edge_samples(target_date: Date, latency_ms: int = 0) -> list[dict[str, Any]]:
    store = DuckDBStore()
    samples: list[dict[str, Any]] = []
    try:
        mappings = store.fetch_all("SELECT * FROM market_game_mappings")
        for mapping in mappings:
            states = store.fetch_all(
                "SELECT * FROM mlb_game_states WHERE game_pk=? AND game_date=? ORDER BY observed_at_utc",
                [mapping["game_pk"], target_date.isoformat()],
            )
            for row in states:
                state = _state_from_stored_json(row["state"])
                book_row = _book_at_or_before(
                    store,
                    mapping["kalshi_ticker"],
                    ensure_utc(row["observed_at_utc"]) + timedelta(milliseconds=latency_ms),
                )
                if not book_row:
                    continue
                book = book_from_normalized_json(book_row["normalized_book"])
                prediction = MLBWinProbabilityModel().predict(state, market_ticker=mapping["kalshi_ticker"])
                uncertainty = Decimal(str(prediction.home_win_p_high - prediction.home_win_p_low))
                edge = evaluate_yes_edge(book, prediction.home_win_p_mid, size=1, model_uncertainty_width=uncertainty)
                samples.append(
                    {
                        "observed_at_utc": row["observed_at_utc"],
                        "game_pk": mapping["game_pk"],
                        "ticker": mapping["kalshi_ticker"],
                        "home_team": mapping["home_team"],
                        "away_team": mapping["away_team"],
                        "model_home_win_prob": prediction.home_win_p_mid,
                        "market_yes_best_bid": book.yes_best_bid,
                        "market_yes_best_ask": book.yes_best_ask,
                        "buy_yes_vwap_size_1": edge.buy_yes_vwap,
                        "sell_yes_vwap_size_1": edge.sell_yes_vwap,
                        "net_edge_buy_yes": edge.net_edge_buy_yes,
                        "net_edge_sell_yes": edge.net_edge_sell_yes,
                        "decision": edge.decision,
                        "skip_reason": edge.skip_reason,
                    }
                )
    finally:
        store.close()
    return samples


@app.command("report-edge")
def report_edge(date: str = "today") -> None:
    target_date = parse_date_arg(date)
    rows = _edge_samples(target_date)
    md_path, csv_path = _report_paths(target_date, "edge_report")
    write_markdown_table(md_path, "Edge Report", rows, note="No edge samples. Check mappings, MLB states, and Kalshi snapshots.")
    write_csv(csv_path.with_name("edge_samples.csv"), rows)
    _echo_json({"markdown": str(md_path), "csv": str(csv_path.with_name("edge_samples.csv")), "rows": len(rows)})


def _run_paper_replay(target_date: Date, latency_ms: int) -> dict[str, Any]:
    run_id = str(uuid.uuid4())
    broker = PaperBroker()
    risk = RiskManager()
    store = DuckDBStore()
    skip_count = 0
    gross_pnl = Decimal("0")
    estimated_fees = Decimal("0")
    try:
        samples = _edge_samples(target_date, latency_ms=latency_ms)
        for sample in samples:
            ticker = sample["ticker"]
            if sample["decision"] == "HOLD":
                skip_count += 1
                store.append_json(
                    "skip_log",
                    {
                        "observed_at_utc": sample["observed_at_utc"],
                        "run_id": run_id,
                        "game_pk": sample["game_pk"],
                        "ticker": ticker,
                        "reason": sample["skip_reason"] or "UNKNOWN",
                        "payload": sample,
                    },
                )
                continue
            decision = risk.check_order(ticker, 1, {key: value.yes_contracts for key, value in broker.positions.items()})
            if not decision.allowed:
                skip_count += 1
                continue
            book_row = _book_at_or_before(store, ticker, ensure_utc(sample["observed_at_utc"]))
            if not book_row:
                skip_count += 1
                continue
            order = broker.create_order(ticker, sample["decision"], decision.size, "REPLAY_EDGE")
            store.append_json(
                "paper_orders",
                {
                    "observed_at_utc": order.created_at_utc,
                    "run_id": run_id,
                    "order_id": order.order_id,
                    "game_pk": sample["game_pk"],
                    "ticker": ticker,
                    "side": order.side,
                    "size": order.size,
                    "reason": order.reason,
                    "payload": order,
                },
            )
            fill = broker.simulate_fill(order, book_from_normalized_json(book_row["normalized_book"]))
            if fill:
                estimated_fees += fill.fee
                store.append_json(
                    "paper_fills",
                    {
                        "observed_at_utc": fill.filled_at_utc,
                        "run_id": run_id,
                        "fill_id": fill.fill_id,
                        "order_id": fill.order_id,
                        "game_pk": sample["game_pk"],
                        "ticker": ticker,
                        "side": fill.side,
                        "size": fill.size,
                        "price": float(fill.price),
                        "fee": float(fill.fee),
                        "payload": fill,
                    },
                )
        marks = {}
        for ticker in broker.positions:
            latest = _latest_book_row(store, ticker)
            if latest and yes_mid_from_row(latest) is not None:
                marks[ticker] = Decimal(str(yes_mid_from_row(latest)))
        equity = broker.equity(marks)
        for position in broker.positions.values():
            gross_pnl += position.cash
            store.append_json(
                "paper_positions",
                {
                    "observed_at_utc": utc_now(),
                    "run_id": run_id,
                    "ticker": position.ticker,
                    "yes_contracts": position.yes_contracts,
                    "cash": float(position.cash),
                    "realized_fees": float(position.realized_fees),
                    "payload": position,
                },
            )
        store.append_json("paper_equity", {"observed_at_utc": utc_now(), "run_id": run_id, "equity": float(equity), "payload": {"marks": marks}})
    finally:
        store.close()
    return {
        "run_id": run_id,
        "latency_ms": latency_ms,
        "trade_count": len(broker.orders),
        "fill_count": len(broker.fills),
        "skip_count": skip_count,
        "gross_pnl": str(gross_pnl),
        "estimated_fees": str(estimated_fees),
        "net_pnl": str(broker.equity({})),
        "max_drawdown": "0",
    }


@app.command("paper-trade")
def paper_trade(date: str = "today", duration: int = 3600) -> None:
    result = _run_paper_replay(parse_date_arg(date), latency_ms=0)
    result["duration_seconds"] = duration
    _echo_json(result)


@app.command("replay")
def replay(date: Annotated[str, typer.Option("--date")], latency_ms: int = 1000) -> None:
    _echo_json(_run_paper_replay(parse_date_arg(date), latency_ms))


@app.command("report-pnl")
def report_pnl(run_id: Annotated[str, typer.Option("--run-id")]) -> None:
    store = DuckDBStore()
    try:
        fills = store.fetch_all("SELECT * FROM paper_fills WHERE run_id=?", [run_id])
        skips = store.fetch_all("SELECT reason, COUNT(*) AS count FROM skip_log WHERE run_id=? GROUP BY reason", [run_id])
        equity = store.fetch_all("SELECT * FROM paper_equity WHERE run_id=? ORDER BY observed_at_utc", [run_id])
    finally:
        store.close()
    rows = [
        {"metric": "fill_count", "value": len(fills)},
        {"metric": "skip_reason_distribution", "value": skips},
        {"metric": "last_equity", "value": equity[-1]["equity"] if equity else None},
    ]
    directory = report_dir(load_settings().reports_dir, utc_now().date())
    md_path = directory / f"pnl_{run_id}.md"
    csv_path = directory / f"pnl_{run_id}.csv"
    write_markdown_table(md_path, "Paper PnL Report", rows)
    write_csv(csv_path, rows)
    _echo_json({"markdown": str(md_path), "csv": str(csv_path), "rows": rows})


def main() -> None:
    app()


if __name__ == "__main__":
    main()
