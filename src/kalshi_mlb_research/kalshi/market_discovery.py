from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
from typing import Literal

from kalshi_mlb_research.mlb.team_mapping import normalize_team_name
from kalshi_mlb_research.time_utils import utc_now


@dataclass(frozen=True)
class MarketGameMapping:
    mapping_id: str
    game_pk: str
    kalshi_ticker: str
    home_team: str
    away_team: str
    market_type: Literal["GAME_WINNER", "SERIES_WINNER", "TEAM_TOTAL", "RUN_LINE", "OTHER"]
    settlement_notes: str
    confidence: float
    created_by: Literal["auto", "manual"]
    created_at_utc: datetime


def discover_candidate_markets(markets: list[dict], query: str = "mlb") -> list[dict]:
    query_lower = query.lower()
    sports_terms = ("mlb", "baseball", "world series", "dodgers", "yankees", "mets", "cubs")
    candidates = []
    for market in markets:
        haystack = " ".join(
            str(market.get(key, ""))
            for key in ("ticker", "title", "subtitle", "event_title", "category", "series_ticker")
        ).lower()
        if query_lower in haystack or any(term in haystack for term in sports_terms):
            candidates.append(market)
    return candidates


def _score_title_against_game(title: str, home_team: str, away_team: str) -> float:
    title_norm = normalize_team_name(title)
    home_norm = normalize_team_name(home_team)
    away_norm = normalize_team_name(away_team)
    home_score = SequenceMatcher(None, title_norm, home_norm).ratio()
    away_score = SequenceMatcher(None, title_norm, away_norm).ratio()
    token_bonus = 0.0
    if home_norm and home_norm in title_norm:
        token_bonus += 0.45
    if away_norm and away_norm in title_norm:
        token_bonus += 0.45
    return min(1.0, (home_score + away_score) / 2 + token_bonus)


def map_market_to_game(market: dict, game: dict, threshold: float = 0.90) -> MarketGameMapping | None:
    title = str(market.get("title") or market.get("event_title") or "")
    home = str(game.get("home_team") or game.get("home", ""))
    away = str(game.get("away_team") or game.get("away", ""))
    confidence = _score_title_against_game(title, home, away)
    if confidence < threshold:
        return None
    ticker = str(market.get("ticker"))
    game_pk = str(game.get("game_pk") or game.get("gamePk"))
    return MarketGameMapping(
        mapping_id=f"{game_pk}:{ticker}",
        game_pk=game_pk,
        kalshi_ticker=ticker,
        home_team=home,
        away_team=away,
        market_type="GAME_WINNER",
        settlement_notes=str(market.get("rules_primary") or market.get("subtitle") or ""),
        confidence=confidence,
        created_by="auto",
        created_at_utc=utc_now(),
    )

