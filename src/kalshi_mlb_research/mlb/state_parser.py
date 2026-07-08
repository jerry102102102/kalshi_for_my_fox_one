from __future__ import annotations

from datetime import datetime
from typing import Any

from kalshi_mlb_research.mlb.schemas import MLBGameState
from kalshi_mlb_research.time_utils import parse_iso_datetime, utc_now


def _dig(data: dict, *keys: str, default: Any = None) -> Any:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def _person_id(value: dict | None) -> str | None:
    if not value:
        return None
    raw = value.get("id")
    return str(raw) if raw is not None else None


def _base_occupied(offense: dict, base_name: str) -> bool:
    runner = offense.get(base_name)
    return isinstance(runner, dict) and bool(runner.get("id") or runner.get("fullName"))


class MLBStateParser:
    def parse(self, payload: dict, observed_at_utc: datetime | None = None) -> MLBGameState:
        observed = observed_at_utc or utc_now()
        game_pk = str(_dig(payload, "gamePk", default="") or _dig(payload, "gameData", "game", "pk", default=""))
        game_data = payload.get("gameData", {})
        live_data = payload.get("liveData", {})
        linescore = live_data.get("linescore", {})
        current_play = _dig(live_data, "plays", "currentPlay", default={}) or {}
        result = current_play.get("result", {}) or {}
        about = current_play.get("about", {}) or {}
        count = current_play.get("count") or linescore.get("count") or {}
        matchup = current_play.get("matchup", {}) or {}
        offense = linescore.get("offense", {}) or {}
        teams = game_data.get("teams", {}) or {}

        inning_half = str(linescore.get("inningHalf") or about.get("halfInning") or "top").lower()
        half_inning = "bottom" if inning_half.startswith("bot") else "top"

        source_time = (
            parse_iso_datetime(about.get("endTime"))
            or parse_iso_datetime(about.get("startTime"))
            or parse_iso_datetime(_dig(game_data, "datetime", "officialDate", default=None))
        )

        return MLBGameState(
            game_pk=game_pk,
            observed_at_utc=observed,
            source_event_time_utc=source_time,
            status=str(_dig(game_data, "status", "detailedState", default="UNKNOWN")),
            home_team=str(_dig(teams, "home", "name", default="")),
            away_team=str(_dig(teams, "away", "name", default="")),
            inning=int(linescore.get("currentInning") or about.get("inning") or 1),
            half_inning=half_inning,
            home_score=int(_dig(linescore, "teams", "home", "runs", default=0) or 0),
            away_score=int(_dig(linescore, "teams", "away", "runs", default=0) or 0),
            outs=int(count.get("outs") or linescore.get("outs") or 0),
            balls=int(count.get("balls") or 0),
            strikes=int(count.get("strikes") or 0),
            runner_on_first=_base_occupied(offense, "first"),
            runner_on_second=_base_occupied(offense, "second"),
            runner_on_third=_base_occupied(offense, "third"),
            batter_id=_person_id(matchup.get("batter")),
            pitcher_id=_person_id(matchup.get("pitcher")),
            last_play_type=result.get("eventType") or result.get("event"),
            last_play_description=result.get("description"),
            raw_payload=payload,
        )

