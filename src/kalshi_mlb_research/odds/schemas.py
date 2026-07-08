from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class OddsSnapshot:
    event_id: str
    sport_key: str
    commence_time_utc: datetime | None
    home_team: str
    away_team: str
    bookmaker: str
    market: str
    outcome_name: str
    american_price: int
    point: float | None
    bookmaker_last_update_utc: datetime | None
    observed_at_utc: datetime
    raw_payload: dict = field(default_factory=dict)


@dataclass(frozen=True)
class PregamePrior:
    game_pk: str | None
    home_team: str
    away_team: str
    home_no_vig_prior: float
    away_no_vig_prior: float
    bookmaker_count: int
    home_moneyline_range: tuple[int, int] | None
    away_moneyline_range: tuple[int, int] | None
    observed_at_utc: datetime

