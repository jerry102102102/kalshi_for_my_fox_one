from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

HalfInning = Literal["top", "bottom"]
SportsEventType = Literal[
    "GAME_START",
    "PITCH",
    "BALL",
    "STRIKE",
    "BALL_IN_PLAY",
    "OUT",
    "RUN_SCORED",
    "HOME_RUN",
    "WALK",
    "HIT",
    "ERROR",
    "PITCHING_CHANGE",
    "INNING_END",
    "GAME_END",
    "UNKNOWN",
]


@dataclass(frozen=True)
class MLBGameState:
    game_pk: str
    observed_at_utc: datetime
    source_event_time_utc: datetime | None
    status: str
    home_team: str
    away_team: str
    inning: int
    half_inning: HalfInning
    home_score: int
    away_score: int
    outs: int
    balls: int
    strikes: int
    runner_on_first: bool
    runner_on_second: bool
    runner_on_third: bool
    batter_id: str | None
    pitcher_id: str | None
    last_play_type: str | None
    last_play_description: str | None
    raw_payload: dict = field(default_factory=dict)

    @property
    def score_diff_home(self) -> int:
        return self.home_score - self.away_score

    @property
    def is_home_batting(self) -> bool:
        return self.half_inning == "bottom"

    @property
    def base_state(self) -> tuple[bool, bool, bool]:
        return (self.runner_on_first, self.runner_on_second, self.runner_on_third)


@dataclass(frozen=True)
class SportsEvent:
    event_id: str
    game_pk: str
    observed_at_utc: datetime
    source_event_time_utc: datetime | None
    event_type: SportsEventType
    before_state: MLBGameState | None
    after_state: MLBGameState
    raw_payload: dict = field(default_factory=dict)

