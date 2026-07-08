from __future__ import annotations

import hashlib

from kalshi_mlb_research.mlb.schemas import MLBGameState, SportsEvent, SportsEventType


def _event_id(state: MLBGameState, event_type: str) -> str:
    raw = "|".join(
        [
            state.game_pk,
            state.observed_at_utc.isoformat(),
            str(state.inning),
            state.half_inning,
            str(state.home_score),
            str(state.away_score),
            event_type,
            str(state.last_play_description),
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


class SportsEventNormalizer:
    def normalize(self, before: MLBGameState | None, after: MLBGameState) -> SportsEvent:
        event_type = self.infer_event_type(before, after)
        return SportsEvent(
            event_id=_event_id(after, event_type),
            game_pk=after.game_pk,
            observed_at_utc=after.observed_at_utc,
            source_event_time_utc=after.source_event_time_utc,
            event_type=event_type,
            before_state=before,
            after_state=after,
            raw_payload=after.raw_payload,
        )

    def infer_event_type(self, before: MLBGameState | None, after: MLBGameState) -> SportsEventType:
        if before is None:
            return "GAME_START"
        if "final" in after.status.lower() or "game over" in after.status.lower():
            return "GAME_END"
        if before.inning != after.inning or before.half_inning != after.half_inning:
            return "INNING_END"
        if after.home_score > before.home_score or after.away_score > before.away_score:
            if (after.last_play_type or "").lower().replace("_", " ") in {"home run", "homer"}:
                return "HOME_RUN"
            return "RUN_SCORED"

        play = (after.last_play_type or "").lower().replace("_", " ")
        description = (after.last_play_description or "").lower()
        if "pitching change" in play or "pitching change" in description:
            return "PITCHING_CHANGE"
        if "home run" in play or "homers" in description:
            return "HOME_RUN"
        if "walk" in play:
            return "WALK"
        if any(term in play for term in ("single", "double", "triple", "hit")):
            return "HIT"
        if "error" in play:
            return "ERROR"
        if after.outs > before.outs or "out" in play:
            return "OUT"
        if after.balls > before.balls:
            return "BALL"
        if after.strikes > before.strikes:
            return "STRIKE"
        if play:
            return "BALL_IN_PLAY"
        return "UNKNOWN"

